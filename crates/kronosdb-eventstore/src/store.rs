use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::broadcast;

use crate::api::EventStore;
use crate::append::{AppendCondition, AppendRequest, AppendResponse, AppliedLogId};
use crate::cache::IndexCache;
use crate::criteria::SourcingCondition;
use crate::error::Error;
use crate::event::{Position, SequencedEvent, StoredEvent, Tag};
use crate::stream::{CommitNotification, EventStream};

use crate::index::tag_index::TagIndex;
use crate::metrics::{StoreMetrics, Timer};
use crate::segment::reader::SegmentReader;
use crate::segment::segment_index::SegmentIndex;
use crate::segment::writer::SegmentWriter;
use crate::segment::{self, DEFAULT_SEGMENT_SIZE};

/// Default capacity for the commit notification channel.
const COMMIT_CHANNEL_CAPACITY: usize = 256;

/// Default number of segment indices to cache.
pub const DEFAULT_INDEX_CACHE_SIZE: usize = 50;

/// Default number of bloom filters to cache.
pub const DEFAULT_BLOOM_CACHE_SIZE: usize = 200;

/// Cached list of segment bases, avoiding readdir and stat syscalls on every query.
///
/// All segments before `sealed_count` are known to have companion `.idx`/`.bloom`
/// files. The last segment is the active (writable) segment.
#[derive(Clone)]
struct SegmentList {
    /// Segment base positions in ascending order.
    bases: Vec<u64>,
    /// Number of segments known to be sealed (have .idx/.bloom companions).
    /// Invariant: sealed_count < bases.len() (the active segment is never sealed).
    sealed_count: usize,
}

impl SegmentList {
    fn is_sealed(&self, index: usize) -> bool {
        index < self.sealed_count
    }
}

/// Default group commit interval (0 = disabled, sync per write).
const DEFAULT_GROUP_COMMIT_INTERVAL_MS: u64 = 0;

/// Group commit synchronization.
///
/// Writers write events (no fsync), mark pending, release the writer lock,
/// then wait for the sync thread to fsync and advance the epoch.
/// Multiple writers share one fsync — that's the throughput win.
struct SyncState {
    /// Highest completed group-commit wave.
    completed: StdMutex<u64>,
    synced: Condvar,
    /// Wave currently accepting writes. Read by writers UNDER the writer
    /// lock; advanced by the sync thread at its barrier (also under the
    /// writer lock), so a writer's wave is always the one whose fsync will
    /// cover its bytes.
    wave: AtomicU64,
    pending_writes: AtomicU64,
    enabled: bool,
    shutdown: AtomicBool,
    /// Latches when an fsync fails. A failed fsync means the dirty pages may
    /// already have been dropped by the kernel (fsyncgate semantics) — no
    /// retry can make those writes durable, so the engine is poisoned: every
    /// waiting writer gets an error instead of a durability ack, and all
    /// subsequent appends fail fast until the process restarts and recovers
    /// from what actually reached disk.
    failed: AtomicBool,
    failure_msg: StdMutex<Option<String>>,
}

impl SyncState {
    fn new(enabled: bool) -> Self {
        Self {
            completed: StdMutex::new(0),
            synced: Condvar::new(),
            wave: AtomicU64::new(1),
            pending_writes: AtomicU64::new(0),
            enabled,
            shutdown: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            failure_msg: StdMutex::new(None),
        }
    }

    /// Registers a write with the current wave. MUST be called while holding
    /// the writer lock — that's what orders it against the sync thread's
    /// barrier.
    fn mark_pending(&self) -> u64 {
        self.pending_writes.fetch_add(1, Ordering::Relaxed);
        self.wave.load(Ordering::Acquire)
    }

    /// Blocks until the given wave's fsync completed. Returns an error — the
    /// write is NOT durable — if the fsync failed.
    fn wait_for_sync(&self, target_wave: u64) -> Result<(), Error> {
        let mut completed = self.completed.lock().unwrap();
        while *completed < target_wave {
            if self.failed.load(Ordering::Acquire) {
                return Err(self.failure_error());
            }
            completed = self.synced.wait(completed).unwrap();
        }
        if self.failed.load(Ordering::Acquire) {
            return Err(self.failure_error());
        }
        Ok(())
    }

    /// Barrier: called by the sync thread UNDER the writer lock. Seals the
    /// current wave (every registered write finished before this point) and
    /// opens the next one for writers that arrive during the fsync.
    fn seal_wave(&self) -> u64 {
        self.pending_writes.store(0, Ordering::Relaxed);
        self.wave.fetch_add(1, Ordering::AcqRel)
    }

    /// Marks a sealed wave durable and wakes its waiters.
    fn complete_wave(&self, wave: u64) {
        let mut completed = self.completed.lock().unwrap();
        if *completed < wave {
            *completed = wave;
        }
        self.synced.notify_all();
    }

    /// Poisons the engine after an fsync failure and wakes every waiter so
    /// they observe the failure instead of blocking forever.
    fn fail_sync(&self, err: &Error) {
        *self.failure_msg.lock().unwrap() = Some(err.to_string());
        self.failed.store(true, Ordering::Release);
        // Grab the completed lock so the store/notify pair cannot interleave
        // between a waiter's predicate check and its wait().
        let _completed = self.completed.lock().unwrap();
        self.synced.notify_all();
    }

    fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    fn failure_error(&self) -> Error {
        let msg = self
            .failure_msg
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| "unknown fsync failure".to_string());
        Error::Io(io::Error::other(format!(
            "event store poisoned by fsync failure (writes are NOT durable): {msg}; restart required"
        )))
    }

    fn has_pending(&self) -> bool {
        self.pending_writes.load(Ordering::Relaxed) > 0
    }
}

/// Does an event (by name + tags) match a single DCB criterion? Mirrors the
/// index-side semantics: name must be in `names` (or `names` empty), and
/// EVERY criterion tag must be present on the event.
fn event_matches_criterion(
    criterion: &crate::criteria::Criterion,
    name: &str,
    tags: &[Tag],
) -> bool {
    if !criterion.names.is_empty() && !criterion.names.iter().any(|n| n == name) {
        return false;
    }
    criterion.tags.iter().all(|ct| tags.contains(ct))
}

fn spawn_sync_thread(
    sync_state: Arc<SyncState>,
    writer: Arc<parking_lot::Mutex<SegmentWriter>>,
    interval: Duration,
) {
    std::thread::Builder::new()
        .name("kronosdb-sync".into())
        .spawn(move || {
            loop {
                std::thread::sleep(interval);
                // Read the flag BEFORE the final sync pass: writers that
                // marked pending before shutdown still get their fsync (and
                // their wakeup) instead of hanging on a dead thread.
                let shutting_down = sync_state.shutdown.load(Ordering::Relaxed);
                if sync_state.has_pending() {
                    // Barrier: take the writer lock only long enough to seal
                    // the wave and clone the active file handle, then fsync
                    // OUTSIDE the lock. Holding the lock across the fsync
                    // would serialize every writer behind it — a single
                    // producer (e.g. the raft state-machine worker) would
                    // land exactly one write per fsync window.
                    let sealed = {
                        let w = writer.lock();
                        let wave = sync_state.seal_wave();
                        w.active_file_handle().map(|file| (wave, file))
                    };
                    let result = sealed.and_then(|(wave, file)| {
                        crate::segment::writer::sync_file(&file)?;
                        Ok(wave)
                    });
                    match result {
                        Ok(wave) => sync_state.complete_wave(wave),
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                "group-commit fsync FAILED — poisoning event store; \
                                 pending writes are not durable and new appends will be rejected"
                            );
                            sync_state.fail_sync(&e);
                            // No retry: a post-failure fsync "success" would
                            // not cover the dropped pages. The thread exits;
                            // the poisoned flag gates all further appends.
                            return;
                        }
                    }
                }
                if shutting_down {
                    return;
                }
            }
        })
        .expect("spawn sync thread");
}

/// Configuration options for an event store engine.
#[derive(Debug, Clone)]
pub struct StoreOptions {
    pub max_segment_size: u64,
    pub index_cache_size: usize,
    pub bloom_cache_size: usize,
    /// Group commit interval in milliseconds. 0 = disabled (sync per write).
    pub group_commit_interval_ms: u64,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            max_segment_size: DEFAULT_SEGMENT_SIZE,
            index_cache_size: DEFAULT_INDEX_CACHE_SIZE,
            bloom_cache_size: DEFAULT_BLOOM_CACHE_SIZE,
            group_commit_interval_ms: DEFAULT_GROUP_COMMIT_INTERVAL_MS,
        }
    }
}

impl StoreOptions {
    /// Auto-scales cache sizes based on sealed segment count and a memory budget.
    ///
    /// This is NOT the default — use it explicitly when you know your workload.
    /// The default StoreOptions uses conservative fixed sizes that work for most cases.
    ///
    /// Priority order (highest ROI first):
    /// 1. Bloom filters — always cache all (tiny: ~1KB each)
    /// 2. Index + offset tables — gets the bulk of the budget (~2-8MB each)
    /// 3. Snapshot cache — small fixed allocation (snapshots are a niche optimization)
    ///
    /// `num_contexts` is the number of contexts sharing this machine's memory.
    /// Each context gets an equal share of the total budget.
    pub fn auto_scaled(sealed_segment_count: usize, num_contexts: usize) -> Self {
        let total_memory = detect_available_memory();
        // Use 50% of available memory for KronosDB caching, split across contexts.
        let per_context_budget = total_memory / 2 / num_contexts.max(1);

        Self::auto_scaled_with_budget(sealed_segment_count, per_context_budget)
    }

    /// Auto-scales cache sizes to fit within a specific per-context byte budget.
    pub fn auto_scaled_with_budget(sealed_segment_count: usize, budget_bytes: usize) -> Self {
        let segments = sealed_segment_count.max(1);

        // Bloom filters: ~1KB each. Always cache all. Cost is negligible.
        let bloom_cache_size = segments;
        let bloom_cost = segments * 1_024;

        // Index + offset tables get everything remaining. This is what makes
        // every source query fast. Estimate ~4MB per segment index.
        let remaining = budget_bytes.saturating_sub(bloom_cost);
        let estimated_index_bytes = 4 * 1024 * 1024;
        let max_index_entries = remaining / estimated_index_bytes;
        let index_cache_size = segments
            .min(max_index_entries)
            .max(DEFAULT_INDEX_CACHE_SIZE);

        Self {
            max_segment_size: DEFAULT_SEGMENT_SIZE,
            index_cache_size,
            bloom_cache_size,
            group_commit_interval_ms: DEFAULT_GROUP_COMMIT_INTERVAL_MS,
        }
    }
}

/// Detects total physical memory on the system.
/// Returns a conservative default (4GB) if detection fails.
fn detect_available_memory() -> usize {
    try_detect_memory().unwrap_or(4 * 1024 * 1024 * 1024) // 4GB fallback
}

#[cfg(target_os = "macos")]
fn try_detect_memory() -> Option<usize> {
    use std::mem;
    let mut size: u64 = 0;
    let mut len = mem::size_of::<u64>();
    let mib = [libc::CTL_HW, libc::HW_MEMSIZE];
    let ret = unsafe {
        libc::sysctl(
            mib.as_ptr() as *mut _,
            2,
            &mut size as *mut u64 as *mut _,
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if ret == 0 { Some(size as usize) } else { None }
}

#[cfg(target_os = "linux")]
fn try_detect_memory() -> Option<usize> {
    // In containers (Kubernetes, Docker), the host's total RAM is irrelevant —
    // the cgroup memory limit is what we'll actually get before OOM-kill.
    // Check cgroup first, fall back to system RAM for bare-metal.
    if let Some(cgroup_limit) = try_detect_cgroup_memory() {
        return Some(cgroup_limit);
    }

    let content = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in content.lines() {
        if line.starts_with("MemTotal:") {
            let kb: usize = line.split_whitespace().nth(1)?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// Detects the cgroup memory limit (container environments).
/// Returns None if not in a cgroup or if the limit is "max" (unlimited).
#[cfg(target_os = "linux")]
fn try_detect_cgroup_memory() -> Option<usize> {
    // cgroup v2: single file, plain number or "max".
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let trimmed = content.trim();
        if trimmed != "max" {
            if let Ok(bytes) = trimmed.parse::<usize>() {
                return Some(bytes);
            }
        }
        return None; // "max" = no limit, fall through to system RAM.
    }

    // cgroup v1: different path, large sentinel value means unlimited.
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
        if let Ok(bytes) = content.trim().parse::<usize>() {
            // cgroup v1 uses a very large number (close to usize::MAX) for "no limit".
            if bytes < 1 << 62 {
                return Some(bytes);
            }
        }
    }

    None
}

#[cfg(not(any(target_os = "macos", target_os = "linux")))]
fn try_detect_memory() -> Option<usize> {
    None // Use fallback.
}

/// The main event store. Combines the segment writer, tag index, and
/// concurrency control into a single API.
///
/// All operations take `&self`. The writer is behind a Mutex for interior
/// mutability, allowing reads to proceed concurrently with each other
/// (they don't need the writer lock). Only appends lock the writer.
///
/// Tags are stored on events in segments (source of truth).
/// The tag index is a derived in-memory structure rebuilt from segments on recovery.
/// Tag mutations (retagging, redaction) are handled by the transformation system,
/// not by direct mutation on the store.
pub struct EventStoreEngine {
    /// Directory where segment files are stored.
    dir: PathBuf,

    /// The segment writer. Behind Arc<Mutex> — shared with the group commit sync thread.
    writer: Arc<parking_lot::Mutex<SegmentWriter>>,

    /// TagIndex is internally sharded (DashMap over tag keys + a brief
    /// Mutex on all_positions). No outer lock — concurrent writers with
    /// disjoint tag keys don't contend.
    tag_index: Arc<TagIndex>,

    /// Group commit synchronization.
    sync_state: Arc<SyncState>,

    /// The head position — next-exclusive: the position the next event will
    /// be written at, equivalently the count of events committed.
    head_position: Arc<AtomicU64>,

    /// Broadcast channel for notifying stream subscribers of new commits.
    commit_tx: broadcast::Sender<CommitNotification>,

    /// LRU cache for sealed segment indices, bloom filters, and mmap handles.
    cache: Arc<IndexCache>,

    /// Cached segment list — avoids readdir + stat syscalls on every query.
    /// Updated on rotation within the append path (under writer lock).
    segments: RwLock<SegmentList>,

    /// Lock-free internal metrics. Shared via Arc for external access.
    metrics: Arc<StoreMetrics>,
}

impl EventStoreEngine {
    /// Creates a new event store in the given directory with default options.
    pub fn create(dir: &Path) -> Result<Self, Error> {
        Self::create_with_options(dir, DEFAULT_SEGMENT_SIZE)
    }

    /// Creates a new event store with custom segment size.
    pub fn create_with_options(dir: &Path, max_segment_size: u64) -> Result<Self, Error> {
        Self::create_with_store_options(
            dir,
            &StoreOptions {
                max_segment_size,
                ..Default::default()
            },
        )
    }

    /// Creates a new event store with full options.
    pub fn create_with_store_options(dir: &Path, opts: &StoreOptions) -> Result<Self, Error> {
        std::fs::create_dir_all(dir)?;
        let seg_writer = SegmentWriter::new(dir, Position(0), opts.max_segment_size)?;
        let active_base = seg_writer.active_base_position();
        let (commit_tx, _) = broadcast::channel(COMMIT_CHANNEL_CAPACITY);

        let gc_enabled = opts.group_commit_interval_ms > 0;
        let sync_state = Arc::new(SyncState::new(gc_enabled));
        let writer = Arc::new(parking_lot::Mutex::new(seg_writer));

        if gc_enabled {
            spawn_sync_thread(
                Arc::clone(&sync_state),
                Arc::clone(&writer),
                Duration::from_millis(opts.group_commit_interval_ms),
            );
        }

        Ok(Self {
            dir: dir.to_path_buf(),
            writer,
            tag_index: Arc::new(TagIndex::new()),
            sync_state,
            head_position: Arc::new(AtomicU64::new(0)),
            commit_tx,
            cache: Arc::new(IndexCache::new(
                opts.index_cache_size,
                opts.bloom_cache_size,
            )),
            segments: RwLock::new(SegmentList {
                bases: vec![active_base],
                sealed_count: 0,
            }),
            metrics: Arc::new(StoreMetrics::new()),
        })
    }

    /// Opens an existing event store, recovering from the last valid state.
    pub fn open(dir: &Path) -> Result<Self, Error> {
        Self::open_with_options(dir, DEFAULT_SEGMENT_SIZE)
    }

    /// Opens an existing event store with custom segment size.
    pub fn open_with_options(dir: &Path, max_segment_size: u64) -> Result<Self, Error> {
        Self::open_with_store_options(
            dir,
            &StoreOptions {
                max_segment_size,
                ..Default::default()
            },
        )
    }

    /// Opens an existing event store with full options.
    pub fn open_with_store_options(dir: &Path, opts: &StoreOptions) -> Result<Self, Error> {
        let seg_writer = SegmentWriter::open(dir, opts.max_segment_size)?;
        let head = seg_writer.head();
        let active_base = seg_writer.active_base_position();

        // Rebuild the active segment's tag index from its events.
        // Sealed segments have their own `.idx` files on disk.
        let tag_index = TagIndex::new();
        rebuild_active_segment_index(dir, &tag_index)?;

        // Build the cached segment list from disk (one-time cost on startup).
        let all_bases = segment::list_segment_files(dir)?;
        let sealed_count = count_sealed_segments(dir, &all_bases, active_base);

        // Writer's head() already returns the next-exclusive position, which
        // is exactly our head_position semantics — no adjustment.
        let head_pos = head.0;
        let (commit_tx, _) = broadcast::channel(COMMIT_CHANNEL_CAPACITY);

        let gc_enabled = opts.group_commit_interval_ms > 0;
        let sync_state = Arc::new(SyncState::new(gc_enabled));
        let writer = Arc::new(parking_lot::Mutex::new(seg_writer));

        if gc_enabled {
            spawn_sync_thread(
                Arc::clone(&sync_state),
                Arc::clone(&writer),
                Duration::from_millis(opts.group_commit_interval_ms),
            );
        }

        Ok(Self {
            dir: dir.to_path_buf(),
            writer,
            tag_index: Arc::new(tag_index),
            sync_state,
            head_position: Arc::new(AtomicU64::new(head_pos)),
            commit_tx,
            cache: Arc::new(IndexCache::new(
                opts.index_cache_size,
                opts.bloom_cache_size,
            )),
            segments: RwLock::new(SegmentList {
                bases: all_bases,
                sealed_count,
            }),
            metrics: Arc::new(StoreMetrics::new()),
        })
    }

    /// Returns a shared reference to this engine's metrics counters.
    /// Initiates engine shutdown: new appends are rejected, and the group
    /// commit sync thread performs one final fsync pass (releasing any
    /// in-flight writers) before exiting. Idempotent.
    pub fn shutdown(&self) {
        self.sync_state.shutdown.store(true, Ordering::Release);
    }

    pub fn metrics(&self) -> &Arc<StoreMetrics> {
        &self.metrics
    }

    /// Takes a point-in-time snapshot of all metrics, including cache stats.
    pub fn metrics_snapshot(&self) -> crate::metrics::MetricsSnapshot {
        let mut snap = self.metrics.snapshot();
        // Merge in cache-level counters.
        snap.index_cache_hits = self.cache.index_hits.load(Ordering::Relaxed);
        snap.index_cache_misses = self.cache.index_misses.load(Ordering::Relaxed);
        snap.mmap_cache_hits = self.cache.mmap_hits.load(Ordering::Relaxed);
        snap.mmap_cache_misses = self.cache.mmap_misses.load(Ordering::Relaxed);
        snap
    }

    /// Appends events to the store, optionally with a DCB consistency condition.
    ///
    /// 1. Checks DCB condition against the tag index
    /// 2. Writes events to the active segment (tags included on disk)
    /// 3. Updates the in-memory tag index
    /// 4. Advances the head position (next-exclusive)
    pub fn append(&self, request: AppendRequest) -> Result<AppendResponse, Error> {
        self.append_internal(request, None)
    }

    /// Like `append`, but atomically persists the applied Raft `LogId` alongside
    /// the events by emitting a `RaftMarker::normal(term, index, count)` record
    /// in the same segment fsync. Used by the Raft state machine so boot-time
    /// recovery can reconstruct `last_applied` from segment scan — no sidecar
    /// file, no extra fsync.
    ///
    /// For empty event batches (condition-only or rejected-after-check) this
    /// method does not emit a marker — `last_applied` will be recovered from
    /// the next Normal entry that produces events. Membership/Blank entries
    /// are not persisted here; see `state_machine::apply` for rationale.
    pub fn append_with_raft(
        &self,
        request: AppendRequest,
        applied: AppliedLogId,
    ) -> Result<AppendResponse, Error> {
        self.append_internal(request, Some(applied))
    }

    /// Applies a whole batch of raft-marked appends under ONE writer lock and
    /// ONE fsync. This is the state machine's bulk path: openraft delivers
    /// `apply()` batches, and syncing once per batch instead of once per entry
    /// is what lets concurrent consensus appends share an fsync the same way
    /// concurrent direct appends share one via group commit.
    ///
    /// Per-item DCB violations come back as `Err` in that item's slot (they
    /// are deterministic, valid outcomes); any other error aborts the whole
    /// batch as fatal. Items are applied in order — an item's DCB check sees
    /// every earlier item's writes.
    pub fn append_with_raft_batch(
        &self,
        batch: Vec<(AppendRequest, AppliedLogId)>,
    ) -> Result<Vec<Result<AppendResponse, Error>>, Error> {
        if batch.is_empty() {
            return Ok(vec![]);
        }
        let timer = Timer::start();

        if self.sync_state.is_failed() {
            return Err(self.sync_state.failure_error());
        }
        if self.sync_state.shutdown.load(Ordering::Relaxed) {
            return Err(Error::Io(io::Error::other(
                "event store is shutting down; append rejected",
            )));
        }

        let target_epoch = if self.sync_state.enabled {
            Some(self.sync_state.mark_pending())
        } else {
            None
        };

        let item_count = batch.len() as u64;
        let results = {
            let mut writer = self.writer.lock();
            let mut results = Vec::with_capacity(batch.len());
            for (request, applied) in &batch {
                match self.append_locked(&mut writer, request, Some(*applied)) {
                    Ok(resp) => results.push(Ok(resp)),
                    Err(e @ Error::ConsistencyConditionViolated { .. }) => results.push(Err(e)),
                    // Anything else is fatal for the whole batch: the writer
                    // may hold partially-written earlier items whose fsync
                    // outcome the caller must not assume.
                    Err(fatal) => return Err(fatal),
                }
            }
            // Strict mode: one explicit fsync for the whole batch.
            if !self.sync_state.enabled {
                writer.sync()?;
            }
            results
        };

        // Group-commit mode: NO wait. For consensus appends the client-ack
        // durability guarantee comes from the raft LOG fsync (an entry is
        // only committed once quorum-durable in the log), and the write is
        // replayable: if the process dies before the segment fsync lands,
        // the missing `RaftMarker` makes recovery re-apply these entries
        // from the log (see `reconcile_with_log`). `mark_pending` above has
        // already scheduled the fsync with the sync thread. Not blocking
        // here keeps openraft's state-machine worker free to apply the next
        // batch — waiting would cap consensus throughput at ~1 apply per
        // group-commit interval regardless of concurrency.
        let _ = target_epoch;

        let per_item_us = timer.elapsed_us() / item_count.max(1);
        for result in results.iter().flatten() {
            self.metrics.record_append(result.count, per_item_us);
        }
        Ok(results)
    }

    /// Applies every item of ONE raft `AppendBatch` entry under a single
    /// writer lock, a single raft marker, and (in group-commit mode) zero
    /// fsync waits.
    ///
    /// Crash-safety shape: unlike `append_with_raft_batch` (independent
    /// entries, marker per entry), all items here belong to one log entry —
    /// a torn write must never leave a *prefix* of the entry durable with a
    /// marker claiming the entry applied, or the tail would be lost without
    /// replay. So the whole entry persists as ONE `RaftMarker` followed by
    /// every accepted event: either the marker+events survive recovery
    /// (entry fully applied) or they're truncated (entry replays from the
    /// raft log).
    ///
    /// DCB checks run in a first pass with in-batch visibility: item K's
    /// condition sees committed state plus the accepted events of items
    /// 0..K. Rejections are deterministic per-item outcomes.
    pub fn append_with_raft_entry_batch(
        &self,
        items: Vec<AppendRequest>,
        applied: AppliedLogId,
    ) -> Result<Vec<Result<AppendResponse, Error>>, Error> {
        if items.is_empty() {
            return Ok(vec![]);
        }
        let timer = Timer::start();

        if self.sync_state.is_failed() {
            return Err(self.sync_state.failure_error());
        }
        if self.sync_state.shutdown.load(Ordering::Relaxed) {
            return Err(Error::Io(io::Error::other(
                "event store is shutting down; append rejected",
            )));
        }

        let item_count = items.len() as u64;
        let results = {
            let mut writer = self.writer.lock();
            if self.sync_state.enabled {
                // Register with the current group-commit wave (no wait below;
                // durability comes from the raft log — see batch fn).
                self.sync_state.mark_pending();
            }

            // Pass 1: per-item DCB with in-batch visibility. Provisional
            // positions start at the current head; accepted events extend
            // the in-batch view the next items are checked against.
            let base = writer.head();
            let mut outcomes: Vec<Result<Position, Error>> = Vec::with_capacity(items.len());
            let mut accepted_events: Vec<&crate::event::AppendEvent> = Vec::new();
            for request in &items {
                if let Some(condition) = &request.condition {
                    if let Some(pos) = self.check_dcb_locked(condition)? {
                        outcomes.push(Err(Error::ConsistencyConditionViolated {
                            conflicting_position: pos,
                        }));
                        continue;
                    }
                    // In-batch conflicts: earlier accepted items' events.
                    let conflict = accepted_events.iter().enumerate().find(|(_, e)| {
                        condition
                            .criteria
                            .criteria
                            .iter()
                            .any(|c| event_matches_criterion(c, &e.name, &e.tags))
                    });
                    if let Some((offset, _)) = conflict {
                        self.metrics.record_dcb_violation();
                        outcomes.push(Err(Error::ConsistencyConditionViolated {
                            conflicting_position: Position(base.0 + offset as u64),
                        }));
                        continue;
                    }
                }
                outcomes.push(Ok(Position(base.0 + accepted_events.len() as u64)));
                accepted_events.extend(request.events.iter());
            }

            // Pass 2: one marker covering every accepted event, then the
            // events themselves, all in one segment (write_raft_entry
            // pre-rotates so marker+events never straddle a boundary).
            let total = accepted_events.len();
            if total > 0 {
                let count_u16 = u16::try_from(total).map_err(|_| Error::Corrupted {
                    message: "raft-marked batch exceeds u16::MAX events".into(),
                })?;
                let marker = crate::segment::format::RaftMarker::normal(
                    applied.term,
                    applied.index,
                    count_u16,
                );
                let all_events: Vec<crate::event::AppendEvent> =
                    accepted_events.iter().map(|e| (*e).clone()).collect();
                let old_active_base = writer.active_base_position();
                let (first_position, _count) = writer.write_raft_entry(&marker, &all_events)?;
                debug_assert_eq!(first_position, base);
                if !self.sync_state.enabled {
                    writer.sync()?;
                }

                let new_active_base = writer.active_base_position();
                if new_active_base != old_active_base {
                    let mut seg_list = self.segments.write();
                    seg_list.sealed_count += 1;
                    seg_list.bases.push(new_active_base);
                    self.metrics.record_segment_rotation();
                }

                let mut pos = first_position;
                for event in &all_events {
                    self.tag_index.index_event(pos, &event.name, &event.tags);
                    pos = pos.next();
                }
                let new_head = first_position.0 + total as u64;
                self.head_position.store(new_head, Ordering::Release);
                let _ = self.commit_tx.send(CommitNotification {
                    head_position: new_head,
                });
            }

            let final_head = self.head_position.load(Ordering::Acquire);
            items
                .iter()
                .zip(outcomes)
                .map(|(request, outcome)| {
                    outcome.map(|first_position| AppendResponse {
                        first_position,
                        count: request.events.len() as u32,
                        consistency_marker: Position(final_head),
                    })
                })
                .collect::<Vec<_>>()
        };

        // Group-commit mode: no fsync wait — same durability argument as
        // `append_with_raft_batch` (raft log fsync + marker replay).
        let per_item_us = timer.elapsed_us() / item_count.max(1);
        for result in results.iter().flatten() {
            self.metrics.record_append(result.count, per_item_us);
        }
        Ok(results)
    }

    fn append_internal(
        &self,
        request: AppendRequest,
        applied: Option<AppliedLogId>,
    ) -> Result<AppendResponse, Error> {
        let timer = Timer::start();

        // Fail fast once poisoned: after an fsync failure nothing written
        // here can be made durable, so accepting the write would lie.
        if self.sync_state.is_failed() {
            return Err(self.sync_state.failure_error());
        }
        // Reject writes during shutdown — the sync thread is doing its final
        // pass and a write marked after it would wait for an fsync that
        // never comes.
        if self.sync_state.shutdown.load(Ordering::Relaxed) {
            return Err(Error::Io(io::Error::other(
                "event store is shutting down; append rejected",
            )));
        }

        let target_epoch = if self.sync_state.enabled {
            Some(self.sync_state.mark_pending())
        } else {
            None
        };

        let response = {
            // Lock the writer. DCB check + write + index update must be atomic.
            let mut writer = self.writer.lock();
            let response = self.append_locked(&mut writer, &request, applied)?;
            // Strict (non-group-commit) mode: make the write durable before
            // acking. `append_locked` never fsyncs by itself.
            if !self.sync_state.enabled && response.count > 0 {
                writer.sync()?;
            }
            response
        };

        // Step 6: Wait for fsync (group commit only). A failed fsync surfaces
        // here as an error — the caller must NOT treat the write as durable.
        if let Some(epoch) = target_epoch {
            self.sync_state.wait_for_sync(epoch)?;
        }

        self.metrics
            .record_append(response.count, timer.elapsed_us());
        Ok(response)
    }

    /// Checks a DCB condition against committed state (sealed segments +
    /// active tag index). Must run under the writer lock so the answer can't
    /// be invalidated by a concurrent append. Returns the conflicting
    /// position, if any.
    fn check_dcb_locked(&self, condition: &AppendCondition) -> Result<Option<Position>, Error> {
        let marker = condition.consistency_marker.0;

        // Check sealed segments whose events come after the marker.
        // Uses the same bloom → index → bitmap path as source reads.
        let seg_list = self.segments.read().clone();
        for (i, &base) in seg_list.bases.iter().enumerate() {
            if !seg_list.is_sealed(i) {
                break; // Active segment checked below via tag index.
            }
            // Segment ends below the marker — all its events were
            // already validated by the caller, skip. seg_end is the
            // last position in the segment (next base - 1).
            let seg_end = if i + 1 < seg_list.bases.len() {
                seg_list.bases[i + 1] - 1
            } else {
                continue;
            };
            if seg_end < marker {
                continue;
            }

            let seg_path = segment::segment_path(&self.dir, base);

            // Bloom filter: skip segment if tag definitely not present.
            if let Some(false) = self.cache.bloom_check(&seg_path, base, &condition.criteria) {
                continue;
            }

            // Load index and check for any match after the marker.
            let seg_index = self.cache.get_index(&seg_path, base)?;
            if let Some(conflicting_pos) = seg_index.has_match_after(&condition.criteria, marker) {
                self.metrics.record_dcb_violation();
                return Ok(Some(conflicting_pos));
            }
        }

        // Check the active segment via in-memory tag index.
        // tag_index is internally sharded; no lock needed.
        Ok(self.tag_index.check_condition(condition))
    }

    /// The atomic core of an append: DCB check + write + index/head update,
    /// all under the caller-held writer lock. NEVER fsyncs — durability is
    /// the caller's job (strict-mode sync or group-commit epoch wait).
    fn append_locked(
        &self,
        writer: &mut SegmentWriter,
        request: &AppendRequest,
        applied: Option<AppliedLogId>,
    ) -> Result<AppendResponse, Error> {
        {
            // Step 1: Check DCB condition.
            if let Some(condition) = &request.condition {
                if let Some(conflicting_pos) = self.check_dcb_locked(condition)? {
                    return Err(Error::ConsistencyConditionViolated {
                        conflicting_position: conflicting_pos,
                    });
                }
            }

            if request.events.is_empty() {
                let head = writer.head();
                return Ok(AppendResponse {
                    first_position: head,
                    count: 0,
                    consistency_marker: Position(self.head_position.load(Ordering::Acquire)),
                });
            }

            let old_active_base = writer.active_base_position();

            // Step 2: Write events (+ Raft marker, if threaded in).
            //
            // When `applied` is Some, we route through `SegmentWriter::write_raft_entry`
            // which emits a `RaftMarker::normal(term, index, count)` record *before* the
            // event records in the same segment. That marker is the durable witness of
            // `last_applied` — on restart, scanning raft markers across all segments
            // reconstructs it without an extra fsync or sidecar file. The marker + its
            // events are guaranteed not to straddle a segment boundary (see
            // `write_raft_entry` pre-rotate check).
            let (first_position, count) = if let Some(log_id) = applied {
                let count_u16 =
                    u16::try_from(request.events.len()).map_err(|_| Error::Corrupted {
                        message: "raft-marked append exceeds u16::MAX events".into(),
                    })?;
                let marker = crate::segment::format::RaftMarker::normal(
                    log_id.term,
                    log_id.index,
                    count_u16,
                );
                writer.write_raft_entry(&marker, &request.events)?
            } else {
                // Write without fsync — durability is the caller's job
                // (group-commit epoch wait, or an explicit strict-mode sync).
                writer.write_events(&request.events)?
            };

            // Step 2b: Detect rotation and update cached segment list.
            let new_active_base = writer.active_base_position();
            if new_active_base != old_active_base {
                let mut seg_list = self.segments.write();
                seg_list.sealed_count += 1;
                seg_list.bases.push(new_active_base);
                self.metrics.record_segment_rotation();
            }

            // Step 3: Update in-memory tag index.
            // TagIndex is internally sharded — concurrent callers indexing events
            // with different tag keys proceed in parallel.
            let mut pos = first_position;
            for event in &request.events {
                self.tag_index.index_event(pos, &event.name, &event.tags);
                pos = pos.next();
            }

            // Step 4: Advance head position (next-exclusive: first event's
            // position + count = position the next event will land at).
            let new_head = first_position.0 + count as u64;
            self.head_position.store(new_head, Ordering::Release);

            // Step 5: Notify stream subscribers.
            let _ = self.commit_tx.send(CommitNotification {
                head_position: new_head,
            });

            Ok(AppendResponse {
                first_position,
                count,
                consistency_marker: Position(new_head),
            })
        }
    }

    /// Gets tags for an event at the given position by reading from the segment.
    pub fn get_tags(&self, position: Position) -> Result<Vec<Tag>, Error> {
        let head = self.head_position.load(Ordering::Acquire);
        if position.0 >= head {
            return Err(Error::Corrupted {
                message: format!("position {} does not exist", position.0),
            });
        }

        // Use cached segment list instead of readdir.
        let seg_list = self.segments.read().clone();
        let seg_idx = match seg_list.bases.binary_search(&position.0) {
            Ok(i) => i,
            Err(0) => {
                return Err(Error::Corrupted {
                    message: format!("no segment contains position {}", position.0),
                });
            }
            Err(i) => i - 1,
        };

        let base = seg_list.bases[seg_idx];
        let seg_path = segment::segment_path(&self.dir, base);

        // Use cached mmap for sealed segments.
        let reader = if seg_list.is_sealed(seg_idx) {
            let mmap = self.cache.get_mmap(&seg_path, base)?;
            SegmentReader::from_shared_mmap(mmap)?
        } else {
            SegmentReader::open(&seg_path)?
        };

        for result in reader.iter(None) {
            let event = result?;
            if event.position == position {
                return Ok(event.tags);
            }
            if event.position > position {
                break;
            }
        }

        Err(Error::Corrupted {
            message: format!("event at position {} not found in segment", position.0),
        })
    }

    /// Returns the position of the first event with timestamp >= the given millis-since-epoch.
    ///
    /// Scans segments from oldest to newest, reading events linearly within each segment.
    /// This is an infrequently-called operation so a linear scan is acceptable.
    pub fn get_sequence_at(&self, timestamp_millis: i64) -> Result<Option<Position>, Error> {
        let head = self.head_position.load(Ordering::Acquire);
        if head == 0 {
            return Ok(None);
        }

        let seg_list = self.segments.read().clone();
        if seg_list.bases.is_empty() {
            return Ok(None);
        }

        for (i, &base) in seg_list.bases.iter().enumerate() {
            if base >= head {
                break;
            }

            let seg_path = segment::segment_path(&self.dir, base);

            let reader = if seg_list.is_sealed(i) {
                let mmap = self.cache.get_mmap(&seg_path, base)?;
                SegmentReader::from_shared_mmap(mmap)?
            } else {
                SegmentReader::open(&seg_path)?
            };

            for result in reader.iter(Some(Position(head))) {
                let event = result?;
                if event.timestamp >= timestamp_millis {
                    return Ok(Some(event.position));
                }
            }
        }

        Ok(None)
    }

    /// Creates a live event stream subscription.
    pub fn subscribe(&self, from_position: Position, condition: SourcingCondition) -> EventStream {
        EventStream::new(
            condition,
            from_position,
            self.commit_tx.subscribe(),
            Arc::clone(&self.head_position),
        )
    }

    /// Returns the current head position (next position to be assigned;
    /// equivalently, the count of events committed).
    pub fn head(&self) -> Position {
        self.writer.lock().head()
    }

    /// Returns the tail position (first available event position).
    ///
    /// For an empty store, returns the same as `head()` so that `head - tail == 0`.
    /// For a non-empty, non-truncated store, returns `Position(0)` — the
    /// position of the first event, since the log is 0-based.
    pub fn tail(&self) -> Position {
        let head = self.head_position.load(Ordering::Acquire);
        if head == 0 {
            Position(0) // Empty: tail == head == 0.
        } else {
            Position(0) // TODO: Track actual tail for truncated stores.
        }
    }

    /// Reads events matching a query from `from_position` up to the current head.
    /// This is the "Source" operation — a finite read.
    ///
    /// For sealed segments: uses cached bloom filters, indices, and mmap handles.
    /// For the active segment: uses the in-memory tag index.
    ///
    /// No readdir or stat syscalls — segment list and sealed status are cached.
    pub fn source(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
    ) -> Result<Vec<SequencedEvent>, Error> {
        let timer = Timer::start();
        let head = self.head_position.load(Ordering::Acquire);

        // Read cached segment list — no readdir syscall.
        let seg_list = self.segments.read().clone();
        if seg_list.bases.is_empty() || head == 0 {
            return Ok(vec![]);
        }

        let mut events = Vec::new();

        for (i, &base) in seg_list.bases.iter().enumerate() {
            let seg_path = segment::segment_path(&self.dir, base);
            let is_last = i + 1 == seg_list.bases.len();
            // seg_end is the last position present in this segment.
            let seg_end = if !is_last {
                seg_list.bases[i + 1] - 1
            } else {
                head - 1
            };

            if base >= head {
                break;
            }
            if seg_end < from_position.0 {
                continue;
            }

            // Determine matching positions — no stat syscalls for sealed check.
            let (matching_positions, seg_index) = if seg_list.is_sealed(i) {
                // Sealed segment — check bloom filter first, then load index via cache.
                if let Some(false) = self.cache.bloom_check(&seg_path, base, condition) {
                    self.metrics.record_bloom_check(true);
                    continue; // Bloom filter says definitely no match — skip segment.
                }
                self.metrics.record_bloom_check(false);

                let idx = self.cache.get_index(&seg_path, base)?;
                let bm = idx.matching(condition);
                (bm, Some(idx))
            } else {
                // Active segment — use in-memory index.
                // tag_index is internally sharded; no lock needed.
                (
                    self.tag_index.matching_bitmap(condition, from_position),
                    None,
                )
            };

            let matching_positions = match matching_positions {
                Some(bm) => bm,
                None => continue, // No matches in this segment.
            };

            // Use cached mmap for sealed segments, fresh open for active.
            let reader = if seg_list.is_sealed(i) {
                let mmap = self.cache.get_mmap(&seg_path, base)?;
                SegmentReader::from_shared_mmap(mmap)?
            } else {
                SegmentReader::open(&seg_path)?
            };

            // Sealed segments: direct seek via offset table — O(K) matching events.
            // Active segment: sequential scan with bitmap filter.
            if let Some(idx) = &seg_index {
                self.metrics.record_direct_seek();
                for pos in matching_positions.iter() {
                    if pos < from_position.0 || pos >= head {
                        continue;
                    }
                    if let Some(offset) = idx.get_offset(pos) {
                        let stored = reader.read_event_at(offset as usize)?;
                        events.push(stored.into_sequenced());
                    }
                }
            } else {
                self.metrics.record_sequential_scan();
                for result in reader.iter(Some(Position(head))) {
                    let stored = result?;

                    if stored.position.0 < from_position.0 {
                        continue;
                    }

                    if matching_positions.contains(stored.position.0) {
                        events.push(stored.into_sequenced());
                    }
                }
            }
        }

        self.metrics.record_source(events.len(), timer.elapsed_us());
        Ok(events)
    }

    /// Like `source`, but returns `StoredEvent` (includes tags on each event).
    /// Used by the admin console event browser where tags are displayed inline.
    pub fn source_stored(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, Error> {
        let head = self.head_position.load(Ordering::Acquire);

        let seg_list = self.segments.read().clone();
        if seg_list.bases.is_empty() || head == 0 {
            return Ok(vec![]);
        }

        let mut events = Vec::new();

        for (i, &base) in seg_list.bases.iter().enumerate() {
            let seg_path = segment::segment_path(&self.dir, base);
            let is_last = i + 1 == seg_list.bases.len();
            let seg_end = if !is_last {
                seg_list.bases[i + 1] - 1
            } else {
                head - 1
            };

            if base >= head {
                break;
            }
            if seg_end < from_position.0 {
                continue;
            }

            let (matching_positions, seg_index) = if seg_list.is_sealed(i) {
                if let Some(false) = self.cache.bloom_check(&seg_path, base, condition) {
                    continue;
                }
                let idx = self.cache.get_index(&seg_path, base)?;
                let bm = idx.matching(condition);
                (bm, Some(idx))
            } else {
                // tag_index is internally sharded; no lock needed.
                (
                    self.tag_index.matching_bitmap(condition, from_position),
                    None,
                )
            };

            let matching_positions = match matching_positions {
                Some(bm) => bm,
                None => continue,
            };

            let reader = if seg_list.is_sealed(i) {
                let mmap = self.cache.get_mmap(&seg_path, base)?;
                SegmentReader::from_shared_mmap(mmap)?
            } else {
                SegmentReader::open(&seg_path)?
            };

            if let Some(idx) = &seg_index {
                for pos in matching_positions.iter() {
                    if pos < from_position.0 || pos >= head {
                        continue;
                    }
                    if let Some(offset) = idx.get_offset(pos) {
                        let stored = reader.read_event_at(offset as usize)?;
                        events.push(stored);
                        if events.len() >= limit {
                            return Ok(events);
                        }
                    }
                }
            } else {
                for result in reader.iter(Some(Position(head))) {
                    let stored = result?;

                    if stored.position.0 < from_position.0 {
                        continue;
                    }

                    if matching_positions.contains(stored.position.0) {
                        events.push(stored);
                        if events.len() >= limit {
                            return Ok(events);
                        }
                    }
                }
            }
        }

        Ok(events)
    }

    /// Scans all on-disk segments for persisted `RaftMarker` records and
    /// returns the maximum `(term, index)` seen. Used on boot to reconstruct
    /// the Raft state-machine's `last_applied` without any sidecar file.
    ///
    /// Cheap enough for boot: sealed segments have bounded size (256 MB default)
    /// and only markers are deserialized; event payloads are skipped by the
    /// `iter_raft_markers` iterator. Returns `None` if no markers are present
    /// (fresh store, or legacy data written before this plan).
    pub fn max_applied_log_id(&self) -> Result<Option<AppliedLogId>, Error> {
        use crate::segment::format::RaftEntryType;

        let seg_list = self.segments.read().clone();
        let mut best: Option<(u64, u64)> = None;
        for &base in &seg_list.bases {
            let seg_path = segment::segment_path(&self.dir, base);
            let reader = match SegmentReader::open(&seg_path) {
                Ok(r) => r,
                Err(_) => continue, // Missing / unreadable segments don't contribute.
            };
            for item in reader.iter_raft_markers() {
                let (_, marker) = match item {
                    Ok(m) => m,
                    Err(_) => break, // Torn tail in this segment — stop scanning it.
                };
                // Membership and Blank markers are informational here (we don't
                // persist them in this plan), but if any appear just honour their
                // term/index as well — they cannot decrease the max.
                let _ = RaftEntryType::Normal; // readability
                let candidate = (marker.term, marker.index);
                if best.map(|b| candidate > b).unwrap_or(true) {
                    best = Some(candidate);
                }
            }
        }
        Ok(best.map(|(term, index)| AppliedLogId { term, index }))
    }

    /// Returns every stored event with position >= `from_position` up to the
    /// current committed head, in ascending position order, with tags attached.
    ///
    /// Used by the Raft snapshot builder (Phase 4, SNAP-01). Unlike `source` /
    /// `source_stored`, this applies no criterion filter — every event matches.
    /// Walks sealed segments (via cached mmap) and the active segment with no
    /// readdir / stat calls in the hot loop; iteration mirrors the same
    /// segment-list walk used by `source` / `source_stored`.
    ///
    /// Scope note: this is an inherent method on `EventStoreEngine`, not a
    /// trait method on `EventStore`. The trait is the client-facing contract
    /// (kept wire-stable per PROJECT.md constraint); snapshot building is
    /// internal to the Raft state-machine path.
    pub fn source_all(&self, from_position: Position) -> Result<Vec<StoredEvent>, Error> {
        let head = self.head_position.load(Ordering::Acquire);
        let seg_list = self.segments.read().clone();
        if seg_list.bases.is_empty() || head == 0 {
            return Ok(vec![]);
        }

        let mut events = Vec::new();
        for (i, &base) in seg_list.bases.iter().enumerate() {
            if base >= head {
                break;
            }
            let seg_path = segment::segment_path(&self.dir, base);
            let is_last = i + 1 == seg_list.bases.len();
            let seg_end = if !is_last {
                seg_list.bases[i + 1] - 1
            } else {
                head - 1
            };
            if seg_end < from_position.0 {
                continue;
            }

            let reader = if seg_list.is_sealed(i) {
                let mmap = self.cache.get_mmap(&seg_path, base)?;
                SegmentReader::from_shared_mmap(mmap)?
            } else {
                SegmentReader::open(&seg_path)?
            };

            // `iter(Some(up_to))` stops once it sees a position >= up_to;
            // pass head so every committed event (positions 0..head) is yielded.
            for result in reader.iter(Some(Position(head))) {
                let stored = result?;
                if stored.position.0 < from_position.0 {
                    continue;
                }
                if stored.position.0 >= head {
                    break;
                }
                events.push(stored);
            }
        }
        Ok(events)
    }
}

#[async_trait::async_trait]
impl EventStore for EventStoreEngine {
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, Error> {
        // Delegates to the inherent sync method. No .await points —
        // compiles to a ready future. The inherent method is what the
        // Raft state machine calls directly (bypassing the trait).
        self.append(request)
    }

    fn source(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
    ) -> Result<Vec<SequencedEvent>, Error> {
        self.source(from_position, condition)
    }

    fn subscribe(&self, from_position: Position, condition: SourcingCondition) -> EventStream {
        self.subscribe(from_position, condition)
    }

    fn head(&self) -> Position {
        self.head()
    }

    fn tail(&self) -> Position {
        self.tail()
    }

    fn get_tags(&self, position: Position) -> Result<Vec<Tag>, Error> {
        self.get_tags(position)
    }

    fn get_sequence_at(&self, timestamp_millis: i64) -> Result<Option<Position>, Error> {
        self.get_sequence_at(timestamp_millis)
    }
}

/// Counts how many segments are sealed (have companion .idx/.bloom files).
/// Called once during open to populate the cached segment list.
fn count_sealed_segments(dir: &Path, bases: &[u64], active_base: u64) -> usize {
    let mut count = 0;
    for &base in bases {
        if base == active_base {
            break; // Active segment — not sealed.
        }
        let seg_path = segment::segment_path(dir, base);
        if SegmentIndex::has_companion_files(&seg_path) {
            count += 1;
        } else {
            break; // Gap in sealed segments — stop counting.
        }
    }
    count
}

/// Rebuilds the tag index for the active (unsealed) segment.
///
/// Sealed segments have `.idx` companion files on disk and don't need replay.
/// Only the active segment (the last one without `.idx`) is replayed.
/// If a sealed segment is missing its `.idx`, it's rebuilt from the segment data.
fn rebuild_active_segment_index(dir: &Path, index: &TagIndex) -> Result<(), Error> {
    let segments = segment::list_segment_files(dir)?;

    for base_pos in segments {
        let seg_path = segment::segment_path(dir, base_pos);

        if SegmentIndex::has_companion_files(&seg_path) {
            // Sealed segment with valid index files — skip replay.
            continue;
        }

        // No companion files — either active segment or sealed segment
        // with missing index. Rebuild the index from segment data.
        let reader = SegmentReader::open(&seg_path)?;

        for result in reader.iter(None) {
            let event = result?;
            index.index_event(event.position, &event.name, &event.tags);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::append::AppendCondition;
    use crate::criteria::{Criterion, SourcingCondition};
    use crate::event::AppendEvent;
    use crate::event::Tag;

    fn tag(key: &str, value: &str) -> Tag {
        Tag::from_str(key, value)
    }

    fn make_event(name: &str, tags: Vec<Tag>) -> AppendEvent {
        AppendEvent {
            identifier: format!("id-{name}"),
            name: name.into(),
            version: "1.0".into(),
            timestamp: 1712345678000,
            payload: b"test-data".to_vec(),
            metadata: vec![],
            tags,
        }
    }

    /// A failed fsync must surface as an error to every waiting writer —
    /// never as a silent durability ack — and must poison future appends.
    #[test]
    fn sync_failure_propagates_to_waiters_and_poisons() {
        let state = Arc::new(SyncState::new(true));

        let target = state.mark_pending();
        let waiter = {
            let state = Arc::clone(&state);
            std::thread::spawn(move || state.wait_for_sync(target))
        };

        // Give the waiter time to block, then fail the sync.
        std::thread::sleep(Duration::from_millis(50));
        state.fail_sync(&Error::Io(io::Error::other("simulated ENOSPC")));

        let result = waiter.join().unwrap();
        let err = result.expect_err("waiter must NOT be told the write is durable");
        assert!(err.to_string().contains("fsync failure"), "got: {err}");

        // Poisoned: subsequent waits fail immediately too.
        assert!(state.is_failed());
        assert!(state.wait_for_sync(state.mark_pending()).is_err());
    }

    /// The happy path is unchanged: complete_sync releases waiters with Ok.
    #[test]
    fn sync_success_releases_waiters_ok() {
        let state = Arc::new(SyncState::new(true));
        let target = state.mark_pending();
        let waiter = {
            let state = Arc::clone(&state);
            std::thread::spawn(move || state.wait_for_sync(target))
        };
        std::thread::sleep(Duration::from_millis(20));
        let wave = state.seal_wave();
        state.complete_wave(wave);
        assert!(waiter.join().unwrap().is_ok());
    }

    #[test]
    fn create_and_append() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        let request = AppendRequest {
            condition: None,
            events: vec![
                make_event("OrderPlaced", vec![tag("orderId", "A")]),
                make_event("PaymentReceived", vec![tag("orderId", "A")]),
            ],
        };

        let response = store.append(request).unwrap();
        assert_eq!(response.first_position, Position(0));
        assert_eq!(response.count, 2);
        // marker is the new head after the append (next-exclusive).
        assert_eq!(response.consistency_marker, Position(2));
        assert_eq!(store.head(), Position(2));
    }

    #[test]
    fn dcb_condition_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        store
            .append(AppendRequest {
                condition: None,
                events: vec![make_event("OrderPlaced", vec![tag("orderId", "A")])],
            })
            .unwrap();

        let result = store.append(AppendRequest {
            condition: Some(AppendCondition {
                consistency_marker: Position(1),
                criteria: SourcingCondition {
                    criteria: vec![Criterion {
                        names: vec!["OrderPlaced".into()],
                        tags: vec![tag("orderId", "A")],
                    }],
                },
            }),
            events: vec![make_event("OrderConfirmed", vec![tag("orderId", "A")])],
        });

        assert!(result.is_ok());
    }

    #[test]
    fn dcb_condition_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        store
            .append(AppendRequest {
                condition: None,
                events: vec![
                    make_event("OrderPlaced", vec![tag("orderId", "A")]),
                    make_event("OrderCancelled", vec![tag("orderId", "A")]),
                ],
            })
            .unwrap();

        let result = store.append(AppendRequest {
            condition: Some(AppendCondition {
                consistency_marker: Position(0),
                criteria: SourcingCondition {
                    criteria: vec![Criterion {
                        names: vec![],
                        tags: vec![tag("orderId", "A")],
                    }],
                },
            }),
            events: vec![make_event("OrderPlaced", vec![tag("orderId", "A")])],
        });

        assert!(matches!(
            result,
            Err(Error::ConsistencyConditionViolated { .. })
        ));
    }

    #[test]
    fn source_query() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        store
            .append(AppendRequest {
                condition: None,
                events: vec![
                    make_event("OrderPlaced", vec![tag("orderId", "A")]),
                    make_event("OrderPlaced", vec![tag("orderId", "B")]),
                    make_event("PaymentReceived", vec![tag("orderId", "A")]),
                ],
            })
            .unwrap();

        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("orderId", "A")],
            }],
        };

        let events = store.source(Position(0), &cond).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].name, "OrderPlaced");
        assert_eq!(events[1].name, "PaymentReceived");
    }

    #[test]
    fn get_tags_from_segment() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        store
            .append(AppendRequest {
                condition: None,
                events: vec![make_event(
                    "OrderPlaced",
                    vec![tag("orderId", "A"), tag("region", "EU")],
                )],
            })
            .unwrap();

        let tags = store.get_tags(Position(0)).unwrap();
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&tag("orderId", "A")));
        assert!(tags.contains(&tag("region", "EU")));
    }

    #[test]
    fn tags_survive_recovery() {
        let dir = tempfile::tempdir().unwrap();

        {
            let store = EventStoreEngine::create(dir.path()).unwrap();
            store
                .append(AppendRequest {
                    condition: None,
                    events: vec![
                        make_event("OrderPlaced", vec![tag("orderId", "A")]),
                        make_event(
                            "PaymentReceived",
                            vec![tag("orderId", "A"), tag("paymentId", "P1")],
                        ),
                    ],
                })
                .unwrap();
        }

        {
            let store = EventStoreEngine::open(dir.path()).unwrap();
            assert_eq!(store.head(), Position(2));

            let cond = SourcingCondition {
                criteria: vec![Criterion {
                    names: vec![],
                    tags: vec![tag("orderId", "A")],
                }],
            };
            let events = store.source(Position(0), &cond).unwrap();
            assert_eq!(events.len(), 2);

            let cond = SourcingCondition {
                criteria: vec![Criterion {
                    names: vec![],
                    tags: vec![tag("paymentId", "P1")],
                }],
            };
            let events = store.source(Position(0), &cond).unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].name, "PaymentReceived");

            // Tags readable from segment.
            let tags = store.get_tags(Position(0)).unwrap();
            assert!(tags.contains(&tag("orderId", "A")));
        }
    }

    #[test]
    fn head_and_tail() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        assert_eq!(store.head(), Position(0));
        assert_eq!(store.tail(), Position(0)); // Empty: tail == head == 0.
    }

    #[test]
    fn dcb_condition_checks_sealed_segments_after_restart() {
        let dir = tempfile::tempdir().unwrap();

        // Use tiny segments to force rotation, creating sealed segments.
        let opts = StoreOptions {
            max_segment_size: 200, // tiny — each event triggers rotation
            ..Default::default()
        };

        // Write an event that will end up in a sealed segment.
        {
            let store = EventStoreEngine::create_with_store_options(dir.path(), &opts).unwrap();
            store
                .append(AppendRequest {
                    condition: None,
                    events: vec![
                        make_event("OrderPlaced", vec![tag("orderId", "A")]),
                        make_event("OrderPlaced", vec![tag("orderId", "B")]),
                    ],
                })
                .unwrap();
        }

        // Reopen — tag index only has the active segment. The OrderPlaced events
        // for orderId=A and orderId=B are in sealed segments.
        {
            let store = EventStoreEngine::open_with_store_options(dir.path(), &opts).unwrap();

            // This condition says: "reject if any event with orderId=A exists after position 0"
            // The event IS at position 1 (in a sealed segment). This MUST be rejected.
            let result = store.append(AppendRequest {
                condition: Some(AppendCondition {
                    consistency_marker: Position(0),
                    criteria: SourcingCondition {
                        criteria: vec![Criterion {
                            names: vec![],
                            tags: vec![tag("orderId", "A")],
                        }],
                    },
                }),
                events: vec![make_event("OrderPlaced", vec![tag("orderId", "A")])],
            });

            assert!(
                matches!(result, Err(Error::ConsistencyConditionViolated { .. })),
                "DCB condition must detect conflicts in sealed segments after restart"
            );

            // But a condition with a marker AFTER the conflicting event should pass.
            let result = store.append(AppendRequest {
                condition: Some(AppendCondition {
                    consistency_marker: Position(2), // after both existing events
                    criteria: SourcingCondition {
                        criteria: vec![Criterion {
                            names: vec![],
                            tags: vec![tag("orderId", "A")],
                        }],
                    },
                }),
                events: vec![make_event("OrderConfirmed", vec![tag("orderId", "A")])],
            });

            assert!(result.is_ok(), "condition with fresh marker should pass");
        }
    }

    // ------------------------------------------------------------------
    // source_all — Phase 4 SNAP-01 (Task 1)
    // Tag-bearing sequential dump of all events. Used by the Raft
    // snapshot builder. Unlike `source` / `source_stored`, no criterion
    // filter is applied — every committed event is emitted in ascending
    // position order with its original tags.
    // ------------------------------------------------------------------
    #[test]
    fn source_all_returns_all_events_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let engine = EventStoreEngine::create(dir.path()).unwrap();
        for (i, name) in ["A", "B", "C"].iter().enumerate() {
            engine
                .append(AppendRequest {
                    condition: None,
                    events: vec![AppendEvent {
                        identifier: format!("id-{i}"),
                        name: (*name).to_string(),
                        version: "1.0".into(),
                        timestamp: 1712345678000,
                        payload: vec![],
                        metadata: vec![],
                        tags: vec![Tag::from_str("k", &format!("v{i}"))],
                    }],
                })
                .unwrap();
        }
        let all = engine.source_all(Position(0)).unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].position, Position(0));
        assert_eq!(all[1].position, Position(1));
        assert_eq!(all[2].position, Position(2));
        assert_eq!(all[0].name, "A");
        assert_eq!(all[0].tags.len(), 1);
    }

    #[test]
    fn source_all_empty_engine_returns_empty() {
        let dir = tempfile::tempdir().unwrap();
        let engine = EventStoreEngine::create(dir.path()).unwrap();
        let all = engine.source_all(Position(0)).unwrap();
        assert!(all.is_empty());
    }

    #[test]
    fn source_all_honours_from_position() {
        let dir = tempfile::tempdir().unwrap();
        let engine = EventStoreEngine::create(dir.path()).unwrap();
        for i in 0..3 {
            engine
                .append(AppendRequest {
                    condition: None,
                    events: vec![AppendEvent {
                        identifier: format!("id-{i}"),
                        name: "E".into(),
                        version: "1.0".into(),
                        timestamp: 0,
                        payload: vec![],
                        metadata: vec![],
                        tags: vec![],
                    }],
                })
                .unwrap();
        }
        let tail = engine.source_all(Position(1)).unwrap();
        assert_eq!(tail.len(), 2);
        assert_eq!(tail[0].position, Position(1));
        assert_eq!(tail[1].position, Position(2));
    }
}
