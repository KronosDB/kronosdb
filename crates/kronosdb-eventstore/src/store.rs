use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::broadcast;

use crate::api::EventStore;
use crate::append::{AppendCondition, AppendRequest, AppendResponse};
use crate::cache::IndexCache;
use crate::criteria::SourcingCondition;
use crate::error::Error;
use crate::event::{Position, SequencedEvent, StoredEvent, Tag};
use crate::stream::{CommitNotification, EventStream};

use crate::index::tag_index::TagIndex;
use crate::metrics::{StoreMetrics, Timer};
use crate::replication::dispatcher::{WaveDescriptor, WavePublisher, WaveSlice};
use crate::replication::watermark::WatermarkState;
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

/// Default extra coalescing window before sealing a commit wave (0 = seal as
/// soon as the sync thread observes pending writes). Batching under load does
/// not depend on this: writes arriving during an in-flight fdatasync always
/// coalesce into the next wave, so the fsync duration itself is the window.
const DEFAULT_GROUP_COMMIT_INTERVAL_MS: u64 = 0;

/// Default node id for direct engine use and tests. The server always
/// supplies its configured control-plane node id through `StoreOptions`.
const DEFAULT_NODE_ID: crate::replication::watermark::NodeId = 0;

/// Initial epoch before the first control-plane LeaderClaim.
const INITIAL_EPOCH: crate::replication::watermark::Epoch = 0;

/// Group commit synchronization.
///
/// Writers write events (no fsync), mark pending, release the writer lock,
/// then wait for the sync thread to fsync and advance the epoch.
/// Multiple writers share one fsync — that's the throughput win.
///
/// The sync thread is event-driven: the first write of a wave wakes it, and
/// writes arriving while a wave's fdatasync is in flight accumulate into the
/// next wave. A lone writer therefore pays only the fsync itself, while
/// concurrent writers batch on the fsync duration — no polling interval.
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
    shutdown: AtomicBool,
    /// Wakes the sync thread on a wave's first write and on shutdown. The
    /// mutex pairs each notify with the thread's predicate re-check so a
    /// wake landing between check and wait cannot be lost.
    wake: StdMutex<()>,
    wake_cv: Condvar,
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
    fn new() -> Self {
        Self {
            completed: StdMutex::new(0),
            synced: Condvar::new(),
            wave: AtomicU64::new(1),
            pending_writes: AtomicU64::new(0),
            shutdown: AtomicBool::new(false),
            wake: StdMutex::new(()),
            wake_cv: Condvar::new(),
            failed: AtomicBool::new(false),
            failure_msg: StdMutex::new(None),
        }
    }

    /// Registers a write with the current wave and wakes the sync thread on
    /// the wave's first write. MUST be called while holding the writer lock —
    /// that's what orders it against the sync thread's barrier (which resets
    /// the pending count under the same lock, so 0→1 happens once per wave).
    fn mark_pending(&self) -> u64 {
        let first_of_wave = self.pending_writes.fetch_add(1, Ordering::Relaxed) == 0;
        let wave = self.wave.load(Ordering::Acquire);
        if first_of_wave {
            let _wake = self.wake.lock().unwrap();
            self.wake_cv.notify_one();
        }
        wave
    }

    /// Latches shutdown and wakes the sync thread so it runs its final fsync
    /// pass immediately instead of waiting for a write that never comes.
    fn begin_shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        let _wake = self.wake.lock().unwrap();
        self.wake_cv.notify_one();
    }

    /// Blocks until the given wave's fsync completed. Returns an error — the
    /// write is NOT durable — if the fsync failed.
    ///
    /// Production leader appends use `WatermarkState::wait_for`; follower Tail
    /// tasks use this direct wave wait before sending a durable acknowledgement.
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

#[allow(clippy::too_many_arguments)]
fn spawn_sync_thread(
    dir: PathBuf,
    sync_state: Arc<SyncState>,
    writer: Arc<parking_lot::Mutex<SegmentWriter>>,
    local_tail: Arc<AtomicU64>,
    durable_tail: Arc<AtomicU64>,
    node_id: crate::replication::watermark::NodeId,
    replication: Arc<WavePublisher>,
    watermark: Arc<WatermarkState>,
    commit_tx: broadcast::Sender<CommitNotification>,
    coalesce_window: Duration,
) {
    std::thread::Builder::new()
        .name("kronosdb-sync".into())
        .spawn(move || {
            // Cursor delimiting the start of the next wave's raw byte range.
            // Existing bytes on open are not live-republished; a Tail session
            // catches them up from its durable position before subscribing.
            let (mut wave_base, mut wave_offset, mut wave_position) = {
                let w = writer.lock();
                (w.active_base_position(), w.write_offset(), w.head().0)
            };

            loop {
                // Sleep until a wave has its first write or shutdown begins.
                // The wake mutex pairs this predicate check with the notify
                // in mark_pending/begin_shutdown, so a signal landing between
                // check and wait cannot be lost.
                {
                    let mut wake = sync_state.wake.lock().unwrap();
                    while !sync_state.has_pending() && !sync_state.shutdown.load(Ordering::Relaxed)
                    {
                        wake = sync_state.wake_cv.wait(wake).unwrap();
                    }
                }
                // Optional extra coalescing window. Off by default: writes
                // arriving during the fdatasync below already join the next
                // wave, so batching self-clocks on fsync duration.
                if !coalesce_window.is_zero() {
                    std::thread::sleep(coalesce_window);
                }
                // Read the flag BEFORE the final sync pass: writers that
                // marked pending before shutdown still get their fsync (and
                // their wakeup) instead of hanging on a dead thread.
                let shutting_down = sync_state.shutdown.load(Ordering::Relaxed);
                if sync_state.has_pending() {
                    // Barrier: take the writer lock only long enough to seal
                    // the wave, snapshot its raw byte ranges and covered tail,
                    // and clone the active file handle. Dispatch and fsync
                    // proceed independently after the lock is released.
                    let sealed: Result<_, Error> = (|| {
                        let w = writer.lock();
                        let wave = sync_state.seal_wave();
                        let durable = local_tail.load(Ordering::Acquire);
                        let current_base = w.active_base_position();
                        let current_offset = w.write_offset();

                        let epoch = watermark.epoch();
                        let descriptor = if replication.has_subscribers() {
                            build_wave_descriptor(
                                &dir,
                                wave,
                                epoch,
                                wave_base,
                                wave_offset,
                                wave_position,
                                current_base,
                                current_offset,
                                durable,
                            )?
                        } else {
                            WaveDescriptor {
                                wave_id: wave,
                                epoch,
                                previous_segment_base: wave_base,
                                first_position: wave_position,
                                next_position: durable,
                                slices: Vec::new(),
                            }
                        };

                        // Advance the next wave's starting cursor regardless
                        // of subscriber presence. A newly-opened Tail catches
                        // up from disk before joining live dispatch.
                        wave_base = current_base;
                        wave_offset = current_offset;
                        wave_position = durable;

                        let file = w.active_file_handle()?;
                        Ok((wave, durable, descriptor, file))
                    })();
                    let result = sealed.and_then(|(wave, durable, descriptor, file)| {
                        // Queue before fsync: the dispatcher preads on its own
                        // thread while this thread enters fdatasync.
                        replication.try_publish(descriptor);
                        if crate::relaxed_acks()
                            && durable.saturating_sub(durable_tail.load(Ordering::Acquire))
                                <= crate::ack_lag_limit()
                        {
                            // Replicated-ack mode: the append path already
                            // advanced this node's cursor at write, so this
                            // bump is usually a no-op — but subscribers are
                            // woken per wave, so notify unconditionally.
                            // Past the lag limit the pre-fsync advance is
                            // skipped (disk-stall backpressure).
                            let wm = watermark
                                .advance(node_id, watermark.epoch(), durable)
                                .unwrap_or_else(|| watermark.get());
                            let _ = commit_tx.send(CommitNotification { watermark: wm });
                        }
                        crate::segment::writer::sync_file(&file)?;
                        Ok((wave, durable))
                    });
                    match result {
                        Ok((wave, durable)) => {
                            durable_tail.store(durable, Ordering::Release);
                            sync_state.complete_wave(wave);
                            // This node's durable cursor advances. With a
                            // quorum of one this IS the watermark bump that
                            // releases ack waiters; under replication
                            // follower cursors join the math.
                            if let Some(wm) = watermark.advance(node_id, watermark.epoch(), durable)
                            {
                                let _ = commit_tx.send(CommitNotification { watermark: wm });
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                "group-commit fsync FAILED — poisoning event store; \
                                 pending writes are not durable and new appends will be rejected"
                            );
                            sync_state.fail_sync(&e);
                            watermark.abort_all(&e.to_string());
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

#[allow(clippy::too_many_arguments)]
fn build_wave_descriptor(
    dir: &Path,
    wave_id: u64,
    epoch: u64,
    previous_base: u64,
    previous_offset: u64,
    first_position: u64,
    current_base: u64,
    current_offset: u64,
    next_position: u64,
) -> Result<WaveDescriptor, Error> {
    let bases = segment::list_segment_files(dir)?;
    let mut slices = Vec::new();
    let selected: Vec<u64> = bases
        .into_iter()
        .filter(|base| *base >= previous_base && *base <= current_base)
        .collect();
    for (index, &base) in selected.iter().enumerate() {
        let path = segment::segment_path(dir, base);
        let byte_start = if base == previous_base {
            previous_offset
        } else {
            segment::SEGMENT_HEADER_SIZE as u64
        };
        let byte_end = if base == current_base {
            current_offset
        } else {
            // Sealed segments are truncated to their exact data length at
            // rotation; unlike the active file this is not preallocation.
            std::fs::metadata(&path)?.len()
        };
        if byte_end > byte_start {
            slices.push(WaveSlice {
                path,
                segment_base: base,
                byte_start,
                byte_end,
                first_position: if base == previous_base {
                    first_position
                } else {
                    base
                },
                next_position: selected.get(index + 1).copied().unwrap_or(next_position),
            });
        }
    }

    Ok(WaveDescriptor {
        wave_id,
        epoch,
        previous_segment_base: previous_base,
        first_position,
        next_position,
        slices,
    })
}

#[derive(Debug, Clone, Copy)]
pub struct ReplicationCursor {
    pub position: Position,
    pub segment_base: u64,
    pub byte_offset: u64,
    pub last_record_crc: u32,
}

/// Result returned to a follower Tail task after bytes enter its local group-
/// commit wave. The task waits for `wave` before acknowledging upstream.
#[derive(Debug, Clone, Copy)]
pub struct ReplicatedWrite {
    pub wave: u64,
    pub durable_position: Position,
    pub byte_count: u64,
}

/// An append whose writer-lock section has completed but whose durability
/// has not been awaited yet. Produced by `EventStoreEngine::append_stage`;
/// resolved by `append_finish_async` (or the blocking `append`).
pub struct StagedAppend {
    /// The write's response, or the error the ack gate must cover (a DCB
    /// rejection waits for the conflicting event's commit before surfacing).
    outcome: Result<AppendResponse, Error>,
    /// Started at stage entry; finalization records the full duration.
    timer: Timer,
}

impl StagedAppend {
    /// The watermark position the ack gate must reach before this append
    /// resolves: the consistency marker on success, the conflicting
    /// position's commit on a DCB rejection, nothing for immediate errors.
    fn wait_pos(&self) -> Option<u64> {
        match &self.outcome {
            Ok(response) => Some(response.consistency_marker.0),
            Err(Error::ConsistencyConditionViolated {
                conflicting_position,
            }) => Some(conflicting_position.0 + 1),
            Err(_) => None,
        }
    }
}

/// Configuration options for an event store engine.
#[derive(Debug, Clone)]
pub struct StoreOptions {
    pub max_segment_size: u64,
    pub index_cache_size: usize,
    pub bloom_cache_size: usize,
    /// Extra group-commit coalescing window in milliseconds. The sync thread
    /// is woken by a wave's first write; with 0 (default) it seals and syncs
    /// immediately, and concurrent writes batch on the fdatasync duration.
    /// A positive value trades that much added latency for larger waves.
    pub group_commit_interval_ms: u64,
    /// This node's control-plane identity for durable-cursor acknowledgements.
    pub node_id: crate::replication::watermark::NodeId,
    /// Exact voter set used for native segment quorum calculations.
    pub voters: Vec<crate::replication::watermark::NodeId>,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            max_segment_size: DEFAULT_SEGMENT_SIZE,
            index_cache_size: DEFAULT_INDEX_CACHE_SIZE,
            bloom_cache_size: DEFAULT_BLOOM_CACHE_SIZE,
            group_commit_interval_ms: DEFAULT_GROUP_COMMIT_INTERVAL_MS,
            node_id: DEFAULT_NODE_ID,
            voters: vec![DEFAULT_NODE_ID],
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
            node_id: DEFAULT_NODE_ID,
            voters: vec![DEFAULT_NODE_ID],
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

    /// This node's control-plane identity.
    node_id: crate::replication::watermark::NodeId,

    /// The local tail — next-exclusive: the position the next event will be
    /// written at locally, equivalently the count of events written to the
    /// local segment log. Advanced at write time, under the writer lock.
    local_tail: Arc<AtomicU64>,

    /// Highest next-exclusive event position known to have completed local
    /// fdatasync. Unlike `local_tail`, this never includes a pending wave.
    durable_tail: Arc<AtomicU64>,

    /// Publishes raw byte ranges at the group-commit wave barrier. It is
    /// dormant (one subscriber-count check per wave) unless a Tail session is
    /// attached.
    replication: Arc<WavePublisher>,

    /// The watermark — the quorum-committed position (next-exclusive). THE
    /// bound for everything externally visible: reads, subscriptions,
    /// consistency markers, and acks. On a single node the quorum is one, so
    /// the watermark tracks the local durable cursor; under native replication
    /// it advances when a quorum of durable cursors passes a position. Split from `local_tail` so no client can
    /// ever observe an event that a leader change could truncate.
    watermark: Arc<WatermarkState>,

    /// Broadcast channel for notifying stream subscribers of new commits.
    commit_tx: broadcast::Sender<CommitNotification>,

    /// The active segment's in-memory index (bitmaps + position→offset
    /// table), maintained incrementally by the writer. Lets source queries
    /// direct-seek into the active segment instead of sequentially scanning
    /// it, without taking the writer lock.
    active_index: Arc<parking_lot::RwLock<SegmentIndex>>,

    /// LRU cache for sealed segment indices, bloom filters, and mmap handles.
    cache: Arc<IndexCache>,

    /// Cached segment list — avoids readdir + stat syscalls on every query.
    /// Updated on rotation within the append path (under writer lock).
    segments: RwLock<SegmentList>,

    /// Cached mmap of the active segment, keyed by its base position. The
    /// active file is preallocated to `max_segment_size` at creation, so one
    /// mapping covers the whole segment lifetime; callers clamp reads to
    /// committed offsets. Rotation changes the base, which retires the entry
    /// by key mismatch — in-flight readers keep their `Arc<Mmap>` and only
    /// ever touch offsets that existed when they resolved positions.
    active_mmap: parking_lot::Mutex<Option<(u64, Arc<memmap2::Mmap>)>>,

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
        let active_index = seg_writer.active_index_handle();
        let (commit_tx, _) = broadcast::channel(COMMIT_CHANNEL_CAPACITY);

        let sync_state = Arc::new(SyncState::new());
        let writer = Arc::new(parking_lot::Mutex::new(seg_writer));
        let local_tail = Arc::new(AtomicU64::new(0));
        let durable_tail = Arc::new(AtomicU64::new(0));
        let replication = WavePublisher::new();
        // Fresh stores begin at watermark zero for any voter topology.
        let voters = if opts.voters.is_empty() {
            vec![opts.node_id]
        } else {
            opts.voters.clone()
        };
        let watermark = Arc::new(WatermarkState::new(INITIAL_EPOCH, voters, 0));

        spawn_sync_thread(
            dir.to_path_buf(),
            Arc::clone(&sync_state),
            Arc::clone(&writer),
            Arc::clone(&local_tail),
            Arc::clone(&durable_tail),
            opts.node_id,
            Arc::clone(&replication),
            Arc::clone(&watermark),
            commit_tx.clone(),
            Duration::from_millis(opts.group_commit_interval_ms),
        );

        Ok(Self {
            dir: dir.to_path_buf(),
            writer,
            tag_index: Arc::new(TagIndex::new()),
            sync_state,
            node_id: opts.node_id,
            local_tail,
            durable_tail,
            replication,
            watermark,
            commit_tx,
            active_index,
            cache: Arc::new(IndexCache::new(
                opts.index_cache_size,
                opts.bloom_cache_size,
            )),
            segments: RwLock::new(SegmentList {
                bases: vec![active_base],
                sealed_count: 0,
            }),
            active_mmap: parking_lot::Mutex::new(None),
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
        let active_index = seg_writer.active_index_handle();

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

        let sync_state = Arc::new(SyncState::new());
        let writer = Arc::new(parking_lot::Mutex::new(seg_writer));
        let local_tail = Arc::new(AtomicU64::new(head_pos));
        let durable_tail = Arc::new(AtomicU64::new(head_pos));
        let replication = WavePublisher::new();
        // A standalone node may expose every recovered durable byte
        // immediately. In a cluster, recovery starts at zero until the
        // control-plane checkpoint/claim re-establishes a quorum; local tail
        // alone is not evidence of cluster commitment.
        let voters = if opts.voters.is_empty() {
            vec![opts.node_id]
        } else {
            opts.voters.clone()
        };
        let (recovered_epoch, checkpoint_floor) = recover_native_control_state(dir, head_pos)?;
        let recovered_watermark = if voters.len() == 1 {
            head_pos
        } else {
            checkpoint_floor
        };
        let watermark = Arc::new(WatermarkState::new(
            recovered_epoch,
            voters,
            recovered_watermark,
        ));

        spawn_sync_thread(
            dir.to_path_buf(),
            Arc::clone(&sync_state),
            Arc::clone(&writer),
            Arc::clone(&local_tail),
            Arc::clone(&durable_tail),
            opts.node_id,
            Arc::clone(&replication),
            Arc::clone(&watermark),
            commit_tx.clone(),
            Duration::from_millis(opts.group_commit_interval_ms),
        );

        Ok(Self {
            dir: dir.to_path_buf(),
            writer,
            tag_index: Arc::new(tag_index),
            sync_state,
            node_id: opts.node_id,
            local_tail,
            durable_tail,
            replication,
            watermark,
            commit_tx,
            active_index,
            cache: Arc::new(IndexCache::new(
                opts.index_cache_size,
                opts.bloom_cache_size,
            )),
            segments: RwLock::new(SegmentList {
                bases: all_bases,
                sealed_count,
            }),
            active_mmap: parking_lot::Mutex::new(None),
            metrics: Arc::new(StoreMetrics::new()),
        })
    }

    /// Returns a shared reference to this engine's metrics counters.
    /// Initiates engine shutdown: new appends are rejected, and the group
    /// commit sync thread performs one final fsync pass (releasing any
    /// in-flight writers) before exiting. Idempotent.
    pub fn shutdown(&self) {
        self.sync_state.begin_shutdown();
    }

    /// Reader over the active segment, backed by a cached mmap. The mapping
    /// is created once per active segment (the file is preallocated to its
    /// full size, so it never needs remapping) and retired when rotation
    /// changes the base position.
    fn active_segment_reader(&self, base: u64, path: &Path) -> Result<SegmentReader, Error> {
        {
            let cached = self.active_mmap.lock();
            if let Some((cached_base, mmap)) = cached.as_ref()
                && *cached_base == base
            {
                return SegmentReader::from_shared_mmap(Arc::clone(mmap));
            }
        }
        let file = std::fs::File::open(path)?;
        let mmap = Arc::new(unsafe { memmap2::Mmap::map(&file)? });
        *self.active_mmap.lock() = Some((base, Arc::clone(&mmap)));
        SegmentReader::from_shared_mmap(mmap)
    }

    pub fn metrics(&self) -> &Arc<StoreMetrics> {
        &self.metrics
    }

    /// Subscribes to raw records published at future wave seals. Tail service
    /// callers must first source the on-disk catch-up range and only then
    /// switch to this receiver; its lag error is a reconnect-from-cursor
    /// signal, never permission to skip bytes.
    pub fn subscribe_replication(
        &self,
    ) -> tokio::sync::broadcast::Receiver<crate::replication::dispatcher::LiveFrame> {
        self.replication.subscribe()
    }

    /// Local (possibly uncommitted) next-exclusive segment cursor.
    pub fn local_tail(&self) -> Position {
        Position(self.local_tail.load(Ordering::Acquire))
    }

    /// Local next-exclusive cursor that has completed fdatasync.
    pub fn durable_tail(&self) -> Position {
        Position(self.durable_tail.load(Ordering::Acquire))
    }

    /// Waits for all bytes currently admitted to the local writer to complete
    /// their group-commit wave. The native write gate must be closed when this
    /// is used as a catch-up/truncation barrier.
    pub fn drain_pending(&self) -> Result<(), Error> {
        let target = {
            let _writer = self.writer.lock();
            let wave = self.sync_state.wave.load(Ordering::Acquire);
            if self.sync_state.has_pending() {
                wave
            } else {
                wave.saturating_sub(1)
            }
        };
        self.sync_state.wait_for_sync(target)
    }

    pub fn replication_cursor(&self) -> Result<ReplicationCursor, Error> {
        self.drain_pending()?;
        let writer = self.writer.lock();
        let path = writer.active_segment_path();
        let byte_offset = writer.write_offset();
        Ok(ReplicationCursor {
            position: writer.head(),
            segment_base: writer.active_base_position(),
            byte_offset,
            last_record_crc: last_physical_record_crc(&path, byte_offset)?,
        })
    }

    pub fn verify_replication_probe(
        &self,
        segment_base: u64,
        byte_offset: u64,
        expected_crc: u32,
    ) -> Result<bool, Error> {
        let path = segment::segment_path(&self.dir, segment_base);
        if !path.exists() || byte_offset < segment::SEGMENT_HEADER_SIZE as u64 {
            return Ok(false);
        }
        let valid_end = {
            let writer = self.writer.lock();
            if writer.active_base_position() == segment_base {
                writer.write_offset()
            } else {
                std::fs::metadata(&path)?.len()
            }
        };
        if byte_offset > valid_end {
            return Ok(false);
        }
        Ok(last_physical_record_crc(&path, byte_offset)? == expected_crc)
    }

    pub fn replication_epoch(&self) -> u64 {
        self.watermark.epoch()
    }

    /// Sources a suffix from an exact verified physical cursor. Unlike the
    /// position-only fallback, this includes control records written at an
    /// unchanged event position (notably EpochChange).
    pub fn replication_catchup_from_cursor(
        &self,
        cursor: ReplicationCursor,
    ) -> Result<(Position, Vec<WaveSlice>), Error> {
        let writer = self.writer.lock();
        let local_tail = self.local_tail.load(Ordering::Acquire);
        if cursor.position.0 > local_tail {
            return Err(Error::Corrupted {
                message: format!(
                    "replication cursor {} is beyond local tail {local_tail}",
                    cursor.position.0
                ),
            });
        }
        let active_base = writer.active_base_position();
        let active_end = writer.write_offset();
        let seg_list = self.segments.read().clone();
        drop(writer);
        let first_index = seg_list
            .bases
            .binary_search(&cursor.segment_base)
            .map_err(|_| Error::Corrupted {
                message: format!(
                    "replication cursor segment {} is not present",
                    cursor.segment_base
                ),
            })?;

        let mut slices = Vec::new();
        for (index, &base) in seg_list.bases.iter().enumerate().skip(first_index) {
            let path = segment::segment_path(&self.dir, base);
            let byte_end = if base == active_base {
                active_end
            } else {
                std::fs::metadata(&path)?.len()
            };
            let byte_start = if index == first_index {
                cursor.byte_offset
            } else {
                segment::SEGMENT_HEADER_SIZE as u64
            };
            if byte_start > byte_end {
                return Err(Error::Corrupted {
                    message: format!(
                        "replication cursor offset {byte_start} exceeds segment {base} end {byte_end}"
                    ),
                });
            }
            if byte_start < byte_end {
                slices.push(WaveSlice {
                    path,
                    segment_base: base,
                    byte_start,
                    byte_end,
                    first_position: if index == first_index {
                        cursor.position.0
                    } else {
                        base
                    },
                    next_position: seg_list.bases.get(index + 1).copied().unwrap_or(local_tail),
                });
            }
        }
        Ok((Position(local_tail), slices))
    }

    /// Builds byte ranges that reproduce the local native segment suffix
    /// beginning at the first event position `from`. Segment headers are
    /// represented by Rotate boundaries, not data.
    pub fn replication_catchup_slices(
        &self,
        from: Position,
    ) -> Result<(Position, Vec<WaveSlice>), Error> {
        // Snapshot tail, active byte end, and segment list under the writer
        // lock in the same lock order as append (writer → segments). This is
        // the catch-up/live handoff boundary.
        let writer = self.writer.lock();
        let local_tail = self.local_tail.load(Ordering::Acquire);
        if from.0 > local_tail {
            return Err(Error::Corrupted {
                message: format!(
                    "replication cursor {} is beyond local tail {}",
                    from.0, local_tail
                ),
            });
        }
        if from.0 == local_tail {
            return Ok((Position(local_tail), Vec::new()));
        }

        let active_base = writer.active_base_position();
        let active_end = writer.write_offset();
        let seg_list = self.segments.read().clone();
        drop(writer);
        let first_index = match seg_list.bases.binary_search(&from.0) {
            Ok(index) => index,
            Err(0) => 0,
            Err(index) => index - 1,
        };

        let mut slices = Vec::new();
        for (index, &base) in seg_list.bases.iter().enumerate().skip(first_index) {
            let path = segment::segment_path(&self.dir, base);
            let byte_end = if base == active_base {
                active_end
            } else {
                std::fs::metadata(&path)?.len()
            };
            let byte_start = if index == first_index {
                find_replication_start(&path, from)?
            } else {
                segment::SEGMENT_HEADER_SIZE as u64
            };
            if byte_start < byte_end {
                slices.push(WaveSlice {
                    path,
                    segment_base: base,
                    byte_start,
                    byte_end,
                    first_position: if index == first_index { from.0 } else { base },
                    next_position: seg_list.bases.get(index + 1).copied().unwrap_or(local_tail),
                });
            }
        }
        Ok((Position(local_tail), slices))
    }

    /// Applies raw Tail bytes to this follower's authoritative segment log.
    /// No DCB evaluation occurs: the claimed leader already serialized the
    /// write under the same epoch. The returned wave must be durable before a
    /// TailAck is sent.
    pub fn apply_replicated_records(
        &self,
        epoch: u64,
        segment_base: u64,
        first_position: Position,
        bytes: &[u8],
    ) -> Result<ReplicatedWrite, Error> {
        if epoch != self.watermark.epoch() {
            return Err(Error::Io(io::Error::other(format!(
                "replication frame epoch {epoch} does not match current epoch {}",
                self.watermark.epoch()
            ))));
        }
        if self.sync_state.is_failed() {
            return Err(self.sync_state.failure_error());
        }
        if self.sync_state.shutdown.load(Ordering::Acquire) {
            return Err(Error::Io(io::Error::other(
                "event store is shutting down; replicated append rejected",
            )));
        }

        let (result, wave) = {
            let mut writer = self.writer.lock();
            if writer.active_base_position() != segment_base {
                return Err(Error::Corrupted {
                    message: format!(
                        "replication frame targets segment {segment_base}, active segment is {}",
                        writer.active_base_position()
                    ),
                });
            }
            let wave = self.sync_state.mark_pending();
            let result = writer.append_raw_replicated(bytes, first_position)?;
            self.local_tail
                .store(result.durable_position.0, Ordering::Release);
            (result, wave)
        };

        for fields in &result.events {
            self.tag_index
                .index_event(fields.position, &fields.name, &fields.tags);
        }

        Ok(ReplicatedWrite {
            wave,
            durable_position: result.durable_position,
            byte_count: bytes.len() as u64,
        })
    }

    /// Waits until a replicated frame's local segment bytes are fdatasync'd.
    pub fn wait_replicated_durable(&self, write: ReplicatedWrite) -> Result<(), Error> {
        self.sync_state.wait_for_sync(write.wave)
    }

    /// Applies an explicit leader-decided rotation. This is the only way a
    /// follower changes segment boundaries, preserving byte-identical files.
    pub fn rotate_replicated(&self, epoch: u64, new_base: Position) -> Result<(), Error> {
        if epoch != self.watermark.epoch() {
            return Err(Error::Io(io::Error::other("stale Rotate epoch")));
        }
        let mut writer = self.writer.lock();
        let old_base = writer.active_base_position();
        if old_base == new_base.0 && writer.head() == new_base {
            return Ok(()); // Idempotent replay after reconnect.
        }
        writer.rotate_replicated(new_base)?;
        let mut seg_list = self.segments.write();
        seg_list.sealed_count += 1;
        seg_list.bases.push(new_base.0);
        self.metrics.record_segment_rotation();
        drop(seg_list);
        drop(writer);
        self.spawn_tag_index_prune(new_base.0);
        Ok(())
    }

    /// Truncates an uncommitted suffix during divergence repair. This is the
    /// sole destructive entry point and will crash rather than cross the
    /// watermark invariant.
    pub fn truncate_to(&self, pos: Position) -> Result<(), Error> {
        self.truncate_to_matching(pos, None)
    }

    /// Truncates to `pos` and, when the leader supplied its byte-exact
    /// boundary, heals trailing divergence that position-granular truncation
    /// cannot see: control records (watermark checkpoints, epoch changes)
    /// written locally by a deposed leader but never replicated sit *below*
    /// the position boundary and would otherwise survive forever, leaving the
    /// node byte-divergent from the cluster.
    ///
    /// `expected_prev` is the leader's view of the record immediately before
    /// the truncation boundary: `None` = no boundary info (legacy behavior),
    /// `Some(None)` = the boundary is the segment start, `Some(Some(crc))` =
    /// the CRC of the leader's preceding record. Mismatching trailing
    /// *control* records are dropped — they carry no client data and no
    /// position, so the watermark invariant (no committed event is ever lost)
    /// holds even though bytes below the position boundary are removed. A
    /// mismatching *event* record means the committed prefix itself diverges,
    /// which is unrecoverable here and fails loudly.
    pub fn truncate_to_matching(
        &self,
        pos: Position,
        expected_prev: Option<Option<u32>>,
    ) -> Result<(), Error> {
        self.drain_pending()?;
        let watermark = self.watermark.get();
        assert!(
            pos.0 >= watermark,
            "refusing to truncate committed data: target {} < watermark {}",
            pos.0,
            watermark
        );
        let old_tail = self.local_tail.load(Ordering::Acquire);
        if pos.0 > old_tail {
            return Err(Error::Corrupted {
                message: format!("truncate target {} exceeds local tail {old_tail}", pos.0),
            });
        }
        if pos.0 == old_tail && expected_prev.is_none() {
            return Ok(());
        }

        let mut writer = self.writer.lock();
        let mut seg_list = self.segments.write();
        let target_index = match seg_list.bases.binary_search(&pos.0) {
            Ok(index) => index,
            Err(0) => {
                return Err(Error::Corrupted {
                    message: format!("no segment contains truncate position {}", pos.0),
                });
            }
            Err(index) => index - 1,
        };
        let target_base = seg_list.bases[target_index];
        let target_path = segment::segment_path(&self.dir, target_base);
        let (position_offset, mut preceding) = scan_replication_boundary(&target_path, pos)?;
        let mut truncate_offset = position_offset;

        // Byte-exact healing: walk backward over trailing control records
        // until this node's boundary agrees with the leader's.
        if let Some(expected) = expected_prev {
            loop {
                match (expected, preceding.last().copied()) {
                    (None, None) if target_index == 0 => break,
                    (Some(crc), Some(last)) if last.crc == crc => break,
                    (_, Some(last)) if !last.is_event => {
                        truncate_offset = last.start_offset;
                        preceding.pop();
                    }
                    (_, None) => {
                        // The boundary reached this segment's start; the true
                        // preceding record is the last record of the prior
                        // segment (comparison only — healing across a sealed
                        // segment edge is not supported and fails loudly).
                        let prev_last = if target_index == 0 {
                            None
                        } else {
                            let prev_path =
                                segment::segment_path(&self.dir, seg_list.bases[target_index - 1]);
                            scan_replication_boundary(&prev_path, Position(u64::MAX))?
                                .1
                                .pop()
                        };
                        match (expected, prev_last) {
                            (None, None) => break,
                            (Some(crc), Some(last)) if last.crc == crc => break,
                            (_, Some(last)) if !last.is_event => {
                                return Err(Error::Corrupted {
                                    message: format!(
                                        "trailing control record at the end of sealed \
                                         segment {} diverges from the leader; sealed-\
                                         segment healing is not supported — full resync \
                                         required",
                                        seg_list.bases[target_index - 1]
                                    ),
                                });
                            }
                            _ => {
                                return Err(Error::Corrupted {
                                    message: format!(
                                        "committed prefix diverges from leader at \
                                         position {} (segment base {target_base}) — the \
                                         record before the boundary does not match the \
                                         leader's history",
                                        pos.0
                                    ),
                                });
                            }
                        }
                    }
                    (None, Some(_)) | (Some(_), Some(_)) => {
                        return Err(Error::Corrupted {
                            message: format!(
                                "committed prefix diverges from leader at position {} \
                                 (segment base {target_base}) — an event record below the \
                                 watermark does not match the leader's history",
                                pos.0
                            ),
                        });
                    }
                }
            }
            if pos.0 == old_tail && truncate_offset == position_offset {
                // Boundary already byte-identical: nothing to heal.
                return Ok(());
            }
        }

        for &base in &seg_list.bases[target_index + 1..] {
            let path = segment::segment_path(&self.dir, base);
            remove_if_exists(&path)?;
            remove_if_exists(&path.with_extension("idx"))?;
            remove_if_exists(&path.with_extension("bloom"))?;
            self.cache.invalidate(base);
        }
        // The containing segment becomes active; its sealed companions no
        // longer describe the truncated file.
        remove_if_exists(&target_path.with_extension("idx"))?;
        remove_if_exists(&target_path.with_extension("bloom"))?;
        self.cache.invalidate(target_base);

        writer.reopen_truncated(target_base, truncate_offset, pos)?;
        seg_list.bases.truncate(target_index + 1);
        seg_list.sealed_count = target_index;
        self.local_tail.store(pos.0, Ordering::Release);
        self.durable_tail.store(pos.0, Ordering::Release);
        // The reopened segment may keep its base position, so the cached
        // active mapping cannot be trusted across a truncation — drop it.
        *self.active_mmap.lock() = None;
        drop(seg_list);
        drop(writer);

        self.tag_index.prune_from(pos.0);
        // If truncation reopened a previously sealed segment, restore its
        // retained prefix to the active TagIndex.
        rebuild_active_segment_index(&self.dir, &self.tag_index)?;
        Ok(())
    }

    /// The leader's view of the record immediately before position `pos` in
    /// its own byte layout: `None` when the boundary is the segment start,
    /// otherwise the stored CRC of the preceding record. Sent with Truncate
    /// frames so followers can verify — and heal — their boundary byte-exactly.
    pub fn replication_boundary_prev(&self, pos: Position) -> Result<Option<u32>, Error> {
        let (target_index, bases) = {
            let seg_list = self.segments.read();
            let index = match seg_list.bases.binary_search(&pos.0) {
                Ok(index) => index,
                Err(0) => {
                    return Err(Error::Corrupted {
                        message: format!("no segment contains boundary position {}", pos.0),
                    });
                }
                Err(index) => index - 1,
            };
            (index, seg_list.bases.clone())
        };
        let path = segment::segment_path(&self.dir, bases[target_index]);
        let (_, preceding) = scan_replication_boundary(&path, pos)?;
        if let Some(record) = preceding.last() {
            return Ok(Some(record.crc));
        }
        // Boundary at a segment start (e.g. the leader sealed at its claim):
        // the true preceding record is the last record of the prior segment.
        // Only the very first segment has no predecessor at all.
        if target_index == 0 {
            return Ok(None);
        }
        let prev_path = segment::segment_path(&self.dir, bases[target_index - 1]);
        let (_, prev_records) = scan_replication_boundary(&prev_path, Position(u64::MAX))?;
        Ok(prev_records.last().map(|record| record.crc))
    }

    /// Sealed segments fully covered by the quorum watermark — the archival
    /// units for tiered storage (ADR-0002). Such segments are immutable:
    /// failover truncation never reaches below the watermark, so their bytes
    /// are final on every node.
    pub fn archivable_segments(&self) -> Vec<ArchivableSegment> {
        let watermark = self.watermark.get();
        let seg_list = self.segments.read();
        let bases = &seg_list.bases;
        let mut out = Vec::new();
        // The last base is the active segment; every earlier one is sealed.
        for window in bases.windows(2) {
            let (base, end) = (window[0], window[1]);
            if end <= watermark {
                out.push(ArchivableSegment {
                    base,
                    end,
                    path: segment::segment_path(&self.dir, base),
                });
            }
        }
        out
    }

    /// Installs a claimed epoch and exact voter set, aborting all prior-epoch
    /// append waiters. The caller must durably append the leader's EpochChange
    /// before opening its write gate.
    pub fn begin_replication_epoch(
        &self,
        epoch: u64,
        voters: impl IntoIterator<Item = u64>,
    ) -> Result<(), Error> {
        self.watermark.begin_epoch(epoch, voters)
    }

    /// Writes and durably commits the EpochChange record at a hard segment
    /// boundary. Called only by the newly claimed leader before accepting
    /// native writes.
    pub fn persist_epoch_change(&self, epoch: u64, leader_id: u64) -> Result<(), Error> {
        if epoch != self.watermark.epoch() || leader_id != self.node_id {
            return Err(Error::Io(io::Error::other(
                "cannot persist an epoch not claimed by this node",
            )));
        }
        let wave = {
            let mut writer = self.writer.lock();
            if writer.has_records() {
                let new_base = writer.head();
                writer.rotate_replicated(new_base)?;
                let mut seg_list = self.segments.write();
                seg_list.sealed_count += 1;
                seg_list.bases.push(new_base.0);
                self.metrics.record_segment_rotation();
            }
            let wave = self.sync_state.mark_pending();
            let start_position = writer.head().0;
            writer.write_control(&crate::segment::format::ControlRecord::EpochChange {
                epoch,
                leader_id,
                start_position,
            })?;
            wave
        };
        self.sync_state.wait_for_sync(wave)
    }

    /// Persists a coarse committed-watermark floor in the authoritative segment
    /// log. The control record enters the normal group-commit wave, so live
    /// followers receive and fdatasync the exact same bytes.
    pub fn persist_watermark_checkpoint(&self) -> Result<(), Error> {
        let epoch = self.watermark.epoch();
        let position = self.watermark.get();
        let wave = {
            let mut writer = self.writer.lock();
            let wave = self.sync_state.mark_pending();
            writer.write_control(
                &crate::segment::format::ControlRecord::WatermarkCheckpoint { epoch, position },
            )?;
            wave
        };
        self.sync_state.wait_for_sync(wave)
    }

    /// Waits until the current epoch's quorum watermark reaches `pos`.
    pub fn wait_for_watermark(&self, pos: Position) -> Result<(), Error> {
        self.watermark.wait_for(pos.0)
    }

    /// Leader-side durable cursor acknowledgement. If quorum moves, wakes
    /// append waiters and publishes the new externally-visible bound.
    pub fn acknowledge_replica(&self, node: u64, epoch: u64, pos: Position) -> Option<Position> {
        let watermark = self.watermark.advance(node, epoch, pos.0)?;
        let _ = self.commit_tx.send(CommitNotification { watermark });
        Some(Position(watermark))
    }

    /// Follower-side adoption of the claimed leader's computed watermark.
    pub fn adopt_watermark(&self, epoch: u64, pos: Position) {
        if pos.0 > self.local_tail.load(Ordering::Acquire) {
            return; // Never expose bytes this follower has not applied.
        }
        if let Some(watermark) = self.watermark.adopt(epoch, pos.0) {
            let _ = self.commit_tx.send(CommitNotification { watermark });
        }
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
    ///
    /// Blocks until the write is quorum-durable. The gRPC path uses
    /// `append_stage` + `append_finish_async` instead so no thread is pinned
    /// during the wait.
    pub fn append(&self, request: AppendRequest) -> Result<AppendResponse, Error> {
        let staged = self.append_stage(request)?;
        let wait = match staged.wait_pos() {
            Some(pos) => self.watermark.wait_for(pos),
            None => Ok(()),
        };
        self.append_finalize(staged, wait)
    }

    /// The synchronous half of an append: fail-fast checks plus the atomic
    /// writer-lock section (DCB check, write, index). On return the write —
    /// or the DCB verdict justifying its rejection — is registered in the
    /// current commit wave, but durability has NOT been awaited. Finish with
    /// `append_finish_async` (or the blocking `append` wrapper).
    pub fn append_stage(&self, request: AppendRequest) -> Result<StagedAppend, Error> {
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

        let outcome = {
            // Lock the writer. DCB check + write + index update must be atomic.
            let mut writer = self.writer.lock();
            self.sync_state.mark_pending();
            self.append_locked(&mut writer, &request)
        };

        let staged = StagedAppend { outcome, timer };
        if crate::relaxed_acks()
            && let Some(pos) = staged.wait_pos()
            && pos.saturating_sub(self.durable_tail.load(Ordering::Acquire))
                <= crate::ack_lag_limit()
        {
            // Replicated-ack fast path: this node's cursor advances at write
            // (bytes are already pread-visible — the writer write_alls into
            // the file). Quorum-of-one opens the ack gate inline; with more
            // voters the follower cursors still gate the quorum math. Past
            // the lag limit the advance is skipped and release waits on the
            // sync loop — durable-mode behavior until the disk catches up.
            let _ = self
                .watermark
                .advance(self.node_id, self.watermark.epoch(), pos);
        }
        Ok(staged)
    }

    /// Awaits a staged append's durability without pinning a thread, then
    /// resolves it exactly like the blocking path.
    pub async fn append_finish_async(&self, staged: StagedAppend) -> Result<AppendResponse, Error> {
        let wait = match staged.wait_pos() {
            Some(pos) => self.watermark.wait_for_async(pos).await,
            None => Ok(()),
        };
        self.append_finalize(staged, wait)
    }

    /// Applies the ack gate's verdict to a staged outcome. A poisoned wait
    /// (fsync failure, epoch loss, shutdown) overrides everything — the
    /// caller must never treat the write as durable, and a DCB rejection may
    /// not be exposed until the event justifying it is quorum-committed.
    fn append_finalize(
        &self,
        staged: StagedAppend,
        wait: Result<(), Error>,
    ) -> Result<AppendResponse, Error> {
        wait?;
        match staged.outcome {
            Ok(response) => {
                self.metrics
                    .record_append(response.count, staged.timer.elapsed_us());
                Ok(response)
            }
            Err(error) => Err(error),
        }
    }

    /// Prunes sealed-segment positions from the in-memory tag index on a
    /// background thread. Runs after rotation, once the sealed segment's
    /// `.idx`/`.bloom` are durable (rotation writes them synchronously), so
    /// every pruned position is resolvable through the sealed-segment
    /// indexes. Off-thread because a prune walks every tag bitmap — doing
    /// that under the writer lock would reintroduce a seal-time stall.
    fn spawn_tag_index_prune(&self, base: u64) {
        let tag_index = Arc::clone(&self.tag_index);
        std::thread::Builder::new()
            .name("kronos-tagindex-prune".into())
            .spawn(move || tag_index.prune_below(base))
            .ok();
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
    /// the sync thread's job after the wave seals.
    fn append_locked(
        &self,
        writer: &mut SegmentWriter,
        request: &AppendRequest,
    ) -> Result<AppendResponse, Error> {
        {
            // Step 1: Check DCB condition.
            if let Some(condition) = &request.condition
                && let Some(conflicting_pos) = self.check_dcb_locked(condition)?
            {
                return Err(Error::ConsistencyConditionViolated {
                    conflicting_position: conflicting_pos,
                });
            }

            if request.events.is_empty() {
                let head = writer.head();
                return Ok(AppendResponse {
                    first_position: head,
                    count: 0,
                    consistency_marker: Position(self.local_tail.load(Ordering::Acquire)),
                });
            }

            let old_active_base = writer.active_base_position();

            // Step 2: Write events without fsync. Durability is the sync
            // thread's wave fsync; callers wait on the watermark.
            let (first_position, count) = writer.write_events(&request.events)?;

            // Step 2b: Detect rotation and update cached segment list.
            let new_active_base = writer.active_base_position();
            if new_active_base != old_active_base {
                let mut seg_list = self.segments.write();
                seg_list.sealed_count += 1;
                seg_list.bases.push(new_active_base);
                self.metrics.record_segment_rotation();
                drop(seg_list);
                self.spawn_tag_index_prune(new_active_base);
            }

            // Step 3: Update in-memory tag index.
            // TagIndex is internally sharded — concurrent callers indexing events
            // with different tag keys proceed in parallel.
            let mut pos = first_position;
            for event in &request.events {
                self.tag_index.index_event(pos, &event.name, &event.tags);
                pos = pos.next();
            }

            // Step 4: Advance the local tail (next-exclusive: first event's
            // position + count = position the next event will land at).
            // Watermark publication — and with it the subscriber wakeup —
            // happens on the sync thread after the wave's fsync.
            let new_head = first_position.0 + count as u64;
            self.local_tail.store(new_head, Ordering::Release);

            Ok(AppendResponse {
                first_position,
                count,
                consistency_marker: Position(new_head),
            })
        }
    }

    /// Gets tags for an event at the given position by reading from the segment.
    pub fn get_tags(&self, position: Position) -> Result<Vec<Tag>, Error> {
        let head = self.watermark.get();
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
            self.active_segment_reader(base, &seg_path)?
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
        let head = self.watermark.get();
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
                self.active_segment_reader(base, &seg_path)?
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

    /// Creates a live event stream subscription. Delivery is bounded by the
    /// watermark — subscribers only ever see quorum-committed events.
    pub fn subscribe(&self, from_position: Position, condition: SourcingCondition) -> EventStream {
        EventStream::new(
            condition,
            from_position,
            self.commit_tx.subscribe(),
            self.watermark.handle(),
        )
    }

    /// Returns the current head position (next position to be assigned;
    /// equivalently, the count of events committed). This is the watermark —
    /// the externally visible, quorum-committed head.
    ///
    /// Reads an atomic — no writer lock, so read-path callers (Source/
    /// GetHead) never contend with appends or the group commit sync thread.
    /// Mid-batch writes are not visible here until the batch commits, which
    /// is exactly the externally-observable head.
    pub fn head(&self) -> Position {
        Position(self.watermark.get())
    }

    /// Returns the tail position (first available event position).
    ///
    /// For an empty store, returns the same as `head()` so that `head - tail == 0`.
    /// For a non-empty, non-truncated store, returns `Position(0)` — the
    /// position of the first event, since the log is 0-based.
    pub fn tail(&self) -> Position {
        // Authoritative event segments are never truncated.
        Position(0)
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
        let head = self.watermark.get();

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
                // Active segment — use the in-memory tag index, clamped to
                // this segment's position range. The tag index holds every
                // position since boot, so without the clamp a tag whose
                // matches are all in sealed segments would still produce a
                // non-empty bitmap here and force needless work.
                let clamped = Position(from_position.0.max(base));
                (self.tag_index.matching_bitmap(condition, clamped), None)
            };

            let matching_positions = match matching_positions {
                Some(bm) => bm,
                None => continue, // No matches in this segment.
            };

            // Sealed segments come from the LRU cache; the active segment from
            // its own cached mapping.
            let reader = if seg_list.is_sealed(i) {
                let mmap = self.cache.get_mmap(&seg_path, base)?;
                SegmentReader::from_shared_mmap(mmap)?
            } else {
                self.active_segment_reader(base, &seg_path)?
            };

            if let Some(idx) = &seg_index {
                // Sealed segment: direct seek via offset table — O(K) matching events.
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
                // Active segment: direct seek via the incrementally-built
                // in-memory index. Falls back to a sequential scan when the
                // index doesn't cover this segment (rotation raced the
                // segment-list snapshot, or an unindexed sealed segment
                // after crash recovery).
                let active_idx = self.active_index.read();
                if active_idx.base_position() == base {
                    self.metrics.record_direct_seek();
                    for pos in matching_positions.iter() {
                        if pos < from_position.0 || pos >= head {
                            continue;
                        }
                        if let Some(offset) = active_idx.get_offset(pos) {
                            let stored = reader.read_event_at(offset as usize)?;
                            events.push(stored.into_sequenced());
                        }
                    }
                } else {
                    drop(active_idx);
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
        self.source_stored_bounded(from_position, condition, None, limit)
    }

    /// Reads up to `limit` matching events with position in
    /// `[from_position, up_to)`. Chunked-streaming building block: callers
    /// freeze `up_to` at the head once, then advance `from_position` past the
    /// last returned event — memory stays bounded by `limit` and dropping the
    /// caller stops the work between pages.
    pub fn source_page(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
        up_to: Position,
        limit: usize,
    ) -> Result<Vec<SequencedEvent>, Error> {
        let stored = self.source_stored_bounded(from_position, condition, Some(up_to), limit)?;
        Ok(stored.into_iter().map(|e| e.into_sequenced()).collect())
    }

    /// Bounded variant of `source_stored`: up to `limit` matching events with
    /// position in `[from_position, min(head, up_to))`, tags included.
    /// pub(crate) for the Raft snapshot builder, which pages the whole store
    /// through this with a frozen `up_to` so builds are consistent with the
    /// snapshot's `last_applied` and memory stays bounded.
    pub(crate) fn source_stored_bounded(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
        up_to: Option<Position>,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, Error> {
        let head = self.watermark.get();
        let head = match up_to {
            Some(bound) => head.min(bound.0),
            None => head,
        };

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
                let mut bm = idx.matching(condition);
                // Drop already-consumed positions up front so chunked callers
                // (cursor advancing through the log) don't re-skip the prefix
                // one position at a time on every call.
                if from_position.0 > base
                    && let Some(bm) = &mut bm
                {
                    bm.remove_range(0..from_position.0);
                }
                (bm.filter(|bm| !bm.is_empty()), Some(idx))
            } else {
                // Active segment — in-memory tag index, clamped to this
                // segment's range (see `source` for rationale).
                let clamped = Position(from_position.0.max(base));
                (self.tag_index.matching_bitmap(condition, clamped), None)
            };

            let matching_positions = match matching_positions {
                Some(bm) => bm,
                None => continue,
            };

            let reader = if seg_list.is_sealed(i) {
                let mmap = self.cache.get_mmap(&seg_path, base)?;
                SegmentReader::from_shared_mmap(mmap)?
            } else {
                self.active_segment_reader(base, &seg_path)?
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
                let active_idx = self.active_index.read();
                if active_idx.base_position() == base {
                    for pos in matching_positions.iter() {
                        if pos < from_position.0 || pos >= head {
                            continue;
                        }
                        if let Some(offset) = active_idx.get_offset(pos) {
                            let stored = reader.read_event_at(offset as usize)?;
                            events.push(stored);
                            if events.len() >= limit {
                                return Ok(events);
                            }
                        }
                    }
                } else {
                    drop(active_idx);
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
        }

        Ok(events)
    }
}

#[async_trait::async_trait]
impl EventStore for EventStoreEngine {
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, Error> {
        // Delegates to the inherent synchronous native append method.
        self.append(request)
    }

    fn source(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
    ) -> Result<Vec<SequencedEvent>, Error> {
        self.source(from_position, condition)
    }

    fn source_page(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
        up_to: Position,
        limit: usize,
    ) -> Result<Vec<SequencedEvent>, Error> {
        self.source_page(from_position, condition, up_to, limit)
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

fn remove_if_exists(path: &Path) -> Result<(), Error> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
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

fn recover_native_control_state(dir: &Path, local_tail: u64) -> Result<(u64, u64), Error> {
    use std::io::Read;

    let mut recovered_epoch = INITIAL_EPOCH;
    let mut checkpoint = 0u64;
    for base in segment::list_segment_files(dir)? {
        let path = segment::segment_path(dir, base);
        let mut file = std::fs::File::open(&path)?;
        let mut segment_header = [0u8; segment::SEGMENT_HEADER_SIZE];
        file.read_exact(&mut segment_header)?;
        loop {
            let mut header = [0u8; segment::RECORD_HEADER_SIZE];
            match file.read_exact(&mut header) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(error) => return Err(error.into()),
            }
            let header = match crate::segment::record::parse_header(&header)? {
                Some(header) => header,
                None => break,
            };
            let mut payload = vec![0u8; header.payload_len];
            if let Err(error) = file.read_exact(&mut payload) {
                if error.kind() == io::ErrorKind::UnexpectedEof {
                    break;
                }
                return Err(error.into());
            }
            if !crate::segment::record::validate_crc(header, &payload) {
                break;
            }
            let crate::segment::record::NativeRecord::Control(control) =
                crate::segment::record::decode_native(header, &payload)?
            else {
                continue;
            };
            match control {
                crate::segment::format::ControlRecord::EpochChange { epoch, .. } => {
                    recovered_epoch = recovered_epoch.max(epoch);
                }
                crate::segment::format::ControlRecord::WatermarkCheckpoint { epoch, position }
                    if position <= local_tail =>
                {
                    checkpoint = checkpoint.max(position);
                    recovered_epoch = recovered_epoch.max(epoch);
                }
                crate::segment::format::ControlRecord::WatermarkCheckpoint { .. } => {}
            }
        }
    }
    Ok((recovered_epoch, checkpoint))
}

fn last_physical_record_crc(path: &Path, byte_end: u64) -> Result<u32, Error> {
    use std::io::{Read, Seek, SeekFrom};

    if byte_end <= segment::SEGMENT_HEADER_SIZE as u64 {
        return Ok(0);
    }
    let mut file = std::fs::File::open(path)?;
    file.seek(SeekFrom::Start(segment::SEGMENT_HEADER_SIZE as u64))?;
    let mut offset = segment::SEGMENT_HEADER_SIZE as u64;
    let mut last_crc = 0;
    while offset < byte_end {
        let mut header = [0u8; segment::RECORD_HEADER_SIZE];
        file.read_exact(&mut header)?;
        let header =
            crate::segment::record::parse_header(&header)?.ok_or_else(|| Error::Corrupted {
                message: format!("zero record inside physical prefix in {}", path.display()),
            })?;
        if offset + header.total_len() as u64 > byte_end {
            return Err(Error::Corrupted {
                message: format!("invalid physical record boundary in {}", path.display()),
            });
        }
        last_crc = header.stored_crc;
        file.seek(SeekFrom::Current(header.payload_len as i64))?;
        offset += header.total_len() as u64;
    }
    Ok(last_crc)
}

/// Finds the raw native record offset from which a follower at `from` resumes.
/// Control records before the requested event position remain behind the
/// cursor; control records at the current physical cursor are preserved by the
/// exact `(segment_base, byte_offset, crc)` resume path.
/// One sealed, watermark-covered segment eligible for archival (ADR-0002).
#[derive(Debug, Clone)]
pub struct ArchivableSegment {
    /// First position in the segment (also its filename base).
    pub base: u64,
    /// End position (exclusive) — the next segment's base.
    pub end: u64,
    /// Local path of the `.seg` file.
    pub path: PathBuf,
}

/// One physical record preceding a replication boundary, retained so
/// byte-exact truncation can walk trailing control records backward.
#[derive(Clone, Copy)]
struct BoundaryRecord {
    start_offset: u64,
    crc: u32,
    is_event: bool,
}

fn find_replication_start(path: &Path, from: Position) -> Result<u64, Error> {
    Ok(scan_replication_boundary(path, from)?.0)
}

/// Locates the byte offset where position `from` starts and returns every
/// valid record before that boundary (offset, stored CRC, kind). The boundary
/// is the first event record with position >= `from`; when no such event
/// exists the boundary is the end of the valid prefix — trailing control
/// records are then part of `preceding`, which is exactly what byte-exact
/// truncation needs to heal them.
fn scan_replication_boundary(
    path: &Path,
    from: Position,
) -> Result<(u64, Vec<BoundaryRecord>), Error> {
    use std::io::Read;

    let mut file = std::fs::File::open(path)?;
    let mut segment_header = [0u8; segment::SEGMENT_HEADER_SIZE];
    file.read_exact(&mut segment_header)?;
    let base = u64::from_le_bytes(segment_header[5..13].try_into().unwrap());
    if from.0 <= base {
        return Ok((segment::SEGMENT_HEADER_SIZE as u64, Vec::new()));
    }

    let mut offset = segment::SEGMENT_HEADER_SIZE as u64;
    let mut preceding = Vec::new();
    loop {
        let mut header = [0u8; segment::RECORD_HEADER_SIZE];
        match file.read_exact(&mut header) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok((offset, preceding));
            }
            Err(error) => return Err(error.into()),
        }
        let header = match crate::segment::record::parse_header(&header)? {
            Some(header) => header,
            None => return Ok((offset, preceding)),
        };
        let record_end = offset + header.total_len() as u64;
        let mut payload = vec![0u8; header.payload_len];
        file.read_exact(&mut payload)?;
        if !crate::segment::record::validate_crc(header, &payload) {
            return Err(Error::Corrupted {
                message: format!("CRC mismatch in replication source at byte {offset}"),
            });
        }
        let is_event = match crate::segment::record::decode_native(header, &payload)? {
            crate::segment::record::NativeRecord::Event { position } => {
                if position >= from.0 {
                    return Ok((offset, preceding));
                }
                true
            }
            crate::segment::record::NativeRecord::Control(_) => false,
        };
        preceding.push(BoundaryRecord {
            start_offset: offset,
            crc: header.stored_crc,
            is_event,
        });
        offset = record_end;
    }
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

    /// After rotation prunes sealed positions from the in-memory tag index,
    /// tag queries and DCB checks must still resolve them via the sealed
    /// segments' `.idx`/`.bloom` files — pruning changes memory footprint,
    /// never answers.
    #[test]
    fn tag_index_prune_preserves_queries_and_dcb() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny segments force several rotations.
        let store = EventStoreEngine::create_with_options(dir.path(), 4 * 1024).unwrap();

        for i in 0..100 {
            store
                .append(AppendRequest {
                    condition: None,
                    events: vec![make_event(
                        "OrderPlaced",
                        vec![tag("orderId", &format!("ord-{i}"))],
                    )],
                })
                .unwrap();
        }
        let sealed = store.segments.read().clone();
        assert!(sealed.sealed_count > 0, "expected at least one rotation");
        let active_base = *sealed.bases.last().unwrap();

        // Deterministic prune (the rotation-spawned threads race the test).
        store.tag_index.prune_below(active_base);

        // Query for an early (sealed-only) tag still finds its event.
        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("orderId", "ord-0")],
            }],
        };
        let events = store.source(Position(0), &cond).unwrap();
        assert_eq!(events.len(), 1, "sealed event must resolve via .idx");

        // DCB against an early tag still detects the conflict.
        let result = store.append(AppendRequest {
            condition: Some(AppendCondition {
                consistency_marker: Position(0),
                criteria: SourcingCondition {
                    criteria: vec![Criterion {
                        names: vec![],
                        tags: vec![tag("orderId", "ord-0")],
                    }],
                },
            }),
            events: vec![make_event("Dup", vec![tag("orderId", "ord-0")])],
        });
        match result {
            Err(Error::ConsistencyConditionViolated { .. }) => {}
            Err(e) => panic!("expected DCB violation, got other error: {e}"),
            Ok(_) => panic!("DCB must still see sealed conflicts after prune"),
        }

        // The index really did shrink: no positions below the active base.
        let all = store
            .tag_index
            .matching_bitmap(&cond_match_all(), Position(0));
        if let Some(bm) = all {
            assert!(bm.min().unwrap_or(u64::MAX) >= active_base);
        }
    }

    fn cond_match_all() -> SourcingCondition {
        SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![],
            }],
        }
    }

    /// A failed fsync must surface as an error to every waiting writer —
    /// never as a silent durability ack — and must poison future appends.
    #[test]
    fn sync_failure_propagates_to_waiters_and_poisons() {
        let state = Arc::new(SyncState::new());

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
        let state = Arc::new(SyncState::new());
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

    /// Group-commit acknowledgements gate on the watermark: an acknowledged
    /// append implies the watermark — and therefore every read path —
    /// covers its events.
    #[test]
    fn group_commit_append_acks_through_watermark() {
        let dir = tempfile::tempdir().unwrap();
        let opts = StoreOptions {
            group_commit_interval_ms: 1,
            ..Default::default()
        };
        let store = EventStoreEngine::create_with_store_options(dir.path(), &opts).unwrap();

        let resp = store
            .append(AppendRequest {
                condition: None,
                events: vec![make_event("OrderPlaced", vec![tag("orderId", "A")])],
            })
            .unwrap();
        assert_eq!(resp.first_position, Position(0));
        assert_eq!(resp.consistency_marker, Position(1));
        // Acked ⇒ quorum-committed ⇒ externally visible.
        assert_eq!(store.head(), Position(1));
        assert_eq!(store.watermark.get(), 1);

        // A condition-only append (zero events) still acks: its wait target
        // is the tail the DCB check read, which the next wave covers.
        let resp = store
            .append(AppendRequest {
                condition: Some(AppendCondition {
                    consistency_marker: Position(1),
                    criteria: cond_match_all(),
                }),
                events: vec![],
            })
            .unwrap();
        assert_eq!(resp.count, 0);
        store.shutdown();
    }

    /// Concurrent group-commit appenders all ack and the watermark converges
    /// on the final tail — no waiter is stranded by wave/watermark races.
    #[test]
    fn group_commit_concurrent_appends_all_ack() {
        let dir = tempfile::tempdir().unwrap();
        let opts = StoreOptions {
            group_commit_interval_ms: 1,
            ..Default::default()
        };
        let store =
            Arc::new(EventStoreEngine::create_with_store_options(dir.path(), &opts).unwrap());

        let threads: Vec<_> = (0..8)
            .map(|t| {
                let store = Arc::clone(&store);
                std::thread::spawn(move || {
                    for i in 0..25 {
                        store
                            .append(AppendRequest {
                                condition: None,
                                events: vec![make_event(
                                    &format!("E-{t}-{i}"),
                                    vec![tag("t", &t.to_string())],
                                )],
                            })
                            .unwrap();
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }

        assert_eq!(store.head(), Position(200));
        assert_eq!(store.watermark.get(), 200);
        store.shutdown();
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
}
