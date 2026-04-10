use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::broadcast;

use crate::api::EventStore;
use crate::append::{AppendRequest, AppendResponse};
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
    epoch: StdMutex<u64>,
    synced: Condvar,
    pending_writes: AtomicU64,
    enabled: bool,
    shutdown: AtomicBool,
}

impl SyncState {
    fn new(enabled: bool) -> Self {
        Self {
            epoch: StdMutex::new(0),
            synced: Condvar::new(),
            pending_writes: AtomicU64::new(0),
            enabled,
            shutdown: AtomicBool::new(false),
        }
    }

    fn mark_pending(&self) -> u64 {
        self.pending_writes.fetch_add(1, Ordering::Relaxed);
        *self.epoch.lock().unwrap() + 1
    }

    fn wait_for_sync(&self, target_epoch: u64) {
        let mut epoch = self.epoch.lock().unwrap();
        while *epoch < target_epoch {
            epoch = self.synced.wait(epoch).unwrap();
        }
    }

    fn complete_sync(&self) {
        self.pending_writes.store(0, Ordering::Relaxed);
        let mut epoch = self.epoch.lock().unwrap();
        *epoch += 1;
        self.synced.notify_all();
    }

    fn has_pending(&self) -> bool {
        self.pending_writes.load(Ordering::Relaxed) > 0
    }
}

fn spawn_sync_thread(
    sync_state: Arc<SyncState>,
    writer: Arc<parking_lot::Mutex<SegmentWriter>>,
    interval: Duration,
) {
    std::thread::Builder::new()
        .name("kronosdb-sync".into())
        .spawn(move || {
            while !sync_state.shutdown.load(Ordering::Relaxed) {
                std::thread::sleep(interval);
                if sync_state.has_pending() {
                    let mut w = writer.lock();
                    let _ = w.sync();
                    drop(w);
                    sync_state.complete_sync();
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

    /// The tag index for the active segment. Protected by RwLock for concurrent read access.
    tag_index: Arc<RwLock<TagIndex>>,

    /// Group commit synchronization.
    sync_state: Arc<SyncState>,

    /// The committed position — the position of the last event visible to readers.
    committed_position: Arc<AtomicU64>,

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
        let seg_writer = SegmentWriter::new(dir, Position(1), opts.max_segment_size)?;
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
            tag_index: Arc::new(RwLock::new(TagIndex::new())),
            sync_state,
            committed_position: Arc::new(AtomicU64::new(0)),
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
        let mut tag_index = TagIndex::new();
        rebuild_active_segment_index(dir, &mut tag_index)?;

        // Build the cached segment list from disk (one-time cost on startup).
        let all_bases = segment::list_segment_files(dir)?;
        let sealed_count = count_sealed_segments(dir, &all_bases, active_base);

        let committed = if head.0 > 0 { head.0 - 1 } else { 0 };
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
            tag_index: Arc::new(RwLock::new(tag_index)),
            sync_state,
            committed_position: Arc::new(AtomicU64::new(committed)),
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
    /// 4. Advances the committed position
    pub fn append(&self, request: AppendRequest) -> Result<AppendResponse, Error> {
        let timer = Timer::start();

        let target_epoch = if self.sync_state.enabled {
            Some(self.sync_state.mark_pending())
        } else {
            None
        };

        let response = {
            // Lock the writer. DCB check + write + index update must be atomic.
            let mut writer = self.writer.lock();

            // Step 1: Check DCB condition.
            if let Some(condition) = &request.condition {
                let marker = condition.consistency_marker.0;

                // Check sealed segments whose events come after the marker.
                // Uses the same bloom → index → bitmap path as source reads.
                let seg_list = self.segments.read().clone();
                for (i, &base) in seg_list.bases.iter().enumerate() {
                    if !seg_list.is_sealed(i) {
                        break; // Active segment checked below via tag index.
                    }
                    // Segment ends before the marker — all its events are older, skip.
                    let seg_end = if i + 1 < seg_list.bases.len() {
                        seg_list.bases[i + 1] - 1
                    } else {
                        continue;
                    };
                    if seg_end <= marker {
                        continue;
                    }

                    let seg_path = segment::segment_path(&self.dir, base);

                    // Bloom filter: skip segment if tag definitely not present.
                    if let Some(false) =
                        self.cache.bloom_check(&seg_path, base, &condition.criteria)
                    {
                        continue;
                    }

                    // Load index and check for any match after the marker.
                    let seg_index = self.cache.get_index(&seg_path, base)?;
                    if let Some(conflicting_pos) =
                        seg_index.has_match_after(&condition.criteria, marker)
                    {
                        self.metrics.record_dcb_violation();
                        return Err(Error::ConsistencyConditionViolated {
                            conflicting_position: conflicting_pos,
                        });
                    }
                }

                // Check the active segment via in-memory tag index.
                let index = self.tag_index.read();
                if let Some(conflicting_pos) = index.check_condition(condition) {
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
                    consistency_marker: Position(self.committed_position.load(Ordering::Acquire)),
                });
            }

            let old_active_base = writer.active_base_position();

            // Step 2: Write events.
            let (first_position, count) = if self.sync_state.enabled {
                // Group commit: write without fsync. Sync thread handles it.
                writer.write_events(&request.events)?
            } else {
                // Immediate: write + fsync in one call.
                writer.append(&request.events)?
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
            {
                let mut index = self.tag_index.write();
                let mut pos = first_position;
                for event in &request.events {
                    index.index_event(pos, &event.name, &event.tags);
                    pos = pos.next();
                }
            }

            // Step 4: Advance committed position.
            let last_position = Position(first_position.0 + count as u64 - 1);
            self.committed_position
                .store(last_position.0, Ordering::Release);

            // Step 5: Notify stream subscribers.
            let _ = self.commit_tx.send(CommitNotification {
                committed_position: last_position.0,
            });

            AppendResponse {
                first_position,
                count,
                consistency_marker: last_position,
            }
            // Writer lock released here — other appends can proceed
            // and batch into the same upcoming fsync.
        };

        // Step 6: Wait for fsync (group commit only).
        if let Some(epoch) = target_epoch {
            self.sync_state.wait_for_sync(epoch);
        }

        self.metrics
            .record_append(response.count, timer.elapsed_us());
        Ok(response)
    }

    /// Gets tags for an event at the given position by reading from the segment.
    pub fn get_tags(&self, position: Position) -> Result<Vec<Tag>, Error> {
        let committed = self.committed_position.load(Ordering::Acquire);
        if position.0 > committed || position.0 == 0 {
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
        let committed = self.committed_position.load(Ordering::Acquire);
        if committed == 0 {
            return Ok(None);
        }

        let seg_list = self.segments.read().clone();
        if seg_list.bases.is_empty() {
            return Ok(None);
        }

        for (i, &base) in seg_list.bases.iter().enumerate() {
            if base > committed {
                break;
            }

            let seg_path = segment::segment_path(&self.dir, base);

            let reader = if seg_list.is_sealed(i) {
                let mmap = self.cache.get_mmap(&seg_path, base)?;
                SegmentReader::from_shared_mmap(mmap)?
            } else {
                SegmentReader::open(&seg_path)?
            };

            for result in reader.iter(Some(Position(committed + 1))) {
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
            Arc::clone(&self.committed_position),
        )
    }

    /// Returns the current head position (next position to be assigned).
    pub fn head(&self) -> Position {
        self.writer.lock().head()
    }

    /// Returns the tail position (first event in the store).
    ///
    /// For an empty store, returns the same as `head()` so that `head - tail == 0`.
    /// For a non-empty, non-truncated store, returns `Position(1)` (events are 1-based).
    pub fn tail(&self) -> Position {
        let committed = self.committed_position.load(Ordering::Acquire);
        if committed == 0 {
            self.writer.lock().head() // Empty: tail == head → 0 events.
        } else {
            Position(1) // TODO: Track actual tail for truncated stores.
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
        let committed = self.committed_position.load(Ordering::Acquire);

        // Read cached segment list — no readdir syscall.
        let seg_list = self.segments.read().clone();
        if seg_list.bases.is_empty() {
            return Ok(vec![]);
        }

        let mut events = Vec::new();

        for (i, &base) in seg_list.bases.iter().enumerate() {
            let seg_path = segment::segment_path(&self.dir, base);
            let is_last = i + 1 == seg_list.bases.len();
            let seg_end = if !is_last {
                seg_list.bases[i + 1] - 1
            } else {
                committed
            };

            if base > committed {
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
                let index = self.tag_index.read();
                (index.matching_bitmap(condition, from_position), None)
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
                    if pos < from_position.0 || pos > committed {
                        continue;
                    }
                    if let Some(offset) = idx.get_offset(pos) {
                        let stored = reader.read_event_at(offset as usize)?;
                        events.push(stored.into_sequenced());
                    }
                }
            } else {
                self.metrics.record_sequential_scan();
                for result in reader.iter(Some(Position(committed + 1))) {
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
        let committed = self.committed_position.load(Ordering::Acquire);

        let seg_list = self.segments.read().clone();
        if seg_list.bases.is_empty() {
            return Ok(vec![]);
        }

        let mut events = Vec::new();

        for (i, &base) in seg_list.bases.iter().enumerate() {
            let seg_path = segment::segment_path(&self.dir, base);
            let is_last = i + 1 == seg_list.bases.len();
            let seg_end = if !is_last {
                seg_list.bases[i + 1] - 1
            } else {
                committed
            };

            if base > committed {
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
                let index = self.tag_index.read();
                (index.matching_bitmap(condition, from_position), None)
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
                    if pos < from_position.0 || pos > committed {
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
                for result in reader.iter(Some(Position(committed + 1))) {
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
fn rebuild_active_segment_index(dir: &Path, index: &mut TagIndex) -> Result<(), Error> {
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
        assert_eq!(response.first_position, Position(1));
        assert_eq!(response.count, 2);
        assert_eq!(response.consistency_marker, Position(2));
        assert_eq!(store.head(), Position(3));
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

        let events = store.source(Position(1), &cond).unwrap();
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

        let tags = store.get_tags(Position(1)).unwrap();
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
            assert_eq!(store.head(), Position(3));

            let cond = SourcingCondition {
                criteria: vec![Criterion {
                    names: vec![],
                    tags: vec![tag("orderId", "A")],
                }],
            };
            let events = store.source(Position(1), &cond).unwrap();
            assert_eq!(events.len(), 2);

            let cond = SourcingCondition {
                criteria: vec![Criterion {
                    names: vec![],
                    tags: vec![tag("paymentId", "P1")],
                }],
            };
            let events = store.source(Position(1), &cond).unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].name, "PaymentReceived");

            // Tags readable from segment.
            let tags = store.get_tags(Position(1)).unwrap();
            assert!(tags.contains(&tag("orderId", "A")));
        }
    }

    #[test]
    fn head_and_tail() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        assert_eq!(store.head(), Position(1));
        assert_eq!(store.tail(), Position(1)); // Empty: tail == head → 0 events.
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
