use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex as StdMutex};
use std::time::Duration;

use parking_lot::RwLock;

use crate::api::EventStore;
use crate::append::{AppendCondition, AppendRequest, AppendResponse, RecordAppend, RecordBatch};
use crate::cache::IndexCache;
use crate::criteria::SourcingCondition;
use crate::error::Error;
use crate::event::{Position, SequencedEvent, StoredEvent, Tag};
use crate::stream::EventStream;
use crate::subscription::CommitFeed;
use crate::system::Visibility;

use crate::inline_breaker::{InlineBreaker, Verdict};
use crate::log_index::{MergeContext, MergeTotals};
use crate::metrics::{StoreMetrics, Timer};
use crate::replication::dispatcher::{WaveDescriptor, WavePublisher, WaveSlice};
use crate::replication::watermark::WatermarkState;
use crate::segment::blocks::ReadMode;
use crate::segment::key_table::KeyTableFile;
use crate::segment::merge;
use crate::segment::query::{Locator, PositionSet, Probe, Resolved};
use crate::segment::reader::{RecordReader, SegmentReader};
use crate::segment::sealed_index::LoggedControl;
use crate::segment::segment_index::SegmentIndex;
use crate::segment::writer::{PendingSeal, SegmentWriter, sync_dir, sync_file};
use crate::segment::{self, DEFAULT_SEGMENT_SIZE, format};

/// The active segment's cached mapping, keyed by its base position.
type ActiveMmap = Arc<parking_lot::Mutex<Option<(u64, Arc<memmap2::Mmap>)>>>;

/// The log's layout: its segments and the key tables that index them. One
/// snapshot behind one lock, so a reader never sees a merged table without
/// the per-segment tables it replaced, or the reverse (ADR-0010).
///
/// Segments before `sealed_count` are sealed: located through their `.idx`
/// (or a pinned in-memory index while it is being written), their keys in a
/// merged table or in their own `.keys`. The last segment is the active
/// (writable) one.
#[derive(Clone)]
pub(crate) struct SegmentList {
    /// Segment base positions in ascending order.
    pub(crate) bases: Arc<Vec<u64>>,
    /// Number of sealed segments.
    /// Invariant: sealed_count < bases.len() (the active segment is never sealed).
    pub(crate) sealed_count: usize,
    /// The own key table of every sealed segment no merged table covers.
    pub(crate) keys: Arc<std::collections::BTreeMap<u64, SegmentKeys>>,
    /// Merged key tables, by base, disjoint.
    pub(crate) tables: Arc<Vec<Arc<KeyTableFile>>>,
}

/// A sealed segment's own key table: in memory while its seal is in flight,
/// its mapped `.keys` after.
#[derive(Clone)]
pub(crate) enum SegmentKeys {
    Pinned(Arc<SegmentIndex>),
    Mapped(Arc<KeyTableFile>),
}

impl SegmentList {
    fn is_sealed(&self, index: usize) -> bool {
        index < self.sealed_count
    }

    /// Where segment `index` ends: the next base, or past everything for
    /// the active segment.
    fn end_of(&self, index: usize) -> u64 {
        self.bases.get(index + 1).copied().unwrap_or(u64::MAX)
    }

    /// The merged table holding `position`, if any.
    pub(crate) fn table_at(&self, position: u64) -> Option<&Arc<KeyTableFile>> {
        let at = self
            .tables
            .partition_point(|table| table.base() <= position);
        at.checked_sub(1)
            .map(|i| &self.tables[i])
            .filter(|table| position < table.end())
    }

    /// The disjoint sources whose keys cover `[from, to)`, in position
    /// order, each clipped to the range: merged tables where they exist,
    /// segments elsewhere.
    fn sources(&self, from: u64, to: u64) -> Vec<Source> {
        let mut out = Vec::new();
        if from >= to || self.bases.is_empty() {
            return out;
        }
        let mut at = from.max(self.bases[0]);
        while at < to {
            if let Some(table) = self.table_at(at) {
                let hi = table.end().min(to);
                out.push(Source::Table(Arc::clone(table), at, hi));
                at = hi;
                continue;
            }
            let index = self.bases.partition_point(|&b| b <= at) - 1;
            let next_table = self.tables[self.tables.partition_point(|t| t.base() <= at)..]
                .first()
                .map_or(u64::MAX, |table| table.base());
            let hi = self.end_of(index).min(next_table).min(to);
            out.push(Source::Segment(index, at, hi));
            at = hi;
        }
        out
    }
}

/// One source of keys for a read, clipped to `[lo, hi)`.
#[derive(Clone)]
enum Source {
    Table(Arc<KeyTableFile>, u64, u64),
    /// A segment, by its index in the list.
    Segment(usize, u64, u64),
}

impl Source {
    fn range(&self) -> (u64, u64) {
        match self {
            Source::Table(_, lo, hi) | Source::Segment(_, lo, hi) => (*lo, *hi),
        }
    }
}

/// A source's keys, held for the length of a read.
enum KeyHandle {
    Table(Arc<KeyTableFile>),
    Pinned(Arc<SegmentIndex>),
    /// The active segment's live index, taken under its lock each time.
    Active,
}

/// Read access to the committed log that owns its handles, so a thread that
/// outlives any one borrow of the engine (the subscription dispatcher) can
/// read like the engine does. Shares the engine's segment list, index cache
/// and active segment: nothing here is a copy.
#[derive(Clone)]
pub(crate) struct LogView {
    dir: PathBuf,
    segments: Arc<RwLock<SegmentList>>,
    cache: Arc<IndexCache>,
    active_index: Arc<parking_lot::RwLock<SegmentIndex>>,
    active_mmap: ActiveMmap,
    max_segment_size: u64,
    watermark: Arc<AtomicU64>,
}

impl LogView {
    /// The watermark: positions below it are committed and visible.
    pub(crate) fn watermark(&self) -> u64 {
        self.watermark.load(Ordering::Acquire)
    }

    pub(crate) fn watermark_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.watermark)
    }

    /// Reader over the active segment, backed by a cached mmap. The mapping
    /// is created once per active segment and retired when rotation changes
    /// the base position.
    ///
    /// Reusing one mapping for the segment's whole lifetime is only sound
    /// while the file is already its full length, because a mapping does not
    /// grow with the file behind it. That is the normal case — segment files
    /// are grown up front — but it is a property of the file, not something
    /// to assume: a store carried over from a release that tolerated a failed
    /// preallocation, or a segment just rotated away from and trimmed to its
    /// data length, maps short. Those reads remap instead of reporting the
    /// bytes past the mapping as corruption.
    fn active_segment_reader(&self, base: u64, path: &Path) -> Result<SegmentReader, Error> {
        {
            let cached = self.active_mmap.lock();
            if let Some((cached_base, mmap)) = cached.as_ref()
                && *cached_base == base
                && mmap.len() as u64 >= self.max_segment_size
            {
                return SegmentReader::from_shared_mmap(Arc::clone(mmap));
            }
        }
        let file = std::fs::File::open(path)?;
        let mmap = Arc::new(unsafe { memmap2::Mmap::map(&file)? });
        *self.active_mmap.lock() = Some((base, Arc::clone(&mmap)));
        SegmentReader::from_shared_mmap(mmap)
    }

    /// The index that answers for segment `base`: the active segment's live
    /// index when it is still active, otherwise the sealed index from the
    /// cache (pinned if the seal is in flight, its mapped `.idx` if not).
    /// A segment-list snapshot may say "active" about a segment that has
    /// just been rotated away from; this resolves it correctly either way.
    fn index_for(&self, base: u64, seg_path: &Path) -> Result<IndexRef<'_>, Error> {
        let active = self.active_index.read();
        if active.base_position() == base {
            return Ok(IndexRef::Active(active));
        }
        drop(active);
        Ok(IndexRef::Sealed(self.cache.get_index(seg_path, base)?))
    }

    /// The reader matching `index_for`'s answer.
    fn reader_for(
        &self,
        index: &IndexRef<'_>,
        base: u64,
        seg_path: &Path,
    ) -> Result<SegmentReader, Error> {
        match index {
            IndexRef::Active(_) => self.active_segment_reader(base, seg_path),
            IndexRef::Sealed(_) => {
                SegmentReader::from_shared_mmap(self.cache.get_mmap(seg_path, base)?)
            }
        }
    }

    /// The keys of segment `index` of `list`, a snapshot of the layout:
    /// its own table or pinned index there, or the live index if it is still
    /// active. A segment rotated or merged since the snapshot is found in
    /// the cache's pinned indexes or the current layout, and as a last
    /// resort its `.keys` is mapped, unless `disk` is false (`None` then).
    fn segment_keys(
        &self,
        list: &SegmentList,
        index: usize,
        disk: bool,
    ) -> Result<Option<KeyHandle>, Error> {
        let base = list.bases[index];
        let from_layout = |list: &SegmentList| {
            list.keys
                .get(&base)
                .map(|keys| match keys {
                    SegmentKeys::Pinned(index) => KeyHandle::Pinned(Arc::clone(index)),
                    SegmentKeys::Mapped(table) => KeyHandle::Table(Arc::clone(table)),
                })
                .or_else(|| list.table_at(base).map(|t| KeyHandle::Table(Arc::clone(t))))
        };
        if let Some(handle) = from_layout(list) {
            return Ok(Some(handle));
        }
        if self.active_index.read().base_position() == base {
            return Ok(Some(KeyHandle::Active));
        }
        if let Some(index) = self.cache.pinned(base) {
            return Ok(Some(KeyHandle::Pinned(index)));
        }
        if let Some(handle) = from_layout(&self.segments.read()) {
            return Ok(Some(handle));
        }
        if !disk {
            return Ok(None);
        }
        let path = segment::segment_path(&self.dir, base).with_extension("keys");
        Ok(Some(KeyHandle::Table(Arc::new(KeyTableFile::open(&path)?))))
    }

    /// The keys of `source`; see [`segment_keys`](Self::segment_keys).
    fn source_keys(
        &self,
        list: &SegmentList,
        source: &Source,
        disk: bool,
    ) -> Result<Option<KeyHandle>, Error> {
        match source {
            Source::Table(table, ..) => Ok(Some(KeyHandle::Table(Arc::clone(table)))),
            Source::Segment(index, ..) => self.segment_keys(list, *index, disk),
        }
    }

    /// Runs `f` on `probe` resolved against `source`'s keys. For the active
    /// segment that happens under its index's read lock, so `f` must be
    /// quick. `None` when the keys are not at hand without the disk.
    fn with_resolved<T>(
        &self,
        list: &SegmentList,
        source: &Source,
        probe: &Probe,
        disk: bool,
        f: impl FnOnce(&Resolved<'_>) -> Result<T, Error>,
    ) -> Result<Option<T>, Error> {
        let Some(handle) = self.source_keys(list, source, disk)? else {
            return Ok(None);
        };
        match handle {
            KeyHandle::Table(table) => f(&Resolved::new(&*table, probe)?).map(Some),
            KeyHandle::Pinned(index) => f(&Resolved::new(&*index, probe)?).map(Some),
            KeyHandle::Active => {
                let Source::Segment(at, ..) = source else {
                    unreachable!("only a segment can be active");
                };
                let active = self.active_index.read();
                if active.base_position() == list.bases[*at] {
                    return f(&Resolved::new(&*active, probe)?).map(Some);
                }
                drop(active);
                // Rotated since: now pinned, or sealed.
                let Some(handle) = self.segment_keys(list, *at, disk)? else {
                    return Ok(None);
                };
                match handle {
                    KeyHandle::Table(table) => f(&Resolved::new(&*table, probe)?).map(Some),
                    KeyHandle::Pinned(index) => f(&Resolved::new(&*index, probe)?).map(Some),
                    KeyHandle::Active => Err(Error::Internal {
                        message: format!("segment {} is active and not", list.bases[*at]),
                    }),
                }
            }
        }
    }

    /// Calls `each` with the positions in `[from, to)` matching `probe`,
    /// a window of at most [`MATCH_WINDOW`] positions at a time, in position
    /// order, until it says stop. Each source's keys are looked up once
    /// (the active segment's once per window, under its lock); a window
    /// starts at the next match, so empty stretches are skipped whole.
    /// `ruled_out` hears, per source, whether its keys rule the query out.
    fn for_each_match(
        &self,
        list: &SegmentList,
        probe: &Probe,
        from: u64,
        to: u64,
        mut ruled_out: impl FnMut(bool),
        mut each: impl FnMut(&PositionSet) -> Result<Walk, Error>,
    ) -> Result<Walk, Error> {
        /// The next window at or after `cursor` below `hi`, and where it ends.
        fn window(
            resolved: &Resolved<'_>,
            cursor: u64,
            hi: u64,
        ) -> Result<Option<(PositionSet, u64)>, Error> {
            let Some(start) = resolved.first_at_or_after(cursor)? else {
                return Ok(None);
            };
            if start >= hi {
                return Ok(None);
            }
            let end = start.saturating_add(MATCH_WINDOW).min(hi);
            Ok(resolved
                .matching_in(start, end)?
                .map(|set| (set, end))
                .or(Some((PositionSet::range(start, start, start), end))))
        }

        for source in list.sources(from, to) {
            let (lo, hi) = source.range();
            let handle = self
                .source_keys(list, &source, true)?
                .ok_or_else(|| Error::Internal {
                    message: "a read found no keys for a segment".into(),
                })?;
            let mut walk = |resolved: &Resolved<'_>| -> Result<Walk, Error> {
                ruled_out(resolved.is_dead());
                if resolved.is_dead() {
                    return Ok(Walk::Next);
                }
                let mut cursor = lo;
                while let Some((set, end)) = window(resolved, cursor, hi)? {
                    if !set.is_empty()
                        && let Walk::Stop = each(&set)?
                    {
                        return Ok(Walk::Stop);
                    }
                    cursor = end;
                }
                Ok(Walk::Next)
            };
            let walked = match handle {
                KeyHandle::Table(table) => walk(&Resolved::new(&*table, probe)?)?,
                KeyHandle::Pinned(index) => walk(&Resolved::new(&*index, probe)?)?,
                KeyHandle::Active => {
                    // One window at a time under the lock, so the writer
                    // is never held up for longer than a window takes.
                    let mut cursor = lo;
                    let mut first = true;
                    loop {
                        let next = self.with_resolved(list, &source, probe, true, |resolved| {
                            if first {
                                ruled_out(resolved.is_dead());
                            }
                            window(resolved, cursor, hi)
                        })?;
                        first = false;
                        let Some(Some((set, end))) = next else {
                            break Walk::Next;
                        };
                        if !set.is_empty()
                            && let Walk::Stop = each(&set)?
                        {
                            break Walk::Stop;
                        }
                        cursor = end;
                    }
                }
            };
            if let Walk::Stop = walked {
                return Ok(Walk::Stop);
            }
        }
        Ok(Walk::Next)
    }

    /// Calls `visit` with the position, event type name, tags and stored
    /// record of every event in `[from, to)`, in log order, reading the log
    /// sequentially: one index lookup per segment to find where `from`
    /// starts, then a walk. `to` must not pass the watermark, which is what
    /// makes every byte read final.
    ///
    /// `visit` returns whether to go on; the result is the position after the
    /// last event visited, which is `to` unless the visitor stopped early.
    pub(crate) fn visit_events(
        &self,
        from: u64,
        to: u64,
        mut visit: impl FnMut(
            u64,
            &[u8],
            format::Tags<'_>,
            &crate::segment::reader::StoredRecord<'_>,
        ) -> bool,
    ) -> Result<u64, Error> {
        let mut next = from;
        while next < to {
            let (base, end) = {
                let list = self.segments.read();
                let at = list.bases.partition_point(|&b| b <= next);
                let Some(i) = at.checked_sub(1) else {
                    return Err(Error::PositionNotFound { position: next });
                };
                (
                    list.bases[i],
                    list.bases.get(i + 1).copied().unwrap_or(u64::MAX),
                )
            };
            let path = segment::segment_path(&self.dir, base);
            let (offset, reader) = {
                let index = self.index_for(base, &path)?;
                let offset = index
                    .offset_of(next)?
                    .ok_or(Error::PositionNotFound { position: next })?;
                (offset, self.reader_for(&index, base, &path)?)
            };
            let stop = to.min(end);
            for record in reader.events_from(offset as usize, Position(stop)) {
                let (position, record) = record?;
                if position != next {
                    return Err(Error::Internal {
                        message: format!(
                            "segment {base} holds position {position} where {next} was expected"
                        ),
                    });
                }
                let scan = format::scan(record.body)?;
                next += 1;
                if !visit(position, scan.name, scan.tags(), &record) {
                    return Ok(next);
                }
            }
            if next < stop {
                return Err(Error::Internal {
                    message: format!(
                        "segment {base} ends at position {next}, below the committed {stop}"
                    ),
                });
            }
        }
        Ok(next)
    }
}

/// Most positions a read takes from one source in one go: a window starts
/// at the next match, so this bounds what a dense key costs per step, never
/// what skipping costs.
const MATCH_WINDOW: u64 = 1 << 20;

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
        let (wave, first_of_wave) = self.register_write();
        if first_of_wave {
            self.wake_sync_thread();
        }
        wave
    }

    /// The registration half of `mark_pending`, for a caller that wakes the
    /// sync thread itself once it has let go of the writer lock: the wake is
    /// a futex syscall, and every other appender waits on that lock.
    /// Returns the wave and whether this write opened it — the caller of the
    /// first write of a wave owes the wake.
    fn register_write(&self) -> (u64, bool) {
        let first_of_wave = self.pending_writes.fetch_add(1, Ordering::Relaxed) == 0;
        (self.wave.load(Ordering::Acquire), first_of_wave)
    }

    /// Wakes the sync thread. Safe to run after the writer lock is released:
    /// the thread re-checks for pending writes under the same mutex, so a
    /// wake that lands late or after it has already synced the write is a
    /// no-op round.
    fn wake_sync_thread(&self) {
        let _wake = self.wake.lock().unwrap();
        self.wake_cv.notify_one();
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

/// Seals whose index files are being written on a background thread.
///
/// A rotation leaves the sealed segment's `.idx` and `.keys` to be written
/// off the writer lock, after the wave that carries the rotation has made
/// the segment's bytes durable. Until then the segment answers reads from
/// its pinned in-memory index. Truncation (failover repair) may delete or
/// reopen such a segment while its files are in flight; it cancels the seal
/// and waits, so no index file is ever written for a file it does not
/// describe.
struct SealQueue {
    /// Bases with a finalize in flight, and whether it was cancelled.
    in_flight: StdMutex<std::collections::BTreeMap<u64, bool>>,
    done: Condvar,
    dir: PathBuf,
    cache: Arc<IndexCache>,
    segments: Arc<RwLock<SegmentList>>,
    merge: Arc<MergeContext>,
}

impl SealQueue {
    fn new(
        dir: &Path,
        cache: &Arc<IndexCache>,
        segments: &Arc<RwLock<SegmentList>>,
        merge: &Arc<MergeContext>,
    ) -> Self {
        Self {
            in_flight: StdMutex::new(std::collections::BTreeMap::new()),
            done: Condvar::new(),
            dir: dir.to_path_buf(),
            cache: Arc::clone(cache),
            segments: Arc::clone(segments),
            merge: Arc::clone(merge),
        }
    }

    /// Writes the seals' index files on a background thread. The segments'
    /// bytes are already durable (the wave fsync'd them), so the files may
    /// land in any order relative to further appends.
    fn finalize(self: &Arc<Self>, seals: Vec<PendingSeal>) {
        {
            let mut in_flight = self.in_flight.lock().unwrap();
            for seal in &seals {
                in_flight.insert(seal.base, false);
            }
        }
        let queue = Arc::clone(self);
        let spawned = std::thread::Builder::new()
            .name("kronosdb-seal".into())
            .spawn(move || {
                for seal in seals {
                    queue.finish(seal);
                }
            });
        if let Err(error) = spawned {
            tracing::error!(%error, "failed to spawn the seal thread; index files will be rebuilt at the next open");
            self.in_flight.lock().unwrap().clear();
            self.done.notify_all();
        }
    }

    fn finish(&self, seal: PendingSeal) {
        let path = segment::segment_path(&self.dir, seal.base);
        let written = seal
            .index
            .write_to_disk(&path)
            .and_then(|()| KeyTableFile::open(&path.with_extension("keys")));
        let cancelled = self
            .in_flight
            .lock()
            .unwrap()
            .remove(&seal.base)
            .unwrap_or(true);
        match written {
            Ok(keys) if !cancelled => {
                // Only the incarnation this seal is of: a truncation that
                // got in since replaced or dropped its pinned entry.
                let current = {
                    let mut layout = self.segments.write();
                    let current = match layout.keys.get(&seal.base) {
                        Some(SegmentKeys::Pinned(pinned)) => Arc::ptr_eq(pinned, &seal.index),
                        Some(SegmentKeys::Mapped(_)) => false,
                        // Not noted while pending: still this incarnation if
                        // the layout has it sealed and no table covers it.
                        None => {
                            let at = layout.bases.binary_search(&seal.base);
                            at.is_ok_and(|at| layout.is_sealed(at))
                                && layout.table_at(seal.base).is_none()
                        }
                    };
                    if current {
                        Arc::make_mut(&mut layout.keys)
                            .insert(seal.base, SegmentKeys::Mapped(Arc::new(keys)));
                    }
                    current
                };
                if current {
                    self.cache.unpin_sealed(seal.base, &path);
                    self.merge.poke();
                }
            }
            Ok(_) => {
                // Truncated away or reopened meanwhile: the files describe
                // a segment that no longer exists in this shape.
                let _ = std::fs::remove_file(path.with_extension("idx"));
                let _ = std::fs::remove_file(path.with_extension("keys"));
            }
            Err(error) => {
                // The segment stays readable through its pinned index; the
                // next open rebuilds the files.
                tracing::error!(
                    base = seal.base,
                    %error,
                    "failed to write a sealed segment's index files; \
                     they will be rebuilt at the next open"
                );
            }
        }
        self.done.notify_all();
    }

    /// Cancels every in-flight seal at or past `base` and waits for the
    /// thread to acknowledge, so the caller may delete or reopen those
    /// segment files.
    fn cancel_from(&self, base: u64) {
        let mut in_flight = self.in_flight.lock().unwrap();
        for (_, cancelled) in in_flight.range_mut(base..) {
            *cancelled = true;
        }
        while in_flight.range(base..).next().is_some() {
            in_flight = self.done.wait(in_flight).unwrap();
        }
    }

    /// Waits until no seal is in flight. Test and shutdown helper.
    #[cfg(test)]
    fn wait_idle(&self) {
        let mut in_flight = self.in_flight.lock().unwrap();
        while !in_flight.is_empty() {
            in_flight = self.done.wait(in_flight).unwrap();
        }
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
    feed: Arc<CommitFeed>,
    coalesce_window: Duration,
    seals: Arc<SealQueue>,
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
                        let mut w = writer.lock();
                        let wave = sync_state.seal_wave();
                        let durable = local_tail.load(Ordering::Acquire);
                        let current_base = w.active_base_position();
                        let current_offset = w.write_offset();
                        // Segments rotated away from since the last barrier.
                        // Their files are fdatasync'd with this wave, and
                        // their companions written afterwards, off the lock.
                        let taken_seals = w.take_pending_seals();

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
                                &taken_seals,
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
                        Ok((wave, durable, descriptor, file, taken_seals))
                    })();
                    let result =
                        sealed.and_then(|(wave, durable, descriptor, file, taken_seals)| {
                            // Queue before fsync: the dispatcher preads on its own
                            // thread while this thread enters fdatasync.
                            replication.try_publish(descriptor);
                            if crate::written_acks(watermark.voter_count())
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
                                let _ = wm;
                                feed.committed();
                            }
                            // A rotation in this wave: the sealed file's last
                            // records and the new file's directory entry must
                            // be durable before the wave's writers are released.
                            for seal in &taken_seals {
                                sync_file(&seal.file)?;
                            }
                            if !taken_seals.is_empty() {
                                sync_dir(&dir)?;
                            }
                            sync_file(&file)?;
                            Ok((wave, durable, taken_seals))
                        });
                    match result {
                        Ok((wave, durable, taken_seals)) => {
                            durable_tail.store(durable, Ordering::Release);
                            sync_state.complete_wave(wave);
                            if !taken_seals.is_empty() {
                                seals.finalize(taken_seals);
                            }
                            // This node's durable cursor advances. With a
                            // quorum of one this IS the watermark bump that
                            // releases ack waiters; under replication
                            // follower cursors join the math.
                            if watermark
                                .advance(node_id, watermark.epoch(), durable)
                                .is_some()
                            {
                                feed.committed();
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
    seals: &[PendingSeal],
) -> Result<WaveDescriptor, Error> {
    // The segments the wave spans are the previous barrier's active one,
    // every segment sealed since, and the current active one — all known
    // from the seals taken at this barrier, so no readdir or stat runs
    // under the writer lock.
    let mut selected: Vec<u64> = vec![previous_base];
    for seal in seals {
        if seal.base > previous_base && seal.base < current_base {
            selected.push(seal.base);
        }
    }
    if current_base > previous_base {
        selected.push(current_base);
    }
    let mut slices = Vec::new();
    for (index, &base) in selected.iter().enumerate() {
        let path = segment::segment_path(dir, base);
        let byte_start = if base == previous_base {
            previous_offset
        } else {
            segment::SEGMENT_HEADER_SIZE as u64
        };
        let byte_end = if base == current_base {
            current_offset
        } else if let Some(seal) = seals.iter().find(|seal| seal.base == base) {
            seal.len
        } else {
            // A segment sealed before this barrier (its seal was taken by
            // an earlier wave): truncated to its data length at rotation.
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

/// Applies the ack gate's verdict to a staged outcome (see `append_finalize`).
fn finalize_append(
    metrics: &StoreMetrics,
    staged: StagedAppend,
    wait: Result<(), Error>,
) -> Result<AppendResponse, Error> {
    wait?;
    match staged.outcome {
        Ok(response) => {
            metrics.record_append(response.count, staged.timer.elapsed_us());
            Ok(response)
        }
        Err(error) => Err(error),
    }
}

/// Which places an append may be staged in, tried in order: inline on the
/// async worker, the staging thread, the blocking pool (always available).
/// Production uses all of them; tests narrow it to exercise each alone.
#[derive(Clone, Copy)]
struct Routes {
    inline: bool,
    thread: bool,
}

impl Routes {
    const ALL: Routes = Routes {
        inline: true,
        thread: true,
    };
}

/// What a non-blocking staging attempt came to.
enum Staging {
    Done(Result<StagedAppend, Error>),
    /// It would have had to block; the request, untouched, for the pool.
    NeedsPool(Prepared),
}

/// An append ready to stage: checked, and its events as records. Everything
/// after this point is one path, whatever the append arrived as.
struct Prepared {
    condition: Option<AppendCondition>,
    batch: RecordBatch,
}

/// The pre-lock half's verdict.
enum Pre {
    /// A sealed segment holds a conflicting event.
    Conflict(Position),
    /// No conflict in the sealed segments; this many were covered.
    Checked(usize),
}

enum SealedScan {
    Conflict(Position),
    Clear,
    NeedsDisk,
}

/// What the staging thread answers a queued append with.
enum Settled {
    /// Staged, committed and finalized: nothing left for the caller to do.
    Final(Result<AppendResponse, Error>),
    /// The thread could not stage it without reading the disk; the request,
    /// untouched, for the blocking pool.
    Pool(Prepared),
}

/// One append queued for the staging thread.
struct StageJob {
    engine: Arc<EventStoreEngine>,
    prepared: Prepared,
    reply: tokio::sync::oneshot::Sender<Settled>,
}

/// Most appends one pass of the staging thread takes before it answers them.
const STAGE_DRAIN_MAX: usize = 64;

/// What one hold of the writer lock takes on while it stages a group: past
/// any of these the group is written and the lock let go, so the appenders
/// and the wave barrier waiting on it are not kept waiting on a long queue.
const GROUP_MAX_APPENDS: usize = 32;
const GROUP_MAX_EVENTS: u32 = 256;
const GROUP_MAX_BYTES: usize = 64 * 1024;

/// Starts a context's staging thread: it takes the appends that found the
/// writer lock busy, stages whatever has queued in arrival order under as
/// few holds of the lock as it can, opens the written-ack gate once for the
/// group, and gives each caller its final answer when the watermark reaches
/// its place.
fn spawn_stage_thread() -> std::sync::mpsc::Sender<StageJob> {
    let (tx, rx) = std::sync::mpsc::channel::<StageJob>();
    std::thread::Builder::new()
        .name("kronosdb-stage".into())
        .spawn(move || {
            let mut jobs: Vec<StageJob> = Vec::with_capacity(STAGE_DRAIN_MAX);
            let mut staged: Vec<(tokio::sync::oneshot::Sender<Settled>, StagedAppend)> =
                Vec::with_capacity(STAGE_DRAIN_MAX);
            while let Ok(first) = rx.recv() {
                jobs.push(first);
                while jobs.len() < STAGE_DRAIN_MAX {
                    match rx.try_recv() {
                        Ok(job) => jobs.push(job),
                        Err(_) => break,
                    }
                }
                // One thread per engine, so a pass is one engine's.
                let engine = Arc::clone(&jobs[0].engine);
                let (appends, replies): (Vec<_>, Vec<_>) =
                    jobs.drain(..).map(|job| (job.prepared, job.reply)).unzip();
                let mut release: Option<u64> = None;
                for (reply, staging) in replies.into_iter().zip(engine.stage_group(appends)) {
                    match staging {
                        Staging::Done(Ok(append)) => {
                            release = release.max(append.wait_pos());
                            staged.push((reply, append));
                        }
                        Staging::Done(Err(error)) => {
                            let _ = reply.send(Settled::Final(Err(error)));
                        }
                        Staging::NeedsPool(prepared) => {
                            let _ = reply.send(Settled::Pool(prepared));
                        }
                    }
                }
                // One ack-gate advance covers the whole group; then each
                // append is answered when the watermark reaches it.
                engine.release_written(release);
                for (reply, append) in staged.drain(..) {
                    engine.settle_when_committed(append, reply);
                }
            }
        })
        .expect("spawn append staging thread");
    tx
}

/// Checks a client's events and condition against the `$` namespace. A client
/// must not be able to forge an event the server claims to have written about
/// itself.
fn prepare_client_records(request: RecordAppend) -> Result<Prepared, Error> {
    request.events.check_client()?;
    if let Some(condition) = &request.condition {
        crate::system::validate_client_condition(&condition.criteria)?;
    }
    Ok(Prepared {
        condition: request.condition,
        batch: request.events,
    })
}

/// [`prepare_client_records`] for a client append that arrived as events.
fn prepare_client_append(request: AppendRequest) -> Result<Prepared, Error> {
    prepare_client_records(RecordAppend {
        condition: request.condition,
        events: RecordBatch::from_events(&request.events),
    })
}

/// Validates and stamps the `$` framing of a server-authored append.
fn prepare_system_append(mut request: AppendRequest) -> Result<Prepared, Error> {
    // Stamped here rather than trusted from callers: the read path judges
    // visibility by event type while `visible_head` judges it by this tag,
    // and a system event missing it would be unreadable yet still counted
    // in the head clients are shown — phantom lag no consumer can clear.
    for event in &mut request.events {
        if !crate::system::is_system_name(&event.name) {
            continue;
        }
        // A system event may only carry `$` tags. DCB checks match by
        // tag with no visibility filter, so a user tag here would let
        // invisible state reject client appends — a conflict the client
        // can neither see nor explain. Correlate through `$` tags only.
        if let Some(tag) = event
            .tags
            .iter()
            .find(|tag| !crate::system::is_system_tag(tag))
        {
            return Err(Error::ReservedNamespace {
                detail: format!(
                    "system event '{}' may not carry user tag '{}'",
                    event.name,
                    String::from_utf8_lossy(&tag.key)
                ),
            });
        }
        if !event.tags.iter().any(crate::system::is_marker) {
            event.tags.push(crate::system::marker());
        }
    }
    Ok(Prepared {
        batch: RecordBatch::from_events(&request.events),
        condition: request.condition,
    })
}

/// Configuration options for an event store engine.
#[derive(Debug, Clone)]
pub struct StoreOptions {
    pub max_segment_size: u64,
    /// Mappings of sealed segments and their indexes this store may keep,
    /// shared with every other store holding the same budget: by default
    /// the process's, sized from the kernel's limit (`MappingBudget::shared`).
    pub mapping_budget: Arc<crate::cache::MappingBudget>,
    /// How sealed segments are read (prototype switch).
    pub read_mode: crate::segment::blocks::ReadMode,
    /// Memory budget of the verified block cache for sealed segments.
    pub block_cache_bytes: usize,
    /// Target size of one cached block.
    pub block_size: usize,
    /// Blocks read ahead of a sequential reader.
    pub readahead_blocks: usize,
    /// Extra group-commit coalescing window in milliseconds. The sync thread
    /// is woken by a wave's first write; with 0 (default) it seals and syncs
    /// immediately, and concurrent writes batch on the fdatasync duration.
    /// A positive value trades that much added latency for larger waves.
    pub group_commit_interval_ms: u64,
    /// This node's control-plane identity for durable-cursor acknowledgements.
    pub node_id: crate::replication::watermark::NodeId,
    /// Exact voter set used for native segment quorum calculations.
    pub voters: Vec<crate::replication::watermark::NodeId>,
    /// Bytes per second a background index merge may write (ADR-0010);
    /// 0 is unlimited.
    pub index_merge_rate: u64,
    /// Whether index merges run in the background. Off, they run only when
    /// [`EventStoreEngine::merge_index`] is called.
    pub background_index_merge: bool,
}

/// Default write rate of background index merges.
pub const DEFAULT_INDEX_MERGE_RATE: u64 = 64 << 20;

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            max_segment_size: DEFAULT_SEGMENT_SIZE,
            mapping_budget: crate::cache::MappingBudget::shared(),
            read_mode: crate::segment::blocks::ReadMode::default(),
            block_cache_bytes: crate::segment::blocks::DEFAULT_BLOCK_CACHE_BYTES,
            block_size: crate::segment::blocks::DEFAULT_BLOCK_SIZE,
            readahead_blocks: crate::segment::blocks::DEFAULT_READAHEAD_BLOCKS,
            group_commit_interval_ms: DEFAULT_GROUP_COMMIT_INTERVAL_MS,
            node_id: DEFAULT_NODE_ID,
            voters: vec![DEFAULT_NODE_ID],
            index_merge_rate: DEFAULT_INDEX_MERGE_RATE,
            background_index_merge: true,
        }
    }
}

impl StoreOptions {
    /// Auto-scales cache sizes based on sealed segment count and a memory budget.
    ///
    /// This is NOT the default — use it explicitly when you know your workload.
    /// The default StoreOptions uses conservative fixed sizes that work for most cases.
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
    ///
    /// The block cache gets a quarter. Segment indexes take no heap budget:
    /// they are mapped, and the rest of the budget is left to the page cache
    /// that serves both them and the mapped segments.
    pub fn auto_scaled_with_budget(_sealed_segment_count: usize, budget_bytes: usize) -> Self {
        Self {
            block_cache_bytes: budget_bytes / 4,
            ..Self::default()
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
        if trimmed != "max"
            && let Ok(bytes) = trimmed.parse::<usize>()
        {
            return Some(bytes);
        }
        return None; // "max" = no limit, fall through to system RAM.
    }

    // cgroup v1: different path, large sentinel value means unlimited.
    if let Ok(content) = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
        && let Ok(bytes) = content.trim().parse::<usize>()
        // cgroup v1 uses a very large number (close to usize::MAX) for "no limit".
        && bytes < 1 << 62
    {
        return Some(bytes);
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

    /// Where each epoch's `EpochChange` sits in this log. A leader reads it to
    /// find where a returning node's history stops agreeing with its own.
    epochs: parking_lot::Mutex<crate::replication::epochs::EpochIndex>,

    /// Live subscriptions and the dispatcher that wakes them as commits
    /// land. Every path that advances the watermark tells it.
    feed: Arc<CommitFeed>,

    /// What the dispatcher needs to read committed events: shares this
    /// engine's segment list, index cache and active segment.
    view: LogView,

    /// The active segment's in-memory index (bitmaps + position→offset
    /// table), maintained incrementally by the writer. Lets source queries
    /// direct-seek into the active segment instead of sequentially scanning
    /// it, without taking the writer lock.
    active_index: Arc<parking_lot::RwLock<SegmentIndex>>,

    /// Mapped indexes of sealed segments, and their mmap handles and files.
    /// Also pins the index of every segment sealed in memory but not yet on
    /// disk — see `PendingSeal`.
    cache: Arc<IndexCache>,

    /// Seals whose companion files are being written in the background.
    seals: Arc<SealQueue>,

    /// Cached segment list — avoids readdir + stat syscalls on every query.
    /// Updated on rotation within the append path (under writer lock).
    segments: Arc<RwLock<SegmentList>>,

    /// Cached mmap of the active segment, keyed by its base position. The
    /// active file is grown to `max_segment_size` when it is created, so one
    /// mapping covers the whole segment lifetime; callers clamp reads to
    /// committed offsets. Rotation changes the base, which retires the entry
    /// by key mismatch — in-flight readers keep their `Arc<Mmap>` and only
    /// ever touch offsets that existed when they resolved positions.
    active_mmap: ActiveMmap,

    /// How sealed segments are read.
    read_mode: crate::segment::blocks::ReadMode,

    /// Lock-free internal metrics. Shared via Arc for external access.
    metrics: Arc<StoreMetrics>,

    /// Queue to this context's staging thread, started by the first append
    /// that finds the writer lock busy.
    stage_tx: std::sync::OnceLock<std::sync::mpsc::Sender<StageJob>>,

    /// Shuts the inline staging route for a while after one stalls.
    inline_breaker: InlineBreaker,

    /// The log-wide index's merges (ADR-0010), and the thread running them
    /// in the background when it runs.
    merge: Arc<MergeContext>,
    merger: StdMutex<Option<std::thread::JoinHandle<()>>>,
    /// Index files the last open had to rebuild from their segments.
    rebuilt_at_open: usize,
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
        let feed = CommitFeed::new();

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
        let cache = Arc::new(IndexCache::with_blocks(
            crate::segment::blocks::BlockCache::new(
                opts.block_cache_bytes,
                opts.block_size,
                opts.readahead_blocks,
            ),
            Arc::clone(&opts.mapping_budget),
        ));
        writer.lock().attach_index_cache(Arc::clone(&cache));
        std::fs::create_dir_all(dir.join(merge::INDEX_DIR))?;
        merge::write_manifest(dir, &[])?;
        let segments = Arc::new(RwLock::new(SegmentList {
            bases: Arc::new(vec![active_base]),
            sealed_count: 0,
            keys: Arc::default(),
            tables: Arc::default(),
        }));
        let merge = MergeContext::new(
            dir.to_path_buf(),
            Arc::clone(&segments),
            watermark.handle(),
            opts.index_merge_rate,
        );
        let seals = Arc::new(SealQueue::new(dir, &cache, &segments, &merge));

        spawn_sync_thread(
            dir.to_path_buf(),
            Arc::clone(&sync_state),
            Arc::clone(&writer),
            Arc::clone(&local_tail),
            Arc::clone(&durable_tail),
            opts.node_id,
            Arc::clone(&replication),
            Arc::clone(&watermark),
            Arc::clone(&feed),
            Duration::from_millis(opts.group_commit_interval_ms),
            Arc::clone(&seals),
        );

        let view = LogView {
            dir: dir.to_path_buf(),
            segments: Arc::clone(&segments),
            cache: Arc::clone(&cache),
            active_index: Arc::clone(&active_index),
            active_mmap: Arc::new(parking_lot::Mutex::new(None)),
            max_segment_size: opts.max_segment_size,
            watermark: watermark.handle(),
        };
        let merger = opts
            .background_index_merge
            .then(|| crate::log_index::spawn(Arc::clone(&merge)));

        Ok(Self {
            dir: dir.to_path_buf(),
            writer,
            sync_state,
            node_id: opts.node_id,
            local_tail,
            durable_tail,
            replication,
            watermark,
            epochs: parking_lot::Mutex::default(),
            feed,
            active_mmap: Arc::clone(&view.active_mmap),
            view,
            active_index,
            cache,
            seals,
            segments,
            read_mode: opts.read_mode,
            metrics: Arc::new(StoreMetrics::new()),
            stage_tx: std::sync::OnceLock::new(),
            inline_breaker: InlineBreaker::new(),
            merge,
            merger: StdMutex::new(merger),
            rebuilt_at_open: 0,
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

        // Build the cached segment list from disk (one-time cost on startup).
        let all_bases = segment::list_segment_files(dir)?;
        let sealed_count = all_bases.iter().filter(|&&base| base < active_base).count();

        // Every sealed segment's `.idx` is mapped, and the key tables: the
        // merged ones the manifest names, and the own `.keys` of segments
        // none covers. Only a missing or damaged file is rebuilt. The
        // locators' control summaries are what recovery reads below, so no
        // sealed segment's records are read at all.
        let opened = crate::log_index::open(dir, &all_bases, active_base)?;
        let rebuilt_at_open = opened.rebuilt;
        let sealed = opened.locators;

        // Writer's head() already returns the next-exclusive position, which
        // is exactly our head_position semantics — no adjustment.
        let head_pos = head.0;
        let feed = CommitFeed::new();

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
        // The active segment's control records were read once already, by
        // the writer rebuilding its index.
        let recovered = recover_native_control_state(
            sealed.iter().map(|(_, index)| index.controls()),
            active_index.read().controls(),
            head_pos,
        );
        let recovered_watermark = if voters.len() == 1 {
            head_pos
        } else {
            recovered.checkpoint
        };
        let watermark = Arc::new(WatermarkState::new(
            recovered.epoch,
            voters,
            recovered_watermark,
        ));
        let cache = Arc::new(IndexCache::with_blocks(
            crate::segment::blocks::BlockCache::new(
                opts.block_cache_bytes,
                opts.block_size,
                opts.readahead_blocks,
            ),
            Arc::clone(&opts.mapping_budget),
        ));
        for (base, index) in sealed {
            cache.adopt(base, index);
        }
        writer.lock().attach_index_cache(Arc::clone(&cache));
        let segments = Arc::new(RwLock::new(SegmentList {
            bases: Arc::new(all_bases),
            sealed_count,
            keys: Arc::new(opened.keys),
            tables: Arc::new(opened.tables),
        }));
        let merge = MergeContext::new(
            dir.to_path_buf(),
            Arc::clone(&segments),
            watermark.handle(),
            opts.index_merge_rate,
        );
        let seals = Arc::new(SealQueue::new(dir, &cache, &segments, &merge));

        spawn_sync_thread(
            dir.to_path_buf(),
            Arc::clone(&sync_state),
            Arc::clone(&writer),
            Arc::clone(&local_tail),
            Arc::clone(&durable_tail),
            opts.node_id,
            Arc::clone(&replication),
            Arc::clone(&watermark),
            Arc::clone(&feed),
            Duration::from_millis(opts.group_commit_interval_ms),
            Arc::clone(&seals),
        );

        let view = LogView {
            dir: dir.to_path_buf(),
            segments: Arc::clone(&segments),
            cache: Arc::clone(&cache),
            active_index: Arc::clone(&active_index),
            active_mmap: Arc::new(parking_lot::Mutex::new(None)),
            max_segment_size: opts.max_segment_size,
            watermark: watermark.handle(),
        };

        Ok(Self {
            dir: dir.to_path_buf(),
            writer,
            sync_state,
            node_id: opts.node_id,
            local_tail,
            durable_tail,
            replication,
            watermark,
            epochs: parking_lot::Mutex::new(recovered.epochs),
            feed,
            active_mmap: Arc::clone(&view.active_mmap),
            view,
            active_index,
            cache,
            seals,
            segments,
            read_mode: opts.read_mode,
            metrics: Arc::new(StoreMetrics::new()),
            stage_tx: std::sync::OnceLock::new(),
            inline_breaker: InlineBreaker::new(),
            merger: StdMutex::new(
                opts.background_index_merge
                    .then(|| crate::log_index::spawn(Arc::clone(&merge))),
            ),
            merge,
            rebuilt_at_open,
        })
    }

    /// Returns a shared reference to this engine's metrics counters.
    /// Initiates engine shutdown: new appends are rejected, and the group
    /// commit sync thread performs one final fsync pass (releasing any
    /// in-flight writers) before exiting. Idempotent.
    pub fn shutdown(&self) {
        self.sync_state.begin_shutdown();
    }

    /// Reader over the active segment; see [`LogView::active_segment_reader`].
    fn active_segment_reader(&self, base: u64, path: &Path) -> Result<SegmentReader, Error> {
        self.view.active_segment_reader(base, path)
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
    /// Size of the current quorum voter set (lock-free).
    pub fn voter_count(&self) -> u64 {
        self.watermark.voter_count()
    }

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
        Ok(record_crc_ending_at(&path, byte_offset)? == Some(expected_crc))
    }

    pub fn replication_epoch(&self) -> u64 {
        self.watermark.epoch()
    }

    /// The newest epoch whose `EpochChange` is in this node's log. Unlike
    /// [`replication_epoch`](Self::replication_epoch), which a node adopts the
    /// moment it learns of a claim, this says what the log's bytes were
    /// written under: it is what a leader needs to know to find where this
    /// node's history stops agreeing with its own.
    pub fn log_epoch(&self) -> u64 {
        self.epochs.lock().last_epoch().unwrap_or(INITIAL_EPOCH)
    }

    /// Where `epoch` ends in this node's log.
    pub fn epoch_extent(&self, epoch: u64) -> crate::replication::epochs::EpochExtent {
        self.epochs.lock().extent(epoch)
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
    /// beginning right after the last event before position `from`: the
    /// control records that follow that event are part of the suffix, because
    /// a follower cut back to the same event (see
    /// [`truncate_to_matching`](Self::truncate_to_matching)) holds none of
    /// this log's. Segment headers are represented by Rotate boundaries, not
    /// data.
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
        // No shortcut when `from` is the tail: a leader that has just taken
        // over rotated to a fresh segment at its head and wrote an EpochChange
        // there, so bytes can follow the last event without any new position.
        // A follower cut back to this boundary needs them; when there really
        // is nothing past the boundary the loop below yields no slice.

        let active_base = writer.active_base_position();
        let active_end = writer.write_offset();
        let seg_list = self.segments.read().clone();
        drop(writer);
        let (first_index, first_offset) =
            match locate_event_anchor(&self.dir, &seg_list.bases, from)? {
                Some(anchor) => (anchor.segment_index, anchor.end_offset),
                None => (0, segment::SEGMENT_HEADER_SIZE as u64),
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
                first_offset
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
            if !result.epoch_changes.is_empty() {
                let mut epochs = self.epochs.lock();
                for change in &result.epoch_changes {
                    epochs.record(change.epoch, change.start_position);
                }
            }
            (result, wave)
        };

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
        // Wake the group-commit thread so the seal is finished promptly
        // even if no further frame arrives for a while.
        self.sync_state.mark_pending();
        self.note_rotation(&writer, old_base);
        Ok(())
    }

    /// After a writer call that may have rotated: extends the segment list
    /// with every rotation since the last call. The sealed indexes are
    /// already in the cache — the writer pins each one in the same step that
    /// moves the active index on (`SegmentWriter::rotate_segment`), which is
    /// what readers observe, so they never wait on this.
    fn note_rotation(&self, writer: &SegmentWriter, old_base: u64) {
        let new_base = writer.active_base_position();
        if new_base == old_base {
            return;
        }
        let mut seg_list = self.segments.write();
        let mut keys = None;
        for seal in writer.pending_seals() {
            if seg_list.bases.last().is_some_and(|last| seal.base > *last) {
                Arc::make_mut(&mut seg_list.bases).push(seal.base);
            }
            // Its keys, until the seal writes its `.keys`.
            if !seg_list.keys.contains_key(&seal.base) {
                keys.get_or_insert_with(|| (*seg_list.keys).clone())
                    .insert(seal.base, SegmentKeys::Pinned(Arc::clone(&seal.index)));
            }
        }
        if let Some(keys) = keys {
            seg_list.keys = Arc::new(keys);
        }
        if seg_list.bases.last().is_some_and(|last| new_base > *last) {
            Arc::make_mut(&mut seg_list.bases).push(new_base);
        }
        seg_list.sealed_count = seg_list.bases.len().saturating_sub(1);
        self.metrics.record_segment_rotation();
    }

    /// Truncates an uncommitted suffix during divergence repair. This is the
    /// sole destructive entry point and will crash rather than cross the
    /// watermark invariant.
    pub fn truncate_to(&self, pos: Position) -> Result<(), Error> {
        self.truncate_to_matching(pos, None)
    }

    /// Cuts the log back to position `pos` and, when the leader supplied its
    /// boundary, to exactly the bytes the leader's log has there.
    ///
    /// The boundary is the last *event* record before `pos`: `expected_event`
    /// is `None` when the caller has no boundary information (position-granular
    /// truncation), `Some(None)` when the leader has no event before `pos`, and
    /// `Some(Some(crc))` with the stored CRC of the leader's last event before
    /// `pos`. The events before `pos` are the history both logs claim to share
    /// (the follower's epoch up to where it ended on the leader, or its
    /// committed prefix), so the follower's last event there must be the same
    /// record: a different one is real corruption, unrecoverable here, and
    /// fails loudly. Control records (watermark checkpoints, epoch changes)
    /// are not compared. They carry no client data and no position, and each
    /// node may hold some the other never received, so the follower is cut
    /// right after that event and drops whatever control records followed it;
    /// the leader sends its own from the same point
    /// ([`replication_catchup_slices`](Self::replication_catchup_slices)).
    /// That can mean reopening a sealed segment, when the last event ends one.
    /// Dropping control records leaves the watermark invariant intact: no
    /// committed event is lost. Where the leader puts `pos` is its decision
    /// (see `replication::service::plan_tail_start`); this only verifies it.
    pub fn truncate_to_matching(
        &self,
        pos: Position,
        expected_event: Option<Option<u32>>,
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
        if pos.0 == old_tail && expected_event.is_none() {
            return Ok(());
        }

        let mut writer = self.writer.lock();
        let mut seg_list = self.segments.write();
        let (target_index, truncate_offset) = match expected_event {
            None => {
                let target_index = match seg_list.bases.binary_search(&pos.0) {
                    Ok(index) => index,
                    Err(0) => {
                        return Err(Error::Corrupted {
                            message: format!("no segment contains truncate position {}", pos.0),
                        });
                    }
                    Err(index) => index - 1,
                };
                let path = segment::segment_path(&self.dir, seg_list.bases[target_index]);
                let preceding = scan_replication_boundary(&path, pos)?;
                (
                    target_index,
                    preceding
                        .last()
                        .map_or(segment::SEGMENT_HEADER_SIZE as u64, |last| last.end_offset),
                )
            }
            Some(expected) => {
                let anchor = locate_event_anchor(&self.dir, &seg_list.bases, pos)?;
                if expected != anchor.map(|anchor| anchor.crc) {
                    return Err(Error::Corrupted {
                        message: format!(
                            "committed prefix diverges from leader at position {} — the \
                             last event before that boundary is not the leader's",
                            pos.0
                        ),
                    });
                }
                match anchor {
                    Some(anchor) => (anchor.segment_index, anchor.end_offset),
                    None => (0, segment::SEGMENT_HEADER_SIZE as u64),
                }
            }
        };
        let target_base = seg_list.bases[target_index];
        let target_path = segment::segment_path(&self.dir, target_base);

        if pos.0 == old_tail
            && target_index + 1 == seg_list.bases.len()
            && truncate_offset == writer.write_offset()
        {
            // Already byte-identical up to the boundary: nothing to heal.
            return Ok(());
        }

        // Seals in flight for the segments about to go: no companion may be
        // written for a file that is deleted or reopened below.
        writer.discard_pending_seals_from(target_base);
        self.seals.cancel_from(target_base);

        // Merged tables cover only segments that end below the watermark,
        // which no truncation reaches.
        debug_assert!(
            seg_list
                .tables
                .last()
                .is_none_or(|t| t.end() <= target_base)
        );
        for &base in &seg_list.bases[target_index + 1..] {
            let path = segment::segment_path(&self.dir, base);
            remove_if_exists(&path)?;
            remove_if_exists(&path.with_extension("idx"))?;
            remove_if_exists(&path.with_extension("keys"))?;
            self.cache.invalidate(base);
        }
        // The containing segment becomes active; its sealed index files no
        // longer describe the truncated file.
        remove_if_exists(&target_path.with_extension("idx"))?;
        remove_if_exists(&target_path.with_extension("keys"))?;
        self.cache.invalidate(target_base);
        let mut keys = (*seg_list.keys).clone();
        keys.retain(|&base, _| base < target_base);
        seg_list.keys = Arc::new(keys);

        writer.reopen_truncated(target_base, truncate_offset, pos)?;
        Arc::make_mut(&mut seg_list.bases).truncate(target_index + 1);
        seg_list.sealed_count = target_index;
        self.local_tail.store(pos.0, Ordering::Release);
        self.durable_tail.store(pos.0, Ordering::Release);
        // An epoch that took over at or after the cut began in a segment that
        // is gone.
        self.epochs.lock().truncate_from(pos.0);
        // The reopened segment may keep its base position, so the cached
        // active mapping cannot be trusted across a truncation — drop it.
        *self.active_mmap.lock() = None;
        drop(seg_list);
        drop(writer);
        Ok(())
    }

    /// The leader's side of [`truncate_to_matching`](Self::truncate_to_matching):
    /// the stored CRC of this log's last event before position `pos`, or
    /// `None` when there is none. Sent with Truncate frames so followers can
    /// verify that the history they keep is this log's.
    pub fn replication_boundary_event(&self, pos: Position) -> Result<Option<u32>, Error> {
        let bases = self.segments.read().bases.clone();
        Ok(locate_event_anchor(&self.dir, &bases, pos)?.map(|anchor| anchor.crc))
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
                let old_base = writer.active_base_position();
                let new_base = writer.head();
                writer.rotate_replicated(new_base)?;
                self.note_rotation(&writer, old_base);
            }
            let wave = self.sync_state.mark_pending();
            let start_position = writer.head().0;
            let old_base = writer.active_base_position();
            writer.write_control(&crate::segment::format::ControlRecord::EpochChange {
                epoch,
                leader_id,
                start_position,
            })?;
            self.note_rotation(&writer, old_base);
            self.epochs.lock().record(epoch, start_position);
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
            let old_base = writer.active_base_position();
            // A record that does not fit rotates the segment, like an event.
            writer.write_control(
                &crate::segment::format::ControlRecord::WatermarkCheckpoint { epoch, position },
            )?;
            self.note_rotation(&writer, old_base);
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
        self.feed.committed();
        Some(Position(watermark))
    }

    /// Follower-side adoption of the claimed leader's computed watermark.
    pub fn adopt_watermark(&self, epoch: u64, pos: Position) {
        if pos.0 > self.local_tail.load(Ordering::Acquire) {
            return; // Never expose bytes this follower has not applied.
        }
        if self.watermark.adopt(epoch, pos.0).is_some() {
            self.feed.committed();
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

    /// True once an fsync failure has poisoned the engine: every pending and
    /// future append fails until restart. The single most important alerting
    /// signal — a poisoned engine looks healthy from /health (the process is
    /// up) while rejecting all writes.
    pub fn is_poisoned(&self) -> bool {
        self.sync_state.is_failed()
    }

    /// Bytes on disk under this context's data directory (segments, index
    /// files, raft log). Walked per call — the metrics scrape
    /// interval, not the hot path.
    pub fn data_dir_bytes(&self) -> u64 {
        fn walk(dir: &Path) -> u64 {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return 0;
            };
            entries
                .flatten()
                .map(|entry| match entry.metadata() {
                    Ok(meta) if meta.is_dir() => walk(&entry.path()),
                    Ok(meta) => meta.len(),
                    Err(_) => 0,
                })
                .sum()
        }
        walk(&self.dir)
    }

    /// Appends events to the store, optionally with a DCB consistency condition.
    ///
    /// 1. Checks DCB condition against the tag index
    /// 2. Writes events to the active segment (tags included on disk)
    /// 3. Updates the in-memory tag index
    /// 4. Advances the head position (next-exclusive)
    ///
    /// Blocks until the write is quorum-durable. The native path uses
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
        self.stage_prepared(prepare_client_append(request)?, timer)
    }

    /// [`append`](Self::append) for events that are already records.
    pub fn append_records(&self, request: RecordAppend) -> Result<AppendResponse, Error> {
        let staged = self.append_records_stage(request)?;
        let wait = match staged.wait_pos() {
            Some(pos) => self.watermark.wait_for(pos),
            None => Ok(()),
        };
        self.append_finalize(staged, wait)
    }

    /// [`append_stage`](Self::append_stage) for events that are already
    /// records.
    pub fn append_records_stage(&self, request: RecordAppend) -> Result<StagedAppend, Error> {
        let timer = Timer::start();
        self.stage_prepared(prepare_client_records(request)?, timer)
    }

    /// Appends on behalf of the server itself, skipping the checks that stop
    /// a client from writing into the `$` namespace. For the subsystems that
    /// own system events; never reachable from a client request.
    pub fn append_system(&self, request: AppendRequest) -> Result<AppendResponse, Error> {
        let staged = self.append_system_stage(request)?;
        let wait = match staged.wait_pos() {
            Some(pos) => self.watermark.wait_for(pos),
            None => Ok(()),
        };
        self.append_finalize(staged, wait)
    }

    /// The synchronous half of [`append_system`]: validates and stamps the
    /// `$` framing, then stages the write. Finish with `append_finish_async`
    /// — the seam for async callers (the snapshot native path) that must not
    /// pin a thread through the durability wait.
    pub fn append_system_stage(&self, request: AppendRequest) -> Result<StagedAppend, Error> {
        self.stage_prepared(prepare_system_append(request)?, Timer::start())
    }

    /// Stages an append that has passed its namespace checks (or is the
    /// server's own, and exempt).
    fn stage_prepared(&self, prepared: Prepared, timer: Timer) -> Result<StagedAppend, Error> {
        self.check_writable()?;

        // The sealed half of the DCB check runs before the lock: it is the
        // half that can wait on the disk, and sealed segments cannot change
        // under it. What seals in between is re-checked under the lock.
        let (outcome, owes_wake) = match self.stage_pre(&prepared)? {
            Pre::Conflict(conflicting_position) => (
                Err(Error::ConsistencyConditionViolated {
                    conflicting_position,
                }),
                false,
            ),
            Pre::Checked(sealed_checked) => {
                // Lock the writer. DCB check + write + index update must be atomic.
                let mut writer = self.writer.lock();
                self.write_staged(&mut writer, &prepared, sealed_checked)
            }
        };
        if owes_wake {
            self.sync_state.wake_sync_thread();
        }

        let staged = StagedAppend { outcome, timer };
        self.release_written(staged.wait_pos());
        Ok(staged)
    }

    /// Fail fast once poisoned or shutting down.
    fn check_writable(&self) -> Result<(), Error> {
        // After an fsync failure nothing written here can be made durable, so
        // accepting the write would lie.
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
        Ok(())
    }

    /// The pre-lock half of an append: the sealed segments' share of the DCB
    /// check, which may read an evicted index from disk.
    fn stage_pre(&self, request: &Prepared) -> Result<Pre, Error> {
        let Some(condition) = &request.condition else {
            return Ok(Pre::Checked(0));
        };
        match self.scan_sealed(condition, 0, true)? {
            (SealedScan::Conflict(position), _) => Ok(Pre::Conflict(position)),
            (_, covered) => Ok(Pre::Checked(covered)),
        }
    }

    /// `stage_pre` for a caller that must not wait on the disk: `None` when
    /// the check needs a sealed index that is not mapped yet.
    fn stage_pre_cached(&self, request: &Prepared) -> Option<Result<Pre, Error>> {
        let Some(condition) = &request.condition else {
            return Some(Ok(Pre::Checked(0)));
        };
        match self.scan_sealed(condition, 0, false) {
            Err(error) => Some(Err(error)),
            Ok((SealedScan::NeedsDisk, _)) => None,
            Ok((SealedScan::Conflict(position), _)) => Some(Ok(Pre::Conflict(position))),
            Ok((SealedScan::Clear, covered)) => Some(Ok(Pre::Checked(covered))),
        }
    }

    /// The writer-lock half of an append: registers the write with the
    /// current commit wave, then checks, writes and indexes it. The caller
    /// holds the writer lock, and owes `wake_sync_thread` after releasing it
    /// when this returns `true` for the second element.
    fn write_staged(
        &self,
        writer: &mut SegmentWriter,
        request: &Prepared,
        sealed_checked: usize,
    ) -> (Result<AppendResponse, Error>, bool) {
        let (_, owes_wake) = self.sync_state.register_write();
        (
            self.append_locked(writer, request, sealed_checked, false),
            owes_wake,
        )
    }

    /// In `written` mode, opens the ack gate for a staged write up to
    /// `wait_pos`.
    fn release_written(&self, wait_pos: Option<u64>) {
        if !crate::written_acks(self.watermark.voter_count()) {
            return;
        }
        let Some(pos) = wait_pos else { return };
        if pos.saturating_sub(self.durable_tail.load(Ordering::Acquire)) <= crate::ack_lag_limit() {
            // Replicated-ack fast path: this node's cursor advances at write
            // (bytes are already pread-visible — the writer write_alls into
            // the file). Quorum-of-one opens the ack gate inline; with more
            // voters the follower cursors still gate the quorum math.
            let _ = self
                .watermark
                .advance(self.node_id, self.watermark.epoch(), pos);
        } else {
            // Past the lag limit the advance is skipped and release waits
            // on the sync loop — durable-mode behavior until the disk
            // catches up. Counted: a rising rate is the disk-is-behind
            // early warning, before clients notice latency.
            self.metrics.record_ack_degradation();
        }
    }

    /// Appends from async code, to the point the events are committed.
    ///
    /// The writer-lock section runs in the first place that can take it
    /// without stalling anyone: on the calling async worker when the writer
    /// lock is free, the DCB check needs nothing from the disk and the write
    /// does not rotate the segment; else on the context's staging thread,
    /// which stages the appends that queued behind a busy lock together; else
    /// (the check needs a cold index, or the thread is gone) on the blocking
    /// pool. No thread is pinned through the durability wait.
    pub async fn append_async(
        self: &Arc<Self>,
        request: AppendRequest,
        system: bool,
    ) -> Result<AppendResponse, Error> {
        self.append_routed(Routes::ALL, request, system).await
    }

    /// [`append_async`](Self::append_async) for a client's events that are
    /// already records: the route that never builds an `AppendEvent`.
    pub async fn append_records_async(
        self: &Arc<Self>,
        request: RecordAppend,
    ) -> Result<AppendResponse, Error> {
        let prepared = prepare_client_records(request)?;
        self.route_prepared(Routes::ALL, prepared).await
    }

    async fn append_routed(
        self: &Arc<Self>,
        routes: Routes,
        request: AppendRequest,
        system: bool,
    ) -> Result<AppendResponse, Error> {
        let prepared = if system {
            prepare_system_append(request)?
        } else {
            prepare_client_append(request)?
        };
        self.route_prepared(routes, prepared).await
    }

    async fn route_prepared(
        self: &Arc<Self>,
        routes: Routes,
        mut prepared: Prepared,
    ) -> Result<AppendResponse, Error> {
        if routes.inline {
            let started = self.inline_breaker.now();
            if self.inline_breaker.allows(started) {
                let staging = self.stage_inline(prepared);
                self.watch_inline_stage(started);
                match staging {
                    Staging::Done(Ok(staged)) => {
                        self.release_written(staged.wait_pos());
                        return self.append_finish_async(staged).await;
                    }
                    Staging::Done(Err(error)) => return Err(error),
                    Staging::NeedsPool(returned) => prepared = returned,
                }
            }
        }
        if routes.thread {
            match self.stage_on_thread(prepared).await {
                Settled::Final(result) => return result,
                Settled::Pool(returned) => prepared = returned,
            }
        }
        let engine = Arc::clone(self);
        let staged =
            tokio::task::spawn_blocking(move || engine.stage_prepared(prepared, Timer::start()))
                .await
                .map_err(|error| Error::Internal {
                    message: format!("append staging worker panicked: {error}"),
                })??;
        self.append_finish_async(staged).await
    }

    /// Judges an inline staging that began at `started` (on the breaker's
    /// clock). One that stalled counts toward the metric, shuts the inline
    /// route for the cooldown and, at most every few seconds, logs.
    fn watch_inline_stage(&self, started: u64) {
        let ended = self.inline_breaker.now();
        let Verdict::Stalled { elapsed_ns, warn } = self.inline_breaker.finished(started, ended)
        else {
            return;
        };
        self.metrics.record_inline_stall();
        if let Some(stalls) = warn {
            tracing::warn!(
                dir = %self.dir.display(),
                elapsed_ms = elapsed_ns as f64 / 1e6,
                stalls_since_last_warning = stalls,
                "an append staged on an async worker stalled; appends use the staging \
                 thread for {:?} (a slow write path, e.g. a throttled page cache or a \
                 filesystem commit)",
                self.inline_breaker.cooldown(),
            );
        }
    }

    /// Answers `reply` with the final result of `staged` once the watermark
    /// covers it: the staging thread's way to settle an append without the
    /// caller's task waking twice.
    fn settle_when_committed(
        &self,
        staged: StagedAppend,
        reply: tokio::sync::oneshot::Sender<Settled>,
    ) {
        let metrics = Arc::clone(&self.metrics);
        match staged.wait_pos() {
            None => {
                let _ = reply.send(Settled::Final(finalize_append(&metrics, staged, Ok(()))));
            }
            Some(pos) => self.watermark.notify_at(pos, move |verdict| {
                let _ = reply.send(Settled::Final(finalize_append(&metrics, staged, verdict)));
            }),
        }
    }

    /// The staging that neither reads the disk for the DCB check nor waits
    /// for the writer lock nor rotates the segment: for an async worker.
    /// Anything else is handed back as `NeedsPool` with the append intact.
    /// Does not open the written-ack gate: the caller does.
    fn stage_inline(&self, prepared: Prepared) -> Staging {
        let timer = Timer::start();
        if let Err(error) = self.check_writable() {
            return Staging::Done(Err(error));
        }
        let pre = match self.stage_pre_cached(&prepared) {
            None => return Staging::NeedsPool(prepared),
            Some(Err(error)) => return Staging::Done(Err(error)),
            Some(Ok(pre)) => pre,
        };
        let (outcome, owes_wake) = match pre {
            Pre::Conflict(conflicting_position) => (
                Err(Error::ConsistencyConditionViolated {
                    conflicting_position,
                }),
                false,
            ),
            Pre::Checked(sealed_checked) => {
                let Some(mut writer) = self.writer.try_lock() else {
                    return Staging::NeedsPool(prepared);
                };
                // A write that rotates the segment creates and preallocates a
                // file: not for an async worker.
                if !writer.fits_without_rotation(&prepared.batch) {
                    drop(writer);
                    return Staging::NeedsPool(prepared);
                }
                self.write_staged(&mut writer, &prepared, sealed_checked)
            }
        };
        if owes_wake {
            self.sync_state.wake_sync_thread();
        }
        Staging::Done(Ok(StagedAppend { outcome, timer }))
    }

    /// Stages `appends`, in order, for the staging thread: one answer per
    /// append, in the same order.
    ///
    /// What each needs before the lock (the sealed half of its DCB check,
    /// from memory only) is settled first, for all of them. The rest are
    /// written under as few holds of the writer lock as the group limits
    /// allow, each hold with one `write(2)` for everything it staged: the
    /// appends go through the writer deferred, each seeing the ones before it
    /// in its DCB check, and the bytes reach the file before the lock is
    /// let go. Nothing is published to readers before that: the local tail
    /// moves after the write, and the watermark, which readers are bounded
    /// by, only moves after that.
    ///
    /// A write that fails after other appends were accounted for leaves the
    /// log and the writer disagreeing, so it poisons the engine, like a
    /// failed fsync, and fails the appends it took with it.
    fn stage_group(&self, appends: Vec<Prepared>) -> Vec<Staging> {
        let mut answers: Vec<Option<Staging>> = (0..appends.len()).map(|_| None).collect();
        let mut writes: Vec<(usize, Prepared, usize, Timer)> = Vec::with_capacity(appends.len());
        for (at, prepared) in appends.into_iter().enumerate() {
            let timer = Timer::start();
            if let Err(error) = self.check_writable() {
                answers[at] = Some(Staging::Done(Err(error)));
                continue;
            }
            answers[at] = match self.stage_pre_cached(&prepared) {
                None => Some(Staging::NeedsPool(prepared)),
                Some(Err(error)) => Some(Staging::Done(Err(error))),
                Some(Ok(Pre::Conflict(conflicting_position))) => {
                    Some(Staging::Done(Ok(StagedAppend {
                        outcome: Err(Error::ConsistencyConditionViolated {
                            conflicting_position,
                        }),
                        timer,
                    })))
                }
                Some(Ok(Pre::Checked(sealed_checked))) => {
                    writes.push((at, prepared, sealed_checked, timer));
                    None
                }
            };
        }

        let mut writes = writes.into_iter().peekable();
        while writes.peek().is_some() {
            let mut hold = Vec::new();
            let (mut events, mut bytes) = (0u32, 0usize);
            while let Some((_, prepared, _, _)) = writes.peek() {
                if !hold.is_empty()
                    && (hold.len() >= GROUP_MAX_APPENDS
                        || events + prepared.batch.len() > GROUP_MAX_EVENTS
                        || bytes + prepared.batch.bytes().len() > GROUP_MAX_BYTES)
                {
                    break;
                }
                events += prepared.batch.len();
                bytes += prepared.batch.bytes().len();
                hold.push(writes.next().expect("peeked"));
            }
            self.write_group(hold, &mut answers);
        }
        answers
            .into_iter()
            .map(|answer| answer.expect("every append was answered"))
            .collect()
    }

    /// One hold of the writer lock for `group`: see [`stage_group`](Self::stage_group).
    fn write_group(
        &self,
        group: Vec<(usize, Prepared, usize, Timer)>,
        answers: &mut [Option<Staging>],
    ) {
        let mut results: Vec<(usize, Result<AppendResponse, Error>, Timer)> =
            Vec::with_capacity(group.len());
        let mut owes_wake = false;
        let flushed = {
            let mut writer = self.writer.lock();
            for (at, prepared, sealed_checked, timer) in group {
                let (_, first_of_wave) = self.sync_state.register_write();
                owes_wake |= first_of_wave;
                results.push((
                    at,
                    self.append_locked(&mut writer, &prepared, sealed_checked, true),
                    timer,
                ));
            }
            let flushed = writer.flush_deferred();
            // A writer that failed to write an earlier hold has a head that
            // counts bytes the file never got.
            if flushed.is_ok() && !writer.is_broken() {
                self.local_tail.store(writer.head().0, Ordering::Release);
            }
            flushed
        };
        if owes_wake {
            self.sync_state.wake_sync_thread();
        }
        if let Err(error) = &flushed {
            tracing::error!(
                %error,
                "writing a group of appends FAILED — poisoning event store; \
                 the log and the writer no longer agree and new appends will be rejected"
            );
            self.sync_state.fail_sync(error);
            self.watermark.abort_all(&error.to_string());
        }
        for (at, outcome, timer) in results {
            let outcome = match &flushed {
                // Whatever this hold decided rested on bytes that never
                // reached the file, a conflict with one of them included.
                Err(error) => Err(Error::Io(io::Error::other(format!(
                    "append was not written, event store poisoned: {error}"
                )))),
                Ok(()) => outcome,
            };
            answers[at] = Some(Staging::Done(Ok(StagedAppend { outcome, timer })));
        }
    }

    /// Queues the append for this context's staging thread and waits for its
    /// final answer.
    async fn stage_on_thread(self: &Arc<Self>, prepared: Prepared) -> Settled {
        let tx = self.stage_tx.get_or_init(spawn_stage_thread);
        let (reply, answer) = tokio::sync::oneshot::channel();
        let job = StageJob {
            engine: Arc::clone(self),
            prepared,
            reply,
        };
        if let Err(std::sync::mpsc::SendError(job)) = tx.send(job) {
            return Settled::Pool(job.prepared);
        }
        match answer.await {
            Ok(settled) => settled,
            Err(_) => Settled::Final(Err(Error::Internal {
                message: "the append staging thread exited with a request in flight".into(),
            })),
        }
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
        finalize_append(&self.metrics, staged, wait)
    }

    /// The sealed-segment half of a DCB check, run BEFORE the writer lock:
    /// sealed segments are immutable, so their answer cannot change while the
    /// lock is not held. Returns the verdict and how many sealed segments were
    /// covered; `check_dcb_locked` re-checks any segment sealed since, under
    /// the lock, from its pinned index.
    ///
    /// Only what lies at or after the marker is scanned, from the start of
    /// sealed segment `from_sealed`: the keys covering that range (usually
    /// just the newest sealed segment's, or none) are asked for the first
    /// match at or after the marker. Every sealed segment's keys are mapped
    /// or pinned, so this reads no file; with `disk` false, keys that would
    /// have to be opened end the scan with `NeedsDisk` instead.
    fn scan_sealed(
        &self,
        condition: &AppendCondition,
        from_sealed: usize,
        disk: bool,
    ) -> Result<(SealedScan, usize), Error> {
        let marker = condition.consistency_marker.0;
        let list = self.segments.read().clone();
        let sealed_count = list.sealed_count;
        let sealed_end = list.bases[sealed_count];
        let start = marker.max(list.bases[from_sealed.min(sealed_count)]);
        if start >= sealed_end {
            return Ok((SealedScan::Clear, sealed_count));
        }
        let probe = Probe::new(&condition.criteria);
        for source in list.sources(start, sealed_end) {
            let (lo, hi) = source.range();
            let found = self
                .view
                .with_resolved(&list, &source, &probe, disk, |resolved| {
                    Ok(resolved.first_at_or_after(lo)?.filter(|&p| p < hi))
                })?;
            match found {
                None => return Ok((SealedScan::NeedsDisk, sealed_count)),
                Some(Some(position)) => {
                    self.metrics.record_dcb_violation();
                    return Ok((SealedScan::Conflict(Position(position)), sealed_count));
                }
                Some(None) => {}
            }
        }
        Ok((SealedScan::Clear, sealed_count))
    }

    /// The half of a DCB check that must run under the writer lock: any
    /// segment sealed since the pre-lock scan (its index is pinned, so this
    /// never touches the disk) and the active segment.
    fn check_dcb_locked(
        &self,
        condition: &AppendCondition,
        sealed_checked: usize,
    ) -> Result<Option<Position>, Error> {
        if let (SealedScan::Conflict(position), _) =
            self.scan_sealed(condition, sealed_checked, true)?
        {
            return Ok(Some(position));
        }
        let conflict = self
            .active_index
            .read()
            .has_match_after(&condition.criteria, condition.consistency_marker.0);
        if conflict.is_some() {
            self.metrics.record_dcb_violation();
        }
        Ok(conflict)
    }

    /// The atomic core of an append: DCB check + write + index/head update,
    /// all under the caller-held writer lock. NEVER fsyncs — durability is
    /// the sync thread's job after the wave seals.
    fn append_locked(
        &self,
        writer: &mut SegmentWriter,
        request: &Prepared,
        sealed_checked: usize,
        defer: bool,
    ) -> Result<AppendResponse, Error> {
        {
            // Step 1: Check DCB condition (the sealed half ran pre-lock).
            if let Some(condition) = &request.condition
                && let Some(conflicting_pos) = self.check_dcb_locked(condition, sealed_checked)?
            {
                return Err(Error::ConsistencyConditionViolated {
                    conflicting_position: conflicting_pos,
                });
            }

            if request.batch.is_empty() {
                let head = writer.head();
                return Ok(AppendResponse {
                    first_position: head,
                    count: 0,
                    // A deferred group has not moved the local tail yet; the
                    // writer's head counts the appends before this one.
                    consistency_marker: if defer {
                        head
                    } else {
                        Position(self.local_tail.load(Ordering::Acquire))
                    },
                });
            }

            let old_active_base = writer.active_base_position();

            // Step 2: Write events without fsync. Durability is the sync
            // thread's wave fsync; callers wait on the watermark.
            let (first_position, count) = if defer {
                writer.write_batch_deferred(&request.batch)?
            } else {
                writer.write_batch(&request.batch)?
            };

            // Step 2b: Detect rotation — extend the segment list. The writer
            // indexed the events as it wrote them and pinned any sealed index.
            self.note_rotation(writer, old_active_base);

            // Step 3: Advance the local tail (next-exclusive: first event's
            // position + count = position the next event will land at).
            // Watermark publication — and with it the subscriber wakeup —
            // happens on the sync thread after the wave's fsync.
            let new_head = first_position.0 + count as u64;
            if !defer {
                self.local_tail.store(new_head, Ordering::Release);
            }

            Ok(AppendResponse {
                first_position,
                count,
                consistency_marker: Position(new_head),
            })
        }
    }

    /// The index that answers for segment `base`; see [`LogView::index_for`].
    fn index_for(&self, base: u64, seg_path: &Path) -> Result<IndexRef<'_>, Error> {
        self.view.index_for(base, seg_path)
    }

    /// The reader matching `index_for`'s answer: the active segment is read
    /// through its mapping with a CRC check per read; a sealed segment through
    /// the block cache, whose blocks were verified when they were loaded (or
    /// through its mapping, with a check per read, in `ReadMode::Mapped`).
    fn reader_for(
        &self,
        index: &IndexRef<'_>,
        base: u64,
        seg_path: &Path,
    ) -> Result<RecordReader, Error> {
        match index {
            IndexRef::Active(_) => Ok(RecordReader::Mapped(
                self.active_segment_reader(base, seg_path)?,
            )),
            IndexRef::Sealed(sealed) => match self.read_mode {
                ReadMode::Blocks => Ok(RecordReader::Sealed(self.cache.sealed_reader(
                    seg_path,
                    base,
                    Arc::clone(sealed),
                )?)),
                ReadMode::Mapped => Ok(RecordReader::Mapped(SegmentReader::from_shared_mmap(
                    self.cache.get_mmap(seg_path, base)?,
                )?)),
            },
        }
    }

    /// Whether any event in `[from, to)` could match `condition`: a rank
    /// check per source, no set built, no read. A subscriber woken by a
    /// commit asks this before paging; with tag criteria the answer is
    /// usually no, and the wakeup costs a few lookups.
    pub fn has_matches_between(
        &self,
        from: Position,
        to: Position,
        condition: &SourcingCondition,
    ) -> bool {
        let to = to.0.min(self.watermark.get());
        if from.0 >= to {
            return false;
        }
        let list = self.segments.read().clone();
        let probe = Probe::new(condition);
        for source in list.sources(from.0, to) {
            let (lo, hi) = source.range();
            let possible = self
                .view
                .with_resolved(&list, &source, &probe, true, |r| r.any_in(lo, hi));
            match possible {
                Ok(Some(false)) => {}
                // A match, or cannot tell: let the read decide.
                Ok(Some(true)) | Ok(None) | Err(_) => return true,
            }
        }
        false
    }

    /// Gets tags for an event at the given position by reading from the segment.
    pub fn get_tags(&self, position: Position) -> Result<Vec<Tag>, Error> {
        let head = self.watermark.get();
        if position.0 >= head {
            return Err(Error::PositionNotFound {
                position: position.0,
            });
        }

        // Use cached segment list instead of readdir.
        let seg_list = self.segments.read().clone();
        let seg_idx = match seg_list.bases.binary_search(&position.0) {
            Ok(i) => i,
            Err(0) => {
                // Before the first segment: truncated away.
                return Err(Error::PositionNotFound {
                    position: position.0,
                });
            }
            Err(i) => i - 1,
        };

        let base = seg_list.bases[seg_idx];
        let seg_path = segment::segment_path(&self.dir, base);

        let index = self.index_for(base, &seg_path)?;
        let Some(offset) = index.offset_of(position.0)? else {
            return Err(Error::Corrupted {
                message: format!("event at position {} not found in segment", position.0),
            });
        };
        let mut reader = self.reader_for(&index, base, &seg_path)?;
        let mut event = reader.read_event_at(position.0, offset)?;
        // A system event reports no tags rather than an error: an error
        // would itself tell the caller what lives there.
        if crate::system::is_system_name(&event.name) {
            return Ok(Vec::new());
        }
        crate::system::strip_system_tags(&mut event.tags);
        Ok(event.tags)
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
                // First *readable* event: clients turn this position into a
                // replay cursor, and it should land on an event they can
                // actually receive, not on hidden system bookkeeping.
                if event.timestamp >= timestamp_millis
                    && !crate::system::is_system_name(&event.name)
                {
                    return Ok(Some(event.position));
                }
            }
        }

        Ok(None)
    }

    /// Creates a live event stream subscription. Delivery is bounded by the
    /// watermark — subscribers only ever see quorum-committed events.
    pub fn subscribe(
        &self,
        from_position: Position,
        condition: SourcingCondition,
        exclude_names: &[String],
    ) -> EventStream {
        EventStream::new(
            condition,
            from_position,
            exclude_names,
            Arc::clone(&self.feed),
            &self.view,
        )
    }

    /// Limits how many live chunks (and bytes) the subscription dispatcher
    /// keeps for streams at the live tail. A hook for tests of streams that
    /// fall behind it; the defaults suit everything else.
    #[doc(hidden)]
    pub fn limit_live_ring(&self, chunks: usize, bytes: usize) {
        self.feed.limit_ring(chunks, bytes);
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

    /// The head as clients see it: the position after the last event a client
    /// can actually read.
    ///
    /// System events occupy positions but are never returned by the client
    /// read path, so a client whose cursor has drained the log would otherwise
    /// sit permanently short of `head()` — reporting phantom lag, and hanging
    /// any "poll until caught up" loop forever. This is the number to expose
    /// over the wire; `head()` stays the true head for the watermark, read
    /// bounds, and DCB markers, all of which must account for every position.
    ///
    /// Resolved from the tag index rather than by decoding events, and the
    /// walk stops at the first visible position — normally the first one
    /// tested, since system events are rare and rarely trail the log.
    pub fn visible_head(&self) -> Position {
        let head = self.watermark.get();
        if head == 0 {
            return Position(0);
        }

        let list = self.segments.read().clone();
        let probe = Probe::new(&crate::system::marker_condition());
        for source in list.sources(0, head).iter().rev() {
            let (lo, hi) = source.range();
            // The last position of the source that is not a system event.
            let visible = self
                .view
                .with_resolved(&list, source, &probe, true, |system| {
                    let mut position = hi - 1;
                    while system.first_at_or_after(position)? == Some(position) {
                        if position == lo {
                            return Ok(None);
                        }
                        position -= 1;
                    }
                    Ok(Some(position))
                });
            match visible {
                Ok(Some(Some(position))) => return Position(position + 1),
                Ok(Some(None)) => {}
                // Unreadable index: fall back to the true head rather
                // than under-reporting and stranding a client's cursor.
                Ok(None) | Err(_) => return Position(head),
            }
        }

        // Every position is a system event — nothing visible has ever landed.
        Position(0)
    }

    /// Highest position strictly below `below` matching `condition`, or
    /// `None` if nothing matches. The mirror image of the DCB primitive
    /// (`find_matching_after`), resolved newest-first with key-filter
    /// skips: the expected caller — the snapshot lookup — almost always
    /// finds its answer in the newest segment or two, so the walk usually
    /// probes once and stops.
    pub(crate) fn latest_matching(
        &self,
        condition: &SourcingCondition,
        below: Position,
    ) -> Result<Option<Position>, Error> {
        let bound = self.watermark.get().min(below.0);
        if bound == 0 {
            return Ok(None);
        }

        let list = self.segments.read().clone();
        let probe = Probe::new(condition);
        for source in list.sources(0, bound).iter().rev() {
            let (lo, hi) = source.range();
            let found = self.view.with_resolved(&list, source, &probe, true, |r| {
                Ok(r.last_before(hi)?.filter(|&p| p >= lo))
            })?;
            if let Some(Some(position)) = found {
                return Ok(Some(Position(position)));
            }
        }
        Ok(None)
    }

    /// Reads the stored event at a known position, tags and payload intact.
    /// No visibility filter — the callers (the snapshot lookup) own the
    /// records they read; nothing serving a client query may call this.
    pub(crate) fn read_stored_at(&self, position: Position) -> Result<StoredEvent, Error> {
        let head = self.watermark.get();
        if position.0 >= head {
            return Err(Error::PositionNotFound {
                position: position.0,
            });
        }

        let seg_list = self.segments.read().clone();
        let seg_idx = match seg_list.bases.binary_search(&position.0) {
            Ok(i) => i,
            Err(0) => {
                // Before the first segment: truncated away.
                return Err(Error::PositionNotFound {
                    position: position.0,
                });
            }
            Err(i) => i - 1,
        };

        let base = seg_list.bases[seg_idx];
        let seg_path = segment::segment_path(&self.dir, base);

        let index = self.index_for(base, &seg_path)?;
        let Some(offset) = index.offset_of(position.0)? else {
            return Err(Error::Corrupted {
                message: format!("event at position {} not found in segment", position.0),
            });
        };
        self.reader_for(&index, base, &seg_path)?
            .read_event_at(position.0, offset)
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
    /// For sealed segments: uses their mapped indexes (key filter first).
    /// For the active segment: uses the in-memory tag index.
    ///
    /// No readdir or stat syscalls — segment list and sealed status are cached.
    pub fn source(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
    ) -> Result<Vec<SequencedEvent>, Error> {
        let timer = Timer::start();
        crate::system::validate_client_condition(condition)?;
        let mut events = Vec::new();
        self.walk_matching(from_position, condition, None, |_, record| {
            let (stored, _) = format::deserialize_event(record.body)?;
            if !crate::system::is_system_name(&stored.name) {
                events.push(stored.into_sequenced());
            }
            Ok(Walk::Next)
        })?;
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
        self.source_stored_bounded(from_position, condition, None, limit, Visibility::Client)
    }

    /// Like `source_stored`, but on the internal read path: system events are
    /// visible and system tags intact. For the subsystems that own those
    /// events — never for anything serving a client request.
    pub fn source_internal(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, Error> {
        self.source_stored_bounded(from_position, condition, None, limit, Visibility::Internal)
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
        let stored = self.source_stored_bounded(
            from_position,
            condition,
            Some(up_to),
            limit,
            Visibility::Client,
        )?;
        Ok(stored.into_iter().map(|e| e.into_sequenced()).collect())
    }

    /// Bounded variant of `source_stored`: up to `limit` matching events with
    /// position in `[from_position, min(head, up_to))`, tags included.
    ///
    /// `visibility` selects the read path. Under `Client`, system events are
    /// skipped and system tags stripped — and because the skip happens before
    /// the event is counted, `limit` still means "up to `limit` events the
    /// caller can see", which is what keeps chunked callers from mistaking a
    /// filtered page for the end of the log.
    pub(crate) fn source_stored_bounded(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
        up_to: Option<Position>,
        limit: usize,
        visibility: Visibility,
    ) -> Result<Vec<StoredEvent>, Error> {
        if visibility.hides_system() {
            crate::system::validate_client_condition(condition)?;
        }
        let hides_system = visibility.hides_system();
        let mut events = Vec::new();
        self.walk_matching(from_position, condition, up_to, |_, record| {
            let (mut stored, _) = format::deserialize_event(record.body)?;
            if hides_system {
                // Skipped before it is counted, so `limit` stays "events the
                // caller can see" and a filtered page never looks like the end.
                if crate::system::is_system_name(&stored.name) {
                    return Ok(Walk::Next);
                }
                crate::system::strip_system_tags(&mut stored.tags);
            }
            events.push(stored);
            Ok(if events.len() >= limit {
                Walk::Stop
            } else {
                Walk::Next
            })
        })?;
        Ok(events)
    }

    /// Copies up to `limit` matching client-visible records with position in
    /// `[from_position, min(head, up_to))` onto `out` in their wire layout
    /// (`len | flags | body`), stopping early once `out` grew by `max_bytes`.
    /// Events named in `exclude_names` are skipped without being counted.
    ///
    /// The read path of the native protocol: a record is CRC-checked and
    /// copied, never decoded, unless it carries a system tag to strip.
    pub fn read_records(
        &self,
        condition: &SourcingCondition,
        page: &PageBounds<'_>,
        out: &mut Vec<u8>,
    ) -> Result<RecordPage, Error> {
        crate::system::validate_client_condition(condition)?;
        let start = out.len();
        let bounds = page;
        let mut page = RecordPage::default();
        let mut rewrite = Vec::new();
        self.walk_matching(
            bounds.from,
            condition,
            Some(bounds.up_to),
            |position, record| {
                // Skipped events still move the page along.
                page.scanned_to = Some(Position(position));
                let peek = format::peek(record.body)?;
                if peek.is_system_event()
                    || bounds
                        .exclude_names
                        .iter()
                        .any(|n| n.as_bytes() == peek.name)
                {
                    return Ok(Walk::Next);
                }
                copy_client_record(out, &record, peek.system_tags, &mut rewrite)?;
                page.count += 1;
                if page.count == 1 {
                    // The first record sizes the page: one allocation for
                    // the rest instead of doubling a small buffer up to it.
                    let record = out.len() - start;
                    let rest = bounds.limit.min(MAX_PAGE_RESERVE_RECORDS).saturating_sub(1);
                    out.reserve(
                        record
                            .saturating_mul(rest)
                            .min(bounds.max_bytes.saturating_sub(record))
                            .min(MAX_PAGE_RESERVE_BYTES),
                    );
                }
                page.last_position = Some(Position(position));
                page.full =
                    page.count as usize >= bounds.limit || out.len() - start >= bounds.max_bytes;
                Ok(if page.full { Walk::Stop } else { Walk::Next })
            },
        )?;
        Ok(page)
    }

    /// Visits every record matching `condition` with position in
    /// `[from, min(watermark, up_to))`, in log order, CRC-checked, until the
    /// visitor stops. The one segment walk every read is built on.
    ///
    /// Offsets are collected under the index's lock a chunk at a time and the
    /// records read after it is released, so a long read never holds up the
    /// writer, which needs the active index to append.
    fn walk_matching(
        &self,
        from: Position,
        condition: &SourcingCondition,
        up_to: Option<Position>,
        mut visit: impl FnMut(u64, crate::segment::reader::StoredRecord<'_>) -> Result<Walk, Error>,
    ) -> Result<(), Error> {
        let head = match up_to {
            Some(bound) => self.watermark.get().min(bound.0),
            None => self.watermark.get(),
        };
        let list = self.segments.read().clone();
        if list.bases.is_empty() || head == 0 {
            return Ok(());
        }
        let probe = Probe::new(condition);
        let mut locate = Locate::new(self, &list);
        self.view.for_each_match(
            &list,
            &probe,
            from.0,
            head,
            |ruled_out| self.metrics.record_key_table_check(ruled_out),
            |set| {
                self.metrics.record_direct_seek();
                locate.each_record(set.iter(), &mut visit)
            },
        )?;
        Ok(())
    }

    /// Plans a read without reading it: where every event matching
    /// `condition` with position in `[from, min(watermark, up_to))` is, up
    /// to `limit` of them, in position order (ADR-0010).
    ///
    /// Each location names the segment, the event's position, and the
    /// exact byte range of its record in the segment file (header
    /// included), found from the index alone: key tables for which
    /// positions match, the segments' `.idx` locators for where they are.
    /// No segment body is read, so a cold read can fetch exactly these
    /// bytes from wherever the segment lives. The one exception is the last
    /// event written to the active segment, whose end the index does not
    /// know yet; its record header is read from the local file, which an
    /// active segment always is.
    ///
    /// Visibility is the caller's to apply after fetching, as the local
    /// read path does: system events and excluded names are in the plan.
    pub fn plan_read(
        &self,
        condition: &SourcingCondition,
        from: Position,
        up_to: Position,
        limit: usize,
    ) -> Result<ReadPlan, Error> {
        let head = self.watermark.get().min(up_to.0);
        let mut plan = ReadPlan::default();
        if from.0 >= head {
            plan.complete = true;
            return Ok(plan);
        }
        if limit == 0 {
            return Ok(plan);
        }
        let list = self.segments.read().clone();
        let probe = Probe::new(condition);
        let mut locate = Locate::new(self, &list);
        let walked = self.view.for_each_match(
            &list,
            &probe,
            from.0,
            head,
            |_| {},
            |set| {
                locate.each_location(set.iter(), |location| {
                    plan.records.push(location);
                    Ok(if plan.records.len() >= limit {
                        Walk::Stop
                    } else {
                        Walk::Next
                    })
                })
            },
        )?;
        plan.complete = matches!(walked, Walk::Next);
        Ok(plan)
    }

    /// Runs index merges until the final segments have every merged table
    /// they should (ADR-0010), in this thread, and says what they cost.
    /// The background merger does the same on its own; this is for tools
    /// and tests, and for stores opened with background merges off.
    pub fn merge_index(&self) -> Result<MergeTotals, Error> {
        self.merge.run_to_completion()
    }

    /// One merge step, stopped where `at` says: what a crash there leaves.
    #[doc(hidden)]
    pub fn merge_index_step(
        &self,
        at: impl FnMut(crate::log_index::MergeStep) -> bool,
    ) -> Result<bool, Error> {
        Ok(self.merge.step(at)?.is_some())
    }

    /// The shape and size of the log's index right now.
    pub fn index_stats(&self) -> IndexStats {
        let list = self.segments.read().clone();
        let mut stats = IndexStats {
            merged_tables: list.tables.len(),
            merged_bytes: list.tables.iter().map(|t| t.file_len()).sum(),
            merged_until: list.tables.last().map_or(0, |t| t.end()),
            merge_totals: self.merge.totals(),
            rebuilt_at_open: self.rebuilt_at_open,
            ..IndexStats::default()
        };
        for keys in list.keys.values() {
            stats.segment_tables += 1;
            if let SegmentKeys::Mapped(table) = keys {
                stats.segment_table_bytes += table.file_len();
            }
        }
        stats
    }
}

/// Maps positions to their segments' records, keeping the segment it is in
/// at hand: a merged table's matches run across many segments.
struct Locate<'a> {
    engine: &'a EventStoreEngine,
    list: &'a SegmentList,
    reader: Option<(usize, RecordReader)>,
    offsets: Vec<(u64, u64)>,
}

impl<'a> Locate<'a> {
    /// Offsets are collected under the segment's index a chunk at a time and
    /// the records read after it is released, so a long read never holds up
    /// the writer, which needs the active index to append.
    const CHUNK: usize = 256;

    fn new(engine: &'a EventStoreEngine, list: &'a SegmentList) -> Self {
        Self {
            engine,
            list,
            reader: None,
            offsets: Vec::with_capacity(Self::CHUNK),
        }
    }

    /// The segment holding `position`, and where it ends.
    fn segment_of(&self, position: u64) -> Result<(usize, u64, u64), Error> {
        let index = self
            .list
            .bases
            .partition_point(|&b| b <= position)
            .checked_sub(1)
            .ok_or(Error::PositionNotFound { position })?;
        Ok((index, self.list.bases[index], self.list.end_of(index)))
    }

    /// Visits the record of every position, in order, until `visit` stops.
    fn each_record(
        &mut self,
        positions: impl Iterator<Item = u64>,
        visit: &mut impl FnMut(u64, crate::segment::reader::StoredRecord<'_>) -> Result<Walk, Error>,
    ) -> Result<Walk, Error> {
        let mut positions = positions.peekable();
        while let Some(&first) = positions.peek() {
            let (segment, base, end) = self.segment_of(first)?;
            let path = segment::segment_path(&self.engine.dir, base);
            self.offsets.clear();
            {
                let index = self.engine.index_for(base, &path)?;
                while let Some(&position) = positions.peek() {
                    if position >= end || self.offsets.len() == Self::CHUNK {
                        break;
                    }
                    positions.next();
                    if let Some(offset) = index.offset_of(position)? {
                        self.offsets.push((position, offset));
                    }
                }
                if !matches!(self.reader, Some((at, _)) if at == segment) {
                    self.reader = Some((segment, self.engine.reader_for(&index, base, &path)?));
                }
            }
            let reader = &mut self.reader.as_mut().unwrap().1;
            for &(position, offset) in &self.offsets {
                if let Walk::Stop = visit(position, reader.record_at(position, offset)?)? {
                    return Ok(Walk::Stop);
                }
            }
        }
        Ok(Walk::Next)
    }

    /// Calls `found` with the location of every position, in order, from
    /// the index alone, until it stops.
    fn each_location(
        &mut self,
        positions: impl Iterator<Item = u64>,
        mut found: impl FnMut(RecordLocation) -> Result<Walk, Error>,
    ) -> Result<Walk, Error> {
        let mut positions = positions.peekable();
        let mut located = Vec::with_capacity(Self::CHUNK);
        while let Some(&first) = positions.peek() {
            let (_, base, end) = self.segment_of(first)?;
            let path = segment::segment_path(&self.engine.dir, base);
            located.clear();
            let mut unknown_end = None;
            {
                let index = self.engine.index_for(base, &path)?;
                while let Some(&position) = positions.peek() {
                    if position >= end || located.len() == Self::CHUNK {
                        break;
                    }
                    positions.next();
                    let (Some(offset), len) =
                        (index.offset_of(position)?, index.record_len(position)?)
                    else {
                        continue;
                    };
                    if len.is_none() {
                        unknown_end = Some(located.len());
                    }
                    located.push(RecordLocation {
                        segment_base: base,
                        position,
                        offset,
                        len: len.unwrap_or(0),
                    });
                }
            }
            if let Some(at) = unknown_end {
                // The last event of the active segment: its own header says.
                let reader = self.engine.active_segment_reader(base, &path)?;
                let location = &mut located[at];
                location.len = reader.record_len_at(location.offset)?;
            }
            for location in located.drain(..) {
                if let Walk::Stop = found(location)? {
                    return Ok(Walk::Stop);
                }
            }
        }
        Ok(Walk::Next)
    }
}

/// Where one event's record is: what a cold read fetches (ADR-0010).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordLocation {
    /// The segment holding it, by its base position.
    pub segment_base: u64,
    pub position: u64,
    /// Byte offset of the record in the segment file.
    pub offset: u64,
    /// Exact length of the record, its header included.
    pub len: u32,
}

/// The records a read would return, located: see
/// [`EventStoreEngine::plan_read`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadPlan {
    pub records: Vec<RecordLocation>,
    /// Whether the plan reached the end of the range. `false` when it
    /// stopped at the limit, whether or not anything follows.
    pub complete: bool,
}

/// The shape and size of a log's index (ADR-0010).
#[derive(Debug, Clone, Copy, Default)]
pub struct IndexStats {
    pub merged_tables: usize,
    pub merged_bytes: u64,
    /// The position merged tables reach (they cover a prefix of the log).
    pub merged_until: u64,
    /// Sealed segments with their own key table (merged tables do not
    /// cover them yet).
    pub segment_tables: usize,
    pub segment_table_bytes: u64,
    pub merge_totals: MergeTotals,
    /// Index files the last open rebuilt from their segments: zero unless
    /// some were missing or damaged.
    pub rebuilt_at_open: usize,
}

impl Drop for EventStoreEngine {
    fn drop(&mut self) {
        // Stops the subscription dispatcher and lets live streams notice.
        self.feed.close();
        // A merge stops at its next key and leaves no file behind.
        self.merge.stop();
        if let Some(merger) = self.merger.lock().unwrap().take() {
            let _ = merger.join();
        }
    }
}

/// Appends a stored record to `out` the way a client may see it, framed as
/// `len u32 | flags u8 | body`: exactly as stored, unless it carries system
/// tags, which are stripped (the record is then re-serialized, using
/// `scratch`). The caller has already judged the event itself visible.
pub(crate) fn copy_client_record(
    out: &mut Vec<u8>,
    record: &crate::segment::reader::StoredRecord<'_>,
    system_tags: bool,
    scratch: &mut Vec<u8>,
) -> Result<(), Error> {
    if system_tags {
        let (mut stored, _) = format::deserialize_event(record.body)?;
        crate::system::strip_system_tags(&mut stored.tags);
        scratch.clear();
        format::serialize_event(&stored, scratch);
        out.extend_from_slice(&((1 + scratch.len()) as u32).to_le_bytes());
        out.push(record.flags);
        out.extend_from_slice(scratch);
    } else {
        out.extend_from_slice(record.framed);
    }
    Ok(())
}

/// Most records `read_records` reserves room for once it has seen the first.
const MAX_PAGE_RESERVE_RECORDS: usize = 1024;
/// Most bytes it reserves that way.
const MAX_PAGE_RESERVE_BYTES: usize = 1024 * 1024;

/// What a read's visitor wants next.
enum Walk {
    Next,
    Stop,
}

/// Which records one page of `read_records` covers.
#[derive(Debug, Clone, Copy)]
pub struct PageBounds<'a> {
    /// First position to consider.
    pub from: Position,
    /// Positions at and past this are left out (the read's frozen head).
    pub up_to: Position,
    /// Most records to copy.
    pub limit: usize,
    /// Stop once this many bytes were copied; at least one record always is.
    pub max_bytes: usize,
    /// Event types to skip, without counting them against `limit`.
    pub exclude_names: &'a [String],
}

impl PageBounds<'_> {
    pub fn new(from: Position, up_to: Position) -> Self {
        PageBounds {
            from,
            up_to,
            limit: usize::MAX,
            max_bytes: usize::MAX,
            exclude_names: &[],
        }
    }
}

/// What `read_records` copied.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecordPage {
    pub count: u32,
    /// Position of the last record copied.
    pub last_position: Option<Position>,
    /// Position of the last matching record looked at, copied or excluded:
    /// where a cursor resumes from when every match was excluded.
    pub scanned_to: Option<Position>,
    /// The page stopped at its count or byte limit, so more may follow; when
    /// false the read reached its bound.
    pub full: bool,
}

#[async_trait::async_trait]
impl EventStore for EventStoreEngine {
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, Error> {
        // Delegates to the inherent synchronous native append method.
        self.append(request)
    }

    async fn append_records(&self, request: RecordAppend) -> Result<AppendResponse, Error> {
        EventStoreEngine::append_records(self, request)
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

    fn read_records(
        &self,
        condition: &SourcingCondition,
        page: &PageBounds<'_>,
        out: &mut Vec<u8>,
    ) -> Result<RecordPage, Error> {
        self.read_records(condition, page, out)
    }

    fn subscribe(
        &self,
        from_position: Position,
        condition: SourcingCondition,
        exclude_names: &[String],
    ) -> EventStream {
        self.subscribe(from_position, condition, exclude_names)
    }

    fn head(&self) -> Position {
        self.head()
    }

    fn visible_head(&self) -> Position {
        self.visible_head()
    }

    fn tail(&self) -> Position {
        self.tail()
    }

    fn get_tags(&self, position: Position) -> Result<Vec<Tag>, Error> {
        self.get_tags(position)
    }

    fn has_matches_between(
        &self,
        from: Position,
        to: Position,
        condition: &SourcingCondition,
    ) -> bool {
        self.has_matches_between(from, to, condition)
    }

    fn get_sequence_at(&self, timestamp_millis: i64) -> Result<Option<Position>, Error> {
        self.get_sequence_at(timestamp_millis)
    }

    async fn append_snapshot(
        &self,
        key: Vec<u8>,
        state: Vec<u8>,
        fold_position: Position,
    ) -> Result<Position, Error> {
        crate::snapshot::append(self, &key, state, fold_position)
    }

    fn get_snapshot(
        &self,
        key: &[u8],
        below: Option<Position>,
    ) -> Result<Option<crate::snapshot::Snapshot>, Error> {
        crate::snapshot::latest(self, key, below)
    }
}

fn remove_if_exists(path: &Path) -> Result<(), Error> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

/// What a log's control records say, read once at open.
struct RecoveredControl {
    /// The highest epoch any control record names.
    epoch: u64,
    /// The highest watermark a checkpoint within the log's tail recorded.
    checkpoint: u64,
    /// Where each `EpochChange` sits.
    epochs: crate::replication::epochs::EpochIndex,
}

/// Folds the log's control records, segment by segment in log order: the
/// sealed segments' from their indexes' control summaries, the active
/// segment's from its live index. Nothing is read from the segments.
fn recover_native_control_state<'a>(
    sealed: impl Iterator<Item = &'a [LoggedControl]>,
    active: &'a [LoggedControl],
    local_tail: u64,
) -> RecoveredControl {
    let mut recovered = RecoveredControl {
        epoch: INITIAL_EPOCH,
        checkpoint: 0,
        epochs: crate::replication::epochs::EpochIndex::default(),
    };
    for control in sealed.chain(std::iter::once(active)).flatten() {
        match control.record {
            crate::segment::format::ControlRecord::EpochChange {
                epoch,
                start_position,
                ..
            } => {
                recovered.epoch = recovered.epoch.max(epoch);
                recovered.epochs.record(epoch, start_position);
            }
            crate::segment::format::ControlRecord::WatermarkCheckpoint { epoch, position }
                if position <= local_tail =>
            {
                recovered.checkpoint = recovered.checkpoint.max(position);
                recovered.epoch = recovered.epoch.max(epoch);
            }
            crate::segment::format::ControlRecord::WatermarkCheckpoint { .. } => {}
        }
    }
    recovered
}

/// The stored CRC of the record that ends at `byte_end` in this node's own
/// segment (0 for an empty segment). `byte_end` falling inside a record is a
/// broken invariant: it is this node's own end.
fn last_physical_record_crc(path: &Path, byte_end: u64) -> Result<u32, Error> {
    record_crc_ending_at(path, byte_end)?.ok_or_else(|| Error::Corrupted {
        message: format!("invalid physical record boundary in {}", path.display()),
    })
}

/// The stored CRC of the record that ends at `byte_end` (0 for an empty
/// segment), or `None` when no record ends there. Asking about a position
/// another node reports is how a leader finds out whether that node's log is a
/// prefix of its own; a log that is not one has its end somewhere else, which
/// is an answer, not damage.
fn record_crc_ending_at(path: &Path, byte_end: u64) -> Result<Option<u32>, Error> {
    use std::io::{Read, Seek, SeekFrom};

    if byte_end <= segment::SEGMENT_HEADER_SIZE as u64 {
        return Ok(Some(0));
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
            return Ok(None);
        }
        last_crc = header.stored_crc;
        file.seek(SeekFrom::Current(header.payload_len as i64))?;
        offset += header.total_len() as u64;
    }
    Ok(Some(last_crc))
}

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
    end_offset: u64,
    crc: u32,
    is_event: bool,
}

/// The last event record before a position, which is where two logs that
/// share their events up to that position are known to agree. Control records
/// around it (checkpoints, epoch changes) are each node's own: they carry no
/// position and a node may hold some the other never received.
#[derive(Clone, Copy)]
struct EventAnchor {
    /// Index of the segment holding the event in the segment list.
    segment_index: usize,
    /// Byte offset just past the event record.
    end_offset: u64,
    /// The event record's stored CRC.
    crc: u32,
}

/// Finds the last event record whose position is below `pos`, looking in the
/// segment containing `pos` and then in earlier ones (a segment that starts
/// with an `EpochChange` at `pos` holds no earlier event). `None` when the log
/// has no event before `pos`.
fn locate_event_anchor(
    dir: &Path,
    bases: &[u64],
    pos: Position,
) -> Result<Option<EventAnchor>, Error> {
    let start = match bases.binary_search(&pos.0) {
        Ok(index) => index,
        Err(0) => {
            return Err(Error::Corrupted {
                message: format!("no segment contains boundary position {}", pos.0),
            });
        }
        Err(index) => index - 1,
    };
    for index in (0..=start).rev() {
        let from = if index == start {
            pos
        } else {
            Position(u64::MAX)
        };
        let preceding = scan_replication_boundary(&segment::segment_path(dir, bases[index]), from)?;
        if let Some(event) = preceding.iter().rev().find(|record| record.is_event) {
            return Ok(Some(EventAnchor {
                segment_index: index,
                end_offset: event.end_offset,
                crc: event.crc,
            }));
        }
    }
    Ok(None)
}

/// Every valid record of the segment before position `from` (where it ends,
/// stored CRC, kind): up to the first event record with position >= `from`, or
/// the end of the valid prefix when there is none. A segment that starts at or
/// after `from` has none.
fn scan_replication_boundary(path: &Path, from: Position) -> Result<Vec<BoundaryRecord>, Error> {
    use std::io::Read;

    let mut file = std::fs::File::open(path)?;
    let mut segment_header = [0u8; segment::SEGMENT_HEADER_SIZE];
    file.read_exact(&mut segment_header)?;
    let base = u64::from_le_bytes(segment_header[5..13].try_into().unwrap());
    if from.0 <= base {
        return Ok(Vec::new());
    }

    let mut offset = segment::SEGMENT_HEADER_SIZE as u64;
    let mut preceding = Vec::new();
    loop {
        let mut header = [0u8; segment::RECORD_HEADER_SIZE];
        match file.read_exact(&mut header) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(preceding);
            }
            Err(error) => return Err(error.into()),
        }
        let header = match crate::segment::record::parse_header(&header)? {
            Some(header) => header,
            None => return Ok(preceding),
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
                    return Ok(preceding);
                }
                true
            }
            crate::segment::record::NativeRecord::Control(_) => false,
        };
        preceding.push(BoundaryRecord {
            end_offset: record_end,
            crc: header.stored_crc,
            is_event,
        });
        offset = record_end;
    }
}

/// The index answering for one segment: a read guard on the active
/// segment's live index, or a shared handle to a sealed one (pinned in
/// memory while its `.idx` is written, mapped after).
enum IndexRef<'a> {
    Active(parking_lot::RwLockReadGuard<'a, SegmentIndex>),
    Sealed(Arc<dyn Locator>),
}

impl std::ops::Deref for IndexRef<'_> {
    type Target = dyn Locator + 'static;
    fn deref(&self) -> &Self::Target {
        match self {
            IndexRef::Active(guard) => &**guard,
            IndexRef::Sealed(index) => &**index,
        }
    }
}

#[cfg(test)]
mod index_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::append::AppendCondition;
    use crate::criteria::{Criterion, SourcingCondition};
    use crate::event::AppendEvent;
    use crate::event::Tag;
    use crate::segment::sealed_index::SealedIndex;

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

    /// The dispatcher's read of the log: names and tags in place, from the
    /// middle of one segment through several others.
    #[test]
    fn visit_events_walks_names_and_tags_across_rotations() {
        let dir = tempfile::tempdir().unwrap();
        // About seven events per segment.
        let store = EventStoreEngine::create_with_options(dir.path(), 512).unwrap();
        for i in 0..200 {
            store
                .append(AppendRequest {
                    condition: None,
                    events: vec![make_event(
                        &format!("E{}", i % 5),
                        vec![tag("k", &i.to_string()), tag("g", &(i % 3).to_string())],
                    )],
                })
                .unwrap();
        }
        assert!(
            store.segments.read().bases.len() > 20,
            "the log should have rotated many times"
        );

        let visit = |from: u64, to: u64| {
            let mut seen = Vec::new();
            store
                .view
                .visit_events(from, to, |position, name, tags, _| {
                    assert_eq!(position, from + seen.len() as u64);
                    seen.push((
                        String::from_utf8(name.to_vec()).unwrap(),
                        tags.map(|(k, v)| {
                            (
                                String::from_utf8(k.to_vec()).unwrap(),
                                String::from_utf8(v.to_vec()).unwrap(),
                            )
                        })
                        .collect::<Vec<_>>(),
                    ));
                    true
                })
                .unwrap();
            seen
        };
        let seen = visit(37, 163);
        assert_eq!(seen.len(), 126);
        for (offset, (name, tags)) in seen.iter().enumerate() {
            let i = 37 + offset;
            assert_eq!(name, &format!("E{}", i % 5));
            assert_eq!(
                tags,
                &vec![
                    ("k".to_string(), i.to_string()),
                    ("g".to_string(), (i % 3).to_string())
                ]
            );
        }
        assert_eq!(visit(0, 200).len(), 200);
        assert_eq!(visit(199, 200).len(), 1);
        assert!(visit(50, 50).is_empty());
        // A range past the log is an error the dispatcher turns into "wake
        // everyone", never silence.
        assert!(
            store
                .view
                .visit_events(199, 201, |_, _, _, _| true)
                .is_err()
        );
        // The visitor can stop early; the result says where.
        let mut n = 0;
        let stopped = store
            .view
            .visit_events(10, 100, |_, _, _, _| {
                n += 1;
                n < 5
            })
            .unwrap();
        assert_eq!((n, stopped), (5, 15));
    }

    /// The client read path must not reveal the store's own events. Three
    /// distinct leaks are covered: the system event itself, a system tag
    /// riding on a user event, and `get_tags` on a system position.
    #[test]
    fn system_events_are_invisible_to_the_client_path() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        store
            .append(AppendRequest {
                condition: None,
                events: vec![make_event("OrderPlaced", vec![tag("orderId", "A")])],
            })
            .unwrap();

        // System events carry only `$` tags (enforced by `append_system`),
        // so no client tag query can even select one; type-based filtering
        // covers the remaining paths (unfiltered reads, position lookups).
        store
            .append_system(AppendRequest {
                condition: None,
                events: vec![make_event(
                    "$schedule.created",
                    vec![tag("$schedule", "tok")],
                )],
            })
            .unwrap();

        // The event a schedule fires: the application's own type, carrying the
        // system tag that correlates it back to its schedule.
        store
            .append(AppendRequest {
                condition: None,
                events: vec![make_event("PaymentTimedOut", vec![tag("orderId", "A")])],
            })
            .unwrap();
        store
            .append_system(AppendRequest {
                condition: None,
                events: vec![make_event(
                    "ReminderDue",
                    vec![tag("orderId", "A"), tag("$schedule", "tok")],
                )],
            })
            .unwrap();

        // All four consumed positions — system events are hidden, not absent.
        assert_eq!(store.head(), Position(4));

        let by_order = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("orderId", "A")],
            }],
        };

        let names: Vec<String> = store
            .source(Position(0), &by_order)
            .unwrap()
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert_eq!(names, vec!["OrderPlaced", "PaymentTimedOut", "ReminderDue"]);

        // The fired event survives; only its system tag is stripped.
        let fired = store
            .source_stored(Position(0), &by_order, 100)
            .unwrap()
            .into_iter()
            .find(|e| e.name == "ReminderDue")
            .expect("the fired event is the application's own and stays visible");
        assert_eq!(fired.tags, vec![tag("orderId", "A")]);

        // Position 1 holds the system event: no tags rather than an error,
        // which would itself disclose what lives there.
        assert_eq!(store.get_tags(Position(1)).unwrap(), Vec::<Tag>::new());
        assert_eq!(
            store.get_tags(Position(3)).unwrap(),
            vec![tag("orderId", "A")]
        );
    }

    /// A consistency marker taken from an append response must keep working
    /// as system events land after it: they carry only `$` tags, so they can
    /// never be the conflict that rejects a client's conditioned append.
    #[test]
    fn markers_round_trip_across_interleaved_system_events() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        let first = store
            .append(AppendRequest {
                condition: None,
                events: vec![make_event("OrderPlaced", vec![tag("orderId", "A")])],
            })
            .unwrap();

        // System events land after the client took its marker.
        for name in ["$schedule.created", "$schedule.cancelled"] {
            store
                .append_system(AppendRequest {
                    condition: None,
                    events: vec![make_event(name, vec![tag("$schedule", "tok")])],
                })
                .unwrap();
        }

        let guard = AppendCondition {
            consistency_marker: first.consistency_marker,
            criteria: SourcingCondition {
                criteria: vec![Criterion {
                    names: vec![],
                    tags: vec![tag("orderId", "A")],
                }],
            },
        };

        // Only invisible bookkeeping followed the marker: no conflict.
        store
            .append(AppendRequest {
                condition: Some(guard.clone()),
                events: vec![make_event("PaymentReceived", vec![tag("orderId", "A")])],
            })
            .expect("system events must never trip a client's condition");

        // A real user event after the marker still conflicts as ever.
        let stale = store.append(AppendRequest {
            condition: Some(guard),
            events: vec![make_event("PaymentReceived", vec![tag("orderId", "A")])],
        });
        assert!(matches!(
            stale,
            Err(Error::ConsistencyConditionViolated { .. })
        ));
    }

    /// `GetSequenceAt` becomes a replay cursor: it must land on an event the
    /// client can actually receive.
    #[test]
    fn get_sequence_at_skips_system_events() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        let stamped = |name: &str, ts: i64, tags: Vec<Tag>| AppendEvent {
            timestamp: ts,
            ..make_event(name, tags)
        };

        store
            .append(AppendRequest {
                condition: None,
                events: vec![stamped("OrderPlaced", 1_000, vec![tag("orderId", "A")])],
            })
            .unwrap();
        store
            .append_system(AppendRequest {
                condition: None,
                events: vec![stamped("$schedule.created", 2_000, vec![])],
            })
            .unwrap();
        store
            .append(AppendRequest {
                condition: None,
                events: vec![stamped("PaymentReceived", 3_000, vec![tag("orderId", "A")])],
            })
            .unwrap();

        // The system event at position 1 (ts 2000) matches the cutoff first,
        // but a client sourcing from it would receive nothing until 3000.
        assert_eq!(
            store.get_sequence_at(1_500).unwrap(),
            Some(Position(2)),
            "must land on the first readable event, not hidden bookkeeping"
        );
    }

    /// The seam refuses a system event carrying user tags — the misuse that
    /// would let invisible state reject client appends.
    #[test]
    fn system_events_may_not_carry_user_tags() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        let smuggled = store.append_system(AppendRequest {
            condition: None,
            events: vec![make_event(
                "$schedule.created",
                vec![tag("orderId", "A"), tag("$schedule", "tok")],
            )],
        });
        assert!(matches!(smuggled, Err(Error::ReservedNamespace { .. })));
    }

    /// Clients may neither forge system events nor query for them.
    #[test]
    fn the_reserved_namespace_is_closed_to_clients() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        let forged = store.append(AppendRequest {
            condition: None,
            events: vec![make_event("$schedule.created", vec![])],
        });
        assert!(matches!(forged, Err(Error::ReservedNamespace { .. })));

        let probe = store.source(
            Position(0),
            &SourcingCondition {
                criteria: vec![Criterion {
                    names: vec![],
                    tags: vec![tag("$schedule", "tok")],
                }],
            },
        );
        assert!(matches!(probe, Err(Error::ReservedNamespace { .. })));
    }

    /// A rotation seals the segment in memory first and writes its
    /// companions in the background. Both before and after the companions
    /// land, tag queries and DCB checks must resolve the sealed positions.
    /// A store with tiny segments, which force many rotations.
    fn tiny_segment_store(dir: &Path, read_mode: ReadMode) -> EventStoreEngine {
        EventStoreEngine::create_with_store_options(
            dir,
            &StoreOptions {
                max_segment_size: 4 * 1024,
                read_mode,
                block_size: 4096,
                ..StoreOptions::default()
            },
        )
        .unwrap()
    }

    /// A read over more sealed segments than the old fixed cache of 100 held
    /// maps each one once and keeps it: the next read maps nothing. With a
    /// budget too small for them all, reads stay correct and remap instead.
    #[test]
    fn sealed_segments_stay_mapped_within_the_budget() {
        for (limit, keeps_all) in [(10_000, true), (40, false)] {
            let dir = tempfile::tempdir().unwrap();
            let budget = crate::cache::MappingBudget::new(limit);
            let store = EventStoreEngine::create_with_store_options(
                dir.path(),
                &StoreOptions {
                    max_segment_size: 4 * 1024,
                    mapping_budget: Arc::clone(&budget),
                    ..StoreOptions::default()
                },
            )
            .unwrap();
            for batch in 0..500u64 {
                let events = (0..20)
                    .map(|i| {
                        make_event(
                            "OrderPlaced",
                            vec![tag("orderId", &format!("o-{batch}-{i}"))],
                        )
                    })
                    .collect();
                store
                    .append(AppendRequest {
                        condition: None,
                        events,
                    })
                    .unwrap();
            }
            let sealed = store.segments.read().sealed_count;
            assert!(sealed > 150, "only {sealed} sealed segments");
            let misses = || {
                store
                    .cache
                    .mmap_misses
                    .load(std::sync::atomic::Ordering::Relaxed)
            };
            assert_eq!(
                store.source(Position(0), &cond_match_all()).unwrap().len(),
                10_000
            );
            let after_first = misses();
            assert_eq!(
                store.source(Position(0), &cond_match_all()).unwrap().len(),
                10_000
            );
            if keeps_all {
                assert_eq!(misses(), after_first, "a second read mapped segments again");
            } else {
                assert!(misses() > after_first);
                assert!(budget.live() <= limit + 1, "{} mapped", budget.live());
            }
        }
    }

    #[test]
    fn sealed_segments_answer_queries_and_dcb_after_rotation() {
        sealed_segments_answer_queries_and_dcb(ReadMode::Mapped);
    }

    #[test]
    fn sealed_segments_answer_queries_and_dcb_after_rotation_through_blocks() {
        sealed_segments_answer_queries_and_dcb(ReadMode::Blocks);
    }

    fn sealed_segments_answer_queries_and_dcb(read_mode: ReadMode) {
        let dir = tempfile::tempdir().unwrap();
        let store = tiny_segment_store(dir.path(), read_mode);

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

        // Let every background seal land, then every sealed segment must
        // have its companions and no index may still be pinned.
        store.seals.wait_idle();
        for &base in &sealed.bases[..sealed.sealed_count] {
            let path = segment::segment_path(dir.path(), base);
            assert!(
                SealedIndex::open(&path.with_extension("idx"), base).is_ok(),
                "segment {base} lacks its index"
            );
            assert!(!store.cache.is_pinned(base));
        }

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

        // Every event is still reachable across all segments.
        let all = store.source(Position(0), &cond_match_all()).unwrap();
        assert_eq!(
            all.len(),
            100,
            "the rejected append wrote nothing; every accepted event resolves"
        );
    }

    /// Readers never take the writer lock, so a rotation must make the
    /// sealed segment's index reachable in the same step that moves the
    /// active index on to the new segment. Readers hammering a tag while the
    /// writer rotates constantly must never fall through to a `.idx` that
    /// the background seal has not written yet.
    #[test]
    fn reads_racing_rotations_always_find_the_sealed_index() {
        reads_racing_rotations(ReadMode::Mapped);
    }

    #[test]
    fn reads_racing_rotations_always_find_the_sealed_index_through_blocks() {
        reads_racing_rotations(ReadMode::Blocks);
    }

    fn reads_racing_rotations(read_mode: ReadMode) {
        let dir = tempfile::tempdir().unwrap();
        let store = tiny_segment_store(dir.path(), read_mode);
        let hot = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("orderId", "hot")],
            }],
        };
        let done = std::sync::atomic::AtomicBool::new(false);
        // Sets `done` however the writer side ends. Without it a failing
        // append (a full disk, say) panics past the `done.store` below and the
        // scope waits forever on readers that spin until they are told to stop.
        struct StopReaders<'a>(&'a std::sync::atomic::AtomicBool);
        impl Drop for StopReaders<'_> {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Relaxed);
            }
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(120);

        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| {
                    while !done.load(Ordering::Relaxed) {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "the writer did not finish its appends in time"
                        );
                        store
                            .source_page(Position(0), &hot, store.head(), 8192)
                            .expect("a read raced a rotation and missed the sealed index");
                    }
                });
            }
            let _stop = StopReaders(&done);
            for i in 0..200 {
                let events = (0..20)
                    .map(|k| {
                        let id = if k == 0 {
                            "hot".to_string()
                        } else {
                            format!("ord-{i}-{k}")
                        };
                        make_event("OrderPlaced", vec![tag("orderId", &id)])
                    })
                    .collect();
                store
                    .append(AppendRequest {
                        condition: None,
                        events,
                    })
                    .unwrap();
            }
        });

        let segments = store.segments.read().clone();
        assert!(segments.sealed_count > 50, "expected many rotations");
        let hot_events = store.source(Position(0), &hot).unwrap();
        assert_eq!(hot_events.len(), 200);
    }

    /// `read_records` is `source_page` without the decoding: the same events,
    /// in the same order, under the same visibility rules, across rotations.
    #[test]
    fn read_records_copies_exactly_what_source_page_returns() {
        read_records_matches_source_page(ReadMode::Mapped);
    }

    #[test]
    fn read_records_copies_exactly_what_source_page_returns_through_blocks() {
        read_records_matches_source_page(ReadMode::Blocks);
    }

    fn read_records_matches_source_page(read_mode: ReadMode) {
        let dir = tempfile::tempdir().unwrap();
        let store = tiny_segment_store(dir.path(), read_mode);
        for i in 0..300u32 {
            let mut tags = vec![tag("orderId", &format!("o-{}", i % 7))];
            // A visible event carrying a server-owned tag, as a fired
            // schedule writes it, must lose the tag.
            let request = |tags| AppendRequest {
                condition: None,
                events: vec![make_event("OrderPlaced", tags)],
            };
            if i % 50 == 3 {
                tags.push(tag("$schedule", "tok"));
                store.append_system(request(tags)).unwrap();
            } else {
                store.append(request(tags)).unwrap();
            }
            // System events must never be copied.
            if i % 40 == 9 {
                store
                    .append_system(AppendRequest {
                        condition: None,
                        events: vec![make_event(
                            "$snapshot.written",
                            vec![crate::system::marker()],
                        )],
                    })
                    .unwrap();
            }
        }
        let head = store.head();
        for condition in [
            cond_match_all(),
            SourcingCondition {
                criteria: vec![Criterion {
                    names: vec![],
                    tags: vec![tag("orderId", "o-3")],
                }],
            },
        ] {
            let mut cursor = Position(0);
            let mut copied = Vec::new();
            loop {
                let mut out = Vec::new();
                let page = store
                    .read_records(
                        &condition,
                        &PageBounds {
                            limit: 13,
                            ..PageBounds::new(cursor, head)
                        },
                        &mut out,
                    )
                    .unwrap();
                let mut at = 0;
                for _ in 0..page.count {
                    let len = u32::from_le_bytes(out[at..at + 4].try_into().unwrap()) as usize;
                    let (event, used) =
                        format::deserialize_event(&out[at + 5..at + 4 + len]).unwrap();
                    assert_eq!(used, len - 1, "a record is exactly one event");
                    copied.push(event.into_sequenced());
                    at += 4 + len;
                }
                assert_eq!(at, out.len());
                match page.last_position {
                    Some(last) if page.full => cursor = Position(last.0 + 1),
                    _ => break,
                }
            }
            let sourced = store
                .source_page(Position(0), &condition, head, usize::MAX)
                .unwrap();
            assert_eq!(copied.len(), sourced.len());
            for (a, b) in copied.iter().zip(&sourced) {
                assert_eq!(
                    (a.position, &a.name, &a.payload),
                    (b.position, &b.name, &b.payload)
                );
            }
        }
        // Tags travel on the wire, so the stripped one must be gone from the bytes.
        let mut out = Vec::new();
        store
            .read_records(
                &cond_match_all(),
                &PageBounds::new(Position(0), head),
                &mut out,
            )
            .unwrap();
        assert!(!out.windows(9).any(|w| w == b"$schedule"));
        let everything = out;
        // Excluded types are skipped without counting against the limit.
        let mut out = Vec::new();
        let excluded = store
            .read_records(
                &cond_match_all(),
                &PageBounds {
                    exclude_names: &["OrderPlaced".to_string()],
                    ..PageBounds::new(Position(0), head)
                },
                &mut out,
            )
            .unwrap();
        assert_eq!(excluded.count, 0);
        assert!(out.is_empty() && !everything.is_empty());
        assert!(
            excluded.scanned_to.is_some(),
            "the cursor still moves past them"
        );
        // The byte limit stops a page early but always returns at least one record.
        let mut out = Vec::new();
        let page = store
            .read_records(
                &cond_match_all(),
                &PageBounds {
                    max_bytes: 1,
                    ..PageBounds::new(Position(0), head)
                },
                &mut out,
            )
            .unwrap();
        assert_eq!((page.count, page.full), (1, true));
    }

    /// A crash between a rotation and its background seal leaves a sealed
    /// segment without companions. Opening the store rebuilds them.
    #[test]
    fn open_rebuilds_missing_companions() {
        let dir = tempfile::tempdir().unwrap();
        let sealed;
        {
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
            store.seals.wait_idle();
            sealed = store.segments.read().clone();
            assert!(sealed.sealed_count > 0);
            store.shutdown();
        }
        let victim = segment::segment_path(dir.path(), sealed.bases[0]);
        std::fs::remove_file(victim.with_extension("idx")).unwrap();

        let store = EventStoreEngine::open_with_options(dir.path(), 4 * 1024).unwrap();
        assert!(SealedIndex::open(&victim.with_extension("idx"), sealed.bases[0]).is_ok());
        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("orderId", "ord-0")],
            }],
        };
        assert_eq!(store.source(Position(0), &cond).unwrap().len(), 1);
        assert_eq!(
            store.source(Position(0), &cond_match_all()).unwrap().len(),
            100
        );
    }

    /// Segment files are grown to their full length up front so the active
    /// segment can be mapped once and read for its whole lifetime. On a
    /// filesystem that refuses `fallocate` — ZFS, NFS, a FUSE-backed bind
    /// mount under a container — that growth has to come from `set_len`
    /// instead, or the file stays exactly as long as its data.
    #[test]
    fn active_segment_file_is_full_length_after_create_and_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let size = 1024 * 1024;
        {
            let store = EventStoreEngine::create_with_options(dir.path(), size).unwrap();
            let path = store.writer.lock().active_segment_path();
            assert_eq!(
                std::fs::metadata(&path).unwrap().len(),
                size,
                "a freshly created segment is already its full length"
            );
            store
                .append(AppendRequest {
                    condition: None,
                    events: vec![make_event("OrderPlaced", vec![tag("orderId", "A")])],
                })
                .unwrap();
            store.shutdown();
        }
        let store = EventStoreEngine::open_with_options(dir.path(), size).unwrap();
        let path = store.writer.lock().active_segment_path();
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            size,
            "reopening restores the full length; recovery trims to the data \
             tail first, and leaving it trimmed strands every later append \
             outside the reader's mapping"
        );
    }

    /// The reader half of the same invariant. A store whose active segment
    /// is not full-length — written by a release that tolerated a failed
    /// preallocation — must still read events appended after the mapping was
    /// taken, rather than reporting them as corrupt or silently dropping
    /// them. Truncating the file behind the engine's back reproduces exactly
    /// the state such a store comes up in.
    #[test]
    fn reads_follow_an_active_segment_that_was_not_grown_up_front() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create_with_options(dir.path(), 1024 * 1024).unwrap();

        for i in 0..3 {
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

        // Shrink the active segment to its data length: what the file would
        // have looked like all along had preallocation silently failed.
        let (path, data_len) = {
            let writer = store.writer.lock();
            (writer.active_segment_path(), writer.write_offset())
        };
        std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(data_len)
            .unwrap();

        // This read maps the segment at that shortened length.
        assert_eq!(
            store.source(Position(0), &cond_match_all()).unwrap().len(),
            3
        );

        for i in 3..6 {
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

        assert_eq!(
            store.source(Position(0), &cond_match_all()).unwrap().len(),
            6,
            "events appended past the end of the cached mapping are still \
             readable"
        );
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

    fn tag_cond(value: &str) -> SourcingCondition {
        SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("orderId", value)],
            }],
        }
    }

    /// Opens a store that reads sealed segments the given way, with small
    /// blocks so a segment spans several of them.
    fn open_store_reading(dir: &Path, read_mode: ReadMode) -> EventStoreEngine {
        EventStoreEngine::open_with_store_options(
            dir,
            &StoreOptions {
                max_segment_size: 16 * 1024,
                read_mode,
                block_size: 4096,
                ..StoreOptions::default()
            },
        )
        .unwrap()
    }

    /// A byte that went bad on disk in a sealed segment is never served: the
    /// read that would have returned it gets `Corrupted`, while the rest of
    /// the log keeps answering. Through the mapping the check is on every
    /// read; through the block cache it is when the block comes off the disk.
    #[test]
    fn a_flipped_byte_in_a_sealed_segment_is_caught() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = EventStoreEngine::create_with_store_options(
                dir.path(),
                &StoreOptions {
                    max_segment_size: 16 * 1024,
                    ..StoreOptions::default()
                },
            )
            .unwrap();
            for i in 0..600 {
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
            store.seals.wait_idle();
            assert!(store.segments.read().sealed_count >= 2);
        }

        // Event 3 lives in the first, sealed segment. Flip one bit in its
        // body while nothing has the file cached.
        let seg0 = segment::segment_path(dir.path(), 0);
        let offset = SealedIndex::open(&seg0.with_extension("idx"), 0)
            .unwrap()
            .offset_of(3)
            .unwrap()
            .unwrap();
        let mut bytes = std::fs::read(&seg0).unwrap();
        bytes[offset as usize + 40] ^= 0x04;
        std::fs::write(&seg0, &bytes).unwrap();

        for read_mode in [ReadMode::Blocks, ReadMode::Mapped] {
            assert_flipped_byte_is_refused(dir.path(), read_mode);
        }
    }

    fn assert_flipped_byte_is_refused(dir: &Path, read_mode: ReadMode) {
        let store = open_store_reading(dir, read_mode);
        match store.source(Position(0), &tag_cond("ord-3")) {
            Err(Error::Corrupted { message }) => {
                assert!(message.contains("CRC mismatch"), "{message}")
            }
            other => panic!("expected Corrupted, got {:?}", other.map(|e| e.len())),
        }
        assert!(matches!(
            store.get_tags(Position(3)),
            Err(Error::Corrupted { .. })
        ));
        let mut out = Vec::new();
        assert!(matches!(
            store.read_records(
                &cond_match_all(),
                &PageBounds::new(Position(0), store.head()),
                &mut out
            ),
            Err(Error::Corrupted { .. })
        ));
        // Events elsewhere are untouched and served.
        assert_eq!(
            store
                .source(Position(0), &tag_cond("ord-150"))
                .unwrap()
                .len(),
            1
        );
    }

    /// Everything a store holds reads back, byte for byte the same through
    /// the block cache as through the per-read-CRC mapping.
    #[test]
    fn block_reads_return_what_mapped_reads_return() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = EventStoreEngine::create_with_store_options(
                dir.path(),
                &StoreOptions {
                    max_segment_size: 16 * 1024,
                    ..StoreOptions::default()
                },
            )
            .unwrap();
            for i in 0..300 {
                store
                    .append(AppendRequest {
                        condition: None,
                        events: vec![make_event(
                            "OrderPlaced",
                            vec![tag("orderId", &format!("ord-{}", i % 13))],
                        )],
                    })
                    .unwrap();
            }
            store.seals.wait_idle();
        }
        let read = |mode: ReadMode| {
            let store = EventStoreEngine::open_with_store_options(
                dir.path(),
                &StoreOptions {
                    max_segment_size: 16 * 1024,
                    read_mode: mode,
                    block_size: 4096,
                    ..StoreOptions::default()
                },
            )
            .unwrap();
            let mut all = Vec::new();
            let mut from = Position(0);
            loop {
                let mut page = Vec::new();
                let bounds = PageBounds {
                    limit: 37,
                    ..PageBounds::new(from, store.head())
                };
                let result = store
                    .read_records(&cond_match_all(), &bounds, &mut page)
                    .unwrap();
                all.extend_from_slice(&page);
                if !result.full {
                    break;
                }
                from = Position(result.scanned_to.unwrap().0 + 1);
            }
            let by_tag = store.source(Position(0), &tag_cond("ord-5")).unwrap();
            (all, by_tag.len())
        };
        let (blocks, blocks_tagged) = read(ReadMode::Blocks);
        let (mapped, mapped_tagged) = read(ReadMode::Mapped);
        assert!(!blocks.is_empty());
        assert_eq!(blocks, mapped);
        assert_eq!(blocks_tagged, mapped_tagged);
    }

    /// Replays the whole log in pages the way a client does, returning the
    /// number of events delivered.
    fn replay_all(store: &EventStoreEngine, condition: &SourcingCondition) -> usize {
        let mut events = 0;
        let mut from = Position(0);
        loop {
            let mut page = Vec::new();
            let bounds = PageBounds {
                limit: 37,
                ..PageBounds::new(from, store.head())
            };
            let result = store.read_records(condition, &bounds, &mut page).unwrap();
            events += result.count as usize;
            if !result.full {
                return events;
            }
            from = Position(result.scanned_to.unwrap().0 + 1);
        }
    }

    /// Opening a store maps every sealed segment's index once; after that
    /// no read, whether a replay, a read by tag or a point read, opens or
    /// loads an index again, however many segments there are. (Before
    /// indexes were mapped, a replay under a small heap budget reloaded
    /// every segment's index on every pass.)
    #[test]
    fn reads_never_reload_a_sealed_index() {
        let dir = tempfile::tempdir().unwrap();
        let total = 600u64;
        {
            let store = EventStoreEngine::create_with_store_options(
                dir.path(),
                &StoreOptions {
                    max_segment_size: 16 * 1024,
                    ..StoreOptions::default()
                },
            )
            .unwrap();
            for i in 0..total {
                store
                    .append(AppendRequest {
                        condition: None,
                        events: vec![make_event(
                            "OrderPlaced",
                            vec![tag("orderId", &format!("ord-{}", i % 13))],
                        )],
                    })
                    .unwrap();
            }
            store.seals.wait_idle();
        }
        for read_mode in [ReadMode::Mapped, ReadMode::Blocks] {
            let store = EventStoreEngine::open_with_store_options(
                dir.path(),
                &StoreOptions {
                    max_segment_size: 16 * 1024,
                    read_mode,
                    block_size: 4096,
                    ..StoreOptions::default()
                },
            )
            .unwrap();
            let sealed = store.segments.read().sealed_count;
            assert!(sealed >= 3, "need several sealed segments, got {sealed}");
            assert_eq!(store.cache.mapped_indexes(), sealed);

            for pass in 0..3 {
                assert_eq!(
                    replay_all(&store, &cond_match_all()),
                    total as usize,
                    "{read_mode:?} pass {pass}"
                );
            }
            let wanted = (0..total).filter(|i| i % 13 == 3).count();
            assert_eq!(
                store.source(Position(0), &tag_cond("ord-3")).unwrap().len(),
                wanted
            );
            assert_eq!(replay_all(&store, &tag_cond("ord-3")), wanted);
            for position in [0, 1, total / 2, total - 1] {
                store.get_tags(Position(position)).unwrap();
            }
            assert_eq!(
                store.cache.index_misses.load(Ordering::Relaxed),
                0,
                "{read_mode:?}: a read opened an index"
            );
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
    // ---- staging modes ----

    /// An engine with small segments. The default 256 MB segment is
    /// preallocated, which on a tmpfs temp directory is real memory: a dozen
    /// tests running side by side would fill it.
    fn small_engine(dir: &Path) -> EventStoreEngine {
        EventStoreEngine::create_with_options(dir, 1 << 20).unwrap()
    }

    fn conditional(marker: u64, orders: &[&str], events: Vec<AppendEvent>) -> AppendRequest {
        AppendRequest {
            condition: Some(AppendCondition {
                consistency_marker: Position(marker),
                criteria: SourcingCondition {
                    criteria: orders
                        .iter()
                        .map(|order| Criterion {
                            names: vec![],
                            tags: vec![tag("orderId", order)],
                        })
                        .collect(),
                },
            }),
            events,
        }
    }

    fn client(request: AppendRequest) -> Prepared {
        prepare_client_append(request).unwrap()
    }

    fn plain(events: Vec<AppendEvent>) -> AppendRequest {
        AppendRequest {
            condition: None,
            events,
        }
    }

    /// Finishes a non-blocking staging the way `stage_append` does: a request
    /// it handed back goes to the blocking path.
    async fn settle(
        engine: &Arc<EventStoreEngine>,
        staging: Staging,
    ) -> Result<AppendResponse, Error> {
        let staged = match staging {
            Staging::Done(result) => result?,
            Staging::NeedsPool(prepared) => engine.stage_prepared(prepared, Timer::start())?,
        };
        engine.append_finish_async(staged).await
    }

    /// Finishes whatever the staging thread answered with.
    async fn settle_thread(
        engine: &Arc<EventStoreEngine>,
        settled: Settled,
    ) -> Result<AppendResponse, Error> {
        match settled {
            Settled::Final(result) => result,
            Settled::Pool(prepared) => {
                let staged = engine.stage_prepared(prepared, Timer::start())?;
                engine.append_finish_async(staged).await
            }
        }
    }

    fn outcome(result: Result<AppendResponse, Error>) -> Result<(u64, u32, u64), u64> {
        match result {
            Ok(r) => Ok((r.first_position.0, r.count, r.consistency_marker.0)),
            Err(Error::ConsistencyConditionViolated {
                conflicting_position,
            }) => Err(conflicting_position.0),
            Err(other) => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn nonblocking_staging_answers_like_the_blocking_path() {
        let script = |step: usize| -> AppendRequest {
            match step {
                0 => plain(vec![
                    make_event("OrderPlaced", vec![tag("orderId", "A")]),
                    make_event("OrderPlaced", vec![tag("orderId", "B")]),
                ]),
                // Conflicts with position 0 (after marker 0).
                1 => conditional(
                    0,
                    &["A"],
                    vec![make_event("OrderPlaced", vec![tag("orderId", "A")])],
                ),
                // Clear: nothing for C, and A's event sits below the marker.
                2 => conditional(
                    2,
                    &["A", "C"],
                    vec![make_event("OrderPlaced", vec![tag("orderId", "A")])],
                ),
                // Conflicts with step 2's own event.
                3 => conditional(
                    2,
                    &["A"],
                    vec![make_event("OrderPlaced", vec![tag("orderId", "A")])],
                ),
                // Condition only, no events.
                4 => conditional(3, &["B"], vec![]),
                _ => plain(vec![make_event("OrderPlaced", vec![tag("orderId", "D")])]),
            }
        };
        let blocking_dir = tempfile::tempdir().unwrap();
        let inline_dir = tempfile::tempdir().unwrap();
        let blocking = Arc::new(small_engine(blocking_dir.path()));
        let inline = Arc::new(small_engine(inline_dir.path()));
        for step in 0..6 {
            let expected = outcome(blocking.append(script(step)));
            let mut staging = inline.stage_inline(client(script(step)));
            // The sync thread takes the writer lock for an instant after
            // each write, and a request that finds it held is handed to the
            // pool, which is right but not what this test is about: offer
            // it again until the thread has let go.
            for _ in 0..10_000 {
                let Staging::NeedsPool(prepared) = staging else {
                    break;
                };
                tokio::task::yield_now().await;
                staging = inline.stage_inline(prepared);
            }
            assert!(
                matches!(staging, Staging::Done(_)),
                "step {step}: nothing here needs the pool"
            );
            let got = outcome(settle(&inline, staging).await);
            assert_eq!(got, expected, "step {step}");
        }
        assert_eq!(inline.head(), blocking.head());
        blocking.shutdown();
        inline.shutdown();
    }

    #[tokio::test]
    async fn inline_staging_leaves_a_busy_writer_to_the_pool() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(small_engine(dir.path()));
        let request = || plain(vec![make_event("OrderPlaced", vec![tag("orderId", "A")])]);
        {
            let _held = engine.writer.lock();
            let staging = engine.stage_inline(client(request()));
            let Staging::NeedsPool(returned) = staging else {
                panic!("a held writer lock must hand the request back");
            };
            assert_eq!(returned.batch.len(), 1, "the request comes back intact");
            assert_eq!(engine.head(), Position(0), "and nothing was written");
        }
        // The sync thread takes the lock for an instant when it starts, and
        // may have been waiting for ours; let it finish.
        while engine.writer.is_locked() {
            tokio::task::yield_now().await;
        }
        let staging = engine.stage_inline(client(request()));
        assert!(matches!(staging, Staging::Done(_)));
        assert_eq!(outcome(settle(&engine, staging).await), Ok((0, 1, 1)));
        engine.shutdown();
    }

    #[tokio::test]
    async fn inline_staging_leaves_a_rotating_write_to_the_pool() {
        let dir = tempfile::tempdir().unwrap();
        let opts = StoreOptions {
            max_segment_size: 1024,
            ..Default::default()
        };
        let engine =
            Arc::new(EventStoreEngine::create_with_store_options(dir.path(), &opts).unwrap());
        let event = || make_event("OrderPlaced", vec![tag("orderId", "A")]);
        let mut inline = 0;
        let mut pooled = 0;
        for i in 0..20u64 {
            let staging = engine.stage_inline(client(plain(vec![event()])));
            match &staging {
                Staging::Done(_) => inline += 1,
                Staging::NeedsPool(_) => pooled += 1,
            }
            let (first, count, _) = outcome(settle(&engine, staging).await).unwrap();
            assert_eq!(
                (first, count),
                (i, 1),
                "positions stay dense across rotations"
            );
        }
        assert!(inline > 0, "writes that fit stay inline");
        assert!(pooled > 0, "the write that would rotate goes to the pool");
        assert!(engine.metrics().snapshot().segment_rotations > 0);
        assert_eq!(
            engine.source(Position(0), &cond_match_all()).unwrap().len(),
            20
        );
        engine.shutdown();
    }

    /// The DCB check reads key tables only, which stay mapped (or pinned)
    /// for as long as a sealed segment exists: with every sealed segment's
    /// locator unmapped, as the mapping cap would leave them, inline staging
    /// still answers without the disk, conflicts included.
    #[tokio::test]
    async fn inline_staging_needs_no_sealed_locator() {
        let dir = tempfile::tempdir().unwrap();
        let opts = StoreOptions {
            max_segment_size: 1024,
            ..Default::default()
        };
        let engine =
            Arc::new(EventStoreEngine::create_with_store_options(dir.path(), &opts).unwrap());
        for i in 0..40 {
            engine
                .append(plain(vec![make_event(
                    "OrderPlaced",
                    vec![tag("orderId", &format!("o-{i}"))],
                )]))
                .unwrap();
        }
        engine.seals.wait_idle();
        let list = engine.segments.read().clone();
        assert!(list.sealed_count > 2);
        for &base in &list.bases[..list.sealed_count] {
            engine.cache.invalidate(base);
        }

        // Marker 0 reaches every sealed segment.
        let request = || {
            conditional(
                0,
                &["nobody"],
                vec![make_event("OrderPlaced", vec![tag("orderId", "nobody")])],
            )
        };
        assert!(matches!(
            engine.stage_pre_cached(&client(request())),
            Some(Ok(Pre::Checked(_)))
        ));
        let staging = engine.stage_inline(client(request()));
        assert!(matches!(staging, Staging::Done(Ok(_))));
        outcome(settle(&engine, staging).await).unwrap();

        // The conflict the oldest segment holds is found the same way.
        let conflict = || {
            conditional(
                0,
                &["o-0"],
                vec![make_event("OrderPlaced", vec![tag("orderId", "o-0")])],
            )
        };
        assert!(matches!(
            engine.stage_pre_cached(&client(conflict())),
            Some(Ok(Pre::Conflict(Position(0))))
        ));
        let staging = engine.stage_inline(client(conflict()));
        assert_eq!(outcome(settle(&engine, staging).await), Err(0));
        engine.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_staging_thread_orders_every_append_and_one_dcb_winner() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(small_engine(dir.path()));

        let mut tasks = Vec::new();
        for t in 0..8 {
            let engine = Arc::clone(&engine);
            tasks.push(tokio::spawn(async move {
                for i in 0..25 {
                    let staging = engine
                        .stage_on_thread(client(plain(vec![make_event(
                            &format!("E-{t}-{i}"),
                            vec![tag("t", &t.to_string())],
                        )])))
                        .await;
                    settle_thread(&engine, staging).await.unwrap();
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(engine.head(), Position(200));
        let all = engine.source(Position(0), &cond_match_all()).unwrap();
        let positions: Vec<u64> = all.iter().map(|e| e.position.0).collect();
        assert_eq!(positions, (0..200).collect::<Vec<_>>());

        // Sixteen appenders race one condition: the first to stage wins and
        // every later one sees its event.
        let mut racers = Vec::new();
        for r in 0..16 {
            let engine = Arc::clone(&engine);
            racers.push(tokio::spawn(async move {
                let staging = engine
                    .stage_on_thread(client(conditional(
                        200,
                        &["contested"],
                        vec![make_event(
                            &format!("Claim-{r}"),
                            vec![tag("orderId", "contested")],
                        )],
                    )))
                    .await;
                outcome(settle_thread(&engine, staging).await)
            }));
        }
        let mut winners = 0;
        for racer in racers {
            match racer.await.unwrap() {
                Ok(_) => winners += 1,
                Err(conflicting) => assert_eq!(conflicting, 200),
            }
        }
        assert_eq!(winners, 1);
        assert_eq!(engine.head(), Position(201));
        engine.shutdown();
    }

    /// Each place an append can be staged, alone and together. The pool is
    /// always the last resort, so `POOL_ONLY` is every route switched off.
    const ALL_ROUTES: [(&str, Routes); 4] = [
        (
            "pool",
            Routes {
                inline: false,
                thread: false,
            },
        ),
        (
            "inline",
            Routes {
                inline: true,
                thread: false,
            },
        ),
        (
            "thread",
            Routes {
                inline: false,
                thread: true,
            },
        ),
        ("all", Routes::ALL),
    ];

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_staging_mode_keeps_positions_dense_across_rotations() {
        for (mode, routes) in ALL_ROUTES {
            let dir = tempfile::tempdir().unwrap();
            let opts = StoreOptions {
                max_segment_size: 4096,
                ..Default::default()
            };
            let engine =
                Arc::new(EventStoreEngine::create_with_store_options(dir.path(), &opts).unwrap());
            let mut tasks = Vec::new();
            for t in 0..6 {
                let engine = Arc::clone(&engine);
                tasks.push(tokio::spawn(async move {
                    for i in 0..40 {
                        // Mixed batch sizes, some of which rotate.
                        let events = (0..1 + (i % 4))
                            .map(|e| make_event(&format!("E-{t}-{i}-{e}"), vec![tag("t", "x")]))
                            .collect();
                        engine
                            .append_routed(routes, plain(events), false)
                            .await
                            .unwrap();
                    }
                }));
            }
            for task in tasks {
                task.await.unwrap();
            }
            let expected: u64 = 6 * (0..40u64).map(|i| 1 + (i % 4)).sum::<u64>();
            assert_eq!(engine.head(), Position(expected), "{mode}");
            let all = engine.source(Position(0), &cond_match_all()).unwrap();
            let positions: Vec<u64> = all.iter().map(|e| e.position.0).collect();
            assert_eq!(positions, (0..expected).collect::<Vec<_>>(), "{mode}");
            assert!(
                engine.metrics().snapshot().segment_rotations > 2,
                "{mode}: the run must have rotated"
            );
            engine.shutdown();
        }
    }

    fn breaker_engine(dir: &std::path::Path) -> Arc<EventStoreEngine> {
        Arc::new(EventStoreEngine::create(dir).unwrap())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_stalled_inline_staging_moves_appends_to_the_staging_thread() {
        let dir = tempfile::tempdir().unwrap();
        let engine = breaker_engine(dir.path());
        // Any inline staging counts as a stall, and the route stays shut for
        // the rest of the test.
        engine
            .inline_breaker
            .set_limits(Duration::ZERO, Duration::from_secs(3600));
        for i in 0..20 {
            engine
                .append_routed(
                    Routes::ALL,
                    plain(vec![make_event(&format!("E-{i}"), vec![tag("t", "x")])]),
                    false,
                )
                .await
                .unwrap();
        }
        // Only the first append went inline: had the route stayed open, every
        // one of the 20 would have stalled and been counted.
        assert_eq!(engine.metrics().snapshot().inline_stalls, 1);
        assert_eq!(engine.head(), Position(20));
        let all = engine.source(Position(0), &cond_match_all()).unwrap();
        assert_eq!(all.len(), 20);
        engine.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn inline_staging_resumes_after_the_cooldown() {
        let dir = tempfile::tempdir().unwrap();
        let engine = breaker_engine(dir.path());
        engine
            .inline_breaker
            .set_limits(Duration::ZERO, Duration::from_millis(20));
        for i in 0..2 {
            engine
                .append_routed(
                    Routes::ALL,
                    plain(vec![make_event(&format!("E-{i}"), vec![tag("t", "x")])]),
                    false,
                )
                .await
                .unwrap();
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
        // Each append found the route open again, went inline and stalled.
        assert_eq!(engine.metrics().snapshot().inline_stalls, 2);
        engine.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn quick_inline_stagings_keep_the_route_open() {
        let dir = tempfile::tempdir().unwrap();
        let engine = breaker_engine(dir.path());
        engine
            .inline_breaker
            .set_limits(Duration::from_secs(3600), Duration::from_secs(3600));
        for i in 0..20 {
            engine
                .append_routed(
                    Routes::ALL,
                    plain(vec![make_event(&format!("E-{i}"), vec![tag("t", "x")])]),
                    false,
                )
                .await
                .unwrap();
        }
        assert_eq!(engine.metrics().snapshot().inline_stalls, 0);
        assert!(engine.inline_breaker.allows(engine.inline_breaker.now()));
        engine.shutdown();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_staging_mode_gives_a_dcb_race_one_winner() {
        for (mode, routes) in ALL_ROUTES {
            let dir = tempfile::tempdir().unwrap();
            let engine = Arc::new(small_engine(dir.path()));
            let mut racers = Vec::new();
            for r in 0..16 {
                let engine = Arc::clone(&engine);
                racers.push(tokio::spawn(async move {
                    outcome(
                        engine
                            .append_routed(
                                routes,
                                conditional(
                                    0,
                                    &["contested"],
                                    vec![make_event(
                                        &format!("Claim-{r}"),
                                        vec![tag("orderId", "contested")],
                                    )],
                                ),
                                false,
                            )
                            .await,
                    )
                }));
            }
            let mut winners = 0;
            for racer in racers {
                match racer.await.unwrap() {
                    Ok(_) => winners += 1,
                    Err(conflicting) => assert_eq!(conflicting, 0, "{mode}"),
                }
            }
            assert_eq!(winners, 1, "{mode}");
            assert_eq!(engine.head(), Position(1), "{mode}");
            engine.shutdown();
        }
    }

    /// An fsync failure (or an epoch change) must reach an append in every
    /// staging mode: never a success, whether the append is staged before the
    /// failure, waiting on it, or arrives after.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_staging_mode_refuses_a_write_after_the_engine_is_poisoned() {
        for (mode, routes) in ALL_ROUTES {
            let dir = tempfile::tempdir().unwrap();
            let engine = Arc::new(small_engine(dir.path()));
            engine
                .append_routed(routes, plain(vec![make_event("Ok", vec![])]), false)
                .await
                .unwrap();
            let failure = Error::Io(io::Error::other("simulated ENOSPC"));
            engine.sync_state.fail_sync(&failure);
            engine.watermark.abort_all(&failure.to_string());
            let refused = engine
                .append_routed(routes, plain(vec![make_event("Late", vec![])]), false)
                .await;
            assert!(refused.is_err(), "{mode}: a poisoned engine must not ack");
            engine.shutdown();
        }
    }

    fn rotating_engine(dir: &Path) -> Arc<EventStoreEngine> {
        let opts = StoreOptions {
            max_segment_size: 4096,
            ..Default::default()
        };
        Arc::new(EventStoreEngine::create_with_store_options(dir, &opts).unwrap())
    }

    /// A mixed run of appends: plain and conditional, several events, none,
    /// some that conflict with an earlier one in the same run.
    fn mixed_run() -> Vec<AppendRequest> {
        (0..70u64)
            .map(|i| {
                let order = format!("o-{}", i % 7);
                let event = |n: u64| {
                    let mut e = make_event("OrderPlaced", vec![tag("orderId", &order)]);
                    e.payload = vec![n as u8; 30 + (n as usize * 11) % 120];
                    e
                };
                match i % 6 {
                    1 => conditional(i / 3, &[&order], vec![event(i)]),
                    2 => conditional(i, &["nobody"], vec![event(i), event(i + 100)]),
                    3 => conditional(0, &[&order], vec![]),
                    4 => plain(vec![]),
                    _ => plain((0..=i % 3).map(|k| event(i * 10 + k)).collect()),
                }
            })
            .collect()
    }

    /// Whatever grouping the staging thread happens to find, the log is the
    /// one the appends would have made one at a time: same answers, same
    /// positions, the same bytes in the same files, across rotations.
    #[tokio::test]
    async fn a_staged_group_makes_the_log_one_at_a_time_would() {
        let one_dir = tempfile::tempdir().unwrap();
        let one = rotating_engine(one_dir.path());
        let mut expected = Vec::new();
        for request in mixed_run() {
            expected.push(outcome(one.append(request)));
        }
        assert!(
            expected.iter().any(|o| o.is_err()),
            "the run has conflicts to find"
        );

        // Group sizes that split the run in different places, and one that
        // does not split it at all (the group limits do).
        for size in [1usize, 2, 7, 70] {
            let dir = tempfile::tempdir().unwrap();
            let grouped = rotating_engine(dir.path());
            let mut got = Vec::new();
            let mut run = mixed_run().into_iter().peekable();
            while run.peek().is_some() {
                let group: Vec<Prepared> = run.by_ref().take(size).map(client).collect();
                let stagings = grouped.stage_group(group);
                let mut release = None;
                for staging in &stagings {
                    if let Staging::Done(Ok(staged)) = staging {
                        release = release.max(staged.wait_pos());
                    }
                }
                grouped.release_written(release);
                for staging in stagings {
                    got.push(outcome(settle(&grouped, staging).await));
                }
            }
            assert_eq!(got, expected, "groups of {size}");
            assert_eq!(grouped.head(), one.head());
            one.seals.wait_idle();
            grouped.seals.wait_idle();
            let files = |dir: &Path| {
                let mut files: Vec<_> = std::fs::read_dir(dir)
                    .unwrap()
                    .flatten()
                    .map(|e| e.path())
                    .filter(|p| p.extension().is_some_and(|x| x == "seg"))
                    .collect();
                files.sort();
                files
            };
            let (a, b) = (files(one_dir.path()), files(dir.path()));
            assert!(a.len() > 2, "the run rotated");
            assert_eq!(a.len(), b.len(), "groups of {size}");
            for (x, y) in a.iter().zip(&b) {
                assert!(
                    std::fs::read(x).unwrap() == std::fs::read(y).unwrap(),
                    "groups of {size}: {:?} differs",
                    x.file_name()
                );
            }
            assert_eq!(
                grouped
                    .source(Position(0), &cond_match_all())
                    .unwrap()
                    .len(),
                one.source(Position(0), &cond_match_all()).unwrap().len()
            );
            grouped.shutdown();
        }
        one.shutdown();
    }

    /// A group longer than one hold of the writer lock takes is staged in
    /// several, and still in order.
    #[tokio::test]
    async fn a_long_group_stages_every_append_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(small_engine(dir.path()));
        let group: Vec<Prepared> = (0..100)
            .map(|i| {
                client(plain(vec![make_event(
                    "OrderPlaced",
                    vec![tag("n", &i.to_string())],
                )]))
            })
            .collect();
        let stagings = engine.stage_group(group);
        assert_eq!(stagings.len(), 100);
        let mut release = None;
        let mut firsts = Vec::new();
        for staging in &stagings {
            let Staging::Done(Ok(staged)) = staging else {
                panic!("every append stages");
            };
            release = release.max(staged.wait_pos());
            firsts.push(staged.outcome.as_ref().unwrap().first_position.0);
        }
        assert_eq!(firsts, (0..100).collect::<Vec<_>>(), "positions in order");
        engine.release_written(release);
        for staging in stagings {
            outcome(settle(&engine, staging).await).unwrap();
        }
        assert_eq!(engine.head(), Position(100));
        engine.shutdown();
    }

    /// A group whose write fails was accounted for but is not in the log:
    /// nothing in it may be acknowledged, nothing after it may be accepted,
    /// and no reader is told the log moved.
    #[tokio::test]
    async fn a_failed_group_write_poisons_the_engine_and_acknowledges_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(small_engine(dir.path()));
        outcome(engine.append(plain(vec![make_event("Before", vec![])]))).unwrap();
        let before = engine.local_tail();

        engine.writer.lock().fail_next_flush();
        // More than one hold's worth: the first fails, the rest meet a writer
        // that has given up.
        let group: Vec<Prepared> = (0..GROUP_MAX_APPENDS as i32 + 8)
            .map(|i| {
                client(plain(vec![make_event(
                    "Lost",
                    vec![tag("n", &i.to_string())],
                )]))
            })
            .chain(std::iter::once(client(conditional(
                0,
                &["x"],
                vec![make_event("Lost", vec![tag("orderId", "x")])],
            ))))
            .collect();
        for staging in engine.stage_group(group) {
            let result = settle(&engine, staging).await;
            assert!(result.is_err(), "a lost write must not be acknowledged");
        }
        assert!(engine.is_poisoned());
        assert_eq!(engine.local_tail(), before, "readers are not told");
        assert_eq!(engine.head(), before);
        let refused = engine
            .append(plain(vec![make_event("After", vec![])]))
            .map(|_| ());
        assert!(refused.is_err(), "a poisoned engine takes no more writes");
        engine.shutdown();
    }

    /// A leader that took over and has written no events yet holds only
    /// control records past its last event: a fresh segment starting with the
    /// `EpochChange`, at the same event position as its tail. A follower
    /// rejoining at that position (after cutting an unreplicated suffix) still
    /// needs those bytes, or it stays byte-divergent for as long as the leader
    /// stays idle.
    #[test]
    fn position_catch_up_at_the_tail_carries_the_new_epochs_control_records() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();
        for i in 0..3 {
            store
                .append(AppendRequest {
                    condition: None,
                    events: vec![make_event(&format!("E{i}"), vec![tag("k", "v")])],
                })
                .unwrap();
        }

        // Nothing past the last event: nothing to send.
        let head = store.head();
        let (tail, slices) = store.replication_catchup_slices(head).unwrap();
        assert_eq!(tail, head);
        assert!(slices.is_empty(), "no bytes follow the last event yet");

        // Taking over rotates at the head and writes an EpochChange there.
        store.begin_replication_epoch(1, [0]).unwrap();
        store.persist_epoch_change(1, 0).unwrap();
        assert_eq!(store.head(), head, "control records carry no position");

        let (tail, slices) = store.replication_catchup_slices(head).unwrap();
        assert_eq!(tail, head);
        assert_eq!(slices.len(), 1, "the new segment's records must be sent");
        let slice = &slices[0];
        assert_eq!(slice.segment_base, head.0);
        assert_eq!(slice.first_position, head.0);
        assert_eq!(slice.byte_start, segment::SEGMENT_HEADER_SIZE as u64);
        assert!(slice.byte_end > slice.byte_start);
    }

    /// A follower whose log ends somewhere that is not a record boundary of
    /// this one has a different log; that is an answer to the probe, not
    /// damage in this node's segment.
    #[test]
    fn a_probe_at_an_offset_inside_a_record_is_a_mismatch_not_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();
        for i in 0..3 {
            store
                .append(AppendRequest {
                    condition: None,
                    events: vec![make_event(&format!("E{i}"), vec![tag("k", "v")])],
                })
                .unwrap();
        }
        let cursor = store.replication_cursor().unwrap();
        assert!(
            store
                .verify_replication_probe(
                    cursor.segment_base,
                    cursor.byte_offset,
                    cursor.last_record_crc
                )
                .unwrap()
        );
        assert!(
            !store
                .verify_replication_probe(
                    cursor.segment_base,
                    cursor.byte_offset - 3,
                    cursor.last_record_crc
                )
                .unwrap()
        );
    }

    // ── Sealed index v3: rebuilds, old formats, recovery from summaries ──

    /// A log of several sealed segments with control records spread through
    /// it: four epochs (each rotating to a fresh segment and opening it with
    /// an EpochChange), watermark checkpoints between appends, varied types
    /// and tags. Returns the head.
    fn log_with_controls(dir: &Path) -> u64 {
        let store = tiny_segment_store(dir, ReadMode::Mapped);
        let mut n = 0u64;
        for epoch in 1..=4u64 {
            store.begin_replication_epoch(epoch, [0]).unwrap();
            store.persist_epoch_change(epoch, 0).unwrap();
            for burst in 0..6 {
                for _ in 0..7 {
                    let name = ["OrderPlaced", "ItemAdded", "OrderShipped"][(n % 3) as usize];
                    store
                        .append(AppendRequest {
                            condition: None,
                            events: vec![make_event(
                                name,
                                vec![
                                    tag("orderId", &format!("o-{}", n / 5)),
                                    tag("customerId", &format!("c-{}", n % 11)),
                                ],
                            )],
                        })
                        .unwrap();
                    n += 1;
                }
                if burst % 2 == 1 {
                    store.persist_watermark_checkpoint().unwrap();
                }
            }
        }
        store.seals.wait_idle();
        let sealed = store.segments.read().sealed_count;
        assert!(sealed >= 6, "need several sealed segments, got {sealed}");
        let head = store.head().0;
        store.shutdown();
        head
    }

    fn sealed_bases(dir: &Path) -> Vec<u64> {
        let mut bases = segment::list_segment_files(dir).unwrap();
        bases.pop(); // the active segment
        bases
    }

    fn index_bytes(dir: &Path) -> Vec<(u64, Vec<u8>)> {
        sealed_bases(dir)
            .into_iter()
            .map(|base| {
                let path = segment::segment_path(dir, base).with_extension("idx");
                (base, std::fs::read(path).unwrap())
            })
            .collect()
    }

    /// The `.idx` of a segment is a function of the segment: the one the
    /// seal wrote from the live index, the one a rebuild from the file
    /// writes, and the one a reopened store writes after losing it are the
    /// same bytes. (Archived indexes, ADR-0002, rely on this.)
    #[test]
    fn a_rebuilt_index_is_byte_identical_to_the_sealed_one() {
        let dir = tempfile::tempdir().unwrap();
        log_with_controls(dir.path());
        let sealed = index_bytes(dir.path());
        for (base, bytes) in &sealed {
            let path = segment::segment_path(dir.path(), *base);
            let rebuilt = SegmentIndex::build_from_segment(&path)
                .unwrap()
                .encode_locator()
                .unwrap();
            assert!(&rebuilt == bytes, "segment {base}: rebuild differs");
            std::fs::remove_file(path.with_extension("idx")).unwrap();
        }

        let store = EventStoreEngine::open_with_options(dir.path(), 4 * 1024).unwrap();
        let reopened = index_bytes(dir.path());
        assert_eq!(reopened.len(), sealed.len());
        for ((base, before), (_, after)) in sealed.iter().zip(&reopened) {
            assert!(
                before == after,
                "segment {base}: reopened store wrote different bytes"
            );
        }
        assert_eq!(
            store
                .source(Position(0), &tag_cond_key("customerId", "c-3"))
                .unwrap()
                .len(),
            (0..store.head().0).filter(|n| n % 11 == 3).count()
        );
    }

    fn tag_cond_key(key: &str, value: &str) -> SourcingCondition {
        SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag(key, value)],
            }],
        }
    }

    /// An index open cannot read (another format, or damaged) is rebuilt
    /// from its segment, and so is a missing one.
    #[test]
    fn open_rebuilds_unreadable_and_missing_indexes() {
        let dir = tempfile::tempdir().unwrap();
        log_with_controls(dir.path());
        let expected = index_bytes(dir.path());
        let bases = sealed_bases(dir.path());
        for (i, &base) in bases.iter().enumerate() {
            let path = segment::segment_path(dir.path(), base);
            let idx = path.with_extension("idx");
            match i % 3 {
                0 => {
                    // Another format version.
                    let mut other = b"KIDX\x02".to_vec();
                    other.extend_from_slice(&[0x5A; 300]);
                    std::fs::write(&idx, other).unwrap();
                }
                1 => {
                    let mut damaged = std::fs::read(&idx).unwrap();
                    damaged[60] ^= 0x01;
                    std::fs::write(&idx, damaged).unwrap();
                }
                _ => std::fs::remove_file(&idx).unwrap(),
            }
        }

        let store = EventStoreEngine::open_with_options(dir.path(), 4 * 1024).unwrap();
        assert_eq!(index_bytes(dir.path()), expected);
        let head = store.head().0;
        assert_eq!(
            store.source(Position(0), &cond_match_all()).unwrap().len() as u64,
            head
        );
        assert_eq!(
            store
                .source(Position(0), &tag_cond_key("orderId", "o-3"))
                .unwrap()
                .len(),
            5
        );
    }

    /// The control state recovery computed before indexes carried control
    /// summaries: a scan of every record of every segment. Kept as the
    /// reference the summaries are checked against.
    fn recover_by_full_scan(dir: &Path, local_tail: u64) -> RecoveredControl {
        use std::io::Read;

        let mut recovered = RecoveredControl {
            epoch: INITIAL_EPOCH,
            checkpoint: 0,
            epochs: crate::replication::epochs::EpochIndex::default(),
        };
        for base in segment::list_segment_files(dir).unwrap() {
            let mut file = std::fs::File::open(segment::segment_path(dir, base)).unwrap();
            let mut segment_header = [0u8; segment::SEGMENT_HEADER_SIZE];
            file.read_exact(&mut segment_header).unwrap();
            loop {
                let mut header = [0u8; segment::RECORD_HEADER_SIZE];
                if file.read_exact(&mut header).is_err() {
                    break;
                }
                let Some(header) = crate::segment::record::parse_header(&header).unwrap() else {
                    break;
                };
                let mut payload = vec![0u8; header.payload_len];
                if file.read_exact(&mut payload).is_err()
                    || !crate::segment::record::validate_crc(header, &payload)
                {
                    break;
                }
                let crate::segment::record::NativeRecord::Control(control) =
                    crate::segment::record::decode_native(header, &payload).unwrap()
                else {
                    continue;
                };
                match control {
                    crate::segment::format::ControlRecord::EpochChange {
                        epoch,
                        start_position,
                        ..
                    } => {
                        recovered.epoch = recovered.epoch.max(epoch);
                        recovered.epochs.record(epoch, start_position);
                    }
                    crate::segment::format::ControlRecord::WatermarkCheckpoint {
                        epoch,
                        position,
                    } if position <= local_tail => {
                        recovered.checkpoint = recovered.checkpoint.max(position);
                        recovered.epoch = recovered.epoch.max(epoch);
                    }
                    crate::segment::format::ControlRecord::WatermarkCheckpoint { .. } => {}
                }
            }
        }
        recovered
    }

    fn assert_same_recovery(a: &RecoveredControl, b: &RecoveredControl) {
        assert_eq!(a.epoch, b.epoch);
        assert_eq!(a.checkpoint, b.checkpoint);
        assert_eq!(a.epochs.last_epoch(), b.epochs.last_epoch());
        for epoch in 0..8 {
            assert_eq!(
                a.epochs.extent(epoch),
                b.epochs.extent(epoch),
                "epoch {epoch}"
            );
        }
    }

    /// Recovery from the sealed indexes' control summaries and the active
    /// segment's live index finds exactly what a scan of every record did.
    #[test]
    fn recovery_from_control_summaries_equals_a_full_scan() {
        let dir = tempfile::tempdir().unwrap();
        let head = log_with_controls(dir.path());
        let scanned = recover_by_full_scan(dir.path(), head);
        assert_eq!(scanned.epoch, 4);
        assert!(scanned.checkpoint > 0);
        assert_eq!(scanned.epochs.last_epoch(), Some(4));

        let writer = SegmentWriter::open(dir.path(), 4 * 1024).unwrap();
        let bases = segment::list_segment_files(dir.path()).unwrap();
        let sealed = crate::log_index::open(dir.path(), &bases, writer.active_base_position())
            .unwrap()
            .locators;
        assert!(sealed.iter().any(|(_, index)| !index.controls().is_empty()));
        let active = writer.active_index_handle();
        let summarized = recover_native_control_state(
            sealed.iter().map(|(_, index)| index.controls()),
            active.read().controls(),
            head,
        );
        assert_same_recovery(&summarized, &scanned);
        // A local tail below some checkpoints leaves those out, both ways.
        let low = scanned.checkpoint - 1;
        assert_same_recovery(
            &recover_native_control_state(
                sealed.iter().map(|(_, index)| index.controls()),
                active.read().controls(),
                low,
            ),
            &recover_by_full_scan(dir.path(), low),
        );
        drop(writer);

        // And the engine opens to the same state.
        let store = EventStoreEngine::open_with_options(dir.path(), 4 * 1024).unwrap();
        assert_eq!(store.log_epoch(), 4);
        for epoch in 0..8 {
            assert_eq!(store.epoch_extent(epoch), scanned.epochs.extent(epoch));
        }
    }

    /// Opening a log reads no sealed segment's records: with every sealed
    /// segment's body gone (its file emptied, its index intact), the store
    /// still opens, to the same control state.
    #[test]
    fn open_reads_no_sealed_segment_body() {
        let dir = tempfile::tempdir().unwrap();
        let head = log_with_controls(dir.path());
        let scanned = recover_by_full_scan(dir.path(), head);
        for base in sealed_bases(dir.path()) {
            std::fs::OpenOptions::new()
                .write(true)
                .open(segment::segment_path(dir.path(), base))
                .unwrap()
                .set_len(0)
                .unwrap();
        }
        let store = EventStoreEngine::open_with_options(dir.path(), 4 * 1024).unwrap();
        assert_eq!(store.head().0, head);
        assert_eq!(store.log_epoch(), scanned.epoch);
        for epoch in 0..8 {
            assert_eq!(store.epoch_extent(epoch), scanned.epochs.extent(epoch));
        }
        // Indexes still answer what lives where; only reading the records
        // themselves needs the bodies.
        assert!(store.has_matches_between(
            Position(0),
            Position(head),
            &tag_cond_key("orderId", "o-1")
        ));
    }
}
