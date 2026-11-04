//! Cold reads (ADR-0011): the bytes of evicted segments, read from the
//! archive by range.
//!
//! A segment whose body was evicted keeps its locator and key tables, so a
//! read is planned exactly as for a local one; only the bytes come from the
//! archive's `segments/<base>.seg`. They are fetched in aligned blocks of
//! [`BLOCK`] bytes: a block several readers want is downloaded once (the
//! first to ask fetches it, the others wait for it), and recent blocks stay
//! in a small in-memory cache, so processors replaying the same cold range
//! together fetch it once. Neighbouring missing blocks go in one request of
//! up to [`MAX_GET`] bytes, and the requests run concurrently.
//!
//! Reads from the archive are rare (it holds what a node's disk cannot), so
//! what matters is that a read does not pay a round trip per segment, and a
//! replay runs at the network's pace rather than one round trip at a time.
//! Reads plan ahead from the index alone and [`RemoteSegments::prefetch`]
//! the next [`READAHEAD`] bytes of blocks in one go, across every segment
//! they span.
//!
//! The cache and the downloads in flight belong to the [`ArchiveStore`],
//! one per node, so their memory is bounded for the node however many
//! contexts and readers there are: the cache by its budget, the requests in
//! flight by theirs ([`DEFAULT_IN_FLIGHT_BYTES`]). A read waits its turn for
//! that budget; a prefetch takes only what is free and skips the rest,
//! which its read then fetches when it gets there.
//!
//! Reads are synchronous to their callers, which are blocking read threads:
//! requests run on a small runtime of the archive's own and the caller
//! waits for them. Ranged bytes are not checked here; every record is
//! CRC-checked where it is read, as on the local paths. Whole segments are
//! checked against their summaries' blake3.
//!
//! A request the archive does not answer is `Error::Unavailable`. An object
//! the archive should hold and does not is lost for good, `Error::Internal`;
//! one of another length than its locator or summary says, or whose hash
//! differs, is `Error::Corrupted`.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use bytes::Bytes;
use lru::LruCache;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};

use crate::error::Error;
use crate::segment::sealed_index::SealedIndex;

/// Bytes of one cached block of an archived segment.
pub const BLOCK: u64 = 256 * 1024;

/// The most one request of a read fetches. Whole objects are downloaded in
/// requests of [`CHUNK`] bytes.
pub const MAX_GET: u64 = 4 * 1024 * 1024;

/// Bytes of blocks a read plans and prefetches ahead of itself in evicted
/// segments; it plans the next stretch halfway through this one.
pub const READAHEAD: u64 = 32 * 1024 * 1024;

/// Objects (summaries, locators, key tables) downloaded at once by one call;
/// their bytes in flight are bounded by the node's in-flight budget.
const DOWNLOADS_IN_FLIGHT: usize = 32;

/// What [`RemoteSegments::listing`] finds.
#[derive(Debug, Default)]
pub struct ArchiveListing {
    /// Archived segments by base, with their data length.
    pub segments: std::collections::BTreeMap<u64, u64>,
    /// Ranges of the archived key tables, ascending.
    pub tables: Vec<(u64, u64)>,
}

/// The blocks holding `ranges`.
fn blocks_of(ranges: &[Range<u64>]) -> BTreeSet<u64> {
    ranges
        .iter()
        .filter(|range| range.end > range.start)
        .flat_map(|range| range.start / BLOCK..=(range.end - 1) / BLOCK)
        .collect()
}

/// Default memory budget of the cache of fetched blocks, node-wide.
pub const DEFAULT_CACHE_BYTES: usize = 128 << 20;

/// Default bytes of segment requests in flight at once, node-wide: what
/// downloads hold in memory before they reach the cache or a file. Two
/// reads prefetching [`READAHEAD`] each fill it; past it, prefetches shrink
/// and reads wait their turn.
pub const DEFAULT_IN_FLIGHT_BYTES: u64 = 64 << 20;

/// What an [`ArchiveStore`] may hold in memory and how long it waits.
#[derive(Debug, Clone, Copy)]
pub struct ArchiveLimits {
    /// Bytes of the block cache.
    pub cache_bytes: usize,
    /// Bytes of segment requests in flight; at least one block.
    pub in_flight_bytes: u64,
    /// How long one download of segment bytes may take.
    pub fetch_timeout: std::time::Duration,
}

impl Default for ArchiveLimits {
    fn default() -> Self {
        Self {
            cache_bytes: DEFAULT_CACHE_BYTES,
            in_flight_bytes: DEFAULT_IN_FLIGHT_BYTES,
            fetch_timeout: FETCH_TIMEOUT,
        }
    }
}

/// An archive as cold reads see it: its object store, the runtime its
/// requests run on, the cache of fetched blocks and the downloads in
/// flight. One per process, shared by every context, so its memory bounds
/// hold for the node.
pub struct ArchiveStore {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    io: IoRuntime,
    limits: ArchiveLimits,
    /// Recent blocks of every context's evicted segments.
    blocks: Mutex<LruCache<BlockKey, Bytes>>,
    /// Blocks being downloaded, which other readers wait for.
    flights: Mutex<HashMap<BlockKey, Arc<Flight>>>,
    /// Blocks' worth of requests in flight: a request holds one permit per
    /// block (a whole-object chunk, one per [`BLOCK`] of it) until its bytes
    /// are cached or written.
    in_flight: Arc<tokio::sync::Semaphore>,
    /// The permits `in_flight` holds when nothing is in flight.
    in_flight_blocks: u32,
    /// Numbers each context's cold reads, which key their blocks.
    next_context: AtomicU64,
    /// The cap on sealed bytes this node keeps on disk, if any: a node
    /// catching up from the archive skips the bodies it would evict anyway.
    max_local_bytes: Option<u64>,
}

impl std::fmt::Debug for ArchiveStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArchiveStore")
            .field("prefix", &self.prefix)
            .field("limits", &self.limits)
            .field("max_local_bytes", &self.max_local_bytes)
            .finish()
    }
}

impl ArchiveStore {
    /// The archive at `raw_url`, as for [`crate::tier::Archiver::from_url`],
    /// for a node that keeps at most `max_local_bytes` of sealed segments on
    /// disk (`None`: all of them).
    pub fn from_url(raw_url: &str, max_local_bytes: Option<u64>) -> Result<Arc<Self>, Error> {
        let (store, prefix) = open_store(raw_url)?;
        Self::with_limits(
            Arc::from(store),
            prefix,
            max_local_bytes,
            ArchiveLimits::default(),
        )
    }

    /// An archive over `store` itself, its objects under `prefix`: for
    /// tests that wrap a store (to delay or break it).
    pub fn over_store(
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
        max_local_bytes: Option<u64>,
    ) -> Result<Arc<Self>, Error> {
        Self::with_limits(store, prefix, max_local_bytes, ArchiveLimits::default())
    }

    /// [`over_store`](Self::over_store) within `limits`.
    pub fn with_limits(
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
        max_local_bytes: Option<u64>,
        limits: ArchiveLimits,
    ) -> Result<Arc<Self>, Error> {
        let cached = (limits.cache_bytes as u64 / BLOCK).max(1) as usize;
        let in_flight_blocks = (limits.in_flight_bytes / BLOCK).clamp(1, u32::MAX.into()) as u32;
        Ok(Arc::new(Self {
            store,
            prefix,
            io: IoRuntime::new()?,
            limits,
            blocks: Mutex::new(LruCache::new(
                NonZeroUsize::new(cached).expect("at least one block"),
            )),
            flights: Mutex::new(HashMap::new()),
            in_flight: Arc::new(tokio::sync::Semaphore::new(in_flight_blocks as usize)),
            in_flight_blocks,
            next_context: AtomicU64::new(0),
            max_local_bytes,
        }))
    }

    /// The cold reads of the context whose data directory is `dir`. Its
    /// objects are under the directory's name.
    pub fn segments(self: &Arc<Self>, dir: &Path) -> Result<RemoteSegments, Error> {
        let context = dir
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| Error::Internal {
                message: format!("context directory {} has no name", dir.display()),
            })?;
        Ok(RemoteSegments {
            archive: Arc::clone(self),
            id: self.next_context.fetch_add(1, Ordering::Relaxed),
            context: self.prefix.clone().join(context),
            dir: dir.to_path_buf(),
            data_lens: Mutex::new(HashMap::new()),
            stats: Arc::default(),
        })
    }

    /// Permits a request of `bytes` takes from `in_flight`: one per block,
    /// and never more than the budget holds, so a request larger than the
    /// budget waits for all of it rather than for ever.
    fn permits_for(&self, bytes: u64) -> u32 {
        (bytes.div_ceil(BLOCK).max(1)).min(self.in_flight_blocks.into()) as u32
    }

    /// Runs `future` on the archive's runtime and waits for it.
    fn block_on<T: Send + 'static>(
        &self,
        future: impl Future<Output = Result<T, Error>> + Send + 'static,
    ) -> Result<T, Error> {
        let (done, result) = std::sync::mpsc::sync_channel(1);
        self.io.handle().spawn(async move {
            let _ = done.send(future.await);
        });
        result.recv().map_err(|_| Error::Unavailable {
            message: "the archive's runtime shut down before the request finished".into(),
        })?
    }
}

/// The archive's own runtime, shut down without waiting when dropped, which
/// may happen on a thread that must not block.
struct IoRuntime(Option<tokio::runtime::Runtime>);

impl IoRuntime {
    fn new() -> Result<Self, Error> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("archive-io")
            .enable_all()
            .build()?;
        Ok(Self(Some(runtime)))
    }

    fn handle(&self) -> &tokio::runtime::Handle {
        self.0.as_ref().expect("runtime lives until drop").handle()
    }
}

impl Drop for IoRuntime {
    fn drop(&mut self) {
        if let Some(runtime) = self.0.take() {
            runtime.shutdown_background();
        }
    }
}

/// The blocks a download task owes its waiters. If the task never gets to
/// answer them (it panicked, or the runtime shut down and dropped it), they
/// are answered with an error here, so no reader waits forever.
struct Pending {
    archive: Arc<ArchiveStore>,
    /// The context's number and the segment's base.
    segment: (u64, u64),
    blocks: Option<Waiters>,
}

impl Drop for Pending {
    fn drop(&mut self) {
        let Some(blocks) = self.blocks.take() else {
            return;
        };
        let (context, base) = self.segment;
        let mut flights = lock(&self.archive.flights);
        for (block, flight) in blocks {
            forget(&mut flights, (context, base, block), &flight);
            flight.finish(Err(Failure::Unavailable(
                "the download was abandoned".into(),
            )));
        }
    }
}

/// Why a download from the archive failed, kept apart so every reader of
/// a shared download reports it as what it is.
#[derive(Clone, Debug)]
enum Failure {
    /// The object is not in the archive, though a summary or a locator says
    /// it is. Asking again will not bring it.
    Missing,
    /// The object is not as long as its summary or locator says: an
    /// integrity failure.
    WrongSize {
        size: u64,
        expected: u64,
        /// What says how long it should be.
        by: &'static str,
    },
    /// The archive did not answer, or not in full. Asking again may work.
    Unavailable(String),
    /// Writing the download here failed.
    Local(Arc<std::io::Error>),
}

impl Failure {
    fn of(error: object_store::Error) -> Self {
        match error {
            object_store::Error::NotFound { .. } => Self::Missing,
            error => Self::Unavailable(error.to_string()),
        }
    }

    fn local(error: std::io::Error) -> Self {
        Self::Local(Arc::new(error))
    }

    /// What a read of `what` (a segment, an object) reports.
    fn into_error(self, what: &str) -> Error {
        match self {
            Self::Missing => Error::Internal {
                message: format!("{what} is missing from the archive"),
            },
            Self::WrongSize { size, expected, by } => Error::Corrupted {
                message: format!("archived {what} is {size} bytes; {by} {expected}"),
            },
            Self::Unavailable(message) => Error::Unavailable {
                message: format!("reading {what} from the archive: {message}"),
            },
            Self::Local(error) => Error::Io(std::io::Error::new(error.kind(), error)),
        }
    }
}

/// A download that did not finish within `timeout`.
fn timed_out(timeout: std::time::Duration) -> Failure {
    Failure::Unavailable(format!("no answer within {}s", timeout.as_secs_f64()))
}

/// Bytes `range` of `object`, which `by` says is `len` bytes long. An
/// object of another length is [`Failure::WrongSize`], also when the range
/// starts past its end: the archive refuses such a request, and the size
/// is then asked for, briefly, to tell that from a request that failed.
async fn get_range(
    store: &dyn ObjectStore,
    object: &ObjectPath,
    range: Range<u64>,
    len: u64,
    by: &'static str,
) -> Result<Bytes, Failure> {
    let wrong_size = |size: u64| Failure::WrongSize {
        size,
        expected: len,
        by,
    };
    let options = object_store::GetOptions::new().with_range(Some(range.clone()));
    let result = match store.get_opts(object, options).await {
        Ok(result) => result,
        Err(object_store::Error::NotFound { .. }) => return Err(Failure::Missing),
        Err(error) => {
            let head = tokio::time::timeout(SIZE_CHECK_TIMEOUT, store.head(object)).await;
            return Err(match head {
                Ok(Ok(meta)) if meta.size != len => wrong_size(meta.size),
                Ok(Err(object_store::Error::NotFound { .. })) => Failure::Missing,
                _ => Failure::Unavailable(error.to_string()),
            });
        }
    };
    if result.meta.size != len {
        return Err(wrong_size(result.meta.size));
    }
    let bytes = result.bytes().await.map_err(Failure::of)?;
    if bytes.len() as u64 != range.end - range.start {
        return Err(Failure::Unavailable(format!(
            "the archive returned {} bytes for {}..{}",
            bytes.len(),
            range.start,
            range.end
        )));
    }
    Ok(bytes)
}

/// How long a failed request waits to learn the object's size, which tells
/// a range past its end from a request the archive did not answer.
const SIZE_CHECK_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// One block being downloaded: its download task fills it in, and every
/// reader that wants the block waits on it.
#[derive(Default)]
struct Flight {
    state: Mutex<Option<Result<Bytes, Failure>>>,
    done: Condvar,
}

impl Flight {
    fn finish(&self, result: Result<Bytes, Failure>) {
        *lock(&self.state) = Some(result);
        self.done.notify_all();
    }

    /// Its result, once the download has one, or a failure at `deadline`.
    fn wait(&self, deadline: std::time::Instant) -> Result<Bytes, Failure> {
        let mut state = lock(&self.state);
        loop {
            if let Some(result) = state.as_ref() {
                return result.clone();
            }
            let left = deadline.saturating_duration_since(std::time::Instant::now());
            if left.is_zero() {
                return Err(Failure::Unavailable(
                    "the download did not finish in time".into(),
                ));
            }
            state = self
                .done
                .wait_timeout(state, left)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
    }
}

/// Takes `flight`, the download of block `key`, out of `flights` once it is
/// answered or given up on, unless a later download of the block has taken
/// its place: that one is still under way, and readers share it.
fn forget(flights: &mut HashMap<BlockKey, Arc<Flight>>, key: BlockKey, flight: &Arc<Flight>) {
    if flights
        .get(&key)
        .is_some_and(|current| Arc::ptr_eq(current, flight))
    {
        flights.remove(&key);
    }
}

/// Locks `mutex` whether or not a thread panicked holding it: what it
/// guards (cached blocks, downloads in flight) stays consistent at every
/// point a panic could leave it, and one panicking download must not fail
/// every later read.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A cached or downloading block: the number of its context's cold reads,
/// its segment's base, and the block.
type BlockKey = (u64, u64, u64);

/// Blocks of one segment, each with the download that brings it.
type Waiters = Vec<(u64, Arc<Flight>)>;

/// Who asks for blocks.
#[derive(Clone, Copy)]
enum Ask {
    /// A read, which waits for them.
    Read,
    /// A prefetch, which asks only for what the in-flight budget has room
    /// for now.
    Prefetch,
}

/// The archived segments of one context, read by range.
pub struct RemoteSegments {
    archive: Arc<ArchiveStore>,
    /// Keys this context's blocks in the archive's cache. A context opened
    /// again gets a new number, so it never reads blocks cached before.
    id: u64,
    /// `<prefix>/<context>`.
    context: ObjectPath,
    /// The context's local data directory, where the locators are.
    dir: PathBuf,
    /// Data bytes of each archived segment, from its locator.
    data_lens: Mutex<HashMap<u64, u64>>,
    stats: Arc<ReadStats>,
}

impl std::fmt::Debug for RemoteSegments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteSegments")
            .field("context", &self.context)
            .finish()
    }
}

impl RemoteSegments {
    fn segment_object(&self, base: u64) -> ObjectPath {
        self.context
            .clone()
            .join("segments")
            .join(format!("{base:020}.seg"))
    }

    /// The cap on sealed bytes this node keeps on disk, if any.
    pub fn max_local_bytes(&self) -> Option<u64> {
        self.archive.max_local_bytes
    }

    /// What the archive holds of this context: each archived segment (one
    /// with a summary) by base, with its data length, and the ranges of its
    /// key tables.
    pub fn listing(&self) -> Result<ArchiveListing, Error> {
        let store = Arc::clone(&self.archive.store);
        let prefix = self.context.clone();
        let objects = self.archive.block_on(async move {
            use futures_util::TryStreamExt;
            store
                .list(Some(&prefix))
                .map_ok(|meta| {
                    let name = meta.location.filename().unwrap_or_default().to_string();
                    (name, meta.size)
                })
                .try_collect::<Vec<_>>()
                .await
                .map_err(|error| Error::Unavailable {
                    message: format!("listing the archive: {error}"),
                })
        })?;
        let mut sizes = std::collections::BTreeMap::new();
        let mut summaries = BTreeSet::new();
        let mut tables = Vec::new();
        for (name, size) in objects {
            if let Some(range) = crate::segment::merge::parse_table_file_name(&name) {
                tables.push(range);
            } else if let Some((stem, extension)) = name.split_once('.')
                && let Ok(base) = stem.parse::<u64>()
            {
                match extension {
                    "seg" => {
                        sizes.insert(base, size);
                    }
                    "sum" => {
                        summaries.insert(base);
                    }
                    _ => {}
                }
            }
        }
        sizes.retain(|base, _| summaries.contains(base));
        tables.sort_unstable();
        Ok(ArchiveListing {
            segments: sizes,
            tables,
        })
    }

    /// Downloads every `(kind, name, local)` as [`download`](Self::download)
    /// does, many at once. Returns which the archive held. Each goes in
    /// bounded requests straight to its file, so what the downloads hold in
    /// memory stays within the node's in-flight budget.
    pub fn download_all(&self, files: &[(&str, String, PathBuf)]) -> Result<Vec<bool>, Error> {
        let archive = Arc::clone(&self.archive);
        let requests: Vec<(ObjectPath, PathBuf)> = files
            .iter()
            .map(|(kind, name, local)| {
                (
                    self.context.clone().join(*kind).join(name.as_str()),
                    local.clone(),
                )
            })
            .collect();
        let held = self.archive.block_on(async move {
            use futures_util::StreamExt;
            let archive = &archive;
            let results: Vec<Result<bool, Error>> = futures_util::stream::iter(requests)
                .map(|(object, local)| async move { download_file(archive, &object, &local).await })
                .buffered(DOWNLOADS_IN_FLIGHT)
                .collect()
                .await;
            results.into_iter().collect::<Result<Vec<_>, _>>()
        })?;
        for dir in files
            .iter()
            .filter_map(|(_, _, local)| local.parent())
            .collect::<BTreeSet<_>>()
        {
            crate::segment::writer::sync_dir(dir)?;
        }
        Ok(held)
    }

    /// Requests for segment bytes made so far, by reads and whole-segment
    /// downloads.
    pub fn requests(&self) -> u64 {
        self.stats.requests.load(Ordering::Relaxed)
    }

    /// Reads and whole-segment downloads that failed so far.
    pub fn failures(&self) -> u64 {
        self.stats.failures.load(Ordering::Relaxed)
    }

    /// Bytes of segments fetched from the archive so far.
    pub fn fetched_bytes(&self) -> u64 {
        self.stats.fetched_bytes.load(Ordering::Relaxed)
    }

    /// Bytes of segment `base`'s data, which is what its archived object
    /// holds.
    pub fn data_len(&self, base: u64) -> Result<u64, Error> {
        if let Some(&len) = lock(&self.data_lens).get(&base) {
            return Ok(len);
        }
        let idx = crate::segment::segment_path(&self.dir, base).with_extension("idx");
        let len = SealedIndex::open(&idx, base)?.data_len();
        lock(&self.data_lens).insert(base, len);
        Ok(len)
    }

    /// The bytes of each of `ranges` of segment `base`.
    pub fn read(self: &Arc<Self>, base: u64, ranges: &[Range<u64>]) -> Result<Vec<Vec<u8>>, Error> {
        let data_len = self.data_len(base)?;
        if let Some(range) = ranges.iter().find(|range| range.end > data_len) {
            return Err(Error::Corrupted {
                message: format!(
                    "the index places bytes {}..{} in segment {base}, whose data ends at \
                     {data_len}",
                    range.start, range.end
                ),
            });
        }
        let (mut have, waiting, _) = self.reserve(base, data_len, blocks_of(ranges), Ask::Read);
        // Every download fails by its own deadline; this one is for a
        // download that never answers at all.
        let deadline =
            std::time::Instant::now() + self.archive.limits.fetch_timeout + RETRY_TIMEOUT;
        for (block, flight) in waiting {
            let bytes = flight.wait(deadline).map_err(|failure| {
                // A later read starts a download of its own.
                forget(
                    &mut lock(&self.archive.flights),
                    (self.id, base, block),
                    &flight,
                );
                failure.into_error(&format!("segment {base}"))
            })?;
            have.insert(block, bytes);
        }

        Ok(ranges
            .iter()
            .map(|range| {
                let mut out = Vec::with_capacity((range.end - range.start) as usize);
                let mut at = range.start;
                while at < range.end {
                    let block = at / BLOCK;
                    let bytes = &have[&block];
                    let from = (at - block * BLOCK) as usize;
                    let to = ((range.end - block * BLOCK) as usize).min(bytes.len());
                    out.extend_from_slice(&bytes[from..to]);
                    at = block * BLOCK + to as u64;
                }
                out
            })
            .collect())
    }

    /// Starts downloading the blocks holding `ranges` of each segment that
    /// are neither cached nor on their way, without waiting: a read that
    /// wants them later finds them cached or waits for them. What a read is
    /// about to need, or a scan will need next, nearest first.
    ///
    /// A prefetch only takes what is free of the node's in-flight budget:
    /// it stops at the first request the budget has no room for, so what it
    /// starts is the nearest part of what was asked, and it never queues.
    pub fn prefetch(self: &Arc<Self>, wanted: &[(u64, Vec<Range<u64>>)]) -> Result<(), Error> {
        for (base, ranges) in wanted {
            let data_len = self.data_len(*base)?;
            let blocks = blocks_of(ranges)
                .into_iter()
                .filter(|&block| block * BLOCK < data_len)
                .collect();
            let (_, _, whole) = self.reserve(*base, data_len, blocks, Ask::Prefetch);
            if !whole {
                break;
            }
        }
        Ok(())
    }

    /// For each of `wanted` (blocks of segment `base`): its bytes when
    /// cached, or the download that brings them, which this call starts
    /// for the blocks nobody is fetching yet. A read starts every download
    /// it needs, each waiting its turn for the in-flight budget; a prefetch
    /// starts them as far as the budget has room now, leaves the rest out
    /// of what it returns, and says it stopped short (`false`).
    fn reserve(
        self: &Arc<Self>,
        base: u64,
        data_len: u64,
        wanted: BTreeSet<u64>,
        ask: Ask,
    ) -> (HashMap<u64, Bytes>, Waiters, bool) {
        let mut have = HashMap::new();
        let mut waiting = Vec::new();
        let mut missing = Vec::new();
        let mut cache = lock(&self.archive.blocks);
        let mut flights = lock(&self.archive.flights);
        for block in wanted {
            let key = (self.id, base, block);
            if let Some(bytes) = cache.get(&key) {
                have.insert(block, bytes.clone());
            } else if let Some(flight) = flights.get(&key) {
                waiting.push((block, Arc::clone(flight)));
            } else {
                missing.push(block);
            }
        }

        // Neighbouring blocks in one request, of at most `MAX_GET` bytes
        // and never more than the in-flight budget.
        let largest = MAX_GET.min(u64::from(self.archive.in_flight_blocks) * BLOCK);
        let mut runs: Vec<(Range<u64>, Vec<u64>)> = Vec::new();
        for block in missing {
            let (start, end) = (block * BLOCK, ((block + 1) * BLOCK).min(data_len));
            match runs.last_mut() {
                Some((run, blocks)) if run.end == start && end - run.start <= largest => {
                    run.end = end;
                    blocks.push(block);
                }
                _ => runs.push((start..end, vec![block])),
            }
        }
        for (run, blocks) in runs {
            let permits = self.archive.permits_for(run.end - run.start);
            let permit = match ask {
                Ask::Read => None,
                Ask::Prefetch => {
                    match Arc::clone(&self.archive.in_flight).try_acquire_many_owned(permits) {
                        Ok(permit) => Some(permit),
                        Err(_) => return (have, waiting, false),
                    }
                }
            };
            let mine: Waiters = blocks
                .into_iter()
                .map(|block| {
                    let flight = Arc::new(Flight::default());
                    flights.insert((self.id, base, block), Arc::clone(&flight));
                    waiting.push((block, Arc::clone(&flight)));
                    (block, flight)
                })
                .collect();
            self.spawn_fetch(base, data_len, run, mine, permit);
        }
        (have, waiting, true)
    }

    /// Downloads `run` of segment `base`, which holds `blocks`, on the
    /// archive's runtime: once it holds `permit`, or its share of the
    /// in-flight budget when it has none, it fetches the run, caches its
    /// blocks and lets their waiters go.
    fn spawn_fetch(
        self: &Arc<Self>,
        base: u64,
        data_len: u64,
        run: Range<u64>,
        blocks: Waiters,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) {
        let this = Arc::clone(self);
        let object = self.segment_object(base);
        let mut pending = Pending {
            archive: Arc::clone(&self.archive),
            segment: (self.id, base),
            blocks: Some(blocks),
        };
        self.archive.io.handle().spawn(async move {
            let archive = &this.archive;
            let timeout = archive.limits.fetch_timeout;
            let fetched = tokio::time::timeout(timeout, async {
                let permit = match permit {
                    Some(permit) => permit,
                    None => Arc::clone(&archive.in_flight)
                        .acquire_many_owned(archive.permits_for(run.end - run.start))
                        .await
                        .expect("the in-flight budget is never closed"),
                };
                this.stats.requests.fetch_add(1, Ordering::Relaxed);
                let bytes = get_range(
                    archive.store.as_ref(),
                    &object,
                    run,
                    data_len,
                    "its locator says",
                )
                .await;
                (permit, bytes)
            })
            .await
            .map_or_else(
                |_| Err(timed_out(timeout)),
                // The permit goes once the blocks are cached, below.
                |(permit, bytes)| bytes.map(|bytes| (permit, bytes)),
            );
            let blocks = pending.blocks.take().expect("answered once");
            match fetched {
                Ok((_permit, bytes)) => {
                    this.stats
                        .fetched_bytes
                        .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    // Each block is copied out: a slice would share the
                    // request's buffer, and one block left in the cache
                    // would keep all of it alive.
                    let mut cache = lock(&archive.blocks);
                    let mut flights = lock(&archive.flights);
                    for ((block, flight), bytes) in blocks
                        .iter()
                        .zip(bytes.chunks(BLOCK as usize).map(Bytes::copy_from_slice))
                    {
                        cache.put((this.id, base, *block), bytes.clone());
                        forget(&mut flights, (this.id, base, *block), flight);
                        flight.finish(Ok(bytes));
                    }
                }
                Err(failure) => {
                    this.stats.failures.fetch_add(1, Ordering::Relaxed);
                    let mut flights = lock(&archive.flights);
                    for (block, flight) in &blocks {
                        forget(&mut flights, (this.id, base, *block), flight);
                        flight.finish(Err(failure.clone()));
                    }
                }
            }
        });
    }

    /// The summary of archived segment `base`; `None` when the archive does
    /// not hold it.
    pub fn summary(&self, base: u64) -> Result<Option<crate::tier::SegmentSummary>, Error> {
        Ok(self.summaries(&[base])?.pop().flatten())
    }

    /// The summaries of archived segments `bases`, read many at once, in
    /// the same order; `None` for one the archive does not hold.
    pub fn summaries(
        &self,
        bases: &[u64],
    ) -> Result<Vec<Option<crate::tier::SegmentSummary>>, Error> {
        let store = Arc::clone(&self.archive.store);
        let objects: Vec<(u64, ObjectPath)> = bases
            .iter()
            .map(|&base| {
                let object = self
                    .context
                    .clone()
                    .join("segments")
                    .join(format!("{base:020}.sum"));
                (base, object)
            })
            .collect();
        let timeout = self.archive.limits.fetch_timeout;
        self.archive.block_on(async move {
            use futures_util::StreamExt;
            let results: Vec<Result<Option<crate::tier::SegmentSummary>, Error>> =
                futures_util::stream::iter(objects)
                    .map(|(base, object)| {
                        let store = Arc::clone(&store);
                        async move {
                            let get = async {
                                match store.get(&object).await {
                                    Ok(result) => result.bytes().await.map(Some),
                                    Err(object_store::Error::NotFound { .. }) => Ok(None),
                                    Err(error) => Err(error),
                                }
                            };
                            let bytes = tokio::time::timeout(timeout, get)
                                .await
                                .map_err(|_| timed_out(timeout))
                                .and_then(|got| got.map_err(Failure::of))
                                .map_err(|failure| failure.into_error(object.as_ref()))?;
                            bytes
                                .map(|bytes| {
                                    serde_json::from_slice(&bytes).map_err(|error| {
                                        Error::Corrupted {
                                            message: format!(
                                                "archived summary of segment {base}: {error}"
                                            ),
                                        }
                                    })
                                })
                                .transpose()
                        }
                    })
                    .buffered(DOWNLOADS_IN_FLIGHT)
                    .collect()
                    .await;
            results.into_iter().collect()
        })
    }

    /// Downloads archived segment `summary`'s body, checked against its
    /// blake3, to `local`: written beside it as it arrives, and renamed into
    /// place once on disk and checked. It is never whole in memory.
    pub fn restore_body(
        &self,
        summary: &crate::tier::SegmentSummary,
        local: &Path,
    ) -> Result<(), Error> {
        let tmp = download_path(local);
        let sink = Sink::File(tokio::fs::File::from_std(std::fs::File::create(&tmp)?));
        if let Err(error) = self.download_segment(summary, sink) {
            let _ = std::fs::remove_file(&tmp);
            return Err(error);
        }
        std::fs::rename(&tmp, local)?;
        if let Some(parent) = local.parent() {
            crate::segment::writer::sync_dir(parent)?;
        }
        Ok(())
    }

    /// The whole archived segment `summary` describes, checked against its
    /// blake3: what a node catching up from the archive applies. Fetched
    /// past the block cache, as [`download_whole`] does.
    pub fn fetch_segment(&self, summary: &crate::tier::SegmentSummary) -> Result<Vec<u8>, Error> {
        let sink = Sink::Memory(Vec::with_capacity(summary.data_len as usize));
        match self.download_segment(summary, sink)? {
            Sink::Memory(bytes) => Ok(bytes),
            Sink::File(_) => unreachable!("downloaded to memory"),
        }
    }

    /// Downloads archived segment `summary` into `sink`, checked against
    /// the summary's length and blake3.
    fn download_segment(
        &self,
        summary: &crate::tier::SegmentSummary,
        mut sink: Sink,
    ) -> Result<Sink, Error> {
        let archive = Arc::clone(&self.archive);
        let stats = Arc::clone(&self.stats);
        let object = self.segment_object(summary.base);
        let summary = summary.clone();
        self.archive
            .block_on(async move {
                let what = format!("segment {}", summary.base);
                let hash = download_whole(
                    &archive,
                    Some(&stats),
                    &object,
                    summary.data_len,
                    "its summary says",
                    &mut sink,
                )
                .await
                .map_err(|failure| failure.into_error(&what))?;
                if hash.to_hex().as_str() != summary.blake3 {
                    return Err(Error::Corrupted {
                        message: format!(
                            "archived {what} does not hash to its summary's blake3 {}",
                            summary.blake3
                        ),
                    });
                }
                Ok(sink)
            })
            .inspect_err(|_| {
                self.stats.failures.fetch_add(1, Ordering::Relaxed);
            })
    }

    /// Downloads the archived `<kind>/<name>` of this context to `local`,
    /// through a temporary file renamed into place. `false` when the
    /// archive does not hold it.
    pub fn download(&self, kind: &str, name: &str, local: &Path) -> Result<bool, Error> {
        Ok(self.download_all(&[(kind, name.to_string(), local.to_path_buf())])?[0])
    }
}

/// Bytes of one request of a whole-object download.
pub const CHUNK: u64 = 8 * 1024 * 1024;

/// Requests in flight for one whole-object download. The client gives a
/// request 30 seconds; four requests of [`CHUNK`] bytes sharing a link
/// finish in that time on one faster than about 1.1 MB/s, and the node's
/// in-flight budget bounds what all downloads share.
const CHUNKS_IN_FLIGHT: usize = 4;

/// What cold reads of one context have asked of the archive, kept where
/// the download tasks can count too.
#[derive(Debug, Default)]
struct ReadStats {
    requests: AtomicU64,
    fetched_bytes: AtomicU64,
    failures: AtomicU64,
}

/// Where a whole-object download is written, in order.
enum Sink {
    Memory(Vec<u8>),
    File(tokio::fs::File),
}

impl Sink {
    async fn write(&mut self, bytes: &[u8]) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        match self {
            Self::Memory(out) => {
                out.extend_from_slice(bytes);
                Ok(())
            }
            Self::File(file) => file.write_all(bytes).await,
        }
    }

    /// Puts what was written on disk.
    async fn finish(&mut self) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        match self {
            Self::Memory(_) => Ok(()),
            Self::File(file) => {
                file.flush().await?;
                file.sync_all().await
            }
        }
    }
}

/// Downloads all `len` bytes of `object`, which `by` says is that long,
/// into `sink` in order, and returns their blake3.
///
/// The object goes in ranged requests of [`CHUNK`] bytes,
/// [`CHUNKS_IN_FLIGHT`] at a time, each bounded by the fetch timeout and
/// holding its share of the node's in-flight budget until its bytes are
/// written. One request for a whole segment (up to 256 MiB) would outlast
/// the client's request timeout on a slow link, and would be retried from
/// the start each time; and its bytes would all be in memory at once.
async fn download_whole(
    archive: &ArchiveStore,
    stats: Option<&ReadStats>,
    object: &ObjectPath,
    len: u64,
    by: &'static str,
    sink: &mut Sink,
) -> Result<blake3::Hash, Failure> {
    use futures_util::StreamExt;
    let timeout = archive.limits.fetch_timeout;
    let mut chunks = futures_util::stream::iter((0..len).step_by(CHUNK as usize))
        .map(|start| {
            let range = start..(start + CHUNK).min(len);
            async move {
                tokio::time::timeout(timeout, async {
                    let permit = Arc::clone(&archive.in_flight)
                        .acquire_many_owned(archive.permits_for(range.end - range.start))
                        .await
                        .expect("the in-flight budget is never closed");
                    if let Some(stats) = stats {
                        stats.requests.fetch_add(1, Ordering::Relaxed);
                    }
                    let bytes = get_range(archive.store.as_ref(), object, range, len, by).await?;
                    if let Some(stats) = stats {
                        stats
                            .fetched_bytes
                            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                    }
                    Ok((permit, bytes))
                })
                .await
                .unwrap_or_else(|_| Err(timed_out(timeout)))
            }
        })
        .buffered(CHUNKS_IN_FLIGHT);
    let mut hasher = blake3::Hasher::new();
    while let Some(chunk) = chunks.next().await {
        // The permit goes once the bytes are written.
        let (_permit, bytes) = chunk?;
        hasher.update(&bytes);
        sink.write(&bytes).await.map_err(Failure::local)?;
    }
    sink.finish().await.map_err(Failure::local)?;
    Ok(hasher.finalize())
}

/// Downloads `object` to `local`, through a temporary file beside it renamed
/// into place once on disk. `false` when the archive does not hold it.
async fn download_file(
    archive: &ArchiveStore,
    object: &ObjectPath,
    local: &Path,
) -> Result<bool, Error> {
    let what = format!("{object}");
    let timeout = archive.limits.fetch_timeout;
    let size = match tokio::time::timeout(timeout, archive.store.head(object)).await {
        Ok(Ok(meta)) => meta.size,
        Ok(Err(object_store::Error::NotFound { .. })) => return Ok(false),
        Ok(Err(error)) => return Err(Failure::of(error).into_error(&what)),
        Err(_) => return Err(timed_out(timeout).into_error(&what)),
    };
    if let Some(parent) = local.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let tmp = download_path(local);
    let mut sink = Sink::File(tokio::fs::File::create(&tmp).await?);
    let downloaded = download_whole(
        archive,
        None,
        object,
        size,
        "it was when the download began",
        &mut sink,
    )
    .await;
    drop(sink);
    match downloaded {
        Ok(_) => {
            tokio::fs::rename(&tmp, local).await?;
            Ok(true)
        }
        Err(failure) => {
            let _ = tokio::fs::remove_file(&tmp).await;
            match failure {
                // Deleted since it was listed: a superseded key table.
                Failure::Missing => Ok(false),
                failure => Err(failure.into_error(&what)),
            }
        }
    }
}

/// The object store at `raw_url` and the prefix inside it. Cloud settings
/// and credentials come from the environment, the standard variables of
/// each provider (`AWS_REGION`, `AWS_ACCESS_KEY_ID`, ...); without
/// credentials there, S3 uses the instance's role.
pub(crate) fn open_store(raw_url: &str) -> Result<(Box<dyn ObjectStore>, ObjectPath), Error> {
    open_store_with(raw_url, std::iter::empty::<(&str, &str)>())
}

/// [`open_store`] with `options` (keys as the provider names them, such as
/// `aws_endpoint`) over what the environment says.
pub(crate) fn open_store_with<'a>(
    raw_url: &str,
    options: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> Result<(Box<dyn ObjectStore>, ObjectPath), Error> {
    use object_store::ObjectStoreScheme;
    let options: Vec<(&str, &str)> = options.into_iter().collect();
    let url = url::Url::parse(raw_url).map_err(|error| Error::Rejected {
        message: format!("invalid archive url {raw_url}: {error}"),
    })?;
    let unsupported = |error: object_store::Error| Error::Rejected {
        message: format!("unsupported archive url {raw_url}: {error}"),
    };
    let (scheme, path) = ObjectStoreScheme::parse(&url).map_err(|error| Error::Rejected {
        message: format!("unsupported archive url {raw_url}: {error}"),
    })?;
    let path = ObjectPath::parse(path).map_err(|error| Error::Rejected {
        message: format!("invalid archive url {raw_url}: {error}"),
    })?;
    let url = url.to_string();
    let store: Box<dyn ObjectStore> = match scheme {
        ObjectStoreScheme::Local => Box::new(object_store::local::LocalFileSystem::new()),
        ObjectStoreScheme::Memory => Box::new(object_store::memory::InMemory::new()),
        ObjectStoreScheme::AmazonS3 => Box::new(
            options
                .iter()
                .try_fold(s3_builder().with_url(url), |builder, &(key, value)| {
                    key.parse::<object_store::aws::AmazonS3ConfigKey>()
                        .map(|key| builder.with_config(key, value))
                })
                .map_err(unsupported)?
                .with_retry(retry())
                .build()
                .map_err(unsupported)?,
        ),
        ObjectStoreScheme::GoogleCloudStorage => Box::new(
            options
                .iter()
                .try_fold(
                    object_store::gcp::GoogleCloudStorageBuilder::from_env().with_url(url),
                    |builder, &(key, value)| {
                        key.parse::<object_store::gcp::GoogleConfigKey>()
                            .map(|key| builder.with_config(key, value))
                    },
                )
                .map_err(unsupported)?
                .with_retry(retry())
                .build()
                .map_err(unsupported)?,
        ),
        ObjectStoreScheme::MicrosoftAzure => Box::new(
            options
                .iter()
                .try_fold(
                    object_store::azure::MicrosoftAzureBuilder::from_env().with_url(url),
                    |builder, &(key, value)| {
                        key.parse::<object_store::azure::AzureConfigKey>()
                            .map(|key| builder.with_config(key, value))
                    },
                )
                .map_err(unsupported)?
                .with_retry(retry())
                .build()
                .map_err(unsupported)?,
        ),
        // Plain HTTP (WebDAV) has no create-if-absent, which committing a
        // segment by its summary needs.
        other => {
            return Err(Error::Rejected {
                message: format!(
                    "unsupported archive url {raw_url}: {other:?} cannot create an object only \
                     if it is absent; use s3:// (or an S3-compatible store), gs://, az:// or \
                     file://"
                ),
            });
        }
    };
    Ok((store, path))
}

/// How long a request to the archive is retried before it fails. A read
/// waiting on an archive that is down fails after about this long, as
/// [`Error::Unavailable`], instead of the client's default three minutes.
pub(crate) const RETRY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// The longest one download of segment bytes may take, its wait for room
/// in the in-flight budget included, before it fails as
/// [`Error::Unavailable`]. A request is retried for [`RETRY_TIMEOUT`], and
/// its last attempt may take as long again; this bounds it whatever the
/// client does, so a reader never waits on a download for ever.
pub(crate) const FETCH_TIMEOUT: std::time::Duration =
    std::time::Duration::from_secs(3 * RETRY_TIMEOUT.as_secs());

/// S3 from the environment, uploading with a SHA-256 checksum unless the
/// environment names another: a bucket under object lock (WORM) refuses
/// uploads without one, and every upload is checked on arrival.
fn s3_builder() -> object_store::aws::AmazonS3Builder {
    let builder = object_store::aws::AmazonS3Builder::from_env();
    if std::env::var_os("AWS_CHECKSUM_ALGORITHM").is_some() {
        builder
    } else {
        builder.with_checksum_algorithm(object_store::aws::Checksum::SHA256)
    }
}

fn retry() -> object_store::RetryConfig {
    object_store::RetryConfig {
        retry_timeout: RETRY_TIMEOUT,
        ..object_store::RetryConfig::default()
    }
}

/// Where a download is written before it is renamed to `local`: next to
/// it, under its full name, so two downloads never share one.
fn download_path(local: &Path) -> PathBuf {
    let mut name = local.file_name().unwrap_or_default().to_os_string();
    name.push(".download");
    local.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::error::Error;

    /// S3 settings come from the environment: a region set there reaches
    /// the store instead of the builder's default.
    #[test]
    fn s3_urls_take_their_settings_from_the_environment() {
        // SAFETY: no other test in this crate reads or writes these.
        unsafe { std::env::set_var("AWS_REGION", "eu-north-1") };
        let (store, _) = super::open_store("s3://some-bucket/prefix").unwrap();
        let shown = format!("{store:?}");
        assert!(shown.contains("eu-north-1"), "{shown}");
    }

    /// Each provider's store is built from its own url form, and the
    /// archive keeps only the path inside the bucket or container.
    #[test]
    fn gcs_and_azure_urls_open_their_stores() {
        for (url, store, prefix) in [
            (
                "gs://some-bucket/kb/archive",
                "GoogleCloudStorage",
                "kb/archive",
            ),
            (
                "https://account.blob.core.windows.net/container/kb",
                "MicrosoftAzure",
                "kb",
            ),
        ] {
            let (opened, path) = super::open_store(url).unwrap();
            assert!(format!("{opened:?}").contains(store), "{url}: {opened:?}");
            assert_eq!(path.as_ref(), prefix, "{url}");
        }
    }

    /// A store that cannot create an object only if it is absent cannot
    /// commit segments by their summaries.
    #[test]
    fn plain_http_is_refused() {
        let error = super::open_store("http://example.com/archive")
            .err()
            .unwrap();
        assert!(
            error.to_string().contains("only if it is absent"),
            "{error}"
        );
    }

    /// An archive that takes connections and never answers fails a
    /// request after about `RETRY_TIMEOUT`, as unavailable, not after the
    /// client's default of three minutes of retries.
    #[test]
    fn a_request_to_an_archive_that_never_answers_fails_in_bounded_time() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        std::thread::spawn(move || {
            let mut held = Vec::new();
            for stream in listener.incoming() {
                held.push(stream);
            }
        });
        let (store, _) = super::open_store_with(
            "s3://bucket/kb",
            [
                ("aws_endpoint", endpoint.as_str()),
                ("aws_allow_http", "true"),
                ("aws_access_key_id", "test"),
                ("aws_secret_access_key", "test"),
                ("aws_region", "eu-north-1"),
            ],
        )
        .unwrap();
        let archive = super::ArchiveStore::over_store(Arc::from(store), "kb".into(), None).unwrap();
        let segments = archive
            .segments(std::path::Path::new("/nowhere/default"))
            .unwrap();
        let began = std::time::Instant::now();
        let error = segments.summary(0).unwrap_err();
        let took = began.elapsed();
        assert!(matches!(error, Error::Unavailable { .. }), "{error}");
        assert!(
            took < super::RETRY_TIMEOUT * 2,
            "failed after {took:?}, retry timeout {:?}",
            super::RETRY_TIMEOUT
        );
    }

    /// `len` bytes that differ from one offset to the next.
    fn data(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 31 % 251) as u8).collect()
    }

    /// An archive in memory, its objects under `kb`.
    fn memory_archive() -> (
        Arc<object_store::memory::InMemory>,
        Arc<super::ArchiveStore>,
    ) {
        let memory = Arc::new(object_store::memory::InMemory::new());
        let archive = super::ArchiveStore::over_store(memory.clone(), "kb".into(), None).unwrap();
        (memory, archive)
    }

    /// Puts `bytes` as archived segment `base` of `context`.
    fn put_segment(
        store: &dyn object_store::ObjectStore,
        context: &str,
        base: u64,
        bytes: Vec<u8>,
    ) {
        use object_store::ObjectStoreExt;
        let object =
            object_store::path::Path::from(format!("kb/{context}/segments/{base:020}.seg"));
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(store.put(&object, bytes.into()))
            .unwrap();
    }

    /// The cold reads of `context`, which hold segment `base` of `data_len`
    /// bytes: what its locator would say.
    fn remote(
        archive: &Arc<super::ArchiveStore>,
        context: &str,
        segments: &[(u64, u64)],
    ) -> Arc<super::RemoteSegments> {
        let remote = archive
            .segments(&std::path::Path::new("/nowhere").join(context))
            .unwrap();
        remote
            .data_lens
            .lock()
            .unwrap()
            .extend(segments.iter().copied());
        Arc::new(remote)
    }

    /// Bytes `range` of segment `base`.
    fn read_one(
        remote: &Arc<super::RemoteSegments>,
        base: u64,
        range: std::ops::Range<u64>,
    ) -> Result<Vec<u8>, Error> {
        Ok(remote.read(base, std::slice::from_ref(&range))?.remove(0))
    }

    /// Every block a request brings is cached in a buffer of its own: a
    /// slice of the request's buffer would keep all of it alive for as long
    /// as any one block stays cached, and the cache would hold many times
    /// its budget.
    #[test]
    fn cached_blocks_do_not_keep_their_request_alive() {
        let (memory, archive) = memory_archive();
        let len = 3 * super::MAX_GET as usize / 2;
        let bytes = data(len);
        put_segment(memory.as_ref(), "default", 0, bytes.clone());
        let remote = remote(&archive, "default", &[(0, len as u64)]);

        let read = read_one(&remote, 0, 0..len as u64).unwrap();
        assert_eq!(read, bytes);
        drop(read);

        // The download task lets go of its blocks once it has answered.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let shared = {
                let cache = archive.blocks.lock().unwrap();
                assert_eq!(cache.len(), len.div_ceil(super::BLOCK as usize));
                cache.iter().filter(|(_, bytes)| !bytes.is_unique()).count()
            };
            if shared == 0 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "{shared} cached blocks share their buffer"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// The summary of `bytes` as archived segment `base`.
    fn summary_of(base: u64, bytes: &[u8]) -> crate::tier::SegmentSummary {
        crate::tier::SegmentSummary {
            base,
            end: base + 1,
            data_len: bytes.len() as u64,
            blake3: blake3::hash(bytes).to_hex().to_string(),
        }
    }

    /// A segment the locator or the summary says is archived but the
    /// archive does not hold is lost for good, on both read paths: asking
    /// again will not bring it back.
    #[test]
    fn a_segment_missing_from_the_archive_is_an_internal_error_on_both_paths() {
        let (_, archive) = memory_archive();
        let remote = remote(&archive, "default", &[(0, 1000)]);
        for error in [
            read_one(&remote, 0, 0..100).unwrap_err(),
            remote
                .fetch_segment(&summary_of(0, &data(1000)))
                .unwrap_err(),
        ] {
            assert!(matches!(error, Error::Internal { .. }), "{error}");
            assert!(
                error.to_string().contains("missing from the archive"),
                "{error}"
            );
        }
    }

    /// An archived segment of another length than its locator or summary
    /// says failed an integrity check, wherever the read starts in it.
    #[test]
    fn an_archived_segment_of_the_wrong_length_is_corrupted() {
        let (memory, archive) = memory_archive();
        let bytes = data(2 * super::BLOCK as usize);
        put_segment(memory.as_ref(), "default", 0, bytes.clone());

        // Shorter than its locator says: a range in what it holds, and one
        // past its end.
        let len = 3 * super::BLOCK;
        let shorter = remote(&archive, "default", &[(0, len)]);
        for range in [0..100, len - 100..len] {
            let error = read_one(&shorter, 0, range.clone()).unwrap_err();
            assert!(
                matches!(error, Error::Corrupted { .. }),
                "{range:?}: {error}"
            );
        }

        // Shorter and longer than its summary says, the summary's hash
        // taken over what it says.
        let mut longer = bytes.clone();
        longer.extend_from_slice(&data(10 * super::MAX_GET as usize));
        let whole = remote(&archive, "default", &[]);
        for summary in [summary_of(0, &longer), summary_of(0, &bytes[..1000])] {
            let error = whole.fetch_segment(&summary).unwrap_err();
            assert!(
                matches!(error, Error::Corrupted { .. }),
                "{} bytes: {error}",
                summary.data_len
            );
        }
    }

    /// A reader waiting on a download gives up at its deadline, whatever
    /// the download does.
    #[test]
    fn a_waiter_gives_up_at_its_deadline() {
        let flight = super::Flight::default();
        let began = std::time::Instant::now();
        let waited = flight.wait(began + std::time::Duration::from_millis(50));
        assert!(matches!(waited, Err(super::Failure::Unavailable(_))));
        assert!(began.elapsed() >= std::time::Duration::from_millis(50));
    }

    /// A download the archive never answers fails its readers by its
    /// deadline, as unavailable, and a later read starts a download of its
    /// own instead of joining the dead one.
    #[test]
    fn a_download_that_never_answers_fails_its_readers_in_bounded_time() {
        use object_store::throttle::{ThrottleConfig, ThrottledStore};
        let memory = Arc::new(object_store::memory::InMemory::new());
        put_segment(memory.as_ref(), "default", 0, data(1000));
        let hanging = Arc::new(ThrottledStore::new(
            memory,
            ThrottleConfig {
                wait_get_per_call: std::time::Duration::from_secs(3600),
                ..ThrottleConfig::default()
            },
        ));
        let archive = super::ArchiveStore::with_limits(
            hanging,
            "kb".into(),
            None,
            super::ArchiveLimits {
                fetch_timeout: std::time::Duration::from_millis(200),
                ..Default::default()
            },
        )
        .unwrap();
        let remote = remote(&archive, "default", &[(0, 1000)]);

        for _ in 0..2 {
            let reader = Arc::clone(&remote);
            let (done, result) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                let _ = done.send(read_one(&reader, 0, 0..100));
            });
            let error = result
                .recv_timeout(std::time::Duration::from_secs(10))
                .expect("the read waits on the download for ever")
                .unwrap_err();
            assert!(matches!(error, Error::Unavailable { .. }), "{error}");
            assert!(archive.flights.lock().unwrap().is_empty());
        }
        assert_eq!(remote.requests(), 2);
        assert_eq!(remote.failures(), 2);
    }

    /// Every context of one archive shares its block cache, so the cache's
    /// budget holds for the node, not for each context.
    #[test]
    fn contexts_share_one_block_cache() {
        let memory = Arc::new(object_store::memory::InMemory::new());
        let len = 4 * super::BLOCK;
        for context in ["a", "b"] {
            put_segment(memory.as_ref(), context, 0, data(len as usize));
        }
        let archive = super::ArchiveStore::with_limits(
            memory,
            "kb".into(),
            None,
            super::ArchiveLimits {
                cache_bytes: len as usize,
                ..Default::default()
            },
        )
        .unwrap();
        let a = remote(&archive, "a", &[(0, len)]);
        let b = remote(&archive, "b", &[(0, len)]);

        read_one(&a, 0, 0..len).unwrap();
        read_one(&b, 0, 0..len).unwrap();
        assert_eq!(archive.blocks.lock().unwrap().len(), 4);
        // `b`'s blocks took the room `a`'s had.
        let requests = a.requests();
        read_one(&a, 0, 0..len).unwrap();
        assert!(a.requests() > requests);
        let requests = b.requests();
        read_one(&b, 0, 0..super::BLOCK).unwrap();
        assert!(b.requests() > requests);
    }

    /// A prefetch starts downloads only as far as the node's in-flight
    /// budget has room, nearest first, and never waits for it; a second
    /// prefetch of the same blocks shares the downloads under way. A read
    /// waits its turn for the budget instead.
    #[test]
    fn a_prefetch_takes_only_what_the_in_flight_budget_has_room_for() {
        use object_store::throttle::{ThrottleConfig, ThrottledStore};
        let memory = Arc::new(object_store::memory::InMemory::new());
        let len = 8 * super::BLOCK;
        for base in [0, 100] {
            put_segment(memory.as_ref(), "default", base, data(len as usize));
        }
        let hanging = Arc::new(ThrottledStore::new(
            memory,
            ThrottleConfig {
                wait_get_per_call: std::time::Duration::from_secs(3600),
                ..ThrottleConfig::default()
            },
        ));
        let timeout = std::time::Duration::from_millis(300);
        let archive = super::ArchiveStore::with_limits(
            hanging,
            "kb".into(),
            None,
            super::ArchiveLimits {
                in_flight_bytes: 4 * super::BLOCK,
                fetch_timeout: timeout,
                ..Default::default()
            },
        )
        .unwrap();
        let remote = remote(&archive, "default", &[(0, len), (100, len)]);
        let flights = || -> Vec<(u64, u64)> {
            let mut keys: Vec<(u64, u64)> = archive
                .flights
                .lock()
                .unwrap()
                .keys()
                .map(|&(_, base, block)| (base, block))
                .collect();
            keys.sort_unstable();
            keys
        };

        let wanted: Vec<(u64, Vec<std::ops::Range<u64>>)> = [0, 100]
            .into_iter()
            .map(|base| (base, std::iter::once(0..len).collect()))
            .collect();
        let began = std::time::Instant::now();
        remote.prefetch(&wanted).unwrap();
        remote.prefetch(&wanted).unwrap();
        assert!(began.elapsed() < timeout, "{:?}", began.elapsed());
        assert_eq!(flights(), [(0, 0), (0, 1), (0, 2), (0, 3)]);
        assert_eq!(archive.in_flight.available_permits(), 0);

        // The read's download waits for the prefetches to give the budget
        // back (they fail by their deadline), then fails by its own.
        let error = read_one(&remote, 100, 0..100).unwrap_err();
        assert!(matches!(error, Error::Unavailable { .. }), "{error}");
        // A download gives its share back as it fails, then answers its
        // readers.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while archive.in_flight.available_permits() < 4 || !flights().is_empty() {
            assert!(
                std::time::Instant::now() < deadline,
                "the budget never came back"
            );
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// A store that answers every GET after `delay` and records how much
    /// was asked of it at once.
    #[derive(Debug, Default)]
    struct Recording {
        inner: object_store::memory::InMemory,
        delay: std::time::Duration,
        gets: std::sync::atomic::AtomicU64,
        unranged: std::sync::atomic::AtomicU64,
        largest: std::sync::atomic::AtomicU64,
        at_once: Mutex<(u64, u64)>,
        most_at_once: Mutex<(u64, u64)>,
    }

    impl std::fmt::Display for Recording {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Recording({})", self.inner)
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for Recording {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            use std::sync::atomic::Ordering::Relaxed;
            if options.head {
                return self.inner.get_opts(location, options).await;
            }
            let asked = match &options.range {
                Some(object_store::GetRange::Bounded(range)) => range.end - range.start,
                _ => {
                    self.unranged.fetch_add(1, Relaxed);
                    u64::MAX
                }
            };
            self.gets.fetch_add(1, Relaxed);
            self.largest.fetch_max(asked, Relaxed);
            {
                let mut now = self.at_once.lock().unwrap();
                *now = (now.0 + 1, now.1.saturating_add(asked));
                let mut most = self.most_at_once.lock().unwrap();
                *most = (most.0.max(now.0), most.1.max(now.1));
            }
            tokio::time::sleep(self.delay).await;
            let result = self.inner.get_opts(location, options).await;
            let mut now = self.at_once.lock().unwrap();
            *now = (now.0 - 1, now.1.saturating_sub(asked));
            result
        }

        fn delete_stream(
            &self,
            locations: futures_util::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures_util::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// An archive over a store recording what is asked of it, each GET
    /// answered after 20 ms so requests overlap, within `limits`.
    fn recorded_archive(
        limits: super::ArchiveLimits,
    ) -> (Arc<Recording>, Arc<super::ArchiveStore>) {
        let store = Arc::new(Recording {
            delay: std::time::Duration::from_millis(20),
            ..Recording::default()
        });
        let archive =
            super::ArchiveStore::with_limits(store.clone(), "kb".into(), None, limits).unwrap();
        (store, archive)
    }

    /// A whole segment is downloaded in ranged requests of at most `CHUNK`
    /// bytes, a few at a time: one request for all of it would outlast the
    /// client's request timeout on a slow link.
    #[test]
    fn a_whole_segment_is_downloaded_in_bounded_requests() {
        let (store, archive) = recorded_archive(super::ArchiveLimits::default());
        let bytes = data(5 * super::CHUNK as usize / 2 + 123);
        put_segment(&store.inner, "default", 0, bytes.clone());
        let remote = remote(&archive, "default", &[]);

        assert_eq!(remote.fetch_segment(&summary_of(0, &bytes)).unwrap(), bytes);
        use std::sync::atomic::Ordering::Relaxed;
        assert_eq!(store.unranged.load(Relaxed), 0);
        assert_eq!(store.gets.load(Relaxed), 3);
        assert_eq!(store.largest.load(Relaxed), super::CHUNK);
        assert_eq!(remote.requests(), 3);
        assert_eq!(remote.fetched_bytes(), bytes.len() as u64);
        assert!(store.most_at_once.lock().unwrap().0 <= super::CHUNKS_IN_FLIGHT as u64);
    }

    /// A body restored from the archive is written as it arrives and kept
    /// only once its hash matches the summary's.
    #[test]
    fn a_restored_body_is_kept_only_when_it_matches_its_summary() {
        let (store, archive) = recorded_archive(super::ArchiveLimits::default());
        let bytes = data(3 * super::CHUNK as usize / 2);
        put_segment(&store.inner, "default", 0, bytes.clone());
        let remote = remote(&archive, "default", &[]);
        let dir = tempfile::tempdir().unwrap();
        let local = dir.path().join("0.seg");

        let mut wrong = summary_of(0, &bytes);
        wrong.blake3 = "0".repeat(64);
        let error = remote.restore_body(&wrong, &local).unwrap_err();
        assert!(matches!(error, Error::Corrupted { .. }), "{error}");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);

        remote.restore_body(&summary_of(0, &bytes), &local).unwrap();
        assert_eq!(std::fs::read(&local).unwrap(), bytes);
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }

    /// Many files downloaded at once go in bounded requests straight to
    /// their files, and all of them together keep within the node's
    /// in-flight budget. One the archive does not hold is reported.
    #[test]
    fn downloads_of_many_files_keep_within_the_in_flight_budget() {
        let budget = 2 * super::CHUNK;
        let (store, archive) = recorded_archive(super::ArchiveLimits {
            in_flight_bytes: budget,
            ..Default::default()
        });
        let remote = remote(&archive, "default", &[]);
        let dir = tempfile::tempdir().unwrap();
        let mut files = Vec::new();
        let mut expected = Vec::new();
        for n in 0..6u64 {
            let bytes = data(5 * super::CHUNK as usize / 2 + n as usize);
            let object = object_store::path::Path::from(format!("kb/default/index/{n}.keys"));
            tokio::runtime::Builder::new_current_thread()
                .build()
                .unwrap()
                .block_on(object_store::ObjectStoreExt::put(
                    &store.inner,
                    &object,
                    bytes.clone().into(),
                ))
                .unwrap();
            files.push((
                "index",
                format!("{n}.keys"),
                dir.path().join(format!("{n}.keys")),
            ));
            expected.push(bytes);
        }
        files.push(("index", "lost.keys".into(), dir.path().join("lost.keys")));

        let held = remote.download_all(&files).unwrap();
        assert_eq!(held, [true, true, true, true, true, true, false]);
        for ((_, _, local), bytes) in files.iter().zip(&expected) {
            assert_eq!(&std::fs::read(local).unwrap(), bytes);
        }
        assert!(!dir.path().join("lost.keys").exists());
        use std::sync::atomic::Ordering::Relaxed;
        assert_eq!(store.unranged.load(Relaxed), 0);
        assert_eq!(store.largest.load(Relaxed), super::CHUNK);
        let (_, most_bytes) = *store.most_at_once.lock().unwrap();
        assert!(most_bytes <= budget, "{most_bytes} bytes asked for at once");
    }

    /// A download that ends after its readers gave up on it (and a later
    /// read started another for the same block) leaves the later one in
    /// place, so readers still share it rather than start a third.
    #[test]
    fn a_download_given_up_on_leaves_its_successor_in_place() {
        use object_store::throttle::{ThrottleConfig, ThrottledStore};
        let memory = Arc::new(object_store::memory::InMemory::new());
        put_segment(memory.as_ref(), "default", 0, data(1000));
        let hanging = Arc::new(ThrottledStore::new(
            memory,
            ThrottleConfig {
                wait_get_per_call: std::time::Duration::from_secs(3600),
                ..ThrottleConfig::default()
            },
        ));
        let archive = super::ArchiveStore::over_store(hanging, "kb".into(), None).unwrap();
        let remote = remote(&archive, "default", &[(0, 1000)]);
        let key = (remote.id, 0, 0);

        // The stale download, whose readers took it out of the map.
        let stale = Arc::new(super::Flight::default());
        let pending = super::Pending {
            archive: Arc::clone(&archive),
            segment: (remote.id, 0),
            blocks: Some(vec![(0, Arc::clone(&stale))]),
        };
        let wanted: Vec<(u64, Vec<std::ops::Range<u64>>)> =
            vec![(0, std::iter::once(0..100).collect())];
        remote.prefetch(&wanted).unwrap();
        let successor = Arc::clone(&archive.flights.lock().unwrap()[&key]);

        drop(pending);
        assert!(stale.wait(std::time::Instant::now()).is_err());
        let current = archive.flights.lock().unwrap().get(&key).cloned();
        assert!(current.is_some_and(|current| Arc::ptr_eq(&current, &successor)));
        remote.prefetch(&wanted).unwrap();
        let current = archive.flights.lock().unwrap().get(&key).cloned();
        assert!(current.is_some_and(|current| Arc::ptr_eq(&current, &successor)));
    }
}
