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
//! up to [`MAX_GET`] bytes, and all requests run concurrently.
//!
//! Reads from the archive are rare (it holds what a node's disk cannot), so
//! what matters is that a read does not pay a round trip per segment, and a
//! replay runs at the network's pace rather than one round trip at a time.
//! Reads plan ahead from the index alone and [`RemoteSegments::prefetch`]
//! the next [`READAHEAD`] bytes of blocks in one go, across every segment
//! they span.
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

/// The most one request fetches.
pub const MAX_GET: u64 = 4 * 1024 * 1024;

/// Bytes of blocks a read plans and prefetches ahead of itself in evicted
/// segments; it plans the next stretch halfway through this one.
pub const READAHEAD: u64 = 32 * 1024 * 1024;

/// Files downloaded at once when a node installs archived segments.
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

/// Default memory budget of the cache of fetched blocks, per context.
pub const DEFAULT_CACHE_BYTES: usize = 128 << 20;

/// An archive as cold reads see it: its object store, and the runtime its
/// requests run on. One per process, shared by every context.
pub struct ArchiveStore {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    io: IoRuntime,
    cache_bytes: usize,
    /// How long one download of segment bytes may take: [`FETCH_TIMEOUT`].
    fetch_timeout: std::time::Duration,
    /// The cap on sealed bytes this node keeps on disk, if any: a node
    /// catching up from the archive skips the bodies it would evict anyway.
    max_local_bytes: Option<u64>,
}

impl std::fmt::Debug for ArchiveStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArchiveStore")
            .field("prefix", &self.prefix)
            .field("cache_bytes", &self.cache_bytes)
            .field("fetch_timeout", &self.fetch_timeout)
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
        Ok(Arc::new(Self {
            store: Arc::from(store),
            prefix,
            io: IoRuntime::new()?,
            cache_bytes: DEFAULT_CACHE_BYTES,
            fetch_timeout: FETCH_TIMEOUT,
            max_local_bytes,
        }))
    }

    /// An archive over `store` itself, its objects under `prefix`: for
    /// tests that wrap a store (to delay or break it).
    pub fn over_store(
        store: Arc<dyn ObjectStore>,
        prefix: ObjectPath,
        max_local_bytes: Option<u64>,
    ) -> Result<Arc<Self>, Error> {
        Ok(Arc::new(Self {
            store,
            prefix,
            io: IoRuntime::new()?,
            cache_bytes: DEFAULT_CACHE_BYTES,
            fetch_timeout: FETCH_TIMEOUT,
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
        let blocks = (self.cache_bytes as u64 / BLOCK).max(1) as usize;
        Ok(RemoteSegments {
            archive: Arc::clone(self),
            context: self.prefix.clone().join(context),
            dir: dir.to_path_buf(),
            data_lens: Mutex::new(HashMap::new()),
            blocks: Mutex::new(LruCache::new(NonZeroUsize::new(blocks).unwrap())),
            flights: Mutex::new(HashMap::new()),
            requests: AtomicU64::new(0),
            fetched_bytes: AtomicU64::new(0),
            failures: AtomicU64::new(0),
        })
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
            message: "the archive's runtime has shut down".into(),
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
    segments: Arc<RemoteSegments>,
    base: u64,
    blocks: Option<Waiters>,
}

impl Drop for Pending {
    fn drop(&mut self) {
        let Some(blocks) = self.blocks.take() else {
            return;
        };
        let mut flights = lock(&self.segments.flights);
        for (block, flight) in blocks {
            flights.remove(&(self.base, block));
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
}

impl Failure {
    fn of(error: object_store::Error) -> Self {
        match error {
            object_store::Error::NotFound { .. } => Self::Missing,
            error => Self::Unavailable(error.to_string()),
        }
    }

    /// What a read of segment `base` reports.
    fn into_error(self, base: u64) -> Error {
        match self {
            Self::Missing => Error::Internal {
                message: format!("segment {base} is missing from the archive"),
            },
            Self::WrongSize { size, expected, by } => Error::Corrupted {
                message: format!(
                    "archived segment {base} is {size} bytes, where {by} says {expected}"
                ),
            },
            Self::Unavailable(message) => Error::Unavailable {
                message: format!("reading segment {base} from the archive: {message}"),
            },
        }
    }
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

/// One block being fetched: the reader that fetches it fills it in, the
/// others wait on it.
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

/// Locks `mutex` whether or not a thread panicked holding it: what it
/// guards (cached blocks, downloads in flight) stays consistent at every
/// point a panic could leave it, and one panicking download must not fail
/// every later read.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

type BlockKey = (u64, u64);

/// Blocks of one segment, each with the download that brings it.
type Waiters = Vec<(u64, Arc<Flight>)>;

/// The archived segments of one context, read by range.
pub struct RemoteSegments {
    archive: Arc<ArchiveStore>,
    /// `<prefix>/<context>`.
    context: ObjectPath,
    /// The context's local data directory, where the locators are.
    dir: PathBuf,
    /// Data bytes of each archived segment, from its locator.
    data_lens: Mutex<HashMap<u64, u64>>,
    blocks: Mutex<LruCache<BlockKey, Bytes>>,
    flights: Mutex<HashMap<BlockKey, Arc<Flight>>>,
    requests: AtomicU64,
    fetched_bytes: AtomicU64,
    failures: AtomicU64,
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
    /// does, many at once. Returns which the archive held.
    pub fn download_all(&self, files: &[(&str, String, PathBuf)]) -> Result<Vec<bool>, Error> {
        let store = Arc::clone(&self.archive.store);
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
            let results: Vec<Result<bool, Error>> = futures_util::stream::iter(requests)
                .map(|(object, local)| {
                    let store = Arc::clone(&store);
                    async move {
                        let bytes = match store.get(&object).await {
                            Ok(result) => result.bytes().await.map(Some),
                            Err(object_store::Error::NotFound { .. }) => Ok(None),
                            Err(error) => Err(error),
                        }
                        .map_err(|error| Error::Unavailable {
                            message: format!("downloading {object} from the archive: {error}"),
                        })?;
                        let Some(bytes) = bytes else {
                            return Ok(false);
                        };
                        let write = move || -> Result<(), Error> {
                            if let Some(parent) = local.parent() {
                                std::fs::create_dir_all(parent)?;
                            }
                            let tmp = download_path(&local);
                            std::fs::write(&tmp, &bytes)?;
                            std::fs::File::open(&tmp)?.sync_all()?;
                            std::fs::rename(&tmp, &local)?;
                            Ok(())
                        };
                        tokio::task::spawn_blocking(write).await.map_err(|error| {
                            Error::Internal {
                                message: format!("writing a download: {error}"),
                            }
                        })??;
                        Ok(true)
                    }
                })
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

    /// Requests made to the archive so far.
    pub fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    /// Requests for segment bytes that failed so far.
    pub fn failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    /// Bytes fetched from the archive so far.
    pub fn fetched_bytes(&self) -> u64 {
        self.fetched_bytes.load(Ordering::Relaxed)
    }

    /// Bytes of segment `base`'s data, which is what its archived object
    /// holds.
    pub fn data_len(&self, base: u64) -> Result<u64, Error> {
        if let Some(&len) = self.data_lens.lock().unwrap().get(&base) {
            return Ok(len);
        }
        let idx = crate::segment::segment_path(&self.dir, base).with_extension("idx");
        let len = SealedIndex::open(&idx, base)?.data_len();
        self.data_lens.lock().unwrap().insert(base, len);
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
        let (mut have, waiting) = self.reserve(base, data_len, blocks_of(ranges));
        // Every download fails by its own deadline; this one is for a
        // download that never answers at all.
        let deadline = std::time::Instant::now() + self.archive.fetch_timeout + RETRY_TIMEOUT;
        for (block, flight) in waiting {
            let bytes = flight.wait(deadline).map_err(|failure| {
                // A later read starts a download of its own.
                let mut flights = lock(&self.flights);
                if flights
                    .get(&(base, block))
                    .is_some_and(|current| Arc::ptr_eq(current, &flight))
                {
                    flights.remove(&(base, block));
                }
                failure.into_error(base)
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
    /// are neither cached nor on their way, all at once, without waiting:
    /// a read that wants them later finds them cached or waits for them.
    /// What a read is about to need, or a scan will need next.
    pub fn prefetch(self: &Arc<Self>, wanted: &[(u64, Vec<Range<u64>>)]) -> Result<(), Error> {
        for (base, ranges) in wanted {
            let data_len = self.data_len(*base)?;
            let blocks = blocks_of(ranges)
                .into_iter()
                .filter(|&block| block * BLOCK < data_len)
                .collect();
            self.reserve(*base, data_len, blocks);
        }
        Ok(())
    }

    /// For each of `wanted` (blocks of segment `base`): its bytes when
    /// cached, or the download that brings them, which this call starts
    /// for the blocks nobody is fetching yet.
    fn reserve(
        self: &Arc<Self>,
        base: u64,
        data_len: u64,
        wanted: BTreeSet<u64>,
    ) -> (HashMap<u64, Bytes>, Waiters) {
        let mut have = HashMap::new();
        let mut waiting = Vec::new();
        let mut mine = Vec::new();
        {
            let mut blocks = self.blocks.lock().unwrap();
            let mut flights = self.flights.lock().unwrap();
            for block in wanted {
                if let Some(bytes) = blocks.get(&(base, block)) {
                    have.insert(block, bytes.clone());
                } else if let Some(flight) = flights.get(&(base, block)) {
                    waiting.push((block, Arc::clone(flight)));
                } else {
                    let flight = Arc::new(Flight::default());
                    flights.insert((base, block), Arc::clone(&flight));
                    waiting.push((block, Arc::clone(&flight)));
                    mine.push((block, flight));
                }
            }
        }
        if !mine.is_empty() {
            self.spawn_fetch(base, data_len, mine);
        }
        (have, waiting)
    }

    /// Downloads `blocks` (ascending) of segment `base` on the archive's
    /// runtime, neighbours in one request of up to [`MAX_GET`] bytes, each
    /// request a task of its own; each caches its blocks and lets their
    /// waiters go as soon as it has them.
    fn spawn_fetch(self: &Arc<Self>, base: u64, data_len: u64, blocks: Waiters) {
        let mut runs: Vec<(Range<u64>, Waiters)> = Vec::new();
        for (block, flight) in blocks {
            let (start, end) = (block * BLOCK, ((block + 1) * BLOCK).min(data_len));
            match runs.last_mut() {
                Some((run, blocks)) if run.end == start && end - run.start <= MAX_GET => {
                    run.end = end;
                    blocks.push((block, flight));
                }
                _ => runs.push((start..end, vec![(block, flight)])),
            }
        }
        for (run, blocks) in runs {
            let this = Arc::clone(self);
            let object = self.segment_object(base);
            let mut pending = Pending {
                segments: Arc::clone(self),
                base,
                blocks: Some(blocks),
            };
            self.archive.io.handle().spawn(async move {
                this.requests.fetch_add(1, Ordering::Relaxed);
                let timeout = this.archive.fetch_timeout;
                let fetched = tokio::time::timeout(
                    timeout,
                    get_range(
                        this.archive.store.as_ref(),
                        &object,
                        run,
                        data_len,
                        "its locator",
                    ),
                )
                .await
                .unwrap_or_else(|_| {
                    Err(Failure::Unavailable(format!(
                        "no answer within {}s",
                        timeout.as_secs_f64()
                    )))
                });
                let blocks = pending.blocks.take().expect("answered once");
                match fetched {
                    Ok(bytes) => {
                        this.fetched_bytes
                            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        // Each block is copied out: a slice would share the
                        // request's buffer, and one block left in the cache
                        // would keep all of it alive.
                        let mut cache = lock(&this.blocks);
                        let mut flights = lock(&this.flights);
                        for ((block, flight), bytes) in blocks
                            .iter()
                            .zip(bytes.chunks(BLOCK as usize).map(Bytes::copy_from_slice))
                        {
                            cache.put((base, *block), bytes.clone());
                            flights.remove(&(base, *block));
                            flight.finish(Ok(bytes));
                        }
                    }
                    Err(failure) => {
                        this.failures.fetch_add(1, Ordering::Relaxed);
                        let mut flights = lock(&this.flights);
                        for (block, flight) in &blocks {
                            flights.remove(&(base, *block));
                            flight.finish(Err(failure.clone()));
                        }
                    }
                }
            });
        }
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
        self.archive.block_on(async move {
            use futures_util::StreamExt;
            let results: Vec<Result<Option<crate::tier::SegmentSummary>, Error>> =
                futures_util::stream::iter(objects)
                    .map(|(base, object)| {
                        let store = Arc::clone(&store);
                        async move {
                            let bytes = match store.get(&object).await {
                                Ok(result) => result.bytes().await.map(Some),
                                Err(object_store::Error::NotFound { .. }) => Ok(None),
                                Err(error) => Err(error),
                            }
                            .map_err(|error| Error::Unavailable {
                                message: format!("reading {object} from the archive: {error}"),
                            })?;
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
    /// blake3, to `local`: written beside it and renamed into place once on
    /// disk.
    pub fn restore_body(
        &self,
        summary: &crate::tier::SegmentSummary,
        local: &Path,
    ) -> Result<(), Error> {
        let bytes = self.fetch_segment(summary)?;
        let tmp = download_path(local);
        std::fs::write(&tmp, &bytes)?;
        std::fs::File::open(&tmp)?.sync_all()?;
        std::fs::rename(&tmp, local)?;
        if let Some(parent) = local.parent() {
            crate::segment::writer::sync_dir(parent)?;
        }
        Ok(())
    }

    /// The whole archived segment `summary` describes, checked against its
    /// blake3: what a node catching up from the archive applies. Fetched in
    /// requests of [`MAX_GET`] bytes at once, past the block cache.
    pub fn fetch_segment(&self, summary: &crate::tier::SegmentSummary) -> Result<Vec<u8>, Error> {
        let base = summary.base;
        let ranges: Vec<Range<u64>> = (0..summary.data_len)
            .step_by(MAX_GET as usize)
            .map(|start| start..(start + MAX_GET).min(summary.data_len))
            .collect();
        let store = Arc::clone(&self.archive.store);
        let object = self.segment_object(base);
        let summary_len = summary.data_len;
        let parts = self
            .archive
            .block_on(async move {
                futures_util::future::try_join_all(ranges.into_iter().map(|range| {
                    get_range(store.as_ref(), &object, range, summary_len, "its summary")
                }))
                .await
                .map_err(|failure| failure.into_error(base))
            })
            .inspect_err(|_| {
                self.failures.fetch_add(1, Ordering::Relaxed);
            })?;
        let mut bytes = Vec::with_capacity(summary.data_len as usize);
        for part in parts {
            bytes.extend_from_slice(&part);
        }
        self.requests
            .fetch_add(summary.data_len.div_ceil(MAX_GET).max(1), Ordering::Relaxed);
        self.fetched_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        if bytes.len() as u64 != summary.data_len
            || blake3::hash(&bytes).to_hex().as_str() != summary.blake3
        {
            return Err(Error::Corrupted {
                message: format!(
                    "archived segment {base} does not match its summary ({} bytes, expected {})",
                    bytes.len(),
                    summary.data_len
                ),
            });
        }
        Ok(bytes)
    }

    /// Downloads the archived `<kind>/<name>` of this context to `local`,
    /// through a temporary file renamed into place. `false` when the
    /// archive does not hold it.
    pub fn download(&self, kind: &str, name: &str, local: &Path) -> Result<bool, Error> {
        let store = Arc::clone(&self.archive.store);
        let object = self.context.clone().join(kind).join(name);
        let bytes = self.archive.block_on(async move {
            match store.get(&object).await {
                Ok(result) => result.bytes().await.map(Some),
                Err(object_store::Error::NotFound { .. }) => Ok(None),
                Err(error) => Err(error),
            }
            .map_err(|error| Error::Unavailable {
                message: format!("downloading {object} from the archive: {error}"),
            })
        })?;
        let Some(bytes) = bytes else {
            return Ok(false);
        };
        let tmp = download_path(local);
        std::fs::write(&tmp, &bytes)?;
        std::fs::File::open(&tmp)?.sync_all()?;
        std::fs::rename(&tmp, local)?;
        if let Some(parent) = local.parent() {
            crate::segment::writer::sync_dir(parent)?;
        }
        Ok(true)
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

/// The longest one download of segment bytes may take before it fails as
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
    use std::sync::Arc;

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
                let cache = remote.blocks.lock().unwrap();
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
        let mut archive = super::ArchiveStore::over_store(hanging, "kb".into(), None).unwrap();
        Arc::get_mut(&mut archive).unwrap().fetch_timeout = std::time::Duration::from_millis(200);
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
            assert!(remote.flights.lock().unwrap().is_empty());
        }
        assert_eq!(remote.requests(), 2);
        assert_eq!(remote.failures(), 2);
    }
}
