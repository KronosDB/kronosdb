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
//! waits for them. Bytes are not checked here; every record is CRC-checked
//! where it is read, as on the local paths. A failed request is
//! `Error::Unavailable`.

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
    /// The cap on sealed bytes this node keeps on disk, if any: a node
    /// catching up from the archive skips the bodies it would evict anyway.
    max_local_bytes: Option<u64>,
}

impl std::fmt::Debug for ArchiveStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArchiveStore")
            .field("prefix", &self.prefix)
            .field("cache_bytes", &self.cache_bytes)
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
    blocks: Option<Vec<(u64, Arc<Flight>)>>,
}

fn blocks_len(pending: &Pending) -> usize {
    pending.blocks.as_ref().map_or(0, Vec::len)
}

impl Drop for Pending {
    fn drop(&mut self) {
        let Some(blocks) = self.blocks.take() else {
            return;
        };
        let mut flights = self
            .segments
            .flights
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (block, flight) in blocks {
            flights.remove(&(self.base, block));
            flight.finish(Err(format!(
                "the download of segment {} was abandoned",
                self.base
            )));
        }
    }
}

/// One block being fetched: the reader that fetches it fills it in, the
/// others wait on it.
#[derive(Default)]
struct Flight {
    state: Mutex<Option<Result<Bytes, String>>>,
    done: Condvar,
}

impl Flight {
    fn finish(&self, result: Result<Bytes, String>) {
        *self.state.lock().unwrap() = Some(result);
        self.done.notify_all();
    }

    fn wait(&self) -> Result<Bytes, String> {
        let mut state = self.state.lock().unwrap();
        loop {
            if let Some(result) = state.as_ref() {
                return result.clone();
            }
            state = self.done.wait(state).unwrap();
        }
    }
}

type BlockKey = (u64, u64);

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
        for (block, flight) in waiting {
            let bytes = flight.wait().map_err(|message| Error::Unavailable {
                message: format!("reading segment {base} from the archive: {message}"),
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
    #[allow(clippy::type_complexity)]
    fn reserve(
        self: &Arc<Self>,
        base: u64,
        data_len: u64,
        wanted: BTreeSet<u64>,
    ) -> (HashMap<u64, Bytes>, Vec<(u64, Arc<Flight>)>) {
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
    /// runtime, neighbours in one request of up to [`MAX_GET`] bytes, all
    /// requests at once; then caches them and lets their waiters go.
    fn spawn_fetch(self: &Arc<Self>, base: u64, data_len: u64, blocks: Vec<(u64, Arc<Flight>)>) {
        let mut runs: Vec<Range<u64>> = Vec::new();
        for &(block, _) in &blocks {
            let (start, end) = (block * BLOCK, ((block + 1) * BLOCK).min(data_len));
            match runs.last_mut() {
                Some(run) if run.end == start && end - run.start <= MAX_GET => run.end = end,
                _ => runs.push(start..end),
            }
        }
        let this = Arc::clone(self);
        let store = Arc::clone(&self.archive.store);
        let object = self.segment_object(base);
        let mut pending = Pending {
            segments: Arc::clone(self),
            base,
            blocks: Some(blocks),
        };
        let count = blocks_len(&pending);
        self.archive.io.handle().spawn(async move {
            let gets = runs.iter().map(|run| store.get_range(&object, run.clone()));
            let fetched = futures_util::future::try_join_all(gets)
                .await
                .map_err(|error| archive_error(base, error).to_string())
                .and_then(|parts| {
                    this.requests
                        .fetch_add(runs.len() as u64, Ordering::Relaxed);
                    let mut out = Vec::with_capacity(count);
                    for (run, bytes) in runs.iter().zip(parts) {
                        if bytes.len() as u64 != run.end - run.start {
                            return Err(format!(
                                "the archive returned {} bytes of segment {base} for {}..{}",
                                bytes.len(),
                                run.start,
                                run.end
                            ));
                        }
                        this.fetched_bytes
                            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                        let mut at = 0;
                        while at < bytes.len() {
                            let end = (at + BLOCK as usize).min(bytes.len());
                            out.push(bytes.slice(at..end));
                            at = end;
                        }
                    }
                    Ok(out)
                });
            let mut cache = this.blocks.lock().unwrap();
            let mut flights = this.flights.lock().unwrap();
            let blocks = pending.blocks.take().expect("answered once");
            match fetched {
                Ok(fetched) => {
                    for ((block, flight), bytes) in blocks.iter().zip(fetched) {
                        cache.put((base, *block), bytes.clone());
                        flights.remove(&(base, *block));
                        flight.finish(Ok(bytes));
                    }
                }
                Err(error) => {
                    for (block, flight) in &blocks {
                        flights.remove(&(base, *block));
                        flight.finish(Err(error.clone()));
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
        let parts = self.archive.block_on(async move {
            futures_util::future::try_join_all(
                ranges
                    .into_iter()
                    .map(|range| store.get_range(&object, range)),
            )
            .await
            .map_err(|error| archive_error(base, error))
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
    let url = url::Url::parse(raw_url).map_err(|error| Error::Rejected {
        message: format!("invalid archive url {raw_url}: {error}"),
    })?;
    let settings = std::env::vars().map(|(key, value)| (key.to_ascii_lowercase(), value));
    object_store::parse_url_opts(&url, settings).map_err(|error| Error::Rejected {
        message: format!("unsupported archive url {raw_url}: {error}"),
    })
}

/// Where a download is written before it is renamed to `local`: next to
/// it, under its full name, so two downloads never share one.
fn download_path(local: &Path) -> PathBuf {
    let mut name = local.file_name().unwrap_or_default().to_os_string();
    name.push(".download");
    local.with_file_name(name)
}

fn archive_error(base: u64, error: object_store::Error) -> Error {
    match error {
        object_store::Error::NotFound { .. } => Error::Internal {
            message: format!("evicted segment {base} is missing from the archive"),
        },
        error => Error::Unavailable {
            message: format!("reading segment {base} from the archive: {error}"),
        },
    }
}

#[cfg(test)]
mod tests {
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
}
