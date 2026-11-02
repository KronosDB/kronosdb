//! Cold reads (ADR-0011): the bytes of evicted segments, read from the
//! archive by range.
//!
//! A segment whose body was evicted keeps its locator and key tables, so a
//! read is planned exactly as for a local one; only the bytes come from the
//! archive's `segments/<base>.seg`. They are fetched in aligned blocks of
//! [`BLOCK`] bytes: a block several readers want is downloaded once (the
//! first to ask fetches it, the others wait for it), and recent blocks stay
//! in a small in-memory cache, so subscribers catching up over the same cold
//! range, or a hot old aggregate, do not pay a request each. Neighbouring
//! missing blocks go in one request of up to [`MAX_GET`] bytes, and one
//! read's requests run concurrently.
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
pub const MAX_GET: u64 = 8 * 1024 * 1024;

/// Default memory budget of the cache of fetched blocks, per context.
pub const DEFAULT_CACHE_BYTES: usize = 64 << 20;

/// An archive as cold reads see it: its object store, and the runtime its
/// requests run on. One per process, shared by every context.
pub struct ArchiveStore {
    store: Arc<dyn ObjectStore>,
    prefix: ObjectPath,
    io: IoRuntime,
    cache_bytes: usize,
}

impl std::fmt::Debug for ArchiveStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArchiveStore")
            .field("prefix", &self.prefix)
            .field("cache_bytes", &self.cache_bytes)
            .finish()
    }
}

impl ArchiveStore {
    /// The archive at `raw_url`, as for [`crate::tier::Archiver::from_url`].
    pub fn from_url(raw_url: &str) -> Result<Arc<Self>, Error> {
        let url = url::Url::parse(raw_url).map_err(|error| Error::Rejected {
            message: format!("invalid archive url {raw_url}: {error}"),
        })?;
        let (store, prefix) = object_store::parse_url(&url).map_err(|error| Error::Rejected {
            message: format!("unsupported archive url {raw_url}: {error}"),
        })?;
        Ok(Arc::new(Self {
            store: Arc::from(store),
            prefix,
            io: IoRuntime::new()?,
            cache_bytes: DEFAULT_CACHE_BYTES,
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
    pub fn read(&self, base: u64, ranges: &[Range<u64>]) -> Result<Vec<Vec<u8>>, Error> {
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
        let wanted: BTreeSet<u64> = ranges
            .iter()
            .filter(|range| range.end > range.start)
            .flat_map(|range| range.start / BLOCK..=(range.end - 1) / BLOCK)
            .collect();

        let mut have: HashMap<u64, Bytes> = HashMap::new();
        let mut mine = Vec::new();
        let mut theirs = Vec::new();
        {
            let mut blocks = self.blocks.lock().unwrap();
            let mut flights = self.flights.lock().unwrap();
            for &block in &wanted {
                if let Some(bytes) = blocks.get(&(base, block)) {
                    have.insert(block, bytes.clone());
                } else if let Some(flight) = flights.get(&(base, block)) {
                    theirs.push((block, Arc::clone(flight)));
                } else {
                    let flight = Arc::new(Flight::default());
                    flights.insert((base, block), Arc::clone(&flight));
                    mine.push((block, flight));
                }
            }
        }

        if !mine.is_empty() {
            let fetched = self.fetch_blocks(base, data_len, mine.iter().map(|(block, _)| *block));
            let mut blocks = self.blocks.lock().unwrap();
            let mut flights = self.flights.lock().unwrap();
            match &fetched {
                Ok(fetched) => {
                    for ((block, flight), bytes) in mine.iter().zip(fetched) {
                        blocks.put((base, *block), bytes.clone());
                        flights.remove(&(base, *block));
                        flight.finish(Ok(bytes.clone()));
                        have.insert(*block, bytes.clone());
                    }
                }
                Err(error) => {
                    for (block, flight) in &mine {
                        flights.remove(&(base, *block));
                        flight.finish(Err(error.to_string()));
                    }
                }
            }
            drop(flights);
            drop(blocks);
            fetched?;
        }
        for (block, flight) in theirs {
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

    /// Fetches `blocks` (ascending) of segment `base`, neighbours in one
    /// request, all requests at once. Returns each block's bytes.
    fn fetch_blocks(
        &self,
        base: u64,
        data_len: u64,
        blocks: impl Iterator<Item = u64>,
    ) -> Result<Vec<Bytes>, Error> {
        let mut runs: Vec<Range<u64>> = Vec::new();
        for block in blocks {
            let (start, end) = (block * BLOCK, ((block + 1) * BLOCK).min(data_len));
            match runs.last_mut() {
                Some(run) if run.end == start && end - run.start <= MAX_GET => run.end = end,
                _ => runs.push(start..end),
            }
        }
        let store = Arc::clone(&self.archive.store);
        let object = self.segment_object(base);
        let requested = runs.clone();
        let fetched = self.archive.block_on(async move {
            let gets = requested
                .iter()
                .map(|run| store.get_range(&object, run.clone()));
            futures_util::future::try_join_all(gets)
                .await
                .map_err(|error| archive_error(base, error))
        })?;
        self.requests
            .fetch_add(runs.len() as u64, Ordering::Relaxed);

        let mut out = Vec::new();
        for (run, bytes) in runs.iter().zip(fetched) {
            if bytes.len() as u64 != run.end - run.start {
                return Err(Error::Unavailable {
                    message: format!(
                        "the archive returned {} bytes of segment {base} for {}..{}",
                        bytes.len(),
                        run.start,
                        run.end
                    ),
                });
            }
            self.fetched_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            let mut at = 0;
            while at < bytes.len() {
                let end = (at + BLOCK as usize).min(bytes.len());
                out.push(bytes.slice(at..end));
                at = end;
            }
        }
        Ok(out)
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
        let tmp = local.with_extension("download");
        std::fs::write(&tmp, &bytes)?;
        std::fs::File::open(&tmp)?.sync_all()?;
        std::fs::rename(&tmp, local)?;
        if let Some(parent) = local.parent() {
            crate::segment::writer::sync_dir(parent)?;
        }
        Ok(true)
    }
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
