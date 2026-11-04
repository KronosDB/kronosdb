//! An object store for tests of the archiver: another store underneath,
//! with a record of what was asked of it and the failures a test asks for.
//! A bucket under a retention policy, a store that ignores If-None-Match,
//! parts that fail, stall or take their time, deletes that fail.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
    UploadPart,
};

#[derive(Debug, Default)]
pub struct Faults {
    /// Refuse to replace an object, as a bucket under a retention policy
    /// does.
    pub write_once: bool,
    /// Treat `PutMode::Create` as an overwrite, as a store that ignores
    /// If-None-Match does.
    pub ignore_create: bool,
    /// Fail this many more puts of objects whose name ends with this.
    pub failing_puts: Option<(&'static str, usize)>,
    /// How long each part takes.
    pub part_delay: Duration,
    /// Parts never finish.
    pub stall_parts: bool,
    /// Fail this many more deletes, as a 503 does.
    pub failing_deletes: usize,
    /// Refuse every delete, as object lock does.
    pub refuse_deletes: bool,
}

#[derive(Debug, Default)]
pub struct Seen {
    /// Writes by object name: puts, and multipart uploads completed.
    pub writes: BTreeMap<String, usize>,
    /// Sizes of the parts uploaded, in order.
    pub part_sizes: Vec<usize>,
    in_flight: usize,
    /// Most parts in flight at once.
    pub most_in_flight: usize,
    /// Multipart uploads aborted.
    pub aborts: usize,
    /// Deletes asked for, failed ones included.
    pub deletes: usize,
}

#[derive(Debug, Clone)]
pub struct Faulty {
    inner: Arc<dyn ObjectStore>,
    pub faults: Arc<Mutex<Faults>>,
    pub seen: Arc<Mutex<Seen>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|e| e.into_inner())
}

fn failure(what: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "faulty",
        source: what.to_string().into(),
    }
}

fn refused(path: &Path) -> object_store::Error {
    object_store::Error::PermissionDenied {
        path: path.to_string(),
        source: "refused by the retention policy".into(),
    }
}

impl Faulty {
    pub fn over(inner: Box<dyn ObjectStore>, faults: Faults) -> Self {
        Self {
            inner: Arc::from(inner),
            faults: Arc::new(Mutex::new(faults)),
            seen: Arc::new(Mutex::new(Seen::default())),
        }
    }

    pub fn faults(&self) -> MutexGuard<'_, Faults> {
        lock(&self.faults)
    }

    pub fn seen(&self) -> MutexGuard<'_, Seen> {
        lock(&self.seen)
    }
}

impl fmt::Display for Faulty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Faulty({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for Faulty {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        mut opts: PutOptions,
    ) -> Result<PutResult> {
        let (write_once, fail) = {
            let mut faults = self.faults();
            if faults.ignore_create {
                opts.mode = PutMode::Overwrite;
            }
            let fail = match &mut faults.failing_puts {
                Some((suffix, left)) if *left > 0 && location.as_ref().ends_with(*suffix) => {
                    *left -= 1;
                    true
                }
                _ => false,
            };
            (faults.write_once, fail)
        };
        if fail {
            return Err(failure("an injected put failure"));
        }
        if write_once
            && matches!(opts.mode, PutMode::Overwrite)
            && self.inner.head(location).await.is_ok()
        {
            return Err(refused(location));
        }
        let result = self.inner.put_opts(location, payload, opts).await?;
        *self.seen().writes.entry(location.to_string()).or_default() += 1;
        Ok(result)
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(FaultyUpload {
            inner,
            location: location.clone(),
            store: self.clone(),
        }))
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let store = self.clone();
        locations
            .then(move |location| {
                let store = store.clone();
                async move {
                    let location = location?;
                    let refuse = {
                        store.seen().deletes += 1;
                        let mut faults = store.faults();
                        if faults.failing_deletes > 0 {
                            faults.failing_deletes -= 1;
                            return Err(failure("503 Service Unavailable"));
                        }
                        faults.refuse_deletes
                    };
                    if refuse {
                        return Err(refused(&location));
                    }
                    store.inner.delete(&location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

#[derive(Debug)]
struct FaultyUpload {
    inner: Box<dyn MultipartUpload>,
    location: Path,
    store: Faulty,
}

#[async_trait::async_trait]
impl MultipartUpload for FaultyUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let (delay, stall) = {
            let faults = self.store.faults();
            (faults.part_delay, faults.stall_parts)
        };
        self.store.seen().part_sizes.push(data.content_length());
        let part = self.inner.put_part(data);
        let seen = Arc::clone(&self.store.seen);
        Box::pin(async move {
            {
                let mut seen = lock(&seen);
                seen.in_flight += 1;
                seen.most_in_flight = seen.most_in_flight.max(seen.in_flight);
            }
            if stall {
                std::future::pending::<()>().await;
            }
            tokio::time::sleep(delay).await;
            let result = part.await;
            lock(&seen).in_flight -= 1;
            result
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let write_once = self.store.faults().write_once;
        if write_once && self.store.inner.head(&self.location).await.is_ok() {
            return Err(refused(&self.location));
        }
        let result = self.inner.complete().await?;
        *self
            .store
            .seen()
            .writes
            .entry(self.location.to_string())
            .or_default() += 1;
        Ok(result)
    }

    async fn abort(&mut self) -> Result<()> {
        self.store.seen().aborts += 1;
        self.inner.abort().await
    }
}
