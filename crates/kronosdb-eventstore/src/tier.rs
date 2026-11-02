//! The archive tier (ADR-0011): final segments and their index, copied to an
//! object store, so that a node whose disk cannot hold the whole log can
//! evict old segment bodies and read them back by range.
//!
//! The claimed leader archives. Under `<url>/<context>/`:
//!
//! ```text
//! segments/<base>.seg        the segment's data bytes, [0, data_len)
//! segments/<base>.idx        its locator
//! segments/<base>.sum        its summary, written last: the commit
//! index/<base>-<end>.keys    key tables over [base, end)
//! ```
//!
//! Every object is written once and never replaced, so the archive can live
//! in a bucket under object lock. A segment is archived once its summary
//! exists. Summaries are written only if absent and segments are archived
//! in log order, so the archived segments are always a prefix of the log. A
//! writer that finds a summary already there compares it with its own:
//! equal is two leaders racing, different means two logs share the prefix,
//! and the pass fails without writing anything more.
//!
//! The key tables are a segment's own table while no merged table covers
//! it, and the merged tables of ADR-0010, uploaded once the segments they
//! cover are archived. Both are deterministic, so a table's name fixes its
//! bytes. A table inside another archived table's range is superseded and
//! deleted after [`DELETE_GRACE`]; a bucket under object lock refuses, and
//! the table stays.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt, PutMode, PutPayload, WriteMultipart};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncReadExt;

use crate::error::Error;
use crate::segment::merge::{parse_table_file_name, table_file_name};
use crate::segment::sealed_index::SealedIndex;
use crate::store::{ArchivableSegment, EventStoreEngine};

/// Upload chunk size for multipart uploads.
const UPLOAD_CHUNK: usize = 8 * 1024 * 1024;

/// How long a superseded key table stays in the archive after the table
/// that covers it was uploaded, so a node that listed the archive just
/// before can still download what it chose.
pub const DELETE_GRACE: Duration = Duration::from_secs(600);

#[derive(Debug, Clone)]
pub struct TierConfig {
    /// Object store URL, e.g. `file:///archive/kronosdb` or `s3://bucket/prefix`.
    /// Cloud credentials come from the environment (standard SDK variables).
    pub url: String,
    /// Delay between archive passes on the claimed leader, and between
    /// eviction passes on every node.
    pub interval: Duration,
    /// Most bytes of sealed segments this node keeps on disk; past it, the
    /// oldest archived segments are evicted. `None` never evicts.
    pub max_local_bytes: Option<u64>,
}

/// What `segments/<base>.sum` holds: the commit of one archived segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SegmentSummary {
    pub base: u64,
    /// End position (exclusive): the next segment's base.
    pub end: u64,
    /// Bytes of `<base>.seg`.
    pub data_len: u64,
    /// blake3 of `<base>.seg`, hex.
    pub blake3: String,
}

#[derive(Debug, Default)]
pub struct ArchivePassReport {
    /// Segments this pass archived.
    pub segments: usize,
    /// Key tables this pass uploaded, segments' own included.
    pub tables: usize,
    /// Superseded key tables this pass deleted.
    pub deleted: usize,
    /// End of the archived prefix after the pass.
    pub archived_end: u64,
}

#[derive(Debug, Default)]
pub struct EvictionPassReport {
    /// Segments evicted.
    pub evicted: usize,
    /// Bytes their bodies held.
    pub freed: u64,
    /// Bytes of sealed segments still on disk.
    pub local_bytes: u64,
    /// Contexts whose oldest local segment is not archived yet.
    pub waiting: Vec<String>,
}

/// What the archiver knows of one context's archive. Built by listing it
/// the first time a pass runs, then kept up to date by the passes.
struct ContextArchive {
    /// End of the archived prefix: the base of the next segment to archive.
    archived_end: u64,
    /// Archived key tables by range, with when each was written, in
    /// milliseconds since the epoch.
    tables: BTreeMap<(u64, u64), i64>,
    /// Superseded tables the store refused to delete. Left alone.
    kept: BTreeSet<(u64, u64)>,
}

enum Archived {
    Uploaded,
    /// Another writer archived it first, with the same bytes.
    AlreadyThere,
    /// Its `.idx` is not on disk yet: the seal is still in flight.
    SealInFlight,
}

pub struct Archiver {
    store: Box<dyn ObjectStore>,
    prefix: ObjectPath,
    grace: Duration,
    contexts: tokio::sync::Mutex<HashMap<String, ContextArchive>>,
}

impl Archiver {
    /// Builds an archiver from an object-store URL. `file://` and `memory://`
    /// need no credentials; `s3://` reads the standard AWS environment.
    pub fn from_url(raw_url: &str) -> Result<Self, Error> {
        let url = url::Url::parse(raw_url).map_err(|error| Error::Rejected {
            message: format!("invalid archive url {raw_url}: {error}"),
        })?;
        let (store, prefix) = object_store::parse_url(&url).map_err(|error| Error::Rejected {
            message: format!("unsupported archive url {raw_url}: {error}"),
        })?;
        Ok(Self {
            store,
            prefix,
            grace: DELETE_GRACE,
            contexts: tokio::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Sets how long superseded tables stay before they are deleted.
    pub fn with_delete_grace(mut self, grace: Duration) -> Self {
        self.grace = grace;
        self
    }

    /// Drops what this archiver knows of every context, so the next pass
    /// lists the archive again. Called when this node stops being leader:
    /// another node may write in the meantime.
    pub async fn forget(&self) {
        self.contexts.lock().await.clear();
    }

    fn context_path(&self, context: &str, parts: &[&str]) -> ObjectPath {
        let mut path = self.prefix.clone().join(context);
        for part in parts {
            path = path.join(*part);
        }
        path
    }

    fn segment_object(&self, context: &str, base: u64, extension: &str) -> ObjectPath {
        self.context_path(context, &["segments", &format!("{base:020}.{extension}")])
    }

    fn table_object(&self, context: &str, base: u64, end: u64) -> ObjectPath {
        self.context_path(context, &["index", &table_file_name(base, end)])
    }

    /// The summary of the archived segment at `base`, if it is archived.
    pub async fn summary(&self, context: &str, base: u64) -> Result<Option<SegmentSummary>, Error> {
        match self
            .store
            .get(&self.segment_object(context, base, "sum"))
            .await
        {
            Ok(result) => {
                let bytes = result.bytes().await.map_err(tier_err)?;
                serde_json::from_slice(&bytes)
                    .map(Some)
                    .map_err(|error| Error::Corrupted {
                        message: format!(
                            "archived summary of segment {base} in context {context}: {error}"
                        ),
                    })
            }
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(tier_err(error)),
        }
    }

    /// Archives every final segment past the archived prefix, in log order,
    /// then the merged tables the archived prefix covers, then deletes key
    /// tables superseded long enough ago. Idempotent; safe to re-run after
    /// any crash or leader change.
    pub async fn run_archive_pass(
        &self,
        context: &str,
        engine: &EventStoreEngine,
    ) -> Result<ArchivePassReport, Error> {
        let mut contexts = self.contexts.lock().await;
        if !contexts.contains_key(context) {
            let state = self.open_context(context, engine).await?;
            contexts.insert(context.to_string(), state);
        }
        let state = contexts.get_mut(context).expect("inserted above");

        let mut report = ArchivePassReport::default();
        for segment in engine.archivable_segments() {
            if segment.base < state.archived_end {
                continue;
            }
            if segment.base != state.archived_end {
                return Err(Error::Internal {
                    message: format!(
                        "the archive of context {context} ends at {}, but the next final \
                         segment starts at {}",
                        state.archived_end, segment.base
                    ),
                });
            }
            match self.archive_segment(context, &segment, state).await? {
                Archived::Uploaded => report.segments += 1,
                Archived::AlreadyThere => {}
                // A segment whose seal has not reached the disk holds back
                // the ones after it until the next pass.
                Archived::SealInFlight => break,
            }
            state.archived_end = segment.end;
        }
        report.tables += self.archive_tables(context, engine, state).await?;
        report.deleted = self.delete_superseded(context, state).await;
        report.archived_end = state.archived_end;
        Ok(report)
    }

    /// Lists the archive of `context`: how far its segments reach, checked
    /// against the local log at both ends of that prefix, and its tables.
    async fn open_context(
        &self,
        context: &str,
        engine: &EventStoreEngine,
    ) -> Result<ContextArchive, Error> {
        let mut summaries = BTreeSet::new();
        let mut tables = BTreeMap::new();
        let mut listing = self.store.list(Some(&self.context_path(context, &[])));
        while let Some(meta) = listing.try_next().await.map_err(tier_err)? {
            let Some(name) = meta.location.filename() else {
                continue;
            };
            if let Some(base) = name
                .strip_suffix(".sum")
                .and_then(|base| base.parse::<u64>().ok())
            {
                summaries.insert(base);
            } else if let Some(range) = parse_table_file_name(name) {
                tables.insert(range, meta.last_modified.timestamp_millis());
            }
        }

        let segments = engine.archivable_segments();
        let archived: Vec<&ArchivableSegment> = segments
            .iter()
            .take_while(|segment| summaries.contains(&segment.base))
            .collect();
        let mut checked = archived
            .first()
            .into_iter()
            .chain(archived.last())
            .collect::<Vec<_>>();
        checked.dedup_by_key(|segment| segment.base);
        for segment in checked {
            let Some(local) = local_summary(segment).await? else {
                continue;
            };
            self.check_summary(context, &local).await?;
        }
        Ok(ContextArchive {
            archived_end: archived.last().map_or(0, |segment| segment.end),
            tables,
            kept: BTreeSet::new(),
        })
    }

    /// Fails unless the archived summary of `local.base` is `local`.
    async fn check_summary(&self, context: &str, local: &SegmentSummary) -> Result<(), Error> {
        match self.summary(context, local.base).await? {
            Some(archived) if archived == *local => Ok(()),
            Some(archived) => Err(diverged(context, local, &archived)),
            None => Err(Error::Internal {
                message: format!(
                    "the archived summary of segment {} in context {context} disappeared",
                    local.base
                ),
            }),
        }
    }

    /// Uploads one final segment's data bytes, its `.idx` and its own key
    /// table, then commits it with its summary.
    async fn archive_segment(
        &self,
        context: &str,
        segment: &ArchivableSegment,
        state: &mut ContextArchive,
    ) -> Result<Archived, Error> {
        let summary_object = self.segment_object(context, segment.base, "sum");
        // Another leader may have archived it since this node last listed.
        match self.store.head(&summary_object).await {
            Ok(_) => {
                let Some(local) = local_summary(segment).await? else {
                    return Ok(Archived::SealInFlight);
                };
                self.check_summary(context, &local).await?;
                return Ok(Archived::AlreadyThere);
            }
            Err(object_store::Error::NotFound { .. }) => {}
            Err(error) => return Err(tier_err(error)),
        }

        let idx_path = segment.path.with_extension("idx");
        let idx_bytes = match tokio::fs::read(&idx_path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Archived::SealInFlight);
            }
            Err(error) => return Err(error.into()),
        };
        // The segment's data ends where its locator says. The file may run
        // on past that (preallocated space a crash kept from being trimmed),
        // and those bytes are not part of the log.
        let data_len = SealedIndex::open(&idx_path, segment.base)?.data_len();
        let blake3 = self
            .upload_file(
                &segment.path,
                data_len,
                &self.segment_object(context, segment.base, "seg"),
            )
            .await?;
        self.store
            .put(
                &self.segment_object(context, segment.base, "idx"),
                PutPayload::from(idx_bytes),
            )
            .await
            .map_err(tier_err)?;

        // Its own key table, unless a merge has already replaced it with a
        // merged table, which goes up once the archive covers its range.
        let range = (segment.base, segment.end);
        let keys_path = segment.path.with_extension("keys");
        if !state.tables.contains_key(&range)
            && let Some(len) = file_len(&keys_path).await?
        {
            let object = self.table_object(context, segment.base, segment.end);
            match self.upload_file(&keys_path, len, &object).await {
                Ok(_) => {
                    state.tables.insert(range, now_millis());
                }
                Err(error) if is_not_found(&error) => {}
                Err(error) => return Err(error),
            }
        }

        let summary = SegmentSummary {
            base: segment.base,
            end: segment.end,
            data_len,
            blake3,
        };
        let body = serde_json::to_vec(&summary).map_err(|error| Error::Internal {
            message: format!("encode segment summary: {error}"),
        })?;
        match self
            .store
            .put_opts(
                &summary_object,
                PutPayload::from(body),
                PutMode::Create.into(),
            )
            .await
        {
            Ok(_) => Ok(Archived::Uploaded),
            Err(object_store::Error::AlreadyExists { .. }) => {
                self.check_summary(context, &summary).await?;
                Ok(Archived::AlreadyThere)
            }
            Err(error) => Err(tier_err(error)),
        }
    }

    /// Uploads the live merged tables that lie inside the archived prefix
    /// and are not archived yet. Returns how many it uploaded.
    async fn archive_tables(
        &self,
        context: &str,
        engine: &EventStoreEngine,
        state: &mut ContextArchive,
    ) -> Result<usize, Error> {
        let mut uploaded = 0;
        for table in engine.archivable_tables()? {
            let range = (table.base, table.end);
            if table.end > state.archived_end || state.tables.contains_key(&range) {
                continue;
            }
            // A merge may supersede and delete it at any moment; its
            // successor is uploaded on a later pass.
            let Some(len) = file_len(&table.path).await? else {
                continue;
            };
            let object = self.table_object(context, table.base, table.end);
            match self.upload_file(&table.path, len, &object).await {
                Ok(_) => {
                    state.tables.insert(range, now_millis());
                    uploaded += 1;
                }
                Err(error) if is_not_found(&error) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(uploaded)
    }

    /// Deletes archived tables whose range lies inside another archived
    /// table's, once that table has been archived for the grace period.
    /// Best effort: a table the store refuses to delete is kept and not
    /// tried again. Returns how many it deleted.
    async fn delete_superseded(&self, context: &str, state: &mut ContextArchive) -> usize {
        // Tables are aligned blocks: nested or disjoint. In order of base,
        // widest first, a table is inside another exactly when an earlier
        // one reaches at least as far.
        let mut ranges: Vec<((u64, u64), i64)> = state
            .tables
            .iter()
            .map(|(&range, &at)| (range, at))
            .collect();
        ranges.sort_by_key(|&((base, end), _)| (base, std::cmp::Reverse(end)));
        let grace = self.grace.as_millis() as i64;
        let now = now_millis();
        let mut cover: Option<(u64, i64)> = None;
        let mut doomed = Vec::new();
        for ((base, end), at) in ranges {
            match cover {
                Some((cover_end, cover_at)) if end <= cover_end => {
                    if now - cover_at >= grace && !state.kept.contains(&(base, end)) {
                        doomed.push((base, end));
                    }
                }
                _ => cover = Some((end, at)),
            }
        }

        let mut deleted = 0;
        for range in doomed {
            match self
                .store
                .delete(&self.table_object(context, range.0, range.1))
                .await
            {
                Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                    state.tables.remove(&range);
                    deleted += 1;
                }
                Err(error) => {
                    if state.kept.is_empty() {
                        tracing::warn!(
                            context,
                            %error,
                            "the archive refused to delete a superseded key table; keeping \
                             superseded tables (object lock?)"
                        );
                    }
                    state.kept.insert(range);
                }
            }
        }
        deleted
    }

    /// Evicts the oldest archived segments of `engines` while the sealed
    /// segments on this node's disk hold more than `max_local_bytes`: across
    /// contexts, the segment sealed longest ago first. Runs on every node,
    /// each under its own cap.
    ///
    /// A segment is evicted only once its summary is in the archive and the
    /// blake3 of this node's data bytes equals the summary's. A mismatch is
    /// real divergence: the context's segments are kept and the pass fails.
    pub async fn run_eviction_pass(
        &self,
        engines: &[(String, Arc<EventStoreEngine>)],
        max_local_bytes: u64,
    ) -> Result<EvictionPassReport, Error> {
        let mut report = EvictionPassReport::default();
        let mut local = 0u64;
        for (_, engine) in engines {
            for segment in engine.sealed_bodies() {
                local += file_len(&segment.path).await?.unwrap_or(0);
            }
        }
        let mut open: Vec<&(String, Arc<EventStoreEngine>)> = engines.iter().collect();
        while local > max_local_bytes {
            // Each context's oldest final segment with a body, and when it
            // was sealed.
            let mut oldest = None;
            let mut done = Vec::new();
            for (i, (_, engine)) in open.iter().enumerate() {
                let evicted_end = engine.evicted_end();
                let Some(segment) = engine
                    .archivable_segments()
                    .into_iter()
                    .find(|segment| segment.base >= evicted_end)
                else {
                    done.push(i);
                    continue;
                };
                let sealed = tokio::fs::metadata(&segment.path).await?.modified()?;
                if oldest.as_ref().is_none_or(|(_, _, at)| sealed < *at) {
                    oldest = Some((i, segment, sealed));
                }
            }
            let Some((i, segment, _)) = oldest else {
                break;
            };
            let (context, engine) = open[i];
            match self.summary(context, segment.base).await? {
                None => {
                    report.waiting.push(context.clone());
                    done.push(i);
                }
                Some(archived) => match local_summary(&segment).await? {
                    // Its seal is in flight or it was evicted meanwhile:
                    // the next pass looks again.
                    None => done.push(i),
                    Some(own) if own != archived => {
                        return Err(diverged(context, &own, &archived));
                    }
                    Some(own) => {
                        let engine = Arc::clone(engine);
                        let base = segment.base;
                        tokio::task::spawn_blocking(move || engine.evict_segment(base))
                            .await
                            .map_err(|error| Error::Internal {
                                message: format!("evicting segment {base}: {error}"),
                            })??;
                        local = local.saturating_sub(own.data_len);
                        report.evicted += 1;
                        report.freed += own.data_len;
                    }
                },
            }
            done.sort_unstable();
            for i in done.into_iter().rev() {
                open.remove(i);
            }
        }
        report.local_bytes = local;
        Ok(report)
    }

    /// Streams the first `len` bytes of `path` to `object`, verifies the
    /// stored size, and returns their blake3. A file shorter than `len` is
    /// `Error::Corrupted`: its index says it holds more.
    async fn upload_file(
        &self,
        path: &Path,
        len: u64,
        object: &ObjectPath,
    ) -> Result<String, Error> {
        let file = tokio::fs::File::open(path).await?;
        let file_len = file.metadata().await?.len();
        if file_len < len {
            return Err(Error::Corrupted {
                message: format!(
                    "{} is {file_len} bytes, but its index says its data runs to {len}",
                    path.display()
                ),
            });
        }
        let mut data = file.take(len);
        let upload = self.store.put_multipart(object).await.map_err(tier_err)?;
        let mut writer = WriteMultipart::new(upload);
        let mut hasher = blake3::Hasher::new();
        let mut total: u64 = 0;
        let mut chunk = vec![0u8; UPLOAD_CHUNK];
        loop {
            let read = data.read(&mut chunk).await?;
            if read == 0 {
                break;
            }
            hasher.update(&chunk[..read]);
            writer.write(&chunk[..read]);
            total += read as u64;
        }
        writer.finish().await.map_err(tier_err)?;
        if total != len {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!(
                    "{} ended at {total} bytes while uploading its {len}",
                    path.display()
                ),
            )));
        }

        let head = self.store.head(object).await.map_err(tier_err)?;
        if head.size != total {
            return Err(Error::Corrupted {
                message: format!(
                    "archive verification failed for {object}: uploaded {total} bytes, the \
                     store reports {}",
                    head.size
                ),
            });
        }
        Ok(hasher.finalize().to_hex().to_string())
    }
}

/// The summary this node's copy of `segment` would have; `None` while its
/// seal is in flight, or once its body is evicted.
async fn local_summary(segment: &ArchivableSegment) -> Result<Option<SegmentSummary>, Error> {
    let idx_path = segment.path.with_extension("idx");
    if file_len(&idx_path).await?.is_none() {
        return Ok(None);
    }
    let data_len = SealedIndex::open(&idx_path, segment.base)?.data_len();
    let file = match tokio::fs::File::open(&segment.path).await {
        Ok(file) => file,
        // Evicted: the archive is all there is to compare with.
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let mut data = file.take(data_len);
    let mut hasher = blake3::Hasher::new();
    let mut total = 0u64;
    let mut chunk = vec![0u8; UPLOAD_CHUNK];
    loop {
        let read = data.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        hasher.update(&chunk[..read]);
        total += read as u64;
    }
    if total != data_len {
        return Err(Error::Corrupted {
            message: format!(
                "{} is {total} bytes, but its index says its data runs to {data_len}",
                segment.path.display()
            ),
        });
    }
    Ok(Some(SegmentSummary {
        base: segment.base,
        end: segment.end,
        data_len,
        blake3: hasher.finalize().to_hex().to_string(),
    }))
}

fn diverged(context: &str, local: &SegmentSummary, archived: &SegmentSummary) -> Error {
    Error::Corrupted {
        message: format!(
            "the archive of context {context} holds a different segment {}: locally {local:?}, \
             archived {archived:?}. Does another cluster archive to the same prefix?",
            local.base
        ),
    }
}

async fn file_len(path: &Path) -> Result<Option<u64>, Error> {
    match tokio::fs::metadata(path).await {
        Ok(meta) => Ok(Some(meta.len())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn is_not_found(error: &Error) -> bool {
    matches!(error, Error::Io(io) if io.kind() == std::io::ErrorKind::NotFound)
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| since.as_millis() as i64)
}

fn tier_err(error: object_store::Error) -> Error {
    Error::Io(std::io::Error::other(format!("object store: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::append::AppendRequest;
    use crate::event::{AppendEvent, Tag};
    use crate::store::{EventStoreEngine, StoreOptions};
    use std::sync::Arc;

    const SEGMENT_SIZE: u64 = 16 * 1024;

    fn make_event(log: &str, index: usize) -> AppendEvent {
        AppendEvent {
            identifier: format!("{log}-{index}"),
            name: "TierTested".into(),
            version: "1.0".into(),
            timestamp: 1712345678000,
            payload: vec![b'x'; 512],
            metadata: vec![],
            tags: vec![Tag::from_str("agg", &format!("agg-{}", index % 4))],
        }
    }

    fn engine_in(dir: &Path) -> Arc<EventStoreEngine> {
        Arc::new(
            EventStoreEngine::create_with_store_options(
                dir,
                &StoreOptions {
                    max_segment_size: SEGMENT_SIZE,
                    background_index_merge: false,
                    index_merge_rate: 0,
                    ..StoreOptions::default()
                },
            )
            .expect("engine"),
        )
    }

    /// Appends events of the log named `log` until at least
    /// `minimum_final` segments are final and sealed.
    async fn seal_segments(
        engine: &Arc<EventStoreEngine>,
        log: &'static str,
        minimum_final: usize,
    ) {
        let engine = Arc::clone(engine);
        tokio::task::spawn_blocking(move || {
            let mut index = engine.head().0 as usize;
            while engine.archivable_segments().len() < minimum_final {
                engine
                    .append(AppendRequest {
                        condition: None,
                        events: (0..8)
                            .map(|_| {
                                index += 1;
                                make_event(log, index)
                            })
                            .collect(),
                    })
                    .expect("append");
                assert!(index < 100_000, "segments never sealed");
            }
            engine.wait_for_seals();
        })
        .await
        .expect("seal");
    }

    fn archiver_for(dir: &Path, grace: Duration) -> Archiver {
        let url = url::Url::from_directory_path(dir).unwrap();
        Archiver::from_url(url.as_str())
            .expect("archiver")
            .with_delete_grace(grace)
    }

    fn archived(tier: &Path, parts: &[&str]) -> Option<Vec<u8>> {
        let mut path = tier.join("default");
        for part in parts {
            path = path.join(part);
        }
        std::fs::read(path).ok()
    }

    fn archived_tables(tier: &Path) -> BTreeSet<(u64, u64)> {
        std::fs::read_dir(tier.join("default").join("index"))
            .map(|entries| {
                entries
                    .filter_map(|entry| parse_table_file_name(entry.ok()?.file_name().to_str()?))
                    .collect()
            })
            .unwrap_or_default()
    }

    fn data_bytes(segment: &ArchivableSegment) -> Vec<u8> {
        let data_len = SealedIndex::open(&segment.path.with_extension("idx"), segment.base)
            .unwrap()
            .data_len();
        let mut bytes = std::fs::read(&segment.path).unwrap();
        bytes.truncate(data_len as usize);
        bytes
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn each_final_segment_is_committed_by_its_summary() {
        let data = tempfile::tempdir().unwrap();
        let tier = tempfile::tempdir().unwrap();
        let engine = engine_in(data.path());
        seal_segments(&engine, "a", 3).await;

        let report = archiver_for(tier.path(), DELETE_GRACE)
            .run_archive_pass("default", &engine)
            .await
            .expect("archive pass");
        let segments = engine.archivable_segments();
        assert_eq!(report.segments, segments.len());
        assert_eq!(report.archived_end, segments.last().unwrap().end);

        for segment in &segments {
            let name = |extension: &str| format!("{:020}.{extension}", segment.base);
            let bytes = data_bytes(segment);
            assert_eq!(
                archived(tier.path(), &["segments", &name("seg")]).unwrap(),
                bytes
            );
            assert_eq!(
                archived(tier.path(), &["segments", &name("idx")]).unwrap(),
                std::fs::read(segment.path.with_extension("idx")).unwrap()
            );
            let summary: SegmentSummary = serde_json::from_slice(
                &archived(tier.path(), &["segments", &name("sum")]).unwrap(),
            )
            .unwrap();
            assert_eq!(
                summary,
                SegmentSummary {
                    base: segment.base,
                    end: segment.end,
                    data_len: bytes.len() as u64,
                    blake3: blake3::hash(&bytes).to_hex().to_string(),
                }
            );
            // No merge has run, so each segment's own key table went up.
            assert_eq!(
                archived(
                    tier.path(),
                    &["index", &table_file_name(segment.base, segment.end)]
                )
                .unwrap(),
                std::fs::read(segment.path.with_extension("keys")).unwrap()
            );
        }

        // Nothing of the active segment, nor of anything not final.
        let active = crate::segment::list_segment_files(data.path()).unwrap();
        let active = *active.last().unwrap();
        assert!(archived(tier.path(), &["segments", &format!("{active:020}.sum")]).is_none());
    }

    #[test]
    fn a_segment_ending_at_the_watermark_is_not_final() {
        let data = tempfile::tempdir().expect("data dir");
        let engine =
            EventStoreEngine::create_with_options(data.path(), SEGMENT_SIZE).expect("engine");
        engine
            .append(AppendRequest {
                condition: None,
                events: vec![make_event("a", 0)],
            })
            .expect("append");
        // Checkpoints alone until one no longer fits: the segment seals
        // with control records after its last event, and the new active
        // segment holds no event, so the watermark sits at the sealed
        // segment's end.
        let segments = || crate::segment::list_segment_files(data.path()).expect("list");
        while segments().len() < 2 {
            engine.persist_watermark_checkpoint().expect("checkpoint");
        }
        let sealed_end = segments()[1];
        assert_eq!(engine.head().0, sealed_end);
        assert!(
            engine.archivable_segments().is_empty(),
            "a segment ending at the watermark can still be reopened"
        );

        engine
            .append(AppendRequest {
                condition: None,
                events: vec![make_event("a", 1)],
            })
            .expect("append");
        let archivable = engine.archivable_segments();
        assert_eq!(archivable.len(), 1);
        assert_eq!(archivable[0].end, sealed_end);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn only_the_data_bytes_of_an_untrimmed_segment_are_archived() {
        let data = tempfile::tempdir().unwrap();
        let tier = tempfile::tempdir().unwrap();
        let engine = engine_in(data.path());
        seal_segments(&engine, "a", 1).await;
        // What a crash between rotation and the seal's sync leaves: the
        // sealed file still at its preallocated length.
        let segment = engine.archivable_segments().remove(0);
        let bytes = std::fs::read(&segment.path).unwrap();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&segment.path)
            .and_then(|file| file.set_len(SEGMENT_SIZE * 2))
            .unwrap();

        archiver_for(tier.path(), DELETE_GRACE)
            .run_archive_pass("default", &engine)
            .await
            .expect("archive pass");
        let name = format!("{:020}", segment.base);
        assert_eq!(
            archived(tier.path(), &["segments", &format!("{name}.seg")]).unwrap(),
            bytes
        );
        let summary: SegmentSummary = serde_json::from_slice(
            &archived(tier.path(), &["segments", &format!("{name}.sum")]).unwrap(),
        )
        .unwrap();
        assert_eq!(summary.data_len, bytes.len() as u64);
        assert_eq!(summary.blake3, blake3::hash(&bytes).to_hex().to_string());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn passes_resume_where_the_archive_ends_on_any_leader() {
        let data = tempfile::tempdir().unwrap();
        let tier = tempfile::tempdir().unwrap();
        let engine = engine_in(data.path());
        seal_segments(&engine, "a", 2).await;

        let leader = archiver_for(tier.path(), DELETE_GRACE);
        let first = leader.run_archive_pass("default", &engine).await.unwrap();
        assert!(first.segments >= 2);
        let again = leader.run_archive_pass("default", &engine).await.unwrap();
        assert_eq!((again.segments, again.tables), (0, 0));
        assert_eq!(again.archived_end, first.archived_end);

        // A new leader lists the archive and archives only the delta.
        seal_segments(&engine, "a", first.segments + 2).await;
        let next_leader = archiver_for(tier.path(), DELETE_GRACE);
        let delta = next_leader
            .run_archive_pass("default", &engine)
            .await
            .unwrap();
        let segments = engine.archivable_segments();
        assert_eq!(first.segments + delta.segments, segments.len());
        assert_eq!(delta.archived_end, segments.last().unwrap().end);

        // The old leader, whose view is stale, finds the delta archived and
        // writes nothing.
        let stale = leader.run_archive_pass("default", &engine).await.unwrap();
        assert_eq!((stale.segments, stale.tables), (0, 0));
        assert_eq!(stale.archived_end, delta.archived_end);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_different_log_under_the_same_prefix_is_refused() {
        let tier = tempfile::tempdir().unwrap();
        let first = tempfile::tempdir().unwrap();
        let first_engine = engine_in(first.path());
        seal_segments(&first_engine, "first", 2).await;
        archiver_for(tier.path(), DELETE_GRACE)
            .run_archive_pass("default", &first_engine)
            .await
            .unwrap();
        let before = archived(tier.path(), &["segments", &format!("{:020}.seg", 0)]).unwrap();

        let second = tempfile::tempdir().unwrap();
        let second_engine = engine_in(second.path());
        seal_segments(&second_engine, "second", 2).await;
        let error = archiver_for(tier.path(), DELETE_GRACE)
            .run_archive_pass("default", &second_engine)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Corrupted { .. }), "{error}");
        assert_eq!(
            archived(tier.path(), &["segments", &format!("{:020}.seg", 0)]).unwrap(),
            before
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_summary_written_by_another_log_stops_the_pass() {
        let data = tempfile::tempdir().unwrap();
        let tier = tempfile::tempdir().unwrap();
        let engine = engine_in(data.path());
        seal_segments(&engine, "a", 1).await;
        let leader = archiver_for(tier.path(), DELETE_GRACE);
        leader.run_archive_pass("default", &engine).await.unwrap();

        // Another log commits the next segment first.
        seal_segments(&engine, "a", 2).await;
        let next = engine.archivable_segments()[1].clone();
        let foreign = SegmentSummary {
            base: next.base,
            end: next.end + 1,
            data_len: 1,
            blake3: "00".into(),
        };
        let path = tier.path().join("default").join("segments");
        std::fs::write(
            path.join(format!("{:020}.sum", next.base)),
            serde_json::to_vec(&foreign).unwrap(),
        )
        .unwrap();

        let error = leader
            .run_archive_pass("default", &engine)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Corrupted { .. }), "{error}");
        assert!(!path.join(format!("{:020}.seg", next.base)).exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn merged_tables_follow_their_segments_and_supersede_their_parts() {
        let data = tempfile::tempdir().unwrap();
        let tier = tempfile::tempdir().unwrap();
        let engine = engine_in(data.path());
        seal_segments(&engine, "a", 3).await;
        archiver_for(tier.path(), DELETE_GRACE)
            .run_archive_pass("default", &engine)
            .await
            .unwrap();

        seal_segments(&engine, "a", 9).await;
        engine.merge_index().unwrap();
        let merged = engine.archivable_tables().unwrap();
        assert_eq!(merged.len(), 1, "{merged:?}");
        let merged = &merged[0];
        let segments = engine.archivable_segments();
        let covered = |range: &(u64, u64)| range.0 >= merged.base && range.1 <= merged.end;

        // Within the grace period, the parts it replaces stay.
        let leader = archiver_for(tier.path(), DELETE_GRACE);
        let report = leader.run_archive_pass("default", &engine).await.unwrap();
        assert_eq!(report.deleted, 0);
        let tables = archived_tables(tier.path());
        assert!(tables.contains(&(merged.base, merged.end)));
        assert!(tables.iter().filter(|range| covered(range)).count() > 1);
        assert_eq!(
            archived(
                tier.path(),
                &["index", &table_file_name(merged.base, merged.end)]
            )
            .unwrap(),
            std::fs::read(&merged.path).unwrap()
        );

        // After it, only the merged table and the tables of segments it
        // does not cover remain.
        let report = archiver_for(tier.path(), Duration::ZERO)
            .run_archive_pass("default", &engine)
            .await
            .unwrap();
        assert!(report.deleted > 0);
        let mut expected: BTreeSet<(u64, u64)> = segments
            .iter()
            .map(|segment| (segment.base, segment.end))
            .filter(|range| !covered(range))
            .collect();
        expected.insert((merged.base, merged.end));
        assert_eq!(archived_tables(tier.path()), expected);
    }
}
