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
//! in a bucket under object lock: what an interrupted attempt left is
//! checked against the local copy, not written again. A segment is archived
//! once its summary exists. Summaries are written only if absent and
//! segments are archived in log order, so the archived segments are always
//! a prefix of the log. A writer that finds a summary already there
//! compares it with its own: equal is two leaders racing, different means
//! two logs share the prefix, and the pass fails without writing anything
//! more.
//!
//! That needs a store that creates an object only if it is absent. A leader
//! checks it before it archives: it creates [`CONDITIONAL_WRITE_CHECK`]
//! under the URL, if absent, then asks to create it again, which must be
//! refused. A store that accepts gets nothing archived. The object is never
//! deleted: object lock may forbid it, and there is only ever the one.
//!
//! The key tables are a segment's own table while no merged table covers
//! it, and the merged tables of ADR-0010, uploaded once the segments they
//! cover are archived. Both are deterministic, so a table's name fixes its
//! bytes. A table inside another archived table's range is superseded and
//! deleted after [`DELETE_GRACE`]. Under a GCS retention policy or an Azure
//! immutability policy the store refuses, and the table stays. On a
//! versioned S3 bucket under object lock the delete succeeds but only adds
//! a delete marker: the locked version stays, and is billed, until its
//! retention ends and a lifecycle rule expires noncurrent versions.
//!
//! An object up to 8 MiB goes up in one request, a larger one in 8 MiB
//! parts, at most four in flight. A multipart upload that fails, or whose
//! pass is cancelled, is aborted, so the store drops the parts it holds. One
//! cut short by a crash or a lost connection stays open, invisible to a
//! listing, and the store bills its parts until they are removed. Give the
//! bucket a lifecycle rule that does that: on S3, an
//! `AbortIncompleteMultipartUpload` rule with `DaysAfterInitiation` of a day
//! or so; on GCS, the `AbortIncompleteMultipartUpload` action with an `age`
//! condition. Azure discards uncommitted blocks on its own after a week.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::TryStreamExt;
use object_store::path::Path as ObjectPath;
use object_store::{MultipartUpload, ObjectStore, ObjectStoreExt, PutMode, PutPayload};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt};

use crate::criteria::{Criterion, SourcingCondition};
use crate::error::Error;
use crate::event::Position;
use crate::segment::merge::{parse_table_file_name, table_file_name};
use crate::segment::sealed_index::SealedIndex;
use crate::store::{ArchivableSegment, EventStoreEngine};

/// Size of each part of a multipart upload. A file up to this size goes up
/// in one request.
const PART_SIZE: usize = 8 * 1024 * 1024;

/// Most parts of one upload in flight at once. Bounds what an upload holds
/// in memory (these parts and the one being read), and keeps parts from
/// sharing the uplink so thinly that each outlasts its request timeout.
const PARTS_IN_FLIGHT: usize = 4;

/// How long a superseded key table stays in the archive after the table
/// that covers it was uploaded, so a node that listed the archive just
/// before can still download what it chose.
pub const DELETE_GRACE: Duration = Duration::from_secs(600);

/// The object a leader creates, under the archive's URL and outside every
/// context, to check that the store creates an object only if it is
/// absent. Never deleted, so there is only ever the one.
pub const CONDITIONAL_WRITE_CHECK: &str = "conditional-write-check.txt";

const CONDITIONAL_WRITE_CHECK_BODY: &[u8] = b"KronosDB creates this object, then asks the \
store to create it again only if it is absent, which the store must refuse: committing an \
archived segment by its summary relies on that. It is never deleted, as object lock may \
forbid it.\n";

/// Longest wait for the next archive pass of a context after its passes
/// failed.
pub const MAX_PASS_BACKOFF: Duration = Duration::from_secs(300);

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
    /// This node stopped leading during the pass, which ended there.
    pub stopped: bool,
}

#[derive(Debug, Default)]
pub struct EvictionPassReport {
    /// Segments evicted.
    pub evicted: usize,
    /// Bytes their files held.
    pub freed: u64,
    /// Bytes of sealed segments still on disk.
    pub local_bytes: u64,
    /// Contexts whose oldest local segment is not archived yet.
    pub waiting: Vec<String>,
    /// Contexts whose oldest local segment differs from its archived copy:
    /// real divergence, which no retry heals. Their segments are kept.
    pub diverged: Vec<String>,
    /// Contexts the pass could not finish, and why: the archive or the disk
    /// failed. The next pass tries them again.
    pub failed: Vec<(String, Error)>,
}

/// What the archive loops last did on this node (ADR-0011), for metrics:
/// archiving per context while this node leads, and eviction.
#[derive(Debug, Default)]
pub struct ArchiveStatus {
    contexts: std::sync::Mutex<BTreeMap<String, ContextArchiveStatus>>,
    max_local_bytes: AtomicU64,
    evicted_segments: AtomicU64,
    evicted_bytes: AtomicU64,
    eviction_failures: AtomicU64,
    diverged: AtomicU64,
}

/// Archiving of one context, as this node last saw it while leading.
#[derive(Debug, Clone, Default)]
pub struct ContextArchiveStatus {
    /// End of the archived prefix.
    pub archived_end: u64,
    /// When a pass last succeeded, in milliseconds since the epoch; 0 for
    /// never.
    pub last_success_ms: u64,
    /// Passes that failed.
    pub failures: u64,
}

/// Eviction on this node.
#[derive(Debug, Clone, Copy, Default)]
pub struct EvictionStatus {
    /// The cap on sealed segment bytes; 0 for none.
    pub max_local_bytes: u64,
    /// Segments evicted since startup.
    pub evicted_segments: u64,
    /// Bytes those held.
    pub evicted_bytes: u64,
    /// Contexts an eviction pass could not finish, one per context and
    /// pass: their archive or disk failed.
    pub failures: u64,
    /// Contexts an eviction pass found diverged, one per context and pass:
    /// a segment unlike its archived copy, which no retry heals. Alert on
    /// any increase.
    pub diverged: u64,
}

impl ArchiveStatus {
    fn context(&self, context: &str, update: impl FnOnce(&mut ContextArchiveStatus)) {
        let mut contexts = self.contexts.lock().unwrap_or_else(|e| e.into_inner());
        update(contexts.entry(context.to_string()).or_default());
    }

    /// A pass over `context` succeeded.
    pub fn archived(&self, context: &str, archived_end: u64) {
        self.context(context, |status| {
            status.archived_end = archived_end;
            status.last_success_ms = now_millis().max(0) as u64;
        });
    }

    /// A pass over `context` failed.
    pub fn archive_failed(&self, context: &str) {
        self.context(context, |status| status.failures += 1);
    }

    /// This node stopped leading: another archives now.
    pub fn not_archiving(&self) {
        self.contexts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    /// The contexts this node archives.
    pub fn contexts(&self) -> Vec<(String, ContextArchiveStatus)> {
        self.contexts
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .map(|(name, status)| (name.clone(), status.clone()))
            .collect()
    }

    pub fn set_max_local_bytes(&self, max_local_bytes: u64) {
        self.max_local_bytes
            .store(max_local_bytes, Ordering::Relaxed);
    }

    /// An eviction pass ran.
    pub fn evicted(&self, report: &EvictionPassReport) {
        self.evicted_segments
            .fetch_add(report.evicted as u64, Ordering::Relaxed);
        self.evicted_bytes
            .fetch_add(report.freed, Ordering::Relaxed);
        self.eviction_failures
            .fetch_add(report.failed.len() as u64, Ordering::Relaxed);
        self.diverged
            .fetch_add(report.diverged.len() as u64, Ordering::Relaxed);
    }

    pub fn eviction(&self) -> EvictionStatus {
        EvictionStatus {
            max_local_bytes: self.max_local_bytes.load(Ordering::Relaxed),
            evicted_segments: self.evicted_segments.load(Ordering::Relaxed),
            evicted_bytes: self.evicted_bytes.load(Ordering::Relaxed),
            failures: self.eviction_failures.load(Ordering::Relaxed),
            diverged: self.diverged.load(Ordering::Relaxed),
        }
    }
}

/// What the archiver knows of one context's archive. Built by listing it
/// the first time a pass runs, then kept up to date by the passes.
struct ContextArchive {
    /// End of the archived prefix: the base of the next segment to archive.
    archived_end: u64,
    /// Archived key tables by range, with when this node first saw each:
    /// when it listed the archive, or uploaded the table. The grace before
    /// a superseded table is deleted runs from then, on this node's
    /// monotonic clock, not from the store's `last_modified`: a store clock
    /// ahead of this one would make a table uploaded a moment ago look old.
    tables: BTreeMap<(u64, u64), Instant>,
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

/// What an archiver knows while its node leads.
#[derive(Default)]
struct Leading {
    /// Whether the store was found to create objects only if absent.
    checked: bool,
    contexts: HashMap<String, ContextArchive>,
}

pub struct Archiver {
    store: Box<dyn ObjectStore>,
    prefix: ObjectPath,
    grace: Duration,
    leading: tokio::sync::Mutex<Leading>,
}

/// When the next archive pass of one context may run: at the next
/// interval while passes succeed; after failures, two intervals later, then
/// four, and so on up to [`MAX_PASS_BACKOFF`]. So a pass that keeps failing
/// does not list the archive and hash segments again every interval.
#[derive(Debug, Default)]
pub struct PassBackoff {
    failures: u32,
    until: Option<Instant>,
}

impl PassBackoff {
    pub fn ready(&self, now: Instant) -> bool {
        self.until.is_none_or(|until| now >= until)
    }

    /// A pass failed at `now`. Returns how long the next waits.
    pub fn failed(&mut self, now: Instant, interval: Duration) -> Duration {
        self.failures = self.failures.saturating_add(1);
        let wait = interval
            .saturating_mul(1 << self.failures.min(16))
            .min(MAX_PASS_BACKOFF)
            .max(interval);
        self.until = Some(now + wait);
        wait
    }

    pub fn succeeded(&mut self) {
        *self = Self::default();
    }
}

impl Archiver {
    /// Builds an archiver from an object-store URL. `file://` and `memory://`
    /// need no credentials; `s3://` reads the standard AWS environment, or
    /// uses the instance's role; `gs://` and `az://` their providers'
    /// environments.
    pub fn from_url(raw_url: &str) -> Result<Self, Error> {
        let (store, prefix) = crate::remote::open_store(raw_url)?;
        Ok(Self::new(store, prefix))
    }

    fn new(store: Box<dyn ObjectStore>, prefix: ObjectPath) -> Self {
        Self {
            store,
            prefix,
            grace: DELETE_GRACE,
            leading: tokio::sync::Mutex::new(Leading::default()),
        }
    }

    /// Sets how long superseded tables stay before they are deleted.
    pub fn with_delete_grace(mut self, grace: Duration) -> Self {
        self.grace = grace;
        self
    }

    /// Drops what this archiver knows, so the next pass checks the store
    /// and lists the archive again. Called when this node stops being
    /// leader: another node may write in the meantime.
    pub async fn forget(&self) {
        *self.leading.lock().await = Leading::default();
    }

    /// Checks that the store creates an object only if it is absent, which
    /// committing a segment by its summary relies on: a store that ignores
    /// If-None-Match lets one leader's summary replace another's unseen.
    /// Creates [`CONDITIONAL_WRITE_CHECK`] if it is absent, then creates it
    /// again, which the store must refuse.
    async fn check_conditional_writes(&self) -> Result<(), Error> {
        let object = self.prefix.clone().join(CONDITIONAL_WRITE_CHECK);
        let create = || {
            self.store.put_opts(
                &object,
                PutPayload::from_static(CONDITIONAL_WRITE_CHECK_BODY),
                PutMode::Create.into(),
            )
        };
        let refuse = |why: String| {
            let message = format!(
                "not archiving: the archive {why}. Committing a segment by its summary needs \
                 an object created only if it is absent (If-None-Match: * on S3)"
            );
            tracing::error!(%object, "{message}");
            Error::Rejected { message }
        };
        match create().await {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {}
            Err(
                error @ (object_store::Error::NotImplemented { .. }
                | object_store::Error::NotSupported { .. }),
            ) => {
                return Err(refuse(format!(
                    "cannot create an object only if it is absent ({error})"
                )));
            }
            Err(error) => return Err(tier_err(error)),
        }
        match create().await {
            Err(object_store::Error::AlreadyExists { .. }) => Ok(()),
            Ok(_) => Err(refuse(format!(
                "replaced {object} when asked to create it only if absent: it ignores \
                 conditional writes"
            ))),
            Err(error) => Err(tier_err(error)),
        }
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
        self.run_archive_pass_while(context, engine, || true).await
    }

    /// [`run_archive_pass`] while `still_leading` says this node leads. It
    /// is asked before each segment, before the tables and before the
    /// deletes; once it says no, the pass ends there, `stopped`. So a node
    /// deposed during a long pass stops writing to the archive within a
    /// segment's upload, not at the end of the backlog.
    ///
    /// [`run_archive_pass`]: Self::run_archive_pass
    pub async fn run_archive_pass_while(
        &self,
        context: &str,
        engine: &EventStoreEngine,
        still_leading: impl Fn() -> bool + Send + Sync,
    ) -> Result<ArchivePassReport, Error> {
        let mut report = ArchivePassReport::default();
        let mut leading = self.leading.lock().await;
        if !leading.checked {
            self.check_conditional_writes().await?;
            leading.checked = true;
        }
        if !leading.contexts.contains_key(context) {
            let state = self.open_context(context, engine).await?;
            leading.contexts.insert(context.to_string(), state);
        }
        let state = leading.contexts.get_mut(context).expect("inserted above");
        report.archived_end = state.archived_end;

        for segment in engine.archivable_segments() {
            if segment.base < state.archived_end {
                continue;
            }
            if !still_leading() {
                report.stopped = true;
                return Ok(report);
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
            report.archived_end = state.archived_end;
        }
        if !still_leading() {
            report.stopped = true;
            return Ok(report);
        }
        report.tables += self.archive_tables(context, engine, state).await?;
        if !still_leading() {
            report.stopped = true;
            return Ok(report);
        }
        report.deleted = self.delete_superseded(context, state).await;
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
                tables.insert(range, Instant::now());
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
        // An attempt that died before its summary leaves the other objects
        // behind. They are checked, never written again: a bucket under a
        // retention policy refuses the overwrite, and under S3 object lock
        // it keeps another locked version.
        let seg_object = self.segment_object(context, segment.base, "seg");
        let blake3 = match self
            .upload_file(&segment.path, data_len, &seg_object)
            .await?
        {
            Upload::Written(blake3) => blake3,
            Upload::Present(size) => {
                self.check_archived_data(context, segment, data_len, &seg_object, size)
                    .await?
            }
        };
        self.put_once(
            &self.segment_object(context, segment.base, "idx"),
            idx_bytes,
        )
        .await?;

        // Its own key table, unless a merge has already replaced it with a
        // merged table, which goes up once the archive covers its range.
        let range = (segment.base, segment.end);
        let keys_path = segment.path.with_extension("keys");
        if !state.tables.contains_key(&range)
            && let Some(len) = file_len(&keys_path).await?
        {
            let object = self.table_object(context, segment.base, segment.end);
            match self.upload_table(&keys_path, len, &object).await {
                Ok(()) => {
                    state.tables.insert(range, Instant::now());
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
    /// and are not archived yet. A table inside a wider archived one is
    /// skipped: the archive superseded it already, on a leader whose merges
    /// ran ahead of this node's, and uploading it again would only have it
    /// deleted again, and under object lock leave another locked version.
    /// Returns how many it uploaded.
    async fn archive_tables(
        &self,
        context: &str,
        engine: &EventStoreEngine,
        state: &mut ContextArchive,
    ) -> Result<usize, Error> {
        let mut uploaded = 0;
        for table in engine.archivable_tables()? {
            let range = (table.base, table.end);
            if table.end > state.archived_end
                || state.tables.contains_key(&range)
                || superseded(&state.tables, range)
            {
                continue;
            }
            // A merge may supersede and delete it at any moment; its
            // successor is uploaded on a later pass.
            let Some(len) = file_len(&table.path).await? else {
                continue;
            };
            let object = self.table_object(context, table.base, table.end);
            match self.upload_table(&table.path, len, &object).await {
                Ok(()) => {
                    state.tables.insert(range, Instant::now());
                    uploaded += 1;
                }
                Err(error) if is_not_found(&error) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(uploaded)
    }

    /// Deletes archived tables whose range lies inside another archived
    /// table's, once this node has known that table for the grace period.
    /// Best effort: a table the store refuses to delete (a retention
    /// policy, or credentials without delete) is kept and not tried again
    /// while this node leads; one whose delete failed otherwise, as on a
    /// 503, is tried again next pass. Returns how many it deleted.
    async fn delete_superseded(&self, context: &str, state: &mut ContextArchive) -> usize {
        // Tables are aligned blocks: nested or disjoint. In order of base,
        // widest first, a table is inside another exactly when an earlier
        // one reaches at least as far.
        let mut ranges: Vec<((u64, u64), Instant)> = state
            .tables
            .iter()
            .map(|(&range, &at)| (range, at))
            .collect();
        ranges.sort_by_key(|&((base, end), _)| (base, std::cmp::Reverse(end)));
        let now = Instant::now();
        let mut cover: Option<(u64, Instant)> = None;
        let mut doomed = Vec::new();
        for ((base, end), at) in ranges {
            match cover {
                Some((cover_end, cover_at)) if end <= cover_end => {
                    if now.duration_since(cover_at) >= self.grace
                        && !state.kept.contains(&(base, end))
                    {
                        doomed.push((base, end));
                    }
                }
                _ => cover = Some((end, at)),
            }
        }

        let mut deleted = 0;
        let mut failed = None;
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
                Err(error) if refuses_deletes(&error) => {
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
                Err(error) => {
                    failed.get_or_insert((error, 0)).1 += 1;
                }
            }
        }
        if let Some((error, tables)) = failed {
            tracing::warn!(
                context,
                %error,
                tables,
                "could not delete superseded key tables; retrying next pass"
            );
        }
        deleted
    }

    /// Evicts the oldest archived segments of `engines` while their files
    /// on this node's disk hold more than `max_local_bytes`: every file a
    /// context keeps counts, bodies, the active segment, locators and key
    /// tables, though only sealed bodies can be evicted. Across contexts,
    /// the oldest segment goes first. Runs on every node, each under its own
    /// cap.
    ///
    /// A segment is evicted only once its summary is in the archive and the
    /// blake3 of this node's data bytes equals the summary's. A mismatch is
    /// real divergence: that context's segments are kept, and it is
    /// reported in `diverged`. A context that fails otherwise, its archive
    /// or its disk, is reported in `failed`. Either way the pass goes on with
    /// the other contexts, so the cap still holds for them.
    pub async fn run_eviction_pass(
        &self,
        engines: &[(String, Arc<EventStoreEngine>)],
        max_local_bytes: u64,
    ) -> EvictionPassReport {
        let mut report = EvictionPassReport::default();
        let mut local = 0u64;
        let mut open = Vec::new();
        for (context, engine) in engines {
            match local_bytes(engine).await {
                Ok(bytes) => {
                    local += bytes;
                    open.push(EvictionCandidate {
                        context,
                        engine,
                        oldest: None,
                    });
                }
                Err(error) => report.failed.push((context.clone(), error)),
            }
        }
        while local > max_local_bytes {
            // Each context's oldest final segment with a body, and its age,
            // kept from one round to the next until it is evicted.
            let mut i = 0;
            while i < open.len() {
                if open[i].oldest.is_none() {
                    match oldest_body(open[i].engine).await {
                        Ok(Some(oldest)) => open[i].oldest = Some(oldest),
                        Ok(None) => {
                            open.remove(i);
                            continue;
                        }
                        Err(error) => {
                            report.failed.push((open[i].context.clone(), error));
                            open.remove(i);
                            continue;
                        }
                    }
                }
                i += 1;
            }
            let Some(i) = (0..open.len()).min_by_key(|&i| open[i].oldest.as_ref().map(|o| o.age))
            else {
                break;
            };
            let candidate = &mut open[i];
            let oldest = candidate.oldest.take().expect("found above");
            match self
                .evict(candidate.context, candidate.engine, &oldest.segment)
                .await
            {
                Ok(Eviction::Evicted) => {
                    local = local.saturating_sub(oldest.bytes);
                    report.evicted += 1;
                    report.freed += oldest.bytes;
                }
                Ok(Eviction::NotArchived) => {
                    report.waiting.push(candidate.context.clone());
                    open.remove(i);
                }
                // Its seal is in flight or it was evicted meanwhile: the
                // next pass looks again.
                Ok(Eviction::Later) => {
                    open.remove(i);
                }
                Ok(Eviction::Diverged(error)) => {
                    tracing::error!(
                        context = candidate.context,
                        %error,
                        "a segment differs from its archived copy; keeping this context's \
                         segments"
                    );
                    report.diverged.push(candidate.context.clone());
                    open.remove(i);
                }
                Err(error) => {
                    report.failed.push((candidate.context.clone(), error));
                    open.remove(i);
                }
            }
        }
        report.local_bytes = local;
        report
    }

    /// Evicts `segment` of `engine` if the archive holds the same bytes.
    async fn evict(
        &self,
        context: &str,
        engine: &Arc<EventStoreEngine>,
        segment: &ArchivableSegment,
    ) -> Result<Eviction, Error> {
        let Some(archived) = self.summary(context, segment.base).await? else {
            return Ok(Eviction::NotArchived);
        };
        let Some(own) = local_summary(segment).await? else {
            return Ok(Eviction::Later);
        };
        if own != archived {
            return Ok(Eviction::Diverged(diverged(context, &own, &archived)));
        }
        let engine = Arc::clone(engine);
        let base = segment.base;
        tokio::task::spawn_blocking(move || engine.evict_segment(base))
            .await
            .map_err(|error| Error::Internal {
                message: format!("evicting segment {base}: {error}"),
            })??;
        Ok(Eviction::Evicted)
    }

    /// Uploads a key table unless the archive holds it already. A table's
    /// name fixes its bytes, so one already there of the same size is it.
    async fn upload_table(&self, path: &Path, len: u64, object: &ObjectPath) -> Result<(), Error> {
        match self.upload_file(path, len, object).await? {
            Upload::Written(_) => Ok(()),
            Upload::Present(size) => same_size(object, size, len),
        }
    }

    /// The blake3 of the segment the archive holds at `object`, `size`
    /// bytes, which must be this node's `data_len` bytes of `segment`: an
    /// earlier attempt's upload, or another leader's. Anything else was
    /// written by another log, and the pass fails before writing more.
    async fn check_archived_data(
        &self,
        context: &str,
        segment: &ArchivableSegment,
        data_len: u64,
        object: &ObjectPath,
        size: u64,
    ) -> Result<String, Error> {
        let foreign = |what: String| Error::Corrupted {
            message: format!(
                "the archive of context {context} holds data for segment {} that {what}, and \
                 no summary commits it. Does another cluster archive to the same prefix? \
                 Nothing is overwritten: remove {object} if it is not this log's",
                segment.base
            ),
        };
        if size != data_len {
            return Err(foreign(format!(
                "is {size} bytes long, not the {data_len} of this node's"
            )));
        }
        let local = hash_file(&segment.path, data_len).await?;
        let mut archived = blake3::Hasher::new();
        let mut stream = self
            .store
            .get(object)
            .await
            .map_err(tier_err)?
            .into_stream();
        while let Some(chunk) = stream.try_next().await.map_err(tier_err)? {
            archived.update(&chunk);
        }
        if archived.finalize().to_hex().as_str() != local {
            return Err(foreign("differs from this node's".to_string()));
        }
        Ok(local)
    }

    /// Writes `bytes` to `object` unless an object is there already, which
    /// must then be as long: names fix bytes, and nothing is overwritten.
    async fn put_once(&self, object: &ObjectPath, bytes: Vec<u8>) -> Result<(), Error> {
        let len = bytes.len() as u64;
        match self
            .store
            .put_opts(object, PutPayload::from(bytes), PutMode::Create.into())
            .await
        {
            Ok(_) | Err(object_store::Error::AlreadyExists { .. }) => {
                self.verify_size(object, len).await
            }
            Err(error) => Err(tier_err(error)),
        }
    }

    /// Streams the first `len` bytes of `path` to `object`, as [`upload`]
    /// does. A file shorter than `len` is `Error::Corrupted`: its index says
    /// it holds more.
    ///
    /// [`upload`]: Self::upload
    async fn upload_file(
        &self,
        path: &Path,
        len: u64,
        object: &ObjectPath,
    ) -> Result<Upload, Error> {
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
        self.upload(file, len, object, &path.display().to_string())
            .await
    }

    /// Streams `len` bytes of `data`, named `name` in errors, to `object`
    /// unless an object is there already, and verifies the stored size.
    /// What fits in one part goes up in one request, created only if absent;
    /// the rest in parts of [`PART_SIZE`], at most [`PARTS_IN_FLIGHT`] at a
    /// time, read as earlier ones finish. A multipart upload cannot be made
    /// conditional, so the object is looked up first. An upload that fails,
    /// for any reason, is aborted.
    async fn upload(
        &self,
        mut data: impl AsyncRead + Unpin,
        len: u64,
        object: &ObjectPath,
        name: &str,
    ) -> Result<Upload, Error> {
        let ended = |at: u64| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("{name} ended at {at} bytes while uploading its {len}"),
            ))
        };
        let present = |head: object_store::Result<object_store::ObjectMeta>| match head {
            Ok(meta) => Ok(Some(Upload::Present(meta.size))),
            Err(object_store::Error::NotFound { .. }) => Ok(None),
            Err(error) => Err(tier_err(error)),
        };
        if len <= PART_SIZE as u64 {
            let mut bytes = vec![0u8; len as usize];
            let read = read_full(&mut data, &mut bytes).await?;
            if read as u64 != len {
                return Err(ended(read as u64));
            }
            let blake3 = blake3::hash(&bytes).to_hex().to_string();
            match self
                .store
                .put_opts(object, PutPayload::from(bytes), PutMode::Create.into())
                .await
            {
                Ok(_) => {}
                Err(object_store::Error::AlreadyExists { .. }) => {
                    if let Some(present) = present(self.store.head(object).await)? {
                        return Ok(present);
                    }
                }
                Err(error) => return Err(tier_err(error)),
            }
            self.verify_size(object, len).await?;
            return Ok(Upload::Written(blake3));
        }

        if let Some(present) = present(self.store.head(object).await)? {
            return Ok(present);
        }
        let mut parts = Parts::start(self.store.as_ref(), object).await?;
        let sent = async {
            let mut hasher = blake3::Hasher::new();
            let mut total = 0u64;
            while total < len {
                let mut part = vec![0u8; (len - total).min(PART_SIZE as u64) as usize];
                let read = read_full(&mut data, &mut part).await?;
                if read < part.len() {
                    // Completing it would store a short object.
                    return Err(ended(total + read as u64));
                }
                hasher.update(&part);
                total += read as u64;
                parts.put(PutPayload::from(part)).await.map_err(tier_err)?;
            }
            Ok(hasher)
        }
        .await;
        let hasher = match sent {
            Ok(hasher) => hasher,
            Err(error) => {
                parts.abort().await;
                return Err(error);
            }
        };
        parts.complete().await.map_err(tier_err)?;
        self.verify_size(object, len).await?;
        Ok(Upload::Written(hasher.finalize().to_hex().to_string()))
    }

    /// Checks the store holds `len` bytes at `object`, just written.
    async fn verify_size(&self, object: &ObjectPath, len: u64) -> Result<(), Error> {
        let head = self.store.head(object).await.map_err(tier_err)?;
        same_size(object, head.size, len)
    }
}

/// What [`Archiver::upload`] did.
#[derive(Debug)]
enum Upload {
    /// It wrote the object, whose bytes have this blake3.
    Written(String),
    /// An object of this size was there already, so it wrote nothing.
    Present(u64),
}

fn same_size(object: &ObjectPath, size: u64, len: u64) -> Result<(), Error> {
    if size != len {
        return Err(Error::Corrupted {
            message: format!(
                "the archive holds {size} bytes at {object}, where this node has {len}"
            ),
        });
    }
    Ok(())
}

/// One context in an eviction pass.
struct EvictionCandidate<'a> {
    context: &'a String,
    engine: &'a Arc<EventStoreEngine>,
    /// Its oldest final segment with a body, once looked up.
    oldest: Option<OldestBody>,
}

struct OldestBody {
    segment: ArchivableSegment,
    /// Bytes of its file.
    bytes: u64,
    /// When it was sealed, as far as this node can tell, in milliseconds
    /// since the epoch: the oldest across contexts is evicted first.
    age: i64,
}

enum Eviction {
    Evicted,
    /// Its summary is not in the archive yet.
    NotArchived,
    /// Its seal is in flight, or it was evicted meanwhile.
    Later,
    /// The archive holds other bytes for it.
    Diverged(Error),
}

/// The timestamp of the last event in `segment`, one record read from its
/// local body.
async fn last_event_timestamp(
    engine: &Arc<EventStoreEngine>,
    segment: &ArchivableSegment,
) -> Option<i64> {
    let last = segment
        .end
        .checked_sub(1)
        .filter(|&last| last >= segment.base)?;
    let engine = Arc::clone(engine);
    let everything = SourcingCondition {
        criteria: vec![Criterion {
            names: vec![],
            tags: vec![],
        }],
    };
    let events =
        tokio::task::spawn_blocking(move || engine.source_internal(Position(last), &everything, 1))
            .await
            .ok()?
            .ok()?;
    events
        .first()
        .filter(|event| event.position.0 == last)
        .map(|event| event.timestamp)
}

/// Bytes of every file `engine` keeps on disk: what the cap counts.
async fn local_bytes(engine: &Arc<EventStoreEngine>) -> Result<u64, Error> {
    let engine = Arc::clone(engine);
    tokio::task::spawn_blocking(move || engine.local_bytes())
        .await
        .map_err(|error| Error::Internal {
            message: format!("counting local bytes: {error}"),
        })?
}

/// The oldest final segment of `engine` with a body, if there is one.
async fn oldest_body(engine: &Arc<EventStoreEngine>) -> Result<Option<OldestBody>, Error> {
    let evicted_end = engine.evicted_end();
    let Some(segment) = engine
        .archivable_segments()
        .into_iter()
        .find(|segment| segment.base >= evicted_end)
    else {
        return Ok(None);
    };
    let meta = match tokio::fs::metadata(&segment.path).await {
        Ok(meta) => meta,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    // The timestamp of its last event: when it was sealed, near enough,
    // and the same on every node. The file's time is not: on a node that
    // joined or caught up from the archive it is when the segment was
    // downloaded. Timestamps are the writers', so a writer's clock skews
    // only the order across contexts; each context still evicts its oldest
    // first. A segment whose last event cannot be read falls back on its
    // file's time.
    let age = match last_event_timestamp(engine, &segment).await {
        Some(timestamp) => timestamp,
        None => meta
            .modified()?
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_millis() as i64),
    };
    Ok(Some(OldestBody {
        segment,
        bytes: meta.len(),
        age,
    }))
}

/// A multipart upload with at most [`PARTS_IN_FLIGHT`] parts in flight.
/// Unless it completes, it is aborted: on a failure, and when dropped, as
/// when the pass driving it is cancelled. So a failed attempt leaves no
/// parts behind that the store goes on billing for; a crash still can (see
/// the module doc).
struct Parts {
    /// `None` once completed or aborted.
    upload: Option<Box<dyn MultipartUpload>>,
    in_flight: tokio::task::JoinSet<object_store::Result<()>>,
}

impl Parts {
    async fn start(store: &dyn ObjectStore, object: &ObjectPath) -> Result<Self, Error> {
        let upload = store.put_multipart(object).await.map_err(tier_err)?;
        Ok(Self {
            upload: Some(upload),
            in_flight: tokio::task::JoinSet::new(),
        })
    }

    /// Starts uploading `part` once fewer than [`PARTS_IN_FLIGHT`] are in
    /// flight.
    async fn put(&mut self, part: PutPayload) -> object_store::Result<()> {
        while self.in_flight.len() >= PARTS_IN_FLIGHT {
            self.next_done().await?;
        }
        let upload = self.upload.as_mut().expect("an unfinished upload");
        self.in_flight.spawn(upload.put_part(part));
        Ok(())
    }

    async fn next_done(&mut self) -> object_store::Result<()> {
        match self.in_flight.join_next().await {
            None => Ok(()),
            Some(Ok(result)) => result,
            Some(Err(error)) => Err(object_store::Error::Generic {
                store: "archive",
                source: Box::new(error),
            }),
        }
    }

    /// Waits for the parts in flight, then completes the upload.
    async fn complete(mut self) -> object_store::Result<()> {
        let completed = async {
            while !self.in_flight.is_empty() {
                self.next_done().await?;
            }
            let upload = self.upload.as_mut().expect("an unfinished upload");
            upload.complete().await
        }
        .await;
        match completed {
            Ok(_) => {
                self.upload = None;
                Ok(())
            }
            Err(error) => {
                self.abort().await;
                Err(error)
            }
        }
    }

    async fn abort(&mut self) {
        self.in_flight.shutdown().await;
        if let Some(mut upload) = self.upload.take() {
            abort_upload(upload.as_mut()).await;
        }
    }
}

impl Drop for Parts {
    fn drop(&mut self) {
        let Some(mut upload) = self.upload.take() else {
            return;
        };
        self.in_flight.abort_all();
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move { abort_upload(upload.as_mut()).await });
        }
    }
}

async fn abort_upload(upload: &mut dyn MultipartUpload) {
    if let Err(error) = upload.abort().await {
        tracing::warn!(
            %error,
            "could not abort a failed multipart upload; the store keeps its parts until a \
             lifecycle rule removes them"
        );
    }
}

/// Reads into `buf` until it is full or `data` ends. Returns how much it
/// read.
async fn read_full(data: &mut (impl AsyncRead + Unpin), buf: &mut [u8]) -> std::io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match data.read(&mut buf[filled..]).await? {
            0 => break,
            read => filled += read,
        }
    }
    Ok(filled)
}

/// The summary this node's copy of `segment` would have; `None` while its
/// seal is in flight, or once its body is evicted.
async fn local_summary(segment: &ArchivableSegment) -> Result<Option<SegmentSummary>, Error> {
    let idx_path = segment.path.with_extension("idx");
    if file_len(&idx_path).await?.is_none() {
        return Ok(None);
    }
    let data_len = SealedIndex::open(&idx_path, segment.base)?.data_len();
    let blake3 = match hash_file(&segment.path, data_len).await {
        Ok(blake3) => blake3,
        // Evicted: the archive is all there is to compare with.
        Err(error) if is_not_found(&error) => return Ok(None),
        Err(error) => return Err(error),
    };
    Ok(Some(SegmentSummary {
        base: segment.base,
        end: segment.end,
        data_len,
        blake3,
    }))
}

/// The blake3 of the first `len` bytes of `path`. A file shorter than that
/// is `Error::Corrupted`: its index says it holds more.
async fn hash_file(path: &Path, len: u64) -> Result<String, Error> {
    let mut data = tokio::fs::File::open(path).await?.take(len);
    let mut hasher = blake3::Hasher::new();
    let mut total = 0u64;
    let mut chunk = vec![0u8; PART_SIZE];
    loop {
        let read = data.read(&mut chunk).await?;
        if read == 0 {
            break;
        }
        hasher.update(&chunk[..read]);
        total += read as u64;
    }
    if total != len {
        return Err(Error::Corrupted {
            message: format!(
                "{} is {total} bytes, but its index says its data runs to {len}",
                path.display()
            ),
        });
    }
    Ok(hasher.finalize().to_hex().to_string())
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

/// Whether an archived table other than `range` covers it.
fn superseded<T>(tables: &BTreeMap<(u64, u64), T>, range: (u64, u64)) -> bool {
    tables
        .range(..=(range.0, u64::MAX))
        .any(|(&(base, end), _)| (base, end) != range && base <= range.0 && end >= range.1)
}

/// Whether `error` is the store refusing a delete, which it will go on
/// refusing: a retention policy (403 on GCS, 409 on Azure), credentials
/// that may not delete, a store without deletes. Anything else, a 503 or a
/// timeout, may pass.
fn refuses_deletes(error: &object_store::Error) -> bool {
    matches!(
        error,
        object_store::Error::PermissionDenied { .. }
            | object_store::Error::NotImplemented { .. }
            | object_store::Error::NotSupported { .. }
            | object_store::Error::Precondition { .. }
            | object_store::Error::AlreadyExists { .. }
    )
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
mod faulty;

#[cfg(test)]
mod tests {
    use super::faulty::{Faults, Faulty};
    use super::*;
    use crate::append::AppendRequest;
    use crate::event::{AppendEvent, Tag};
    use crate::store::{EventStoreEngine, StoreOptions};
    use std::sync::Arc;

    const SEGMENT_SIZE: u64 = 16 * 1024;

    /// An archiver over the directory `dir` through a [`Faulty`] store.
    fn faulty_archiver(dir: &Path, faults: Faults) -> (Archiver, Faulty) {
        let url = url::Url::from_directory_path(dir).unwrap();
        let (store, prefix) = crate::remote::open_store(url.as_str()).unwrap();
        let faulty = Faulty::over(store, faults);
        (Archiver::new(Box::new(faulty.clone()), prefix), faulty)
    }

    fn pattern(len: usize) -> Vec<u8> {
        (0..len).map(|i| (i * 31 % 251) as u8).collect()
    }

    async fn stored(archiver: &Archiver, object: &ObjectPath) -> Option<Vec<u8>> {
        match archiver.store.get(object).await {
            Ok(result) => Some(result.bytes().await.unwrap().to_vec()),
            Err(object_store::Error::NotFound { .. }) => None,
            Err(error) => panic!("{error}"),
        }
    }

    /// A source that fails, as a disk does.
    struct FailingRead;

    impl AsyncRead for FailingRead {
        fn poll_read(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            _: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Err(std::io::Error::other("the disk failed")))
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_large_upload_keeps_a_few_parts_of_its_size_in_flight() {
        let tier = tempfile::tempdir().unwrap();
        let (archiver, store) = faulty_archiver(
            tier.path(),
            Faults {
                part_delay: Duration::from_millis(50),
                ..Faults::default()
            },
        );
        let len = 5 * PART_SIZE + 123;
        let bytes = pattern(len);
        let object = archiver.prefix.clone().join("large");
        let Upload::Written(blake3) = archiver
            .upload(&bytes[..], len as u64, &object, "large")
            .await
            .unwrap()
        else {
            panic!("nothing was uploaded");
        };
        assert_eq!(blake3, blake3::hash(&bytes).to_hex().to_string());
        assert!(stored(&archiver, &object).await.unwrap() == bytes);
        let seen = store.seen();
        let mut sizes = vec![PART_SIZE; 5];
        sizes.push(123);
        assert_eq!(seen.part_sizes, sizes);
        assert!(
            (2..=PARTS_IN_FLIGHT).contains(&seen.most_in_flight),
            "{} parts in flight",
            seen.most_in_flight
        );
        assert_eq!(seen.aborts, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_upload_that_fails_is_aborted() {
        let tier = tempfile::tempdir().unwrap();
        let (archiver, store) = faulty_archiver(tier.path(), Faults::default());
        let object = archiver.prefix.clone().join("failed");
        let bytes = pattern(2 * PART_SIZE + 5);

        // A source that ends early: completing would store a short object.
        let error = archiver
            .upload(&bytes[..], 3 * PART_SIZE as u64, &object, "short")
            .await
            .unwrap_err();
        assert!(
            matches!(&error, Error::Io(io) if io.kind() == std::io::ErrorKind::UnexpectedEof),
            "{error}"
        );
        assert_eq!(store.seen().aborts, 1);
        assert!(stored(&archiver, &object).await.is_none());

        // A source that fails partway.
        let error = archiver
            .upload(
                (&bytes[..]).chain(FailingRead),
                3 * PART_SIZE as u64,
                &object,
                "failing",
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("the disk failed"), "{error}");
        assert_eq!(store.seen().aborts, 2);
        assert!(stored(&archiver, &object).await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn an_upload_dropped_partway_is_aborted() {
        let tier = tempfile::tempdir().unwrap();
        let (archiver, store) = faulty_archiver(
            tier.path(),
            Faults {
                stall_parts: true,
                ..Faults::default()
            },
        );
        let object = archiver.prefix.clone().join("dropped");
        let len = (PARTS_IN_FLIGHT + 1) * PART_SIZE;
        let bytes = pattern(len);
        let upload = archiver.upload(&bytes[..], len as u64, &object, "dropped");
        assert!(
            tokio::time::timeout(Duration::from_millis(200), upload)
                .await
                .is_err()
        );
        // Dropping it starts the abort on the runtime.
        for _ in 0..200 {
            if store.seen().aborts > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(store.seen().aborts, 1);
        assert!(stored(&archiver, &object).await.is_none());
    }

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
        seal_segments_at(engine, log, minimum_final, make_event(log, 0).timestamp).await;
    }

    /// [`seal_segments`] with events timestamped `timestamp`.
    async fn seal_segments_at(
        engine: &Arc<EventStoreEngine>,
        log: &'static str,
        minimum_final: usize,
        timestamp: i64,
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
                                AppendEvent {
                                    timestamp,
                                    ..make_event(log, index)
                                }
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
    async fn a_store_that_ignores_conditional_writes_gets_nothing_archived() {
        let data = tempfile::tempdir().unwrap();
        let tier = tempfile::tempdir().unwrap();
        let engine = engine_in(data.path());
        seal_segments(&engine, "a", 2).await;
        let (archiver, _) = faulty_archiver(
            tier.path(),
            Faults {
                ignore_create: true,
                ..Faults::default()
            },
        );
        let error = archiver
            .run_archive_pass("default", &engine)
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Rejected { .. }), "{error}");
        assert!(!tier.path().join("default").exists());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn every_leader_checks_conditional_writes_on_the_one_object() {
        let data = tempfile::tempdir().unwrap();
        let tier = tempfile::tempdir().unwrap();
        let engine = engine_in(data.path());
        seal_segments(&engine, "a", 1).await;
        // Under a retention policy too: the check never replaces it.
        for _ in 0..2 {
            let (archiver, store) = faulty_archiver(
                tier.path(),
                Faults {
                    write_once: true,
                    ..Faults::default()
                },
            );
            archiver.run_archive_pass("default", &engine).await.unwrap();
            archiver.forget().await;
            archiver.run_archive_pass("default", &engine).await.unwrap();
            assert!(store.seen().writes_ending(CONDITIONAL_WRITE_CHECK) <= 1);
        }
        let probe = std::fs::read(tier.path().join(CONDITIONAL_WRITE_CHECK)).unwrap();
        assert_eq!(probe, CONDITIONAL_WRITE_CHECK_BODY);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_pass_ends_at_the_segment_where_its_node_stops_leading() {
        let data = tempfile::tempdir().unwrap();
        let tier = tempfile::tempdir().unwrap();
        let engine = engine_in(data.path());
        seal_segments(&engine, "a", 4).await;
        let segments = engine.archivable_segments();
        let archiver = archiver_for(tier.path(), DELETE_GRACE);

        let asked = std::sync::atomic::AtomicUsize::new(0);
        let report = archiver
            .run_archive_pass_while("default", &engine, || {
                asked.fetch_add(1, Ordering::SeqCst) < 2
            })
            .await
            .unwrap();
        assert!(report.stopped);
        assert_eq!(report.segments, 2);
        assert_eq!(report.archived_end, segments[1].end);
        let summary = |base: u64| format!("{base:020}.sum");
        assert!(archived(tier.path(), &["segments", &summary(segments[1].base)]).is_some());
        assert!(archived(tier.path(), &["segments", &summary(segments[2].base)]).is_none());

        let report = archiver.run_archive_pass("default", &engine).await.unwrap();
        assert!(!report.stopped);
        assert_eq!(report.segments, segments.len() - 2);
    }

    #[test]
    fn failed_passes_back_off_up_to_a_cap() {
        let interval = Duration::from_secs(30);
        let start = Instant::now();
        let mut backoff = PassBackoff::default();
        assert!(backoff.ready(start));
        let waits: Vec<u64> = (0..6)
            .map(|_| backoff.failed(start, interval).as_secs())
            .collect();
        assert_eq!(waits, [60, 120, 240, 300, 300, 300]);
        assert!(!backoff.ready(start + Duration::from_secs(299)));
        assert!(backoff.ready(start + Duration::from_secs(300)));
        backoff.succeeded();
        assert!(backoff.ready(start));
        assert_eq!(backoff.failed(start, interval).as_secs(), 60);
    }

    /// An attempt that dies before its summary leaves the segment's other
    /// objects behind. The next writes none of them again, so a bucket that
    /// refuses every overwrite, as one under a retention policy does, does
    /// not stop archiving there.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_retried_segment_writes_nothing_twice() {
        let data = tempfile::tempdir().unwrap();
        let tier = tempfile::tempdir().unwrap();
        let engine = engine_in(data.path());
        seal_segments(&engine, "a", 2).await;
        let (archiver, store) = faulty_archiver(
            tier.path(),
            Faults {
                write_once: true,
                failing_puts: Some((".sum", 1)),
                ..Faults::default()
            },
        );
        archiver
            .run_archive_pass("default", &engine)
            .await
            .unwrap_err();
        assert_eq!(store.seen().writes_ending(".seg"), 1);
        assert_eq!(store.seen().writes_ending(".sum"), 0);

        let report = archiver.run_archive_pass("default", &engine).await.unwrap();
        let segments = engine.archivable_segments();
        assert_eq!(report.segments, segments.len());
        let seen = store.seen();
        assert_eq!(seen.writes_ending(".seg"), segments.len());
        assert!(seen.writes.values().all(|&n| n == 1), "{:?}", seen.writes);
    }

    /// Data of another log under a segment's name, with no summary to
    /// commit it, fails the pass and stays as it is, whatever its length.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn segment_data_of_another_log_is_refused_not_overwritten() {
        let data = tempfile::tempdir().unwrap();
        let tier = tempfile::tempdir().unwrap();
        let engine = engine_in(data.path());
        seal_segments(&engine, "a", 1).await;
        let segment = engine.archivable_segments().remove(0);
        let len = data_bytes(&segment).len();
        let dir = tier.path().join("default").join("segments");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join(format!("{:020}.seg", segment.base));

        for foreign in [vec![0u8; len], vec![0u8; len - 1]] {
            std::fs::write(&path, &foreign).unwrap();
            let error = archiver_for(tier.path(), DELETE_GRACE)
                .run_archive_pass("default", &engine)
                .await
                .unwrap_err();
            assert!(matches!(error, Error::Corrupted { .. }), "{error}");
            assert_eq!(std::fs::read(&path).unwrap(), foreign);
            assert!(!dir.join(format!("{:020}.sum", segment.base)).exists());
        }
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

    /// An engine whose archive holds a merged table and, still, the tables
    /// it supersedes. Returns the merged table's range.
    async fn superseded_tables(data: &Path, tier: &Path) -> (Arc<EventStoreEngine>, (u64, u64)) {
        let engine = engine_in(data);
        seal_segments(&engine, "a", 3).await;
        archiver_for(tier, DELETE_GRACE)
            .run_archive_pass("default", &engine)
            .await
            .unwrap();
        seal_segments(&engine, "a", 9).await;
        engine.merge_index().unwrap();
        let merged = engine.archivable_tables().unwrap().remove(0);
        archiver_for(tier, DELETE_GRACE)
            .run_archive_pass("default", &engine)
            .await
            .unwrap();
        let merged = (merged.base, merged.end);
        let tables = archived_tables(tier);
        assert!(tables.contains(&merged));
        assert!(tables.len() > 2, "{tables:?}");
        (engine, merged)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_grace_runs_from_when_the_leader_saw_the_covering_table() {
        let data = tempfile::tempdir().unwrap();
        let tier = tempfile::tempdir().unwrap();
        let (engine, _) = superseded_tables(data.path(), tier.path()).await;
        // The store's clock says every table was written an hour ago.
        let hour_ago = SystemTime::now() - Duration::from_secs(3600);
        for entry in std::fs::read_dir(tier.path().join("default").join("index")).unwrap() {
            std::fs::File::options()
                .write(true)
                .open(entry.unwrap().path())
                .and_then(|file| file.set_modified(hour_ago))
                .unwrap();
        }
        let before = archived_tables(tier.path());

        let report = archiver_for(tier.path(), DELETE_GRACE)
            .run_archive_pass("default", &engine)
            .await
            .unwrap();
        assert_eq!(report.deleted, 0);
        assert_eq!(archived_tables(tier.path()), before);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_delete_that_failed_is_tried_again_one_refused_is_not() {
        let data = tempfile::tempdir().unwrap();
        let tier = tempfile::tempdir().unwrap();
        let (engine, merged) = superseded_tables(data.path(), tier.path()).await;
        let superseded = archived_tables(tier.path())
            .iter()
            .filter(|&&(base, end)| (base, end) != merged && base >= merged.0 && end <= merged.1)
            .count();

        // A 503: the next pass deletes what this one could not.
        let (archiver, store) = faulty_archiver(
            tier.path(),
            Faults {
                failing_deletes: 1,
                ..Faults::default()
            },
        );
        let archiver = archiver.with_delete_grace(Duration::ZERO);
        let first = archiver.run_archive_pass("default", &engine).await.unwrap();
        assert_eq!(first.deleted, superseded - 1);
        let second = archiver.run_archive_pass("default", &engine).await.unwrap();
        assert_eq!(second.deleted, 1);
        assert_eq!(store.seen().deletes, superseded + 1);

        // A retention policy: tried once, then kept.
        let data = tempfile::tempdir().unwrap();
        let tier = tempfile::tempdir().unwrap();
        let (engine, _) = superseded_tables(data.path(), tier.path()).await;
        let before = archived_tables(tier.path());
        let (archiver, store) = faulty_archiver(
            tier.path(),
            Faults {
                refuse_deletes: true,
                ..Faults::default()
            },
        );
        let archiver = archiver.with_delete_grace(Duration::ZERO);
        for _ in 0..2 {
            let report = archiver.run_archive_pass("default", &engine).await.unwrap();
            assert_eq!(report.deleted, 0);
        }
        assert_eq!(store.seen().deletes, superseded);
        assert_eq!(archived_tables(tier.path()), before);
    }

    /// A leader whose merges lag behind the archive's does not upload the
    /// tables the archive has already superseded and deleted.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_table_the_archive_superseded_is_not_uploaded_again() {
        let data = tempfile::tempdir().unwrap();
        let tier = tempfile::tempdir().unwrap();
        let (engine, merged) = superseded_tables(data.path(), tier.path()).await;
        // Another leader merged further: a wider table covers this node's.
        let wider = (merged.0, merged.1 * 4);
        let index = tier.path().join("default").join("index");
        std::fs::write(index.join(table_file_name(wider.0, wider.1)), b"wider").unwrap();

        let archiver = archiver_for(tier.path(), Duration::ZERO);
        let report = archiver.run_archive_pass("default", &engine).await.unwrap();
        assert!(report.deleted > 0);
        assert!(!archived_tables(tier.path()).contains(&merged));
        let report = archiver.run_archive_pass("default", &engine).await.unwrap();
        assert_eq!((report.tables, report.deleted), (0, 0));
        assert!(!archived_tables(tier.path()).contains(&merged));
    }

    /// An engine in `dir`, the context named as the directory, that can
    /// evict to the archive in `tier`.
    fn evictable_engine_in(dir: &Path, tier: &Path) -> Arc<EventStoreEngine> {
        let url = url::Url::from_directory_path(tier).unwrap();
        Arc::new(
            EventStoreEngine::create_with_store_options(
                dir,
                &StoreOptions {
                    max_segment_size: SEGMENT_SIZE,
                    background_index_merge: false,
                    index_merge_rate: 0,
                    archive: Some(
                        crate::remote::ArchiveStore::from_url(url.as_str(), None).unwrap(),
                    ),
                    ..StoreOptions::default()
                },
            )
            .expect("engine"),
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_context_that_fails_or_diverged_holds_back_only_itself() {
        let data = tempfile::tempdir().unwrap();
        let tier = tempfile::tempdir().unwrap();
        let mut engines = Vec::new();
        for name in ["broken", "diverged", "healthy"] {
            let engine = evictable_engine_in(&data.path().join(name), tier.path());
            seal_segments(&engine, "a", 3).await;
            archiver_for(tier.path(), DELETE_GRACE)
                .run_archive_pass(name, &engine)
                .await
                .unwrap();
            engines.push((name.to_string(), engine));
        }
        let summary = |context: &str| {
            tier.path()
                .join(context)
                .join("segments")
                .join(format!("{:020}.sum", 0))
        };
        // The archive fails for one context: its summary cannot be read.
        std::fs::write(summary("broken"), b"not a summary").unwrap();
        // Another log's segment for another.
        let mut other: SegmentSummary =
            serde_json::from_slice(&std::fs::read(summary("diverged")).unwrap()).unwrap();
        other.blake3 = "0".repeat(64);
        std::fs::write(summary("diverged"), serde_json::to_vec(&other).unwrap()).unwrap();

        let report = archiver_for(tier.path(), DELETE_GRACE)
            .run_eviction_pass(&engines, 0)
            .await;
        assert_eq!(report.diverged, ["diverged"]);
        let failed: Vec<&str> = report
            .failed
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        assert_eq!(failed, ["broken"]);
        let healthy = &engines[2].1;
        let segments = healthy.archivable_segments();
        assert_eq!(report.evicted, segments.len());
        assert_eq!(healthy.evicted_end(), segments.last().unwrap().end);
        assert_eq!(engines[0].1.evicted_end(), 0);
        assert_eq!(engines[1].1.evicted_end(), 0);

        let status = ArchiveStatus::default();
        status.evicted(&report);
        let eviction = status.eviction();
        assert_eq!((eviction.failures, eviction.diverged), (1, 1));
        assert_eq!(eviction.evicted_segments, segments.len() as u64);
    }

    /// Across contexts, the segment whose last event is oldest goes first,
    /// whatever its file's time: on a node that caught up from the archive,
    /// that is when the segment was downloaded.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_segment_with_the_oldest_events_is_evicted_first() {
        let data = tempfile::tempdir().unwrap();
        let tier = tempfile::tempdir().unwrap();
        let mut engines = Vec::new();
        for (name, timestamp) in [("late", 2_000_000_000_000), ("early", 1_000_000_000_000)] {
            let engine = evictable_engine_in(&data.path().join(name), tier.path());
            seal_segments_at(&engine, "a", 2, timestamp).await;
            archiver_for(tier.path(), DELETE_GRACE)
                .run_archive_pass(name, &engine)
                .await
                .unwrap();
            engines.push((name.to_string(), engine));
        }
        // The later context's files look older, as downloaded ones can.
        let hour_ago = SystemTime::now() - Duration::from_secs(3600);
        let mut local = 0;
        for (name, engine) in &engines {
            for segment in engine.sealed_bodies() {
                let file = std::fs::File::options()
                    .write(true)
                    .open(&segment.path)
                    .unwrap();
                if name == "late" {
                    file.set_modified(hour_ago).unwrap();
                }
            }
            local += engine.local_bytes().unwrap();
        }

        let report = archiver_for(tier.path(), DELETE_GRACE)
            .run_eviction_pass(&engines, local - 1)
            .await;
        assert_eq!(report.evicted, 1);
        assert_eq!(engines[0].1.evicted_end(), 0);
        assert!(engines[1].1.evicted_end() > 0);
    }

    /// A file larger than one part goes up in parts, on the object store at
    /// `KRONOSDB_TEST_ARCHIVE_URL` (see the eviction tests). Skipped when
    /// unset.
    #[tokio::test]
    async fn a_large_file_reaches_the_environments_archive_in_parts() {
        let Ok(base) = std::env::var("KRONOSDB_TEST_ARCHIVE_URL") else {
            eprintln!("skipped: KRONOSDB_TEST_ARCHIVE_URL is not set");
            return;
        };
        let archiver = Archiver::from_url(&base).unwrap();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large");
        let len = 2 * PART_SIZE + 12_345;
        let bytes: Vec<u8> = (0..len).map(|i| (i * 31 % 251) as u8).collect();
        std::fs::write(&path, &bytes).unwrap();
        let object = archiver
            .prefix
            .clone()
            .join(format!("large-{}", now_millis()));
        let Upload::Written(blake3) = archiver
            .upload_file(&path, len as u64, &object)
            .await
            .unwrap()
        else {
            panic!("nothing was uploaded");
        };
        assert_eq!(blake3, blake3::hash(&bytes).to_hex().to_string());
        let stored = archiver
            .store
            .get(&object)
            .await
            .unwrap()
            .bytes()
            .await
            .unwrap();
        assert!(stored == bytes, "the stored object differs");
    }
}
