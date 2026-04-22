use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::{
    Entry, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend, RaftLogId, RaftLogReader,
    StorageError, Vote,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use super::types::{NodeId, TypeConfig};

#[cfg(feature = "bench-instrumentation")]
use super::bench_instrumentation::{self as bi, Region, Timer};

// --- Phase 2 log-store format + config contracts (D-01, D-04, D-05, D-07) ---
//
// These items are the new-log-store contracts the rest of Phase 2 builds on.
// They are introduced here WITHOUT replacing the existing `RaftLogStorage`
// impl below; Plan 02-03 replaces the impl body against these names.

/// Default segment cap (D-01). Size-based rotation only (D-03).
pub const DEFAULT_SEGMENT_CAP: u64 = 16 * 1024 * 1024; // 16 MiB
/// Default idle window before a group-commit batch fsyncs (D-07).
pub const DEFAULT_IDLE_WINDOW: std::time::Duration = std::time::Duration::from_micros(200);
/// Default buffered-bytes trigger for an early group-commit fsync (D-07).
pub const DEFAULT_BUFFERED_CAP: usize = 1024 * 1024; // 1 MiB

/// Tunable knobs for the Phase 2 segmented Raft log store.
///
/// Introduced by Plan 02-01 so downstream plans reference named fields rather
/// than magic numbers. Defaults match CONTEXT.md D-01 / D-07: 16 MiB segments,
/// 200 µs idle window, 1 MiB buffered-byte trigger.
#[derive(Debug, Clone)]
pub struct LogStoreConfig {
    /// Size cap (bytes) at which the active segment is sealed and rotated.
    pub segment_cap: u64,
    /// Quiet window before the first caller drives a group-commit fsync.
    pub idle_window: std::time::Duration,
    /// Buffered-byte threshold that triggers an early group-commit fsync.
    pub buffered_cap: usize,
}

impl Default for LogStoreConfig {
    fn default() -> Self {
        Self {
            segment_cap: DEFAULT_SEGMENT_CAP,
            idle_window: DEFAULT_IDLE_WINDOW,
            buffered_cap: DEFAULT_BUFFERED_CAP,
        }
    }
}

/// On-disk log-record format helpers (D-04).
///
/// Record layout: `[u32 len_be][bincode(Entry<TypeConfig>)][u32 crc32c_le]`.
/// `len_be` is big-endian so the width matches the LOG-01 spec and is endian-
/// stable across any consumer that reads raw headers. `crc32c_le` is
/// little-endian to match the `crc32c` crate's native output and the existing
/// `segment::writer` record convention.
///
/// Consumed by Plan 02-02's Segment primitive (`read_record_at`, test-path
/// encoding) and Plan 02-04 (startup index rebuild + torn-tail truncation).
mod record {
    /// Size of a log-record header on disk: 4-byte big-endian length.
    pub const LEN_PREFIX: usize = 4;
    /// Size of the trailing CRC on a record.
    pub const CRC_SUFFIX: usize = 4;

    /// Encode a record into `out`. Clears `out` first.
    ///
    /// Layout: 4-byte BE length of `payload`, then `payload`, then 4-byte
    /// LE `crc32c(payload)`.
    #[allow(dead_code)] // Consumed by Plan 02-03's LogStore write path; Plan 02-02 exercises it via tests.
    pub fn encode(payload: &[u8], out: &mut Vec<u8>) {
        out.clear();
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
        let crc = crc32c::crc32c(payload);
        out.extend_from_slice(&crc.to_le_bytes());
    }

    /// Total on-disk size for a record whose bincode payload is `payload_len`.
    #[allow(dead_code)] // Consumed by Plan 02-03 / 02-04; Plan 02-02 exercises it via tests + the `needs_rotation` contract.
    pub const fn total_size(payload_len: usize) -> usize {
        LEN_PREFIX + payload_len + CRC_SUFFIX
    }

    /// Decode a length prefix from a 4-byte BE slice.
    pub fn decode_len_be(buf: &[u8; LEN_PREFIX]) -> u32 {
        u32::from_be_bytes(*buf)
    }

    /// Decode a trailing CRC from a 4-byte LE slice.
    pub fn decode_crc_le(buf: &[u8; CRC_SUFFIX]) -> u32 {
        u32::from_le_bytes(*buf)
    }
}

/// Filename for a log segment keyed by the first log index it contains (D-05).
///
/// Layout mirrors SCOPE §5.2: `log-<16-digit-zero-padded-first-index>.bin`.
/// Consumed by Plan 02-02's `Segment::create` and Plan 02-03's LogStore impl.
fn segment_filename(first_index: u64) -> String {
    format!("log-{:016}.bin", first_index)
}

// --- Phase 2 Plan 02-02: Segment primitive (D-02, D-03, D-14) ---
//
// An internal, single-file append-only segment. Plan 02-03's `LogStore` will
// own a `Vec<SegmentMeta>` for sealed segments plus one active `Segment`.
// This primitive deliberately does NOT implement group commit, does NOT
// know about `Entry<TypeConfig>`, and does NOT touch the
// `RaftLogStorage<TypeConfig>` trait. It owns one file, appends pre-encoded
// record bytes, fsyncs on explicit `sync()`, rotates on size only, and
// reads a single record by `(path, offset)` via `File::read_at` (pread,
// never mmap per D-14).

/// Cheap metadata about a sealed segment, held in LogStore (Plan 02-03).
///
/// `byte_len` is the authoritative length of valid data in the segment
/// (post-`seal()` truncation) and is the source of truth for both the
/// in-memory index and the startup scan — not the on-disk file length,
/// which during the active-segment lifetime is the preallocated `cap`.
#[allow(dead_code)] // Consumed by Plan 02-03's LogStore.
pub(super) struct SegmentMeta {
    pub first_index: u64,
    pub path: std::path::PathBuf,
    pub byte_len: u64,
}

/// Active (writable) segment. Owns one open file.
///
/// Invariants:
/// - `write_offset <= cap` at all times (rotation is size-only per D-03).
/// - On Linux the file is preallocated via `fallocate`; elsewhere via
///   `File::set_len(cap)` (D-02). Either way the on-disk file length is
///   >= `write_offset` during the active lifetime.
/// - `sync()` fires exactly one fdatasync (Linux) / `F_FULLFSYNC` (macOS) /
///   `sync_data` (else); the bench-instrumentation fsync counter bumps
///   exactly once per successful sync.
#[allow(dead_code)] // Fields consumed by Plan 02-03's LogStore.
pub(super) struct Segment {
    pub first_index: u64,
    pub path: std::path::PathBuf,
    file: std::fs::File,
    pub write_offset: u64,
    pub cap: u64,
}

#[allow(dead_code)] // Methods consumed by Plan 02-03's LogStore.
impl Segment {
    /// Create a new, preallocated segment at `dir/log-<16-digit>.bin`.
    ///
    /// Uses `fallocate` on Linux and `File::set_len(cap)` elsewhere (D-02).
    /// The file is opened read+write; `write_offset` starts at 0.
    pub fn create(
        dir: &std::path::Path,
        first_index: u64,
        cap: u64,
    ) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(segment_filename(first_index));
        let file = std::fs::OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        preallocate(&file, cap)?;
        Ok(Self {
            first_index,
            path,
            file,
            write_offset: 0,
            cap,
        })
    }

    /// Open an existing segment for append, positioning `write_offset` at
    /// `byte_len`. Caller provides the recovered length (Plan 02-04 computes
    /// this during startup scan via per-record CRC validation on the active
    /// segment — headers-only scan for sealed segments per D-15).
    pub fn open_for_append(
        path: std::path::PathBuf,
        first_index: u64,
        byte_len: u64,
        cap: u64,
    ) -> std::io::Result<Self> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)?;
        use std::io::Seek;
        (&file).seek(std::io::SeekFrom::Start(byte_len))?;
        Ok(Self {
            first_index,
            path,
            file,
            write_offset: byte_len,
            cap,
        })
    }

    /// Returns true iff `self.write_offset + next_record_len > self.cap`.
    /// Rotation is size-only (D-03); no time or purge-driven triggers.
    pub fn needs_rotation(&self, next_record_len: usize) -> bool {
        self.write_offset
            .checked_add(next_record_len as u64)
            .map(|end| end > self.cap)
            .unwrap_or(true)
    }

    /// Append pre-encoded record bytes (produced by `record::encode`) to the
    /// file WITHOUT fsync. Advances `write_offset`. Returns the byte offset
    /// at which the record started. Cheap by design — Plan 02-03's group
    /// commit batches many of these before one explicit `sync()`.
    pub fn append_bytes(&mut self, record: &[u8]) -> std::io::Result<u64> {
        use std::io::Write;
        let start = self.write_offset;
        (&self.file).write_all(record)?;
        self.write_offset = start + record.len() as u64;
        Ok(start)
    }

    /// One fsync: fdatasync on Linux, F_FULLFSYNC on macOS, `sync_data` else.
    /// Bumps `bench_instrumentation::bump_fsync()` exactly once after success
    /// under the feature flag. No counter bump on error.
    pub fn sync(&mut self) -> std::io::Result<()> {
        fdatasync(&self.file)?;
        #[cfg(feature = "bench-instrumentation")]
        bi::bump_fsync();
        Ok(())
    }

    /// Truncate the on-disk file to `self.write_offset`. Used by `seal()` when
    /// rotating, and by Plan 02-04's startup scan when the torn-tail write
    /// offset is less than the preallocated file length. Does NOT re-
    /// preallocate — callers that intend to keep writing must either pass
    /// through `seal()` + `create()` a fresh segment, or re-run `preallocate`
    /// out-of-band. Plan 02-04's recovery path calls this on the active
    /// segment and then resumes appending (file will grow organically until
    /// rotation; preallocation benefits are bounded to the unrecovered tail).
    pub fn truncate_to_write_offset(&mut self) -> std::io::Result<()> {
        self.file.set_len(self.write_offset)
    }

    /// Seal this segment: fsync, truncate to `write_offset` (trims the
    /// preallocated tail), return `SegmentMeta`. Consumes self. Used when
    /// Plan 02-03 rotates to a new active segment.
    pub fn seal(self) -> std::io::Result<SegmentMeta> {
        fdatasync(&self.file)?;
        #[cfg(feature = "bench-instrumentation")]
        bi::bump_fsync();
        self.file.set_len(self.write_offset)?;
        Ok(SegmentMeta {
            first_index: self.first_index,
            path: self.path,
            byte_len: self.write_offset,
        })
    }
}

/// Pread-read a single record at `offset` in `path`. Returns the payload
/// bytes with the 4-byte length prefix and 4-byte CRC trailer stripped.
/// Verifies CRC; returns `ErrorKind::InvalidData` on mismatch. Uses
/// `File::read_at` on unix (D-14) — never mmap.
#[allow(dead_code)] // Consumed by Plan 02-03's read path.
pub(super) fn read_record_at(
    path: &std::path::Path,
    offset: u64,
) -> std::io::Result<Vec<u8>> {
    use std::os::unix::fs::FileExt;

    let file = std::fs::File::open(path)?;

    let mut len_buf = [0u8; record::LEN_PREFIX];
    file.read_exact_at(&mut len_buf, offset)?;
    let payload_len = record::decode_len_be(&len_buf) as usize;

    let payload_offset = offset + record::LEN_PREFIX as u64;
    let mut payload = vec![0u8; payload_len];
    file.read_exact_at(&mut payload, payload_offset)?;

    let crc_offset = payload_offset + payload_len as u64;
    let mut crc_buf = [0u8; record::CRC_SUFFIX];
    file.read_exact_at(&mut crc_buf, crc_offset)?;
    let stored_crc = record::decode_crc_le(&crc_buf);
    let computed_crc = crc32c::crc32c(&payload);

    if stored_crc != computed_crc {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "record crc mismatch",
        ));
    }
    Ok(payload)
}

/// Pre-allocate disk space for a segment file (D-02).
///
/// On Linux uses `fallocate` (contiguous blocks, fdatasync skips metadata
/// update); elsewhere falls back to `File::set_len(cap)`. Mirrors the arms
/// in `segment::writer::preallocate` — kept inline here to avoid a cross-
/// module dependency (planner's discretion per the plan text). Errors on
/// unsupported platforms fall through to `set_len`.
fn preallocate(file: &std::fs::File, cap: u64) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        // SAFETY: libc::fallocate is a well-defined syscall; fd is valid for
        // the lifetime of `file`.
        let ret = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, cap as i64) };
        if ret != 0 {
            // Fall back to set_len on filesystems that don't support fallocate.
            return file.set_len(cap);
        }
        Ok(())
    }
    #[cfg(not(target_os = "linux"))]
    {
        file.set_len(cap)
    }
}

/// One-fsync helper (D-02 platform split):
/// - Linux: `fdatasync` (skips inode metadata — enabled by preallocation).
/// - macOS: `F_FULLFSYNC` via `fcntl` (the only way past the drive write
///   cache on macOS; regular `fsync`/`sync_data` do not guarantee durability).
/// - Other unix: `sync_data()` as the closest portable equivalent.
fn fdatasync(file: &std::fs::File) -> std::io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let ret = unsafe { libc::fdatasync(file.as_raw_fd()) };
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) };
        if ret != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        file.sync_data()
    }
}

/// Vote persisted to disk.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PersistedVote {
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId<NodeId>>,
}

/// In-memory log store with file-backed persistence.
///
/// Log entries are kept in a BTreeMap and flushed to a bincode file on each write.
/// The Raft log is transient — entries are purged after being applied to the event
/// store. In steady state it holds only the small uncommitted tail.
pub struct LogStore {
    inner: Arc<Mutex<LogStoreInner>>,
}

struct LogStoreInner {
    dir: PathBuf,
    log: BTreeMap<u64, Entry<TypeConfig>>,
    vote: PersistedVote,
    last_purged: Option<LogId<NodeId>>,
}

impl LogStore {
    pub fn new(dir: &Path) -> Result<Self, io::Error> {
        std::fs::create_dir_all(dir)?;

        let vote = read_vote(dir).unwrap_or_default();
        let log = read_log(dir).unwrap_or_default();
        let last_purged = read_purged(dir);

        Ok(Self {
            inner: Arc::new(Mutex::new(LogStoreInner {
                dir: dir.to_path_buf(),
                log,
                vote,
                last_purged,
            })),
        })
    }
}

/// A cloneable log reader sharing the same inner state.
pub struct LogReader {
    inner: Arc<Mutex<LogStoreInner>>,
}

fn io_err(e: io::Error) -> StorageError<NodeId> {
    StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, e)
}

impl RaftLogReader<TypeConfig> for LogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogReader;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock();

        let last_log_id = inner
            .log
            .iter()
            .next_back()
            .map(|(_, e)| *e.get_log_id())
            .or(inner.last_purged);

        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        LogReader {
            inner: Arc::clone(&self.inner),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock();
        inner.vote.vote = Some(*vote);
        write_vote(&inner.dir, &inner.vote)
            .map_err(|e| StorageError::from_io_error(ErrorSubject::Vote, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        Ok(inner.vote.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock();
        inner.vote.committed = committed;
        write_vote(&inner.dir, &inner.vote).map_err(|e| io_err(e))?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        Ok(inner.vote.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = self.inner.lock();

        for entry in entries {
            inner.log.insert(entry.get_log_id().index, entry);
        }

        write_log(&inner.dir, &inner.log).map_err(|e| io_err(e))?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock();
        let to_remove: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in to_remove {
            inner.log.remove(&key);
        }
        write_log(&inner.dir, &inner.log).map_err(|e| io_err(e))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock();
        inner.last_purged = Some(log_id);

        let to_remove: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in to_remove {
            inner.log.remove(&key);
        }

        write_log(&inner.dir, &inner.log).map_err(|e| io_err(e))?;
        write_purged(&inner.dir, &log_id).map_err(|e| io_err(e))?;
        Ok(())
    }
}

// --- File I/O helpers ---

fn vote_path(dir: &Path) -> PathBuf {
    dir.join("vote.bin")
}

fn log_path(dir: &Path) -> PathBuf {
    dir.join("log.bin")
}

fn purged_path(dir: &Path) -> PathBuf {
    dir.join("purged.bin")
}

fn write_vote(dir: &Path, vote: &PersistedVote) -> Result<(), io::Error> {
    let data = bincode::serialize(vote).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    atomic_write(&vote_path(dir), &data)
}

fn read_vote(dir: &Path) -> Option<PersistedVote> {
    let data = std::fs::read(vote_path(dir)).ok()?;
    bincode::deserialize(&data).ok()
}

fn write_log(dir: &Path, log: &BTreeMap<u64, Entry<TypeConfig>>) -> Result<(), io::Error> {
    // NOTE (Plan 02-01, D-19): the Phase 1 bincode-rewrite region timer that
    // used to wrap this helper was removed along with the retired Region
    // variant. `write_log` is slated for deletion in Plan 02-03 when the
    // RaftLogStorage impl moves to the new segmented path; until then it
    // runs without per-region timing.
    let entries: Vec<_> = log.values().cloned().collect();
    let data = bincode::serialize(&entries).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    atomic_write(&log_path(dir), &data)
}

fn read_log(dir: &Path) -> Option<BTreeMap<u64, Entry<TypeConfig>>> {
    let data = std::fs::read(log_path(dir)).ok()?;
    let entries: Vec<Entry<TypeConfig>> = bincode::deserialize(&data).ok()?;
    let mut map = BTreeMap::new();
    for entry in entries {
        map.insert(entry.get_log_id().index, entry);
    }
    Some(map)
}

fn write_purged(dir: &Path, log_id: &LogId<NodeId>) -> Result<(), io::Error> {
    let data = bincode::serialize(log_id).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    atomic_write(&purged_path(dir), &data)
}

fn read_purged(dir: &Path) -> Option<LogId<NodeId>> {
    let data = std::fs::read(purged_path(dir)).ok()?;
    bincode::deserialize(&data).ok()
}

/// Directly inserts entries into the log and persists. For testing only.
#[cfg(test)]
impl LogStore {
    pub fn test_insert_entries(&self, entries: Vec<Entry<TypeConfig>>) {
        let mut inner = self.inner.lock();
        for entry in &entries {
            inner.log.insert(entry.get_log_id().index, entry.clone());
        }
        write_log(&inner.dir, &inner.log).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, Entry, LogId, Vote};

    fn log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId {
            leader_id: CommittedLeaderId::new(term, 0),
            index,
        }
    }

    fn blank_entry(term: u64, index: u64) -> Entry<TypeConfig> {
        let mut e = Entry::<TypeConfig>::default();
        e.set_log_id(&log_id(term, index));
        e
    }

    #[tokio::test]
    async fn fresh_log_state_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::new(dir.path()).unwrap();

        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());
    }

    #[tokio::test]
    async fn vote_persist_and_recover() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut store = LogStore::new(dir.path()).unwrap();
            assert!(store.read_vote().await.unwrap().is_none());

            let vote = Vote::new(3, 1);
            store.save_vote(&vote).await.unwrap();

            let read = store.read_vote().await.unwrap().unwrap();
            assert_eq!(read, vote);
        }

        // Reopen and verify persistence.
        {
            let mut store = LogStore::new(dir.path()).unwrap();
            let read = store.read_vote().await.unwrap().unwrap();
            assert_eq!(read, Vote::new(3, 1));
        }
    }

    #[tokio::test]
    async fn committed_persist_and_recover() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut store = LogStore::new(dir.path()).unwrap();
            assert!(store.read_committed().await.unwrap().is_none());

            let committed = log_id(2, 10);
            store.save_committed(Some(committed)).await.unwrap();

            let read = store.read_committed().await.unwrap().unwrap();
            assert_eq!(read, committed);
        }

        // Reopen.
        {
            let mut store = LogStore::new(dir.path()).unwrap();
            let read = store.read_committed().await.unwrap().unwrap();
            assert_eq!(read, log_id(2, 10));
        }
    }

    #[tokio::test]
    async fn entries_persist_and_recover() {
        let dir = tempfile::tempdir().unwrap();

        {
            let store = LogStore::new(dir.path()).unwrap();
            store.test_insert_entries(vec![
                blank_entry(1, 1),
                blank_entry(1, 2),
                blank_entry(1, 3),
            ]);
        }

        // Reopen and verify.
        {
            let mut store = LogStore::new(dir.path()).unwrap();
            let state = store.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.unwrap().index, 3);

            let entries = store.try_get_log_entries(1..4).await.unwrap();
            assert_eq!(entries.len(), 3);
        }
    }

    #[tokio::test]
    async fn truncate_removes_from_index() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::new(dir.path()).unwrap();

        store.test_insert_entries(vec![
            blank_entry(1, 1),
            blank_entry(1, 2),
            blank_entry(1, 3),
            blank_entry(2, 4),
        ]);

        // Truncate from index 3 inclusive.
        store.truncate(log_id(1, 3)).await.unwrap();

        let entries = store.try_get_log_entries(1..10).await.unwrap();
        assert_eq!(entries.len(), 2); // Only 1, 2 remain.
        assert_eq!(entries[0].get_log_id().index, 1);
        assert_eq!(entries[1].get_log_id().index, 2);
    }

    #[tokio::test]
    async fn purge_removes_up_to_and_tracks_last_purged() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::new(dir.path()).unwrap();

        store.test_insert_entries(vec![
            blank_entry(1, 1),
            blank_entry(1, 2),
            blank_entry(1, 3),
        ]);

        // Purge up to index 2 inclusive.
        store.purge(log_id(1, 2)).await.unwrap();

        let entries = store.try_get_log_entries(1..10).await.unwrap();
        assert_eq!(entries.len(), 1); // Only 3 remains.
        assert_eq!(entries[0].get_log_id().index, 3);

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 2);
        assert_eq!(state.last_log_id.unwrap().index, 3);
    }

    #[tokio::test]
    async fn purge_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut store = LogStore::new(dir.path()).unwrap();
            store.test_insert_entries(vec![
                blank_entry(1, 1),
                blank_entry(1, 2),
                blank_entry(1, 3),
            ]);
            store.purge(log_id(1, 2)).await.unwrap();
        }

        {
            let mut store = LogStore::new(dir.path()).unwrap();
            let state = store.get_log_state().await.unwrap();
            assert_eq!(state.last_purged_log_id.unwrap().index, 2);
            assert_eq!(state.last_log_id.unwrap().index, 3);
        }
    }

    #[tokio::test]
    async fn log_reader_sees_same_entries() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::new(dir.path()).unwrap();

        store.test_insert_entries(vec![blank_entry(1, 1), blank_entry(1, 2)]);

        let mut reader = store.get_log_reader().await;
        let entries = reader.try_get_log_entries(1..3).await.unwrap();
        assert_eq!(entries.len(), 2);
    }

    // --- Phase 2 format/config contract tests (Plan 02-01) ---

    #[test]
    fn log_store_config_defaults_match_constants() {
        let cfg = LogStoreConfig::default();
        assert_eq!(cfg.segment_cap, DEFAULT_SEGMENT_CAP);
        assert_eq!(cfg.segment_cap, 16 * 1024 * 1024);
        assert_eq!(cfg.idle_window, DEFAULT_IDLE_WINDOW);
        assert_eq!(cfg.idle_window, std::time::Duration::from_micros(200));
        assert_eq!(cfg.buffered_cap, DEFAULT_BUFFERED_CAP);
        assert_eq!(cfg.buffered_cap, 1024 * 1024);
    }

    #[test]
    fn record_encode_layout_round_trip() {
        let payload: Vec<u8> = (0u8..37).collect();
        let mut buf = Vec::with_capacity(record::total_size(payload.len()));
        record::encode(&payload, &mut buf);

        // Total encoded length equals total_size(payload).
        assert_eq!(buf.len(), record::total_size(payload.len()));
        assert_eq!(
            buf.len(),
            record::LEN_PREFIX + payload.len() + record::CRC_SUFFIX
        );

        // Length prefix decodes to payload.len() as BE u32.
        let len_bytes: [u8; record::LEN_PREFIX] =
            buf[..record::LEN_PREFIX].try_into().unwrap();
        assert_eq!(record::decode_len_be(&len_bytes), payload.len() as u32);

        // Payload bytes survive encoding.
        assert_eq!(
            &buf[record::LEN_PREFIX..record::LEN_PREFIX + payload.len()],
            payload.as_slice()
        );

        // Trailing CRC decodes to crc32c(payload) as LE u32.
        let crc_bytes: [u8; record::CRC_SUFFIX] = buf
            [record::LEN_PREFIX + payload.len()..]
            .try_into()
            .unwrap();
        assert_eq!(record::decode_crc_le(&crc_bytes), crc32c::crc32c(&payload));
    }

    #[test]
    fn record_encode_clears_existing_buffer() {
        let payload = b"phase-2-record".to_vec();
        let mut buf = vec![0xAA; 128]; // pre-existing garbage
        record::encode(&payload, &mut buf);
        assert_eq!(buf.len(), record::total_size(payload.len()));
    }

    #[test]
    fn record_corrupt_payload_detected_by_crc() {
        let payload = b"the-quick-brown-fox".to_vec();
        let mut buf = Vec::new();
        record::encode(&payload, &mut buf);

        // Flip a byte inside the payload region.
        let mutate_at = record::LEN_PREFIX + 3;
        buf[mutate_at] ^= 0xFF;

        let mutated_payload = &buf[record::LEN_PREFIX..record::LEN_PREFIX + payload.len()];
        let trailing_crc_bytes: [u8; record::CRC_SUFFIX] = buf
            [record::LEN_PREFIX + payload.len()..]
            .try_into()
            .unwrap();
        let stored_crc = record::decode_crc_le(&trailing_crc_bytes);
        let recomputed_crc = crc32c::crc32c(mutated_payload);

        assert_ne!(
            stored_crc, recomputed_crc,
            "CRC must not match a mutated payload"
        );
    }

    #[test]
    fn record_empty_payload_is_just_header_plus_crc() {
        let mut buf = Vec::new();
        record::encode(&[], &mut buf);
        assert_eq!(buf.len(), record::total_size(0));
        let len_bytes: [u8; record::LEN_PREFIX] =
            buf[..record::LEN_PREFIX].try_into().unwrap();
        assert_eq!(record::decode_len_be(&len_bytes), 0);
        let crc_bytes: [u8; record::CRC_SUFFIX] =
            buf[record::LEN_PREFIX..].try_into().unwrap();
        assert_eq!(record::decode_crc_le(&crc_bytes), crc32c::crc32c(&[]));
    }

    #[test]
    fn segment_filename_is_zero_padded_16_digits() {
        assert_eq!(segment_filename(0), "log-0000000000000000.bin");
        assert_eq!(segment_filename(1), "log-0000000000000001.bin");
        assert_eq!(segment_filename(42), "log-0000000000000042.bin");
        assert_eq!(
            segment_filename(u64::MAX),
            format!("log-{:016}.bin", u64::MAX)
        );
    }
}

#[cfg(test)]
mod segment_tests {
    //! Plan 02-02 tests: exercise `Segment` + `SegmentMeta` + `read_record_at`
    //! in isolation, without openraft or `LogStore` involvement.

    use super::*;
    use std::io::Write as _;

    /// Helper: encode a record whose payload is `payload`, returning the
    /// on-disk bytes a caller would hand to `Segment::append_bytes`.
    fn encoded(payload: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(record::total_size(payload.len()));
        record::encode(payload, &mut buf);
        buf
    }

    #[test]
    fn create_then_append_then_sync_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 1, 64 * 1024).unwrap();

        let payload = b"hello".to_vec();
        let rec = encoded(&payload);
        let start = seg.append_bytes(&rec).unwrap();
        assert_eq!(start, 0);
        assert_eq!(seg.write_offset, rec.len() as u64);

        seg.sync().unwrap();

        // Filename should match segment_filename helper (D-05).
        assert_eq!(
            seg.path.file_name().unwrap().to_str().unwrap(),
            "log-0000000000000001.bin"
        );

        // Pread the record back and confirm payload round-trips.
        let got = read_record_at(&seg.path, start).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn needs_rotation_true_when_over_cap() {
        let dir = tempfile::tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 1, 256).unwrap();
        // Simulate a write_offset near cap without actually writing 250 bytes
        // to disk — the predicate is arithmetic only.
        seg.write_offset = 250;

        // A record larger than the remaining 6 bytes must trigger rotation.
        assert!(seg.needs_rotation(32));
        // A record that exactly fits must NOT trigger rotation (== cap is
        // allowed; only `>` does per the invariant).
        assert!(!seg.needs_rotation(6));
        // Tiny record well within remaining space.
        assert!(!seg.needs_rotation(1));
    }

    #[test]
    fn seal_truncates_to_write_offset() {
        let dir = tempfile::tempdir().unwrap();
        let cap: u64 = 1024 * 1024;
        let mut seg = Segment::create(dir.path(), 7, cap).unwrap();

        let payload = b"seal-truncation-check".to_vec();
        let rec = encoded(&payload);
        seg.append_bytes(&rec).unwrap();
        let expected_len = record::total_size(payload.len()) as u64;
        assert_eq!(seg.write_offset, expected_len);

        let meta = seg.seal().unwrap();
        assert_eq!(meta.first_index, 7);
        assert_eq!(meta.byte_len, expected_len);

        // On-disk length must match byte_len (preallocated tail trimmed).
        let md = std::fs::metadata(&meta.path).unwrap();
        assert_eq!(md.len(), expected_len);
    }

    #[cfg(feature = "bench-instrumentation")]
    #[test]
    fn fsync_counter_bumps_on_sync() {
        use crate::raft::bench_instrumentation::fsync_count;

        // NOTE: FSYNC_COUNTER is a process-global `AtomicU64` shared across
        // all tests in the binary. `cargo test` runs tests on multiple
        // threads by default, so we cannot assert an exact delta — other
        // tests' syncs may bump the counter between our snapshots. Assert
        // a lower bound (≥ 2) instead: two `sync()` calls must contribute
        // at least two bumps to the global counter.
        let dir = tempfile::tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 1, 64 * 1024).unwrap();
        let rec = encoded(b"x");
        seg.append_bytes(&rec).unwrap();

        let before = fsync_count();
        seg.sync().unwrap();
        seg.sync().unwrap();
        let after = fsync_count();
        assert!(
            after - before >= 2,
            "two sync() calls must contribute at least 2 fsync bumps (before={before}, after={after})"
        );
    }

    #[test]
    fn read_record_at_detects_crc_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 1, 64 * 1024).unwrap();

        let payload = b"corruption-check".to_vec();
        let rec = encoded(&payload);
        let start = seg.append_bytes(&rec).unwrap();
        seg.sync().unwrap();
        let path = seg.path.clone();
        drop(seg);

        // Corrupt a single payload byte in-place. The record layout is
        // [4 BE len][payload][4 LE crc]; offset `start + 4` is the first
        // payload byte.
        {
            let mut f = std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap();
            use std::io::Seek;
            f.seek(std::io::SeekFrom::Start(start + record::LEN_PREFIX as u64))
                .unwrap();
            f.write_all(&[0xFFu8]).unwrap();
            f.sync_all().unwrap();
        }

        let err = read_record_at(&path, start).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}

fn atomic_write(path: &Path, data: &[u8]) -> Result<(), io::Error> {
    #[cfg(feature = "bench-instrumentation")]
    let _t = Timer::new(Region::LogAtomicWrite);
    let tmp = path.with_extension("tmp");

    // Write + fsync the file contents.
    let file = std::fs::File::create(&tmp)?;
    let mut writer = io::BufWriter::new(file);
    io::Write::write_all(&mut writer, data)?;
    let file = writer.into_inner().map_err(|e| e.into_error())?;
    file.sync_all()?;
    #[cfg(feature = "bench-instrumentation")]
    bi::bump_fsync();

    // Atomic rename.
    std::fs::rename(&tmp, path)?;

    // Fsync the directory to ensure the rename is durable.
    if let Some(parent) = path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
            #[cfg(feature = "bench-instrumentation")]
            bi::bump_fsync();
        }
    }

    Ok(())
}
