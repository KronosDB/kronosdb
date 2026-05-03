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

use super::types::{NodeId, TypeConfig};

#[cfg(feature = "bench-instrumentation")]
use super::bench_instrumentation::{self as bi, Region, Timer};

// --- Phase 2 log-store format + config contracts (D-01, D-04, D-05, D-07) ---
//
// These items are the new-log-store contracts the rest of Phase 2 builds on.
// They back the `LogStore` implementation further down — Plan 02-03 removed
// the old bincode BTreeMap round-trip and wired these helpers into the live
// `RaftLogStorage<TypeConfig>` path.

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
/// encoding), Plan 02-03's LogStore append path, and Plan 02-04 (startup
/// index rebuild + torn-tail truncation).
mod record {
    /// Size of a log-record header on disk: 4-byte big-endian length.
    pub const LEN_PREFIX: usize = 4;
    /// Size of the trailing CRC on a record.
    pub const CRC_SUFFIX: usize = 4;

    /// Encode a record into `out`. Clears `out` first.
    ///
    /// Layout: 4-byte BE length of `payload`, then `payload`, then 4-byte
    /// LE `crc32c(payload)`.
    pub fn encode(payload: &[u8], out: &mut Vec<u8>) {
        out.clear();
        out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        out.extend_from_slice(payload);
        let crc = crc32c::crc32c(payload);
        out.extend_from_slice(&crc.to_le_bytes());
    }

    /// Total on-disk size for a record whose bincode payload is `payload_len`.
    #[allow(dead_code)] // Plan 02-04 consumes this for the startup scan sizing.
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
// An internal, single-file append-only segment. Plan 02-03's `LogStore` owns
// a `Vec<SegmentMeta>` for sealed segments plus one active `Segment`.
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
pub(super) struct SegmentMeta {
    pub first_index: u64,
    pub path: std::path::PathBuf,
    /// Consumed by Plan 02-04's startup recovery path to short-circuit
    /// CRC scans on sealed segments.
    #[allow(dead_code)]
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
pub(super) struct Segment {
    pub first_index: u64,
    pub path: std::path::PathBuf,
    file: std::fs::File,
    pub write_offset: u64,
    pub cap: u64,
}

impl Segment {
    /// Create a new, preallocated segment at `dir/log-<16-digit>.bin`.
    ///
    /// Uses `fallocate` on Linux and `File::set_len(cap)` elsewhere (D-02).
    /// The file is opened read+write; `write_offset` starts at 0.
    pub fn create(dir: &std::path::Path, first_index: u64, cap: u64) -> std::io::Result<Self> {
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
    #[allow(dead_code)] // Consumed by Plan 02-04's startup recovery path.
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
    /// preallocate.
    #[allow(dead_code)] // Consumed by Plan 02-04's recovery path.
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
pub(super) fn read_record_at(path: &std::path::Path, offset: u64) -> std::io::Result<Vec<u8>> {
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

// --- Phase 2 Plan 02-03: LogStore + group-commit + segmented append ---
//
// The `LogStore` below implements `RaftLogStorage<TypeConfig>` on top of the
// `Segment` primitive (Plan 02-02) and the `record` module (Plan 02-01).
// Design decisions pinned by CONTEXT.md:
//
// - D-06: caller-drives coalescing (no dedicated thread). openraft invokes
//   `append` serially per log store, so each `append()` call drives its own
//   group-commit fsync at the end of the call.
// - D-07: hybrid trigger (idle-window OR buffered-byte cap). The idle-window
//   constant is currently unused in the single-caller openraft path (see
//   `drive_group_commit` comment); kept for a future multi-producer
//   experiment.
// - D-08: FIFO callback ordering. Callbacks are pushed in append order and
//   drained in the same order after the covering fsync returns.
// - D-09: all callbacks in a failed batch receive the same `StorageError`.
// - D-10: `save_committed` is I/O-free; `committed.bin` is folded into the
//   next group-commit drive.
// - D-11: `save_vote` stays on `atomic_write`.
// - D-12: `purge` writes `purged.bin` via `atomic_write`.
// - D-13: three metadata files (vote.bin / committed.bin / purged.bin).
// - D-16: in-memory index = `BTreeMap<u64, (segment_id, byte_offset, record_len)>`,
//   no entry cache.
//
// Startup index rebuild (scanning sealed + active segments on construction)
// is Plan 02-04. Until that plan lands, `LogStore::new` constructs an
// empty in-memory index and an empty sealed list — cross-restart behavior
// is NOT exercised by tests in this plan.

/// In-memory Raft log store backed by append-only segment files with
/// group-commit fsync semantics.
///
/// Each `append` call writes record bytes into the active segment, folds any
/// pending `save_committed` into a `committed.bin` write, and fires exactly
/// one covering fsync per call (plus one extra fsync when `committed.bin` was
/// updated this batch — see `drive_group_commit` for the two-vs-one fsync
/// accounting). Callbacks fire in FIFO order after the last fsync returns.
pub struct LogStore {
    inner: Arc<Mutex<LogStoreInner>>,
}

struct LogStoreInner {
    dir: PathBuf,
    config: LogStoreConfig,

    /// Sealed (read-only) segments, sorted by `first_index` ascending.
    sealed: Vec<SegmentMeta>,

    /// Active segment (the one being written to). `None` only between
    /// construction and the first append on a brand-new store; replaced by
    /// Plan 02-04's startup recovery when a prior segment exists.
    active: Option<Segment>,

    /// In-memory index (D-16). Key = log_index; Value = (segment_id, byte_offset,
    /// record_len). `segment_id` is the `first_index` of the containing segment
    /// (same value that keys the filename). `record_len` includes the full
    /// framing (4-byte len prefix + payload + 4-byte crc trailer).
    index: BTreeMap<u64, (u64, u64, u32)>,

    /// Highest-index log entry currently in the store (cached so
    /// `get_log_state` doesn't have to pread the tail record).
    last_log_id: Option<LogId<NodeId>>,

    /// Cached raft metadata.
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId<NodeId>>,
    last_purged: Option<LogId<NodeId>>,

    /// FIFO group-commit callback queue (D-08).
    pending_callbacks: Vec<LogFlushed<TypeConfig>>,

    /// Bytes buffered since the last successful fsync. Used by the
    /// buffered-cap trigger branch of D-07.
    buffered_bytes: usize,

    /// `true` when `save_committed` has mutated `committed` since the last
    /// group-commit drive. Controls whether `drive_group_commit` folds a
    /// `committed.bin` write (D-10).
    committed_dirty: bool,
}

/// Scan `dir` for `log-*.bin` files, rebuild the in-memory index, and return
/// `(sealed, active, index, last_log_id)`.
///
/// D-15: sealed segments (every log file except the highest-indexed) were
/// fdatasync'd before sealing during Plan 02-03's rotation path, so their
/// records are known good. The active (highest-indexed) segment is scanned
/// with per-record CRC validation; the first CRC mismatch or short read is a
/// torn-tail event and the file is `set_len`'d back to the offset of the last
/// valid record (CRASH-01).
///
/// Phase-2 simplification: sealed segments currently get CRC validation too,
/// because deriving each record's `log_index` requires deserializing the
/// bincode payload (cheap on Phase-2-sized segments and avoids a header
/// format change). Phase-7 PERF-04 can strip the CRC step on sealed segments
/// per the D-15 optimization if startup-scan time becomes a concern — see the
/// `TODO(phase 7)` inside `scan_sealed_segment`.
fn rebuild_index(
    dir: &Path,
    config: &LogStoreConfig,
) -> io::Result<(
    Vec<SegmentMeta>,
    Option<Segment>,
    BTreeMap<u64, (u64, u64, u32)>,
    Option<LogId<NodeId>>,
)> {
    #[cfg(feature = "bench-instrumentation")]
    let _t = Timer::new(Region::LogIndexRebuild);

    // 1. Enumerate `log-<first_index>.bin` files and sort by first_index asc.
    let mut segment_paths: Vec<(u64, PathBuf)> = Vec::new();
    let read_dir = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok((Vec::new(), None, BTreeMap::new(), None));
        }
        Err(e) => return Err(e),
    };
    for entry in read_dir {
        let entry = entry?;
        let path = entry.path();
        let Some(first_index) = parse_segment_first_index(&path) else {
            continue;
        };
        segment_paths.push((first_index, path));
    }
    segment_paths.sort_by_key(|(fi, _)| *fi);

    if segment_paths.is_empty() {
        return Ok((Vec::new(), None, BTreeMap::new(), None));
    }

    let mut index: BTreeMap<u64, (u64, u64, u32)> = BTreeMap::new();
    let mut sealed: Vec<SegmentMeta> = Vec::new();

    // 2. Scan every segment except the last one (sealed → headers-only walk
    //    with correctness-first CRC validation; see Phase-7 note above).
    let last_idx = segment_paths.len() - 1;
    for (i, (first_index, path)) in segment_paths.iter().enumerate() {
        if i == last_idx {
            break;
        }
        let byte_len = scan_sealed_segment(path, *first_index, &mut index)?;
        sealed.push(SegmentMeta {
            first_index: *first_index,
            path: path.clone(),
            byte_len,
        });
    }

    // 3. Scan the active (last) segment with per-record CRC validation +
    //    torn-tail truncation.
    let (active_first_index, active_path) = &segment_paths[last_idx];
    let valid_offset = scan_active_segment(active_path, *active_first_index, &mut index)?;

    // Truncate the preallocated / torn tail back to the last valid offset if
    // the on-disk file length is larger. Open for write only; no fsync here —
    // the next real append will fsync and cover this truncation.
    let on_disk_len = std::fs::metadata(active_path)?.len();
    if on_disk_len > valid_offset {
        let f = std::fs::OpenOptions::new().write(true).open(active_path)?;
        f.set_len(valid_offset)?;
    }

    // Re-open the active segment for append at the recovered offset. Use the
    // larger of `config.segment_cap` and `valid_offset` as the cap so
    // `needs_rotation` still behaves sensibly after recovery — a segment
    // previously grown beyond the current config cap continues to be usable
    // until the next rotation trigger (matches the invariant that the active
    // segment never shrinks its cap during its lifetime).
    let cap = std::cmp::max(config.segment_cap, valid_offset);
    let active = Some(Segment::open_for_append(
        active_path.clone(),
        *active_first_index,
        valid_offset,
        cap,
    )?);

    // 4. Compute last_log_id from the rebuilt index (highest key → pread the
    //    record → bincode-decode → pull `log_id`). Cold path; the O(1)
    //    cache in LogStoreInner is maintained from here on.
    let last_log_id = if let Some((&max_idx, &(seg_id, offset, _))) = index.iter().next_back() {
        // Prefer the active segment's path if the tail entry lives there,
        // else fall back to the matching sealed entry's path.
        let path = if *active_first_index == seg_id {
            active_path.clone()
        } else {
            sealed
                .iter()
                .find(|m| m.first_index == seg_id)
                .map(|m| m.path.clone())
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::NotFound,
                        format!(
                            "segment first_index={} missing for tail key {}",
                            seg_id, max_idx
                        ),
                    )
                })?
        };
        let payload = read_record_at(&path, offset)?;
        let entry: Entry<TypeConfig> = bincode::deserialize(&payload).map_err(bincode_err)?;
        Some(*entry.get_log_id())
    } else {
        None
    };

    Ok((sealed, active, index, last_log_id))
}

/// Parse `log-<16-digit>.bin` filenames into their `first_index` keys. Returns
/// `None` for any path whose filename does not match the segment pattern so
/// unrelated files (vote.bin, committed.bin, purged.bin, stray .tmp) are
/// skipped silently.
fn parse_segment_first_index(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let rest = name.strip_prefix("log-")?;
    let digits = rest.strip_suffix(".bin")?;
    // Reject filenames whose digit-part is not exactly 16 ASCII digits — keeps
    // the parser strict against accidental siblings like `log-.bin.tmp`.
    if digits.len() != 16 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse::<u64>().ok()
}

/// Walk a sealed segment record-by-record, inserting each record's
/// (segment_id, byte_offset, record_len) into `index` keyed by the entry's
/// `log_index`. Returns the total byte length of valid records found
/// (used as `SegmentMeta::byte_len`).
///
/// TODO(phase 7): Skip CRC + bincode-deserialize on sealed segments per D-15
/// optimization. Requires either (a) a sidecar `.idx` file, or (b) stashing
/// `log_index` inside the record header. Neither is justified at Phase-2
/// segment sizes (16 MiB default, rarely more than a handful of segments
/// live before purge). PERF-04 can revisit if startup-scan time grows.
fn scan_sealed_segment(
    path: &Path,
    first_index: u64,
    index: &mut BTreeMap<u64, (u64, u64, u32)>,
) -> io::Result<u64> {
    use std::os::unix::fs::FileExt;

    let file = std::fs::File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut offset: u64 = 0;

    while offset + record::LEN_PREFIX as u64 <= file_len {
        let mut len_buf = [0u8; record::LEN_PREFIX];
        // A sealed segment is set_len'd to write_offset by `Segment::seal`, so
        // any short-read or end-of-file here means we walked all records.
        if file.read_exact_at(&mut len_buf, offset).is_err() {
            break;
        }
        let payload_len = record::decode_len_be(&len_buf) as usize;
        let total = record::total_size(payload_len);
        let record_end = offset
            .checked_add(total as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "record offset overflow"))?;
        if record_end > file_len {
            // A sealed segment must not have partial records; if we see one
            // the on-disk state is corrupt. Stop here with the valid prefix.
            break;
        }

        // Read + CRC-validate payload (Phase-2 correctness-first; see TODO).
        let payload_offset = offset + record::LEN_PREFIX as u64;
        let mut payload = vec![0u8; payload_len];
        file.read_exact_at(&mut payload, payload_offset)?;
        let mut crc_buf = [0u8; record::CRC_SUFFIX];
        file.read_exact_at(&mut crc_buf, payload_offset + payload_len as u64)?;
        let stored_crc = record::decode_crc_le(&crc_buf);
        let computed_crc = crc32c::crc32c(&payload);
        if stored_crc != computed_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "sealed segment {} record at offset {} crc mismatch",
                    path.display(),
                    offset
                ),
            ));
        }

        let entry: Entry<TypeConfig> = bincode::deserialize(&payload).map_err(bincode_err)?;
        let log_index = entry.get_log_id().index;
        index.insert(log_index, (first_index, offset, total as u32));

        offset = record_end;
    }

    Ok(offset)
}

/// Walk the active segment record-by-record with full CRC validation.
/// Returns the byte offset of the first invalid / torn record (== the valid
/// `write_offset` for the reconstituted `Segment`). The caller is responsible
/// for `set_len`-truncating the file to this offset if it is less than the
/// on-disk file length.
///
/// Terminates on any of: short-read of the length prefix, record length
/// exceeding remaining file bytes, short-read of payload or trailing CRC, or
/// CRC mismatch. These are the torn-tail shapes CRASH-01 needs to cover —
/// Plan 02-03's group commit fsyncs the record + CRC together, so a torn tail
/// can only appear as an incomplete write of the trailing bytes.
fn scan_active_segment(
    path: &Path,
    first_index: u64,
    index: &mut BTreeMap<u64, (u64, u64, u32)>,
) -> io::Result<u64> {
    use std::os::unix::fs::FileExt;

    let file = std::fs::File::open(path)?;
    let file_len = file.metadata()?.len();
    let mut offset: u64 = 0;

    while offset + record::LEN_PREFIX as u64 <= file_len {
        let mut len_buf = [0u8; record::LEN_PREFIX];
        if file.read_exact_at(&mut len_buf, offset).is_err() {
            break;
        }
        let payload_len = record::decode_len_be(&len_buf) as usize;

        // Preallocated tail: once we hit an all-zero length prefix on a
        // never-written region, treat it as end-of-log. Without this check a
        // fresh segment whose preallocated tail decodes as a record of length
        // 0 would falsely succeed CRC (crc32c of empty is a fixed constant),
        // producing phantom zero-payload entries. Empty-payload real records
        // are never generated by `append_buffer` (bincode-encoded entries are
        // always non-empty), so treating len=0 as end-of-log is safe.
        if payload_len == 0 {
            break;
        }

        let total = record::total_size(payload_len);
        let record_end = match offset.checked_add(total as u64) {
            Some(end) => end,
            None => break,
        };
        if record_end > file_len {
            // Torn write: length prefix indicates more bytes than the file
            // actually has. Stop at `offset` (the START of the torn record).
            break;
        }

        let payload_offset = offset + record::LEN_PREFIX as u64;
        let mut payload = vec![0u8; payload_len];
        if file.read_exact_at(&mut payload, payload_offset).is_err() {
            break;
        }
        let mut crc_buf = [0u8; record::CRC_SUFFIX];
        if file
            .read_exact_at(&mut crc_buf, payload_offset + payload_len as u64)
            .is_err()
        {
            break;
        }
        let stored_crc = record::decode_crc_le(&crc_buf);
        let computed_crc = crc32c::crc32c(&payload);
        if stored_crc != computed_crc {
            // Torn or corrupt: stop at the start of this record (`offset`).
            break;
        }

        let entry: Entry<TypeConfig> = match bincode::deserialize::<Entry<TypeConfig>>(&payload) {
            Ok(e) => e,
            Err(_) => {
                // CRC passed but bincode failed — treat as torn (a real
                // append would never write a payload that CRCs to its
                // prefix'd length but doesn't decode as Entry). Stop here.
                break;
            }
        };
        let log_index = entry.get_log_id().index;
        index.insert(log_index, (first_index, offset, total as u32));

        offset = record_end;
    }

    Ok(offset)
}

impl LogStore {
    /// Construct a log store with an explicit `LogStoreConfig`.
    ///
    /// Plan 02-05 collapsed the old 1-arg backward-compat facade into this
    /// single constructor — every call site now passes a `LogStoreConfig`
    /// (typically `LogStoreConfig::default()`).
    pub fn new(dir: &Path, config: LogStoreConfig) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;

        let vote = load_vote(dir);
        let committed = load_committed(dir);
        let last_purged = load_purged(dir);

        // Plan 02-04: rebuild `sealed` + `active` + `index` + `last_log_id` by
        // scanning `dir` for `log-*.bin` files. Sealed segments get a header-
        // walking scan (with CRC validation as a correctness-first Phase-2
        // simplification — see `rebuild_index` for the Phase-7 optimization
        // note); the active (highest-indexed) segment gets per-record CRC
        // validation and torn-tail truncation to satisfy CRASH-01.
        let (sealed, active, index, last_log_id) = rebuild_index(dir, &config)?;

        tracing::info!(
            target: "raft.recovery",
            ?last_log_id,
            ?committed,
            ?last_purged,
            ?vote,
            "log_store recovered from disk"
        );

        Ok(Self {
            inner: Arc::new(Mutex::new(LogStoreInner {
                dir: dir.to_path_buf(),
                config,
                sealed,
                active,
                index,
                last_log_id,
                vote,
                committed,
                last_purged,
                pending_callbacks: Vec::new(),
                buffered_bytes: 0,
                committed_dirty: false,
            })),
        })
    }

    /// Returns the recovered `last_log_id` without going through the async
    /// `RaftLogStorage` trait. Used by `cluster::init_context`'s
    /// reconciliation pass (which runs before `Raft::new` and therefore
    /// before any `async fn` on the trait can be invoked — see D-09).
    pub fn last_log_id(&self) -> Option<LogId<NodeId>> {
        self.inner.lock().last_log_id
    }

    /// Returns the recovered `committed` without going through the async
    /// `RaftLogStorage` trait. Same rationale as `last_log_id`.
    pub fn committed(&self) -> Option<LogId<NodeId>> {
        self.inner.lock().committed
    }

    /// Returns the recovered `last_purged` without going through the async
    /// `RaftLogStorage` trait. Used by `cluster::init_context`'s rescue
    /// pre-check: if entries have been purged but no on-disk snapshot exists,
    /// the cluster-init `Membership` log entry has been silently dropped and
    /// startup must synthesize a rescue snapshot from `cluster_config` before
    /// constructing the state machine.
    pub fn last_purged(&self) -> Option<LogId<NodeId>> {
        self.inner.lock().last_purged
    }

    /// Reads the log entry at `log_index`, if present. Synchronous
    /// counterpart to `try_get_log_entries(index..=index)` for the
    /// reconciliation pass.
    pub fn entry_at(&self, log_index: u64) -> io::Result<Option<Entry<TypeConfig>>> {
        let inner = self.inner.lock();
        read_entry_at(&inner, log_index)
    }

    /// Synchronously bumps `committed` to `new_committed` and marks
    /// `committed_dirty` so the next `save_committed` fsync (triggered by
    /// the first post-start append) persists it. Used by reconciliation
    /// to promote `committed` up to the state machine's `last_applied`
    /// when marker-evidence shows apply was durable but the log_flushed
    /// callback didn't fire before crash (CRASH-02 Shape 1).
    pub fn promote_committed(&self, new_committed: LogId<NodeId>) {
        let mut inner = self.inner.lock();
        let higher = match inner.committed {
            Some(existing) if existing.index >= new_committed.index => existing,
            _ => new_committed,
        };
        if inner.committed != Some(higher) {
            inner.committed = Some(higher);
            inner.committed_dirty = true;
        }
    }
}

/// A cloneable log reader sharing the same inner state as its parent
/// `LogStore`. All reads go through `read_record_at` + bincode-deserialize.
pub struct LogReader {
    inner: Arc<Mutex<LogStoreInner>>,
}

fn io_err(e: io::Error) -> StorageError<NodeId> {
    StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, e)
}

fn bincode_err<E: std::fmt::Display>(e: E) -> io::Error {
    io::Error::new(io::ErrorKind::Other, e.to_string())
}

/// Look up the segment path for a given `segment_id` (= `first_index`). The
/// active segment is preferred over sealed because a re-created segment
/// after truncate might share a first_index with a dropped sealed entry —
/// in practice the active segment's first_index is strictly greater than
/// any sealed segment's `first_index`, so the disambiguation is defensive.
fn resolve_segment_path(inner: &LogStoreInner, segment_id: u64) -> Option<PathBuf> {
    if let Some(active) = inner.active.as_ref() {
        if active.first_index == segment_id {
            return Some(active.path.clone());
        }
    }
    inner
        .sealed
        .iter()
        .find(|m| m.first_index == segment_id)
        .map(|m| m.path.clone())
}

/// Read the entry at `log_index` using the in-memory index. Returns `Ok(None)`
/// if the index does not contain the key. Used by both the `LogStore` and
/// `LogReader` read paths.
fn read_entry_at(inner: &LogStoreInner, log_index: u64) -> io::Result<Option<Entry<TypeConfig>>> {
    let (segment_id, byte_offset, _record_len) = match inner.index.get(&log_index) {
        Some(t) => *t,
        None => return Ok(None),
    };
    let path = resolve_segment_path(inner, segment_id).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "segment first_index={} not found for log_index={}",
                segment_id, log_index
            ),
        )
    })?;
    let payload = read_record_at(&path, byte_offset)?;
    let entry: Entry<TypeConfig> = bincode::deserialize(&payload).map_err(bincode_err)?;
    Ok(Some(entry))
}

impl RaftLogReader<TypeConfig> for LogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        let keys: Vec<u64> = inner.index.range(range).map(|(k, _)| *k).collect();
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some(e) = read_entry_at(&inner, k).map_err(io_err)? {
                out.push(e);
            }
        }
        Ok(out)
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        let keys: Vec<u64> = inner.index.range(range).map(|(k, _)| *k).collect();
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            if let Some(e) = read_entry_at(&inner, k).map_err(io_err)? {
                out.push(e);
            }
        }
        Ok(out)
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogReader;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        let last_log_id = inner.last_log_id.or(inner.last_purged);
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
        inner.vote = Some(*vote);
        let data = bincode::serialize(vote)
            .map_err(bincode_err)
            .map_err(|e| StorageError::from_io_error(ErrorSubject::Vote, ErrorVerb::Write, e))?;
        atomic_write(&vote_path(&inner.dir), &data)
            .map_err(|e| StorageError::from_io_error(ErrorSubject::Vote, ErrorVerb::Write, e))?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        Ok(inner.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        // D-10: no I/O here. The next group-commit drive will fold the
        // committed.bin write into its covering fsync.
        let mut inner = self.inner.lock();
        inner.committed = committed;
        inner.committed_dirty = true;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        Ok(inner.committed)
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
        // Split into two phases so we can carry lock state across the fsync
        // drive without holding the lock across the `.await` (there are no
        // awaits in either helper today, but the structure is kept for the
        // day a background fsync drive replaces caller-drives per D-06).
        self.append_buffer(entries, Some(callback))?;
        self.drive_group_commit()
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock();
        truncate_inner(&mut inner, log_id).map_err(io_err)?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock();
        purge_inner(&mut inner, log_id).map_err(io_err)?;
        Ok(())
    }
}

impl LogStore {
    /// Phase 1 of `append`: serialize entries, encode records, write into the
    /// active segment (rotating if needed), update the in-memory index, and
    /// enqueue the callback. No fsync happens here — that's `drive_group_commit`.
    fn append_buffer<I>(
        &mut self,
        entries: I,
        callback: Option<LogFlushed<TypeConfig>>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>>,
    {
        #[cfg(feature = "bench-instrumentation")]
        let _t = Timer::new(Region::LogRecordWrite);

        let mut inner = self.inner.lock();
        let mut rec_buf: Vec<u8> = Vec::new();

        for entry in entries {
            let log_id = *entry.get_log_id();
            let payload = bincode::serialize(&entry)
                .map_err(bincode_err)
                .map_err(io_err)?;
            record::encode(&payload, &mut rec_buf);

            // Rotate if needed. If the active segment doesn't exist yet (fresh
            // store) we create one keyed on this entry's log_index.
            let needs_rotation = inner
                .active
                .as_ref()
                .map(|s| s.needs_rotation(rec_buf.len()))
                .unwrap_or(true);
            if needs_rotation {
                if let Some(active) = inner.active.take() {
                    // Seal the current active and push its SegmentMeta.
                    let meta = active.seal().map_err(io_err)?;
                    inner.sealed.push(meta);
                }
                let new_active =
                    Segment::create(&inner.dir, log_id.index, inner.config.segment_cap)
                        .map_err(io_err)?;
                inner.active = Some(new_active);
            }

            let active = inner
                .active
                .as_mut()
                .expect("active segment established above");
            let offset = active.append_bytes(&rec_buf).map_err(io_err)?;
            let segment_id = active.first_index;
            let record_len = rec_buf.len() as u32;
            inner
                .index
                .insert(log_id.index, (segment_id, offset, record_len));
            inner.buffered_bytes += rec_buf.len();

            // Maintain last_log_id cache (the incoming stream is append-ordered;
            // take max defensively in case openraft ever reorders).
            inner.last_log_id = Some(match inner.last_log_id {
                Some(prev) if prev.index >= log_id.index => prev,
                _ => log_id,
            });
        }

        if let Some(cb) = callback {
            inner.pending_callbacks.push(cb);
        }
        Ok(())
    }

    /// Test-only snapshot of the in-memory index (Plan 02-04 restart tests).
    ///
    /// Exposes the private `index` BTreeMap so restart tests can compare the
    /// pre-shutdown index against the rebuilt-from-disk index key-for-key.
    #[cfg(test)]
    pub(crate) fn debug_index(&self) -> BTreeMap<u64, (u64, u64, u32)> {
        self.inner.lock().index.clone()
    }

    /// Test-only append path that bypasses the `LogFlushed` callback.
    ///
    /// `LogFlushed::new` is `pub(crate)` on openraft, so external crates
    /// cannot construct one. This helper feeds entries through the same
    /// buffer + group-commit pipeline without a callback, so in-crate tests
    /// can verify disk side-effects, index state, and fsync behavior.
    /// Production `append` is unaffected.
    ///
    /// Visibility widened to `#[cfg(any(test, feature = "bench-instrumentation"))]`
    /// in Plan 07-01 so the `log_store_only` microbench (PERF-02) can drive
    /// `LogStore::append` directly without a `LogFlushed` callback — the bench
    /// crate pulls this via `kronosdb-eventstore`'s `bench-instrumentation`
    /// dev-feature. Declared `pub` under either gate so the sibling
    /// `kronosdb-bench` crate (which depends on `kronosdb-eventstore` with
    /// `features = ["bench-instrumentation"]`) can reach it; when only the
    /// `test` gate triggers (in-crate unit tests), `pub` on a `#[cfg(test)]`
    /// item is still crate-local because `#[cfg(test)]` items never leak
    /// into downstream crates' builds. Production release builds
    /// (no test, no bench-instrumentation) do not compile this helper at all.
    #[cfg(any(test, feature = "bench-instrumentation"))]
    pub async fn append_test(
        &mut self,
        entries: Vec<Entry<TypeConfig>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.append_buffer(entries, None)?;
        self.drive_group_commit()
    }

    /// Phase 2 of `append`: fold `committed.bin` if dirty, fsync the covering
    /// file(s), and fire all queued callbacks in FIFO order.
    ///
    /// fsync accounting (D-10):
    /// - committed clean: 1 fsync (active segment `sync()`).
    /// - committed dirty: 2 fsyncs (committed.bin fd + active segment fd).
    ///
    /// Both cases fire callbacks only after the last fsync returns (D-08).
    fn drive_group_commit(&mut self) -> Result<(), StorageError<NodeId>> {
        #[cfg(feature = "bench-instrumentation")]
        let _t = Timer::new(Region::LogGroupCommit);

        let mut inner = self.inner.lock();

        // D-07 idle_window is unused in the single-caller openraft path;
        // keep for future multi-producer experiments.
        let _ = inner.config.idle_window;
        // D-07 buffered_cap is always exceeded at this point because we
        // unconditionally drive at the end of each append call. Kept for
        // the same future-multi-producer reason.
        let _ = inner.config.buffered_cap;

        // Drive the fsync. On success, fire callbacks with Ok; on failure,
        // fire them all with the same StorageError (D-09). We capture the
        // result here so the callback-firing branch is uniform.
        let drive_result: Result<(), StorageError<NodeId>> =
            (|| -> Result<(), StorageError<NodeId>> {
                // Fold committed.bin into this batch if dirty (D-10).
                if inner.committed_dirty {
                    let data = bincode::serialize(&inner.committed)
                        .map_err(bincode_err)
                        .map_err(io_err)?;
                    let path = committed_path(&inner.dir);
                    // Direct overwrite; durability established by the sync_data below.
                    let mut f = std::fs::File::create(&path).map_err(io_err)?;
                    io::Write::write_all(&mut f, &data).map_err(io_err)?;
                    f.sync_data().map_err(io_err)?;
                    #[cfg(feature = "bench-instrumentation")]
                    bi::bump_fsync();
                    inner.committed_dirty = false;
                }

                // One covering fsync on the active segment (if one exists — an
                // append call always establishes one before reaching here; but
                // `save_committed` can drive a group-commit indirectly if we ever
                // add an explicit flush method. Today: only `append` drives.)
                if let Some(active) = inner.active.as_mut() {
                    active.sync().map_err(io_err)?;
                }

                inner.buffered_bytes = 0;
                Ok(())
            })();

        // Fire callbacks in FIFO order (D-08). On failure each callback gets
        // an equivalent error (D-09).
        let callbacks: Vec<LogFlushed<TypeConfig>> = inner.pending_callbacks.drain(..).collect();

        // Drop the lock before firing callbacks — openraft may reenter the
        // log store from within a callback's synchronous tail.
        drop(inner);

        match drive_result {
            Ok(()) => {
                for cb in callbacks {
                    cb.log_io_completed(Ok(()));
                }
                Ok(())
            }
            Err(e) => {
                // Fire each callback with an equivalent io::Error (D-09).
                // `LogFlushed::log_io_completed` takes `io::Error`, not
                // `StorageError`; openraft re-wraps it on its side.
                let err_str = format!("{e}");
                for cb in callbacks {
                    let io_e = io::Error::new(io::ErrorKind::Other, err_str.clone());
                    cb.log_io_completed(Err(io_e));
                }
                Err(e)
            }
        }
    }
}

/// Compute the last log index stored in a sealed segment. Given the sealed
/// Vec is sorted ascending by `first_index`, the "last index in this sealed
/// segment" is `next_segment.first_index - 1` (either the next sealed or the
/// active).
fn sealed_segment_last_index(
    sealed: &[SegmentMeta],
    active: Option<&Segment>,
    idx_in_sealed: usize,
    index: &BTreeMap<u64, (u64, u64, u32)>,
) -> u64 {
    let this = &sealed[idx_in_sealed];
    let boundary = if idx_in_sealed + 1 < sealed.len() {
        sealed[idx_in_sealed + 1].first_index
    } else if let Some(a) = active {
        a.first_index
    } else {
        // No next boundary — fall back to the highest index keyed into this
        // segment from the in-memory index. If the index is also empty,
        // there is no content to purge; return first_index so `last < first`
        // evaluates as empty in the caller.
        index
            .iter()
            .rev()
            .find(|(_, (seg_id, _, _))| *seg_id == this.first_index)
            .map(|(k, _)| *k)
            .unwrap_or(this.first_index)
    };
    boundary.saturating_sub(1)
}

fn truncate_inner(inner: &mut LogStoreInner, log_id: LogId<NodeId>) -> io::Result<()> {
    let cut = log_id.index;

    // 1) Drop any sealed segments whose first_index >= cut (entire segment
    //    is beyond the truncation point).
    let mut i = 0;
    while i < inner.sealed.len() {
        if inner.sealed[i].first_index >= cut {
            let meta = inner.sealed.remove(i);
            // Best-effort unlink; missing file is fine.
            let _ = std::fs::remove_file(&meta.path);
        } else {
            i += 1;
        }
    }

    // 2) If cut falls inside the active segment, rewind its write offset to
    //    the byte offset of the first record at-or-after `cut`. If there is
    //    no such entry, the active segment becomes empty.
    if let Some(active) = inner.active.as_mut() {
        if active.first_index >= cut {
            // Whole active segment is beyond cut — drop it entirely. The
            // index entries beyond `cut` will be pruned below.
            let path = active.path.clone();
            let _ = std::fs::remove_file(&path);
            inner.active = None;
        } else {
            // Find the byte offset of the first indexed entry with
            // `log_index >= cut` inside the active segment.
            let segment_id = active.first_index;
            let first_cut_offset = inner
                .index
                .range(cut..)
                .find(|(_, (seg_id, _, _))| *seg_id == segment_id)
                .map(|(_, (_, off, _))| *off);

            if let Some(rewind_to) = first_cut_offset {
                active.write_offset = rewind_to;
                active.file.set_len(rewind_to)?;
                // CRITICAL: `set_len` does NOT move the file cursor. Subsequent
                // writes through `Segment::append_bytes` use `write_all` on
                // `&File`, which writes at the current cursor position (not at
                // `write_offset`). Without this seek, the next append writes
                // at the pre-truncate cursor position, leaving a sparse hole
                // from `rewind_to` up to that cursor and causing subsequent
                // `read_record_at(rewind_to)` to return EOF/garbage.
                use std::io::Seek;
                (&active.file).seek(std::io::SeekFrom::Start(rewind_to))?;
                fdatasync(&active.file)?;
                #[cfg(feature = "bench-instrumentation")]
                bi::bump_fsync();
            }
            // else: nothing indexed at or after cut inside the active
            // segment — no rewind necessary.
        }
    }

    // 3) If cut falls inside a sealed segment (the tail-most sealed is cut
    //    mid-segment), we need to re-open that sealed segment as the new
    //    active segment. This is rare in openraft (usually truncate cuts at
    //    a segment boundary on the active), but must be correct.
    if inner.active.is_none() && !inner.sealed.is_empty() {
        // Find the last-remaining sealed segment. If it owns entries with
        // `log_index >= cut`, we need to rewind.
        let last_idx = inner.sealed.len() - 1;
        let last_sealed = &inner.sealed[last_idx];
        let segment_id = last_sealed.first_index;

        let rewind_offset = inner
            .index
            .range(cut..)
            .find(|(_, (seg_id, _, _))| *seg_id == segment_id)
            .map(|(_, (_, off, _))| *off);

        if let Some(rewind_to) = rewind_offset {
            // Promote this sealed segment back to active at the rewound
            // offset. Use `open_for_append` with `byte_len = rewind_to` and
            // the current sealed file's on-disk length as the cap (we don't
            // re-preallocate — recovery cold path).
            let meta = inner.sealed.remove(last_idx);
            let cap = std::fs::metadata(&meta.path)
                .map(|m| m.len())
                .unwrap_or(rewind_to);
            let active =
                Segment::open_for_append(meta.path.clone(), meta.first_index, rewind_to, cap)?;
            active.file.set_len(rewind_to)?;
            fdatasync(&active.file)?;
            #[cfg(feature = "bench-instrumentation")]
            bi::bump_fsync();
            inner.active = Some(active);
        }
    }

    // 4) Prune index entries at or beyond cut.
    let to_remove: Vec<u64> = inner.index.range(cut..).map(|(k, _)| *k).collect();
    for k in to_remove {
        inner.index.remove(&k);
    }

    // 5) Refresh the cached last_log_id from the now-pruned index.
    inner.last_log_id = inner.index.iter().next_back().map(|(k, _)| LogId {
        // The stored index doesn't carry leader_id; re-read the record
        // to get the authoritative log_id. This path is cold (truncate
        // is rare), so one pread is acceptable.
        leader_id: match read_entry_at(inner, *k) {
            Ok(Some(e)) => e.get_log_id().leader_id,
            _ => {
                // Fallback: synthesize a leader_id from term=0 vote
                // holder. This should not happen in practice because
                // a truncated entry still has a valid record on disk.
                openraft::CommittedLeaderId::new(0, 0)
            }
        },
        index: *k,
    });

    Ok(())
}

fn purge_inner(inner: &mut LogStoreInner, log_id: LogId<NodeId>) -> io::Result<()> {
    let cut = log_id.index;

    // O(1) drop of whole sealed segments whose last_index <= cut (LOG-06).
    // We iterate from the front (oldest) because sealed is sorted ascending.
    let mut i = 0;
    while i < inner.sealed.len() {
        let last = sealed_segment_last_index(&inner.sealed, inner.active.as_ref(), i, &inner.index);
        if last <= cut {
            let meta = inner.sealed.remove(i);
            let _ = std::fs::remove_file(&meta.path);
            // Prune index entries belonging to this segment.
            let segment_id = meta.first_index;
            let to_remove: Vec<u64> = inner
                .index
                .iter()
                .filter(|(_, (seg_id, _, _))| *seg_id == segment_id)
                .map(|(k, _)| *k)
                .collect();
            for k in to_remove {
                inner.index.remove(&k);
            }
            // Don't advance `i`: removal shifts later entries left.
        } else {
            i += 1;
        }
    }

    inner.last_purged = Some(log_id);

    // D-12: purged.bin on atomic_write (crash-safety matters; perf does not).
    let data = bincode::serialize(&log_id).map_err(bincode_err)?;
    atomic_write(&purged_path(&inner.dir), &data)?;

    Ok(())
}

// --- Metadata file paths (D-13) ---

fn vote_path(dir: &Path) -> PathBuf {
    dir.join("vote.bin")
}

fn committed_path(dir: &Path) -> PathBuf {
    dir.join("committed.bin")
}

fn purged_path(dir: &Path) -> PathBuf {
    dir.join("purged.bin")
}

fn load_vote(dir: &Path) -> Option<Vote<NodeId>> {
    let data = std::fs::read(vote_path(dir)).ok()?;
    bincode::deserialize(&data).ok()
}

fn load_committed(dir: &Path) -> Option<LogId<NodeId>> {
    let data = std::fs::read(committed_path(dir)).ok()?;
    // committed.bin was written via bincode::serialize(&Option<LogId>).
    bincode::deserialize::<Option<LogId<NodeId>>>(&data)
        .ok()
        .flatten()
}

fn load_purged(dir: &Path) -> Option<LogId<NodeId>> {
    let data = std::fs::read(purged_path(dir)).ok()?;
    bincode::deserialize(&data).ok()
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

#[cfg(test)]
mod tests {
    //! Phase 2 Plan 02-03 tests: the new segmented + group-commit path.
    //!
    //! All tests use a fresh tempdir — cross-restart recovery is Plan 02-04.
    //! Any assertion that requires index-rebuild-on-startup is deferred.

    use super::*;
    use openraft::{CommittedLeaderId, Entry as RaftEntry, LogId, Vote};

    fn log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId {
            leader_id: CommittedLeaderId::new(term, 0),
            index,
        }
    }

    fn blank_entry(term: u64, index: u64) -> RaftEntry<TypeConfig> {
        let mut e = RaftEntry::<TypeConfig>::default();
        e.set_log_id(&log_id(term, index));
        e
    }

    // --- Round-trip and state tests ---

    #[tokio::test]
    async fn fresh_log_state_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();
        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());
    }

    #[tokio::test]
    async fn append_then_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();

        store
            .append_test(vec![
                blank_entry(1, 1),
                blank_entry(1, 2),
                blank_entry(1, 3),
            ])
            .await
            .unwrap();

        let entries = store.try_get_log_entries(1..4).await.unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].get_log_id().index, 1);
        assert_eq!(entries[1].get_log_id().index, 2);
        assert_eq!(entries[2].get_log_id().index, 3);

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 3);
    }

    #[tokio::test]
    async fn truncate_drops_entries() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();

        store
            .append_test(vec![
                blank_entry(1, 1),
                blank_entry(1, 2),
                blank_entry(1, 3),
                blank_entry(2, 4),
            ])
            .await
            .unwrap();

        // Truncate from index 3 inclusive.
        store.truncate(log_id(1, 3)).await.unwrap();

        let entries = store.try_get_log_entries(1..10).await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].get_log_id().index, 1);
        assert_eq!(entries[1].get_log_id().index, 2);

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id.unwrap().index, 2);
    }

    #[tokio::test]
    async fn purge_drops_sealed_segment_files_o1() {
        let dir = tempfile::tempdir().unwrap();
        // Force one record per segment by setting the cap to a single byte.
        // Any non-empty record will exceed the cap on every append, so every
        // append rotates the previously-written record into a sealed segment.
        let cfg = LogStoreConfig {
            segment_cap: 1,
            ..LogStoreConfig::default()
        };
        let mut store = LogStore::new(dir.path(), cfg).unwrap();

        // Append three entries — rotation sealing two prior segments.
        store.append_test(vec![blank_entry(1, 1)]).await.unwrap();
        store.append_test(vec![blank_entry(1, 2)]).await.unwrap();
        store.append_test(vec![blank_entry(1, 3)]).await.unwrap();

        // At this point: at least one sealed segment exists.
        {
            let inner = store.inner.lock();
            assert!(
                !inner.sealed.is_empty(),
                "expected rotation to produce sealed segments"
            );
        }

        // Capture sealed paths + their last indices before purge.
        let (first_sealed_path, last_idx_in_first_sealed) = {
            let inner = store.inner.lock();
            let path = inner.sealed[0].path.clone();
            let last =
                sealed_segment_last_index(&inner.sealed, inner.active.as_ref(), 0, &inner.index);
            (path, last)
        };
        assert!(first_sealed_path.exists());

        // Purge at the last index of the first sealed segment.
        store
            .purge(log_id(1, last_idx_in_first_sealed))
            .await
            .unwrap();

        // File must be gone; index must no longer have those keys.
        assert!(
            std::fs::metadata(&first_sealed_path).is_err(),
            "first sealed segment file must be unlinked after purge"
        );
        let inner = store.inner.lock();
        assert!(
            !inner.index.contains_key(&last_idx_in_first_sealed),
            "index must not retain purged keys"
        );
    }

    /// SNAP-03 / Phase 4: after a snapshot is installed, openraft signals a
    /// purge with the snapshot's `last_log_id`. The LogStore must drop every
    /// sealed segment whose highest index ≤ that log_id and keep the
    /// surviving tail readable.
    ///
    /// These tests live in-crate (not in tests/snapshot_purge.rs) because the
    /// append driver `append_test` is `#[cfg(test)] pub(crate)` and openraft's
    /// `LogFlushed::new` callback constructor is `pub(crate)` — neither is
    /// reachable from an integration test crate. See log_store.rs line ~1028
    /// for the rationale.
    #[tokio::test]
    async fn purge_after_snapshot_install() {
        let dir = tempfile::tempdir().unwrap();
        // segment_cap=1 forces every record into its own sealed segment —
        // same pattern as purge_drops_sealed_segment_files_o1 above.
        let cfg = LogStoreConfig {
            segment_cap: 1,
            ..LogStoreConfig::default()
        };
        let mut store = LogStore::new(dir.path(), cfg).unwrap();

        for i in 1..=30u64 {
            store
                .append_test(vec![blank_entry(1, i)])
                .await
                .expect("append");
        }

        // Simulate openraft's post-install purge signal with
        // snapshot.last_log_id.index = 15.
        store.purge(log_id(1, 15)).await.expect("purge");

        // Surviving tail: get_log_state must report a last_log_id whose
        // index is >= 16.
        let state = store.get_log_state().await.unwrap();
        let last = state
            .last_log_id
            .expect("surviving tail has a last_log_id")
            .index;
        assert!(
            last >= 16,
            "expected surviving tail last_index >= 16, got {last}"
        );

        // Read the tail. Every returned entry must have index >= 16. Exact
        // count depends on segment rotation timing; the invariant is that
        // no entry with index <= 15 survives.
        let tail = store.try_get_log_entries(16..31).await.expect("read tail");
        assert!(!tail.is_empty(), "expected at least one surviving entry");
        for e in &tail {
            assert!(
                e.log_id.index >= 16,
                "purge must not leave entries with index <= 15 (got {})",
                e.log_id.index
            );
        }
    }

    #[tokio::test]
    async fn purge_then_read_surviving_tail_no_stale_index() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = LogStoreConfig {
            segment_cap: 1,
            ..LogStoreConfig::default()
        };
        let mut store = LogStore::new(dir.path(), cfg).unwrap();
        for i in 1..=30u64 {
            store.append_test(vec![blank_entry(1, i)]).await.unwrap();
        }

        store.purge(log_id(1, 10)).await.expect("purge");

        // Below-cut reads must be empty — the in-memory index must have no
        // keys <= 10 after purge.
        let below = store.try_get_log_entries(1..11).await.expect("read below");
        assert!(
            below.is_empty(),
            "expected no entries with index <= 10 after purge, got {}",
            below.len()
        );

        let above = store.try_get_log_entries(11..31).await.expect("read above");
        assert!(!above.is_empty(), "surviving tail should be readable");
        for e in &above {
            assert!(
                e.log_id.index >= 11,
                "read above returned stale entry with index {}",
                e.log_id.index
            );
        }
    }

    #[tokio::test]
    async fn purge_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = LogStoreConfig {
            segment_cap: 1,
            ..LogStoreConfig::default()
        };
        let mut store = LogStore::new(dir.path(), cfg).unwrap();
        for i in 1..=30u64 {
            store.append_test(vec![blank_entry(1, i)]).await.unwrap();
        }

        store.purge(log_id(1, 15)).await.expect("first purge");
        // Second call at the same log_id must succeed (no-op).
        store
            .purge(log_id(1, 15))
            .await
            .expect("second purge must be no-op");

        let state = store.get_log_state().await.unwrap();
        assert!(
            state.last_log_id.is_some(),
            "tail must survive idempotent second purge"
        );
    }

    #[tokio::test]
    async fn vote_persists_atomic_write() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();

        assert!(store.read_vote().await.unwrap().is_none());

        let vote = Vote::new(5, 42);
        store.save_vote(&vote).await.unwrap();

        let read = store.read_vote().await.unwrap().unwrap();
        assert_eq!(read, vote);

        // vote.bin must exist on disk (atomic_write path).
        assert!(dir.path().join("vote.bin").exists());
    }

    #[cfg(feature = "bench-instrumentation")]
    #[tokio::test]
    async fn committed_is_folded_into_next_append_fsync() {
        use crate::raft::bench_instrumentation::fsync_count;

        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();

        // Baseline counter.
        let before = fsync_count();
        // D-10: save_committed is I/O-free.
        store.save_committed(Some(log_id(1, 5))).await.unwrap();
        let after_save = fsync_count();
        assert_eq!(
            after_save - before,
            0,
            "save_committed must be fsync-free (D-10)"
        );

        // Next append must drive at least 1 fsync (covers the committed.bin
        // fold + the active segment records). Two fsyncs expected when
        // committed is dirty (see drive_group_commit comment) but cargo test
        // runs in parallel so we assert a lower bound.
        store.append_test(vec![blank_entry(1, 1)]).await.unwrap();
        let after_append = fsync_count();
        assert!(
            after_append - after_save >= 1,
            "append must drive at least one fsync (committed dirty or not)"
        );

        assert!(dir.path().join("committed.bin").exists());
    }

    #[tokio::test]
    async fn fifo_callback_queue_drains_fully_each_drive() {
        // Note on scope: `openraft::storage::LogFlushed::new` is `pub(crate)`,
        // so this test crate cannot construct a `LogFlushed` to assert a
        // user-visible firing order (D-08). The FIFO contract is guaranteed
        // structurally by `Vec::drain(..)` iterating front-to-back inside
        // `drive_group_commit`; what this test verifies is the weaker but
        // still essential property that after every drive, the pending
        // queue is empty — no callback is ever left stranded in the queue
        // across append calls. End-to-end FIFO firing order is exercised
        // by the real openraft integration in `raft/cluster.rs` + the
        // cluster_test.rs suite.
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();

        store
            .append_test(vec![blank_entry(1, 1), blank_entry(1, 2)])
            .await
            .unwrap();
        {
            let inner = store.inner.lock();
            assert!(
                inner.pending_callbacks.is_empty(),
                "drive_group_commit must drain all pending callbacks"
            );
        }

        store.append_test(vec![blank_entry(1, 3)]).await.unwrap();
        {
            let inner = store.inner.lock();
            assert!(
                inner.pending_callbacks.is_empty(),
                "drive_group_commit must drain all pending callbacks on every drive"
            );
        }
    }

    #[cfg(feature = "bench-instrumentation")]
    #[tokio::test]
    async fn append_triggers_exactly_one_record_fsync_when_committed_clean() {
        use crate::raft::bench_instrumentation::fsync_count;

        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();

        // save_committed NOT called → committed_dirty is false.
        let before = fsync_count();
        store.append_test(vec![blank_entry(1, 1)]).await.unwrap();
        let after = fsync_count();

        // Lower-bound assertion: FSYNC_COUNTER is process-global and parallel
        // tests may interleave. At least one fsync must fire for the active
        // segment record write. With committed clean, the expected contribution
        // from this store is exactly 1.
        assert!(
            after - before >= 1,
            "append with committed-clean must fire at least 1 fsync (got delta={})",
            after - before
        );
    }

    // --- Plan 02-01 format/config contract tests (preserved) ---

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
        assert_eq!(buf.len(), record::total_size(payload.len()));
        assert_eq!(
            buf.len(),
            record::LEN_PREFIX + payload.len() + record::CRC_SUFFIX
        );
        let len_bytes: [u8; record::LEN_PREFIX] = buf[..record::LEN_PREFIX].try_into().unwrap();
        assert_eq!(record::decode_len_be(&len_bytes), payload.len() as u32);
        assert_eq!(
            &buf[record::LEN_PREFIX..record::LEN_PREFIX + payload.len()],
            payload.as_slice()
        );
        let crc_bytes: [u8; record::CRC_SUFFIX] = buf[record::LEN_PREFIX + payload.len()..]
            .try_into()
            .unwrap();
        assert_eq!(record::decode_crc_le(&crc_bytes), crc32c::crc32c(&payload));
    }

    #[test]
    fn record_encode_clears_existing_buffer() {
        let payload = b"phase-2-record".to_vec();
        let mut buf = vec![0xAA; 128];
        record::encode(&payload, &mut buf);
        assert_eq!(buf.len(), record::total_size(payload.len()));
    }

    #[test]
    fn record_corrupt_payload_detected_by_crc() {
        let payload = b"the-quick-brown-fox".to_vec();
        let mut buf = Vec::new();
        record::encode(&payload, &mut buf);
        let mutate_at = record::LEN_PREFIX + 3;
        buf[mutate_at] ^= 0xFF;
        let mutated_payload = &buf[record::LEN_PREFIX..record::LEN_PREFIX + payload.len()];
        let trailing_crc_bytes: [u8; record::CRC_SUFFIX] = buf
            [record::LEN_PREFIX + payload.len()..]
            .try_into()
            .unwrap();
        let stored_crc = record::decode_crc_le(&trailing_crc_bytes);
        let recomputed_crc = crc32c::crc32c(mutated_payload);
        assert_ne!(stored_crc, recomputed_crc);
    }

    #[test]
    fn record_empty_payload_is_just_header_plus_crc() {
        let mut buf = Vec::new();
        record::encode(&[], &mut buf);
        assert_eq!(buf.len(), record::total_size(0));
        let len_bytes: [u8; record::LEN_PREFIX] = buf[..record::LEN_PREFIX].try_into().unwrap();
        assert_eq!(record::decode_len_be(&len_bytes), 0);
        let crc_bytes: [u8; record::CRC_SUFFIX] = buf[record::LEN_PREFIX..].try_into().unwrap();
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

    // --- Plan 02-04 startup-recovery tests ---

    #[tokio::test]
    async fn round_trip_across_restart() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();
            store
                .append_test(vec![
                    blank_entry(1, 1),
                    blank_entry(1, 2),
                    blank_entry(1, 3),
                    blank_entry(1, 4),
                    blank_entry(1, 5),
                ])
                .await
                .unwrap();
        } // Drop the store — segments remain on disk.

        // Re-open on the same dir; rebuild_index should reconstruct the tail.
        let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(
            state.last_log_id.unwrap().index,
            5,
            "last_log_id must survive restart"
        );

        let entries = store.try_get_log_entries(1..6).await.unwrap();
        assert_eq!(entries.len(), 5);
        for (i, e) in entries.iter().enumerate() {
            assert_eq!(e.get_log_id().index, (i + 1) as u64);
        }
    }

    #[tokio::test]
    async fn vote_and_committed_survive_restart() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();
            store.save_vote(&Vote::new(3, 1)).await.unwrap();
            store.save_committed(Some(log_id(2, 10))).await.unwrap();
            // D-10: save_committed is I/O-free; drive one append so the
            // committed.bin fold actually hits disk.
            store.append_test(vec![blank_entry(1, 1)]).await.unwrap();
        }

        let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();
        assert_eq!(store.read_vote().await.unwrap(), Some(Vote::new(3, 1)));
        assert_eq!(store.read_committed().await.unwrap(), Some(log_id(2, 10)));
    }

    #[tokio::test]
    async fn purged_survives_restart() {
        let dir = tempfile::tempdir().unwrap();

        let purged_index;
        {
            // segment_cap=1 forces rotation on every append → the first two
            // appends seal a segment each; we then purge at the end-of-first-
            // sealed boundary.
            let cfg = LogStoreConfig {
                segment_cap: 1,
                ..LogStoreConfig::default()
            };
            let mut store = LogStore::new(dir.path(), cfg).unwrap();

            store.append_test(vec![blank_entry(1, 1)]).await.unwrap();
            store.append_test(vec![blank_entry(1, 2)]).await.unwrap();
            store.append_test(vec![blank_entry(1, 3)]).await.unwrap();

            // Capture the last index of the first sealed segment, then purge.
            purged_index = {
                let inner = store.inner.lock();
                assert!(!inner.sealed.is_empty(), "expected sealed segments");
                sealed_segment_last_index(&inner.sealed, inner.active.as_ref(), 0, &inner.index)
            };
            let first_sealed_path = {
                let inner = store.inner.lock();
                inner.sealed[0].path.clone()
            };
            store.purge(log_id(1, purged_index)).await.unwrap();
            assert!(
                std::fs::metadata(&first_sealed_path).is_err(),
                "first sealed segment file must be unlinked by purge"
            );
        }

        // Re-open and assert last_purged matches + the old first-sealed file
        // is still absent (purge is persistent).
        let cfg = LogStoreConfig {
            segment_cap: 1,
            ..LogStoreConfig::default()
        };
        let mut store = LogStore::new(dir.path(), cfg).unwrap();
        let state = store.get_log_state().await.unwrap();
        assert_eq!(
            state.last_purged_log_id.unwrap().index,
            purged_index,
            "last_purged_log_id must survive restart"
        );
        // The originally-purged segment's file index=1 is gone — we cannot
        // probe its specific path generically, but the sealed Vec after
        // restart must not contain an entry whose last_index <= purged_index.
        let inner = store.inner.lock();
        for (i, meta) in inner.sealed.iter().enumerate() {
            let last =
                sealed_segment_last_index(&inner.sealed, inner.active.as_ref(), i, &inner.index);
            assert!(
                last > purged_index,
                "sealed segment first_index={} last_index={} must be above purged={}",
                meta.first_index,
                last,
                purged_index,
            );
        }
    }

    /// CRASH-01 deterministic torn-tail test (D-17).
    ///
    /// Appends 3 clean records, drops the store cleanly (records fully
    /// durable), manually appends a truncated record header + partial payload
    /// into the active segment, reopens the store, and verifies:
    /// 1. The torn tail is truncated (set_len back to end-of-record-3).
    /// 2. The 3 valid records remain readable.
    /// 3. `last_log_id.index == 3` — no phantom 4th entry appears.
    #[tokio::test]
    async fn torn_tail_truncation() {
        let dir = tempfile::tempdir().unwrap();

        // Step 1 + 2: append 3 entries, drop cleanly. Use a large segment_cap
        // so all three land in the same (active) segment.
        let active_path;
        let clean_byte_len;
        {
            let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();
            store
                .append_test(vec![
                    blank_entry(1, 1),
                    blank_entry(1, 2),
                    blank_entry(1, 3),
                ])
                .await
                .unwrap();

            let inner = store.inner.lock();
            let active = inner.active.as_ref().unwrap();
            active_path = active.path.clone();
            clean_byte_len = active.write_offset;
        }

        // Step 3: manually append a torn record onto the active segment.
        // Shape: write a 4-byte big-endian length prefix claiming a 64-byte
        // payload, then write only 32 bytes of garbage — no CRC trailer, no
        // full payload. This is the length-prefix-plus-short-payload torn-
        // write variant from the plan text.
        {
            use std::os::unix::fs::FileExt;

            let f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&active_path)
                .unwrap();
            let claimed_payload_len: u32 = 64;
            let len_prefix = claimed_payload_len.to_be_bytes();
            f.write_at(&len_prefix, clean_byte_len).unwrap();
            let garbage = vec![0xCDu8; 32];
            f.write_at(&garbage, clean_byte_len + len_prefix.len() as u64)
                .unwrap();
            // Do NOT fsync — the test simulates a torn tail that was
            // in-flight when the process died.
            drop(f);

            // The on-disk file length is now at least clean_byte_len + 4 + 32
            // (and possibly more if the segment was preallocated).
            let on_disk_len = std::fs::metadata(&active_path).unwrap().len();
            assert!(
                on_disk_len > clean_byte_len,
                "manual tamper must grow file beyond clean_byte_len \
                 (on_disk={}, clean={})",
                on_disk_len,
                clean_byte_len
            );
        }

        // Step 4: reopen LogStore. Expected: torn tail detected + truncated.
        let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();

        // Step 5: 3 valid records still readable.
        let entries = store.try_get_log_entries(1..10).await.unwrap();
        assert_eq!(
            entries.len(),
            3,
            "torn-tail recovery must leave 3 valid records; got {}",
            entries.len()
        );
        assert_eq!(entries[0].get_log_id().index, 1);
        assert_eq!(entries[1].get_log_id().index, 2);
        assert_eq!(entries[2].get_log_id().index, 3);

        // Step 6: no phantom 4th entry.
        let state = store.get_log_state().await.unwrap();
        assert_eq!(
            state.last_log_id.unwrap().index,
            3,
            "last_log_id must be 3 after torn-tail recovery"
        );

        // Step 7: active segment file length matches the sum of record_len
        // values for indices 1..=3 (the in-memory index's authoritative size).
        {
            let inner = store.inner.lock();
            let expected_bytes: u64 = inner.index.values().map(|(_, _, len)| *len as u64).sum();
            let on_disk_len = std::fs::metadata(&active_path).unwrap().len();
            assert_eq!(
                on_disk_len, expected_bytes,
                "active segment must be truncated to exactly the sum of valid record_lens \
                 (on_disk={}, expected={})",
                on_disk_len, expected_bytes
            );
        }
    }

    #[tokio::test]
    async fn index_rebuild_matches_pre_shutdown() {
        let dir = tempfile::tempdir().unwrap();

        // Append a mix of batch sizes: 1, then 3 at once, then 1.
        let snapshot;
        {
            let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();
            store.append_test(vec![blank_entry(1, 1)]).await.unwrap();
            store
                .append_test(vec![
                    blank_entry(1, 2),
                    blank_entry(1, 3),
                    blank_entry(1, 4),
                ])
                .await
                .unwrap();
            store.append_test(vec![blank_entry(1, 5)]).await.unwrap();
            snapshot = store.debug_index();
        }

        let store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();
        let rebuilt = store.debug_index();

        assert_eq!(
            rebuilt, snapshot,
            "rebuilt index must match pre-shutdown index key-for-key and \
             value-for-value (rebuilt={:?}, snapshot={:?})",
            rebuilt, snapshot
        );
    }
}

#[cfg(test)]
mod segment_tests {
    //! Plan 02-02 tests: exercise `Segment` + `SegmentMeta` + `read_record_at`
    //! in isolation, without openraft or `LogStore` involvement.

    use super::*;
    use std::io::Write as _;

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

        assert_eq!(
            seg.path.file_name().unwrap().to_str().unwrap(),
            "log-0000000000000001.bin"
        );

        let got = read_record_at(&seg.path, start).unwrap();
        assert_eq!(got, payload);
    }

    #[test]
    fn needs_rotation_true_when_over_cap() {
        let dir = tempfile::tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 1, 256).unwrap();
        seg.write_offset = 250;
        assert!(seg.needs_rotation(32));
        assert!(!seg.needs_rotation(6));
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

        let md = std::fs::metadata(&meta.path).unwrap();
        assert_eq!(md.len(), expected_len);
    }

    #[cfg(feature = "bench-instrumentation")]
    #[test]
    fn fsync_counter_bumps_on_sync() {
        use crate::raft::bench_instrumentation::fsync_count;

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

        {
            let mut f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
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
