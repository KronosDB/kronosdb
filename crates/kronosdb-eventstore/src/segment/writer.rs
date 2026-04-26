use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::error::Error;
use crate::event::{AppendEvent, Position, StoredEvent};

use crate::segment::{
    RECORD_HEADER_SIZE, SEGMENT_HEADER_SIZE, SEGMENT_MAGIC, SEGMENT_VERSION, flags, format,
    segment_path,
};
use crate::segment::format::RaftMarker;

#[cfg(feature = "bench-instrumentation")]
use crate::raft::bench_instrumentation::{self as bi, Region, Timer};

/// Writes events to segmented append-only log files.
///
/// Manages the active segment, handles rotation when a segment fills up,
/// and ensures data is durable on disk via fdatasync.
///
/// This is NOT thread-safe — it's designed to be owned by a single writer thread.
/// Readers access segments independently via mmap or file I/O.
pub struct SegmentWriter {
    /// Directory where segment files are stored.
    dir: PathBuf,
    /// Maximum size of a segment file in bytes.
    max_segment_size: u64,
    /// The currently active (writable) segment file.
    active_file: File,
    /// Base position of the active segment (the position of the first event in this segment).
    active_base_position: u64,
    /// Current write offset within the active segment file.
    write_offset: u64,
    /// The next position to assign to an event.
    next_position: Position,
    /// Reusable buffer for serializing events (avoids repeated allocation).
    serialize_buf: Vec<u8>,
    /// Reusable buffer for building the full record (header + payload).
    record_buf: Vec<u8>,
}

impl SegmentWriter {
    /// Creates a new SegmentWriter, starting a fresh segment log in the given directory.
    ///
    /// `start_position` is the position to assign to the first event written.
    /// For a new database this is Position(0). For recovery, it's the position
    /// after the last valid event found during recovery.
    pub fn new(dir: &Path, start_position: Position, max_segment_size: u64) -> Result<Self, Error> {
        std::fs::create_dir_all(dir)?;

        let base_position = start_position.0;
        let path = segment_path(dir, base_position);
        let mut file = create_segment_file(&path)?;
        write_segment_header(&mut file, base_position)?;
        preallocate(&file, max_segment_size);

        Ok(Self {
            dir: dir.to_path_buf(),
            max_segment_size,
            active_file: file,
            active_base_position: base_position,
            write_offset: SEGMENT_HEADER_SIZE as u64,
            next_position: start_position,
            serialize_buf: Vec::with_capacity(4096),
            record_buf: Vec::with_capacity(4096),
        })
    }

    /// Opens an existing segment log for appending.
    ///
    /// Finds the latest segment in the directory, validates it, and positions
    /// the writer at the end of valid data. Used during recovery.
    pub fn open(dir: &Path, max_segment_size: u64) -> Result<Self, Error> {
        let mut segments = list_segments(dir)?;
        if segments.is_empty() {
            return Err(Error::Corrupted {
                message: "no segment files found in directory".into(),
            });
        }

        // Sort by base position, pick the latest segment.
        segments.sort();
        let latest_base = *segments.last().unwrap();
        let path = segment_path(dir, latest_base);

        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;

        // Validate the header.
        let header = read_segment_header(&mut file)?;
        if header.base_position != latest_base {
            return Err(Error::Corrupted {
                message: format!(
                    "segment header base_position {} doesn't match filename {}",
                    header.base_position, latest_base
                ),
            });
        }

        // Scan forward to find the last valid record, recovering from torn writes.
        let (write_offset, next_position) = recover_segment(&mut file)?;

        // Truncate any torn write garbage / pre-allocated space at the end,
        // then re-preallocate for future writes.
        file.set_len(write_offset)?;
        preallocate(&file, max_segment_size);

        // CRITICAL: `set_len` / `preallocate` do NOT reposition the file cursor.
        // `recover_segment` left the cursor at end-of-scan (past `write_offset`
        // when recovery truncated torn records or stopped at an orphan marker).
        // Subsequent writes through `write_record` use `write_all` on the file
        // directly, which writes at the cursor position — so without this seek,
        // new records would land past `write_offset` with a sparse hole from
        // `write_offset` up to the stale cursor, and the in-memory `write_offset`
        // would diverge from the actual disk offset. The reader iterator would
        // then see records in the hole as `record_len == 0` and stop early.
        use std::io::Seek;
        file.seek(std::io::SeekFrom::Start(write_offset))?;

        Ok(Self {
            dir: dir.to_path_buf(),
            max_segment_size,
            active_file: file,
            active_base_position: latest_base,
            write_offset,
            next_position,
            serialize_buf: Vec::with_capacity(4096),
            record_buf: Vec::with_capacity(4096),
        })
    }

    /// Appends a batch of events to the log and fsyncs immediately.
    ///
    /// All events are written and then fdatasync'd together.
    /// For higher throughput under concurrent load, use `write_events` + `sync`
    /// separately to batch fsyncs across multiple callers.
    pub fn append(&mut self, events: &[AppendEvent]) -> Result<(Position, u32), Error> {
        #[cfg(feature = "bench-instrumentation")]
        let _t = Timer::new(Region::SegmentAppend);
        let result = self.write_events(events)?;
        if result.1 > 0 {
            self.sync()?;
        }
        Ok(result)
    }

    /// Writes events to the segment WITHOUT fsyncing.
    ///
    /// Events are written to the OS page cache but not guaranteed durable.
    /// Call `sync()` after to make them durable. This enables group commit:
    /// multiple callers write events, then a single `sync()` makes them all durable.
    pub fn write_events(&mut self, events: &[AppendEvent]) -> Result<(Position, u32), Error> {
        if events.is_empty() {
            return Ok((self.next_position, 0));
        }

        let first_position = self.next_position;

        for event in events {
            self.write_one_event(event)?;
        }

        Ok((first_position, events.len() as u32))
    }

    /// Writes a Raft log entry — a `RaftMarker` followed, for Normal entries,
    /// by `event_count` event records. The marker and all its events are
    /// written as one atomic unit: if they don't fit in the current segment,
    /// we rotate first. This guarantees a Raft entry never straddles a
    /// segment boundary, which keeps sealed segments self-contained.
    ///
    /// Returns the first-event position (if any events) and the count.
    /// For Membership/Blank entries (no events), first_position is the
    /// current next_position and count is 0.
    pub fn write_raft_entry(
        &mut self,
        marker: &RaftMarker,
        events: &[AppendEvent],
    ) -> Result<(Position, u32), Error> {
        // Compute the total bytes we need. Rotate up front if the whole
        // entry doesn't fit — this prevents a marker from ending up in one
        // segment and its events in another.
        let marker_len = estimate_marker_size(marker);
        let events_len: usize = events
            .iter()
            .map(|e| RECORD_HEADER_SIZE + estimate_event_size(e))
            .sum();
        let total = marker_len + events_len;

        if self.write_offset + total as u64 > self.max_segment_size {
            self.rotate_segment()?;
        }

        // Write the marker first.
        self.serialize_buf.clear();
        format::serialize_raft_marker(marker, &mut self.serialize_buf);
        let payload = std::mem::take(&mut self.serialize_buf);
        self.write_record(flags::RAFT_MARKER, &payload)?;
        self.serialize_buf = payload;

        let first_position = self.next_position;
        for event in events {
            self.write_one_event(event)?;
        }

        Ok((first_position, events.len() as u32))
    }

    /// Writes a single event record, rotating the segment if needed.
    fn write_one_event(&mut self, event: &AppendEvent) -> Result<(), Error> {
        let stored = StoredEvent {
            position: self.next_position,
            identifier: event.identifier.clone(),
            name: event.name.clone(),
            version: event.version.clone(),
            timestamp: event.timestamp,
            payload: event.payload.clone(),
            metadata: event.metadata.clone(),
            tags: event.tags.clone(),
        };

        self.serialize_buf.clear();
        format::serialize_event(&stored, &mut self.serialize_buf);
        let payload = std::mem::take(&mut self.serialize_buf);
        self.write_record(flags::EVENT, &payload)?;
        self.serialize_buf = payload;

        self.next_position = self.next_position.next();
        Ok(())
    }

    /// Low-level: writes a single record to the active segment.
    /// Rotates the segment if the record doesn't fit.
    fn write_record(&mut self, flags_byte: u8, payload: &[u8]) -> Result<(), Error> {
        let payload_with_flags_len = 1 + payload.len();
        let total_record_size = RECORD_HEADER_SIZE + payload.len();

        if self.write_offset + total_record_size as u64 > self.max_segment_size {
            self.rotate_segment()?;
        }

        let crc = {
            let mut digest = crc32c::crc32c(&[flags_byte]);
            digest = crc32c::crc32c_append(digest, payload);
            digest
        };

        self.record_buf.clear();
        self.record_buf.extend_from_slice(&crc.to_le_bytes());
        self.record_buf
            .extend_from_slice(&(payload_with_flags_len as u32).to_le_bytes());
        self.record_buf.push(flags_byte);
        self.record_buf.extend_from_slice(payload);

        self.active_file.write_all(&self.record_buf)?;
        self.write_offset += total_record_size as u64;
        Ok(())
    }

    /// Fsyncs the active segment to disk, making all written events durable.
    pub fn sync(&mut self) -> Result<(), Error> {
        fdatasync(&self.active_file)?;
        #[cfg(feature = "bench-instrumentation")]
        bi::bump_fsync();
        Ok(())
    }

    /// Returns the current head position (next position to be assigned).
    pub fn head(&self) -> Position {
        self.next_position
    }

    /// Returns the base position of the currently active segment.
    pub fn active_base_position(&self) -> u64 {
        self.active_base_position
    }

    /// Returns the path to the currently active segment file.
    pub fn active_segment_path(&self) -> PathBuf {
        segment_path(&self.dir, self.active_base_position)
    }

    /// Rotates to a new segment file.
    /// Builds the per-segment `.idx` and `.bloom` files for the sealed segment.
    fn rotate_segment(&mut self) -> Result<(), Error> {
        // Sync the current segment before sealing.
        fdatasync(&self.active_file)?;
        #[cfg(feature = "bench-instrumentation")]
        bi::bump_fsync();

        // Truncate the sealed segment to its actual data size.
        // It was pre-allocated to max_segment_size, so we trim the unused space.
        self.active_file.set_len(self.write_offset)?;

        // Build per-segment index for the sealed segment.
        let sealed_path = segment_path(&self.dir, self.active_base_position);
        let index = super::segment_index::SegmentIndex::build_from_segment(&sealed_path)?;
        index.write_to_disk(&sealed_path)?;

        // New segment starts at the current next_position.
        let new_base = self.next_position.0;
        let path = segment_path(&self.dir, new_base);
        let mut file = create_segment_file(&path)?;
        write_segment_header(&mut file, new_base)?;
        preallocate(&file, self.max_segment_size);

        self.active_file = file;
        self.active_base_position = new_base;
        self.write_offset = SEGMENT_HEADER_SIZE as u64;

        Ok(())
    }
}

/// Segment file header as read from disk.
struct SegmentHeader {
    base_position: u64,
}

/// Upper bound on the serialized size of a Raft marker record (header + payload).
fn estimate_marker_size(marker: &RaftMarker) -> usize {
    // Fixed marker payload is 23 bytes; extra is variable.
    RECORD_HEADER_SIZE + 23 + marker.extra.len()
}

/// Upper bound on the serialized size of an event payload (without record header).
/// Matches the layout in format::serialize_event.
fn estimate_event_size(event: &AppendEvent) -> usize {
    let mut n = 8 // position
        + 2 + event.identifier.len()
        + 2 + event.name.len()
        + 2 + event.version.len()
        + 8 // timestamp
        + 2; // metadata_count
    for (k, v) in &event.metadata {
        n += 2 + k.len() + 2 + v.len();
    }
    n += 2; // tag_count
    for tag in &event.tags {
        n += 2 + tag.key.len() + 2 + tag.value.len();
    }
    n += 4 + event.payload.len(); // payload_len + payload
    n
}

fn create_segment_file(path: &Path) -> Result<File, io::Error> {
    OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)
}

fn write_segment_header(file: &mut File, base_position: u64) -> Result<(), io::Error> {
    file.write_all(&SEGMENT_MAGIC)?;
    file.write_all(&[SEGMENT_VERSION])?;
    file.write_all(&base_position.to_le_bytes())?;
    Ok(())
}

fn read_segment_header(file: &mut File) -> Result<SegmentHeader, Error> {
    use std::io::Read;

    let mut header_buf = [0u8; SEGMENT_HEADER_SIZE];
    file.read_exact(&mut header_buf)
        .map_err(|_| Error::Corrupted {
            message: "failed to read segment header".into(),
        })?;

    if header_buf[0..4] != SEGMENT_MAGIC {
        return Err(Error::Corrupted {
            message: "invalid segment magic bytes".into(),
        });
    }
    if header_buf[4] != SEGMENT_VERSION {
        return Err(Error::Corrupted {
            message: format!("unsupported segment version: {}", header_buf[4]),
        });
    }
    let base_position = u64::from_le_bytes(header_buf[5..13].try_into().unwrap());

    Ok(SegmentHeader { base_position })
}

/// Scans the segment from after the header, validating CRCs and enforcing
/// marker-group atomicity.
///
/// Returns `(write_offset, next_position)` — the offset to resume writing
/// and the next position to assign.
///
/// # Marker-authoritative recovery
///
/// A Raft apply fsyncs as a single unit: one `RaftMarker` record followed
/// (for `Normal` entries) by exactly `event_count` event records. A crash
/// mid-fsync can leave three possible on-disk states for a group:
///
/// 1. **Marker not durable** — nothing from this group reached disk; the
///    previous complete group is the tail.
/// 2. **Marker durable, 0..k events durable** (where `k < event_count`) —
///    orphan events with no matching acknowledged Raft log entry. These
///    MUST be truncated: Raft replay from `committed+1` will re-issue the
///    apply, and keeping orphans would either double-apply or diverge the
///    event segment from the cluster (root cause of CRASH-02 Shape 1 /
///    Shape 2 in the three-node convergence tests).
/// 3. **Marker + all `event_count` events durable** — complete group;
///    keep in full.
///
/// Algorithm: walk records, tracking `last_complete_offset` (the tail of
/// the last fully-committed group). When a marker opens a group, it sets
/// `pending_events_remaining` to `event_count`; each subsequent event
/// decrements it. Only when the counter reaches zero do we advance
/// `last_complete_offset`. On corruption, CRC failure, or a marker
/// arriving mid-group, we stop and return `last_complete_offset` — the
/// orphan tail is truncated by the caller via `file.set_len(write_offset)`.
///
/// # Legacy / snapshot-install fallback
///
/// Segments written via `SegmentWriter::append()` (the non-Raft path used
/// by the snapshot-install rebuild in `state_machine::install_snapshot`)
/// contain no markers. For those, marker-authoritative recovery would
/// truncate every event. When we complete the scan without seeing any
/// marker, we fall back to the pre-Option-D behavior: accept every
/// CRC-valid event record up to the first torn/corrupt record.
fn recover_segment(file: &mut File) -> Result<(u64, Position), Error> {
    use std::io::{Read, Seek, SeekFrom};

    let file_len = file.seek(SeekFrom::End(0))?;
    file.seek(SeekFrom::Start(SEGMENT_HEADER_SIZE as u64))?;

    // Marker-group tracking.
    let mut offset = SEGMENT_HEADER_SIZE as u64;
    let mut last_complete_offset = offset;
    let mut last_complete_position: Option<Position> = None;
    // Fallback tracking (no markers in segment).
    let mut fallback_tail_offset = offset;
    let mut fallback_last_position: Option<Position> = None;
    let mut seen_any_marker = false;
    // Pending-group state.
    let mut pending_events_remaining: u32 = 0;
    let mut pending_last_position: Option<Position> = None;
    // Set to true when we observe a malformed record mid-scan; disables
    // the zero-markers fallback (the corruption is real, not a marker-vs-no-marker
    // ambiguity).
    let mut corruption_seen = false;

    while offset + RECORD_HEADER_SIZE as u64 <= file_len {
        // Read the record header.
        let mut header_buf = [0u8; RECORD_HEADER_SIZE];
        if file.read_exact(&mut header_buf).is_err() {
            corruption_seen = true;
            break;
        }

        let stored_crc = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
        let record_len = u32::from_le_bytes(header_buf[4..8].try_into().unwrap()) as usize;
        let flags_byte = header_buf[8];

        // Sanity check record length.
        if record_len < 1 || offset + RECORD_HEADER_SIZE as u64 + (record_len - 1) as u64 > file_len
        {
            corruption_seen = true;
            break; // Torn write — stop here.
        }

        // Read flags + payload (record_len includes the flags byte we already have in header).
        let payload_len = record_len - 1; // subtract the flags byte
        let mut payload_buf = vec![0u8; payload_len];
        if file.read_exact(&mut payload_buf).is_err() {
            corruption_seen = true;
            break;
        }

        // Verify CRC over flags + payload.
        let computed_crc = {
            let mut digest = crc32c::crc32c(&[flags_byte]);
            digest = crc32c::crc32c_append(digest, &payload_buf);
            digest
        };

        if computed_crc != stored_crc {
            corruption_seen = true;
            break; // Corruption or torn write — stop here.
        }

        let record_end = offset + RECORD_HEADER_SIZE as u64 + payload_len as u64;

        // Fallback bookkeeping: track the furthest CRC-valid event record,
        // regardless of marker-group state. Used only when the whole segment
        // contains no markers.
        if flags::is_event(flags_byte) && payload_buf.len() >= 8 {
            let position = u64::from_le_bytes(payload_buf[0..8].try_into().unwrap());
            fallback_last_position = Some(Position(position));
            fallback_tail_offset = record_end;
        }

        if flags::is_raft_marker(flags_byte) {
            // A marker arriving while a previous group is still pending means
            // the previous group's event tail was truncated by a crash. Stop
            // at `last_complete_offset` — the orphan marker and any events
            // after it are dropped.
            if pending_events_remaining > 0 {
                break;
            }
            seen_any_marker = true;

            match format::deserialize_raft_marker(&payload_buf) {
                Ok((marker, _)) => {
                    if marker.event_count == 0 {
                        // Blank / Membership entry: group is trivially complete.
                        last_complete_offset = record_end;
                        // No event written; `last_complete_position` unchanged.
                    } else {
                        pending_events_remaining = marker.event_count as u32;
                        pending_last_position = None;
                        // Do NOT yet advance `last_complete_offset` — we only
                        // commit the group after all `event_count` events.
                    }
                }
                Err(_) => {
                    // Deserialize failure after CRC pass is unexpected; be
                    // conservative and stop at the last complete group.
                    corruption_seen = true;
                    break;
                }
            }
        } else if flags::is_event(flags_byte) {
            if pending_events_remaining > 0 {
                // Event belongs to the currently pending marker-group.
                if payload_buf.len() >= 8 {
                    let position = u64::from_le_bytes(payload_buf[0..8].try_into().unwrap());
                    pending_last_position = Some(Position(position));
                }
                pending_events_remaining -= 1;
                if pending_events_remaining == 0 {
                    // Group complete — commit it.
                    last_complete_offset = record_end;
                    if let Some(pos) = pending_last_position.take() {
                        last_complete_position = Some(pos);
                    }
                }
            } else if !seen_any_marker {
                // Zero-markers fallback (legacy/snapshot-install rebuild):
                // accept every CRC-valid event. `last_complete_offset` is
                // updated so that, should we later encounter a marker, we
                // correctly treat this prefix as committed; but the primary
                // authority for the fallback is `fallback_tail_offset`.
                if payload_buf.len() >= 8 {
                    let position = u64::from_le_bytes(payload_buf[0..8].try_into().unwrap());
                    last_complete_position = Some(Position(position));
                }
                last_complete_offset = record_end;
            } else {
                // Event after a complete group, with no opening marker for
                // this event → orphan. Stop.
                break;
            }
        }
        // Other record types (none defined today): ignore, do not advance
        // any committed offset.

        offset = record_end;
    }

    // Zero-markers fallback: a segment that made it through the scan with
    // no markers and no mid-stream corruption uses pre-Option-D behavior.
    // This path is exercised by the snapshot-install rebuild, where
    // `EventStoreEngine::append` writes events without a Raft marker.
    let (final_offset, final_position) = if !seen_any_marker && !corruption_seen {
        (fallback_tail_offset, fallback_last_position)
    } else {
        (last_complete_offset, last_complete_position)
    };

    let next_position = match final_position {
        Some(pos) => pos.next(),
        None => {
            // No valid records in this segment. Read the base position from the header.
            file.seek(SeekFrom::Start(0))?;
            let header = read_segment_header(file)?;
            Position(header.base_position)
        }
    };

    Ok((final_offset, next_position))
}

/// Pre-allocates disk space for a file.
///
/// On Linux, uses fallocate to reserve contiguous blocks without writing zeros.
/// On other platforms, falls back to setting the file length (which may write zeros).
///
/// Pre-allocation has two benefits:
/// 1. Contiguous blocks on disk → better sequential read/write performance
/// 2. File size doesn't change on each append → fdatasync skips metadata update
///
/// Errors are silently ignored — pre-allocation is an optimization, not a requirement.
fn preallocate(file: &File, size: u64) {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        unsafe {
            libc::fallocate(file.as_raw_fd(), 0, 0, size as i64);
        }
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = file.set_len(size);
    }
}

/// Flushes data to disk.
///
/// On Linux, uses fdatasync (skips metadata sync — faster than fsync).
/// On macOS, uses F_FULLFSYNC via fcntl (the only way to guarantee
/// data hits the physical disk, not just the drive's write cache).
/// On other platforms, falls back to sync_data().
fn fdatasync(file: &File) -> Result<(), io::Error> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        let ret = unsafe { libc::fdatasync(file.as_raw_fd()) };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        let ret = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_FULLFSYNC) };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        file.sync_data()
    }
}

/// Lists all segment base positions in a directory.
fn list_segments(dir: &Path) -> Result<Vec<u64>, io::Error> {
    let mut positions = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if let Some(base) = super::base_position_from_path(&path) {
            positions.push(base);
        }
    }
    Ok(positions)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Tag;
    use crate::segment::DEFAULT_SEGMENT_SIZE;

    fn make_event(name: &str, payload: &[u8]) -> AppendEvent {
        AppendEvent {
            identifier: format!("id-{name}"),
            name: name.into(),
            version: "1.0".into(),
            timestamp: 1712345678000,
            payload: payload.to_vec(),
            metadata: vec![],
            tags: vec![Tag::from_str("test", "true")],
        }
    }

    #[test]
    fn write_and_recover() {
        let dir = tempfile::tempdir().unwrap();

        // Write some events.
        let mut writer = SegmentWriter::new(dir.path(), Position(0), DEFAULT_SEGMENT_SIZE).unwrap();

        let events = vec![
            make_event("OrderPlaced", b"order-1"),
            make_event("PaymentReceived", b"payment-1"),
            make_event("OrderShipped", b"ship-1"),
        ];

        let (first_pos, count) = writer.append(&events).unwrap();
        assert_eq!(first_pos, Position(0));
        assert_eq!(count, 3);
        assert_eq!(writer.head(), Position(3));

        // Drop the writer and reopen via recovery.
        drop(writer);

        let recovered = SegmentWriter::open(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap();
        assert_eq!(recovered.head(), Position(3));
    }

    #[test]
    fn raft_entry_writes_marker_and_events() {
        use crate::segment::format::{RaftEntryType, RaftMarker};
        use crate::segment::reader::SegmentReader;

        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::new(dir.path(), Position(0), DEFAULT_SEGMENT_SIZE).unwrap();

        // Write: marker(normal, 2 events), marker(blank), marker(normal, 1 event).
        let events1 = vec![
            make_event("OrderPlaced", b"o1"),
            make_event("PaymentReceived", b"p1"),
        ];
        let (first, count) = writer
            .write_raft_entry(&RaftMarker::normal(1, 1, 2), &events1)
            .unwrap();
        assert_eq!(first, Position(0));
        assert_eq!(count, 2);

        let (_, count) = writer
            .write_raft_entry(&RaftMarker::blank(1, 2), &[])
            .unwrap();
        assert_eq!(count, 0);

        let events2 = vec![make_event("OrderShipped", b"s1")];
        let (first, _) = writer
            .write_raft_entry(&RaftMarker::normal(1, 3, 1), &events2)
            .unwrap();
        assert_eq!(first, Position(2));

        writer.sync().unwrap();
        let seg_path = writer.active_segment_path();
        drop(writer);

        // Event iterator should skip markers — returns 3 events total.
        let reader = SegmentReader::open(&seg_path).unwrap();
        let events: Vec<_> = reader
            .iter(None)
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].name, "OrderPlaced");
        assert_eq!(events[1].name, "PaymentReceived");
        assert_eq!(events[2].name, "OrderShipped");

        // Raft marker iterator should see all 3 markers.
        let markers: Vec<_> = reader
            .iter_raft_markers()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(markers.len(), 3);
        assert_eq!(markers[0].1.index, 1);
        assert_eq!(markers[0].1.event_count, 2);
        assert_eq!(markers[0].1.entry_type, RaftEntryType::Normal);
        assert_eq!(markers[1].1.index, 2);
        assert_eq!(markers[1].1.entry_type, RaftEntryType::Blank);
        assert_eq!(markers[2].1.index, 3);
        assert_eq!(markers[2].1.event_count, 1);
    }

    #[test]
    fn raft_entry_survives_recovery() {
        use crate::segment::format::RaftMarker;

        let dir = tempfile::tempdir().unwrap();
        {
            let mut writer =
                SegmentWriter::new(dir.path(), Position(0), DEFAULT_SEGMENT_SIZE).unwrap();
            writer
                .write_raft_entry(
                    &RaftMarker::normal(1, 1, 2),
                    &[
                        make_event("OrderPlaced", b"a"),
                        make_event("PaymentReceived", b"b"),
                    ],
                )
                .unwrap();
            writer
                .write_raft_entry(&RaftMarker::blank(1, 2), &[])
                .unwrap();
            writer.sync().unwrap();
        }

        // Reopen — recovery should skip the marker (non-event record) and set
        // next_position based on the last event record's position.
        let recovered = SegmentWriter::open(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap();
        assert_eq!(recovered.head(), Position(2));
    }

    #[test]
    fn empty_append() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::new(dir.path(), Position(0), DEFAULT_SEGMENT_SIZE).unwrap();

        let (first_pos, count) = writer.append(&[]).unwrap();
        assert_eq!(first_pos, Position(0));
        assert_eq!(count, 0);
        assert_eq!(writer.head(), Position(0));
    }

    #[test]
    fn segment_rotation() {
        let dir = tempfile::tempdir().unwrap();

        // Use a tiny segment size to force rotation.
        let tiny_segment = SEGMENT_HEADER_SIZE as u64 + 200;
        let mut writer = SegmentWriter::new(dir.path(), Position(0), tiny_segment).unwrap();

        // Write events until we get a rotation.
        for i in 0..5 {
            let event = make_event(&format!("Event{i}"), &vec![0u8; 50]);
            writer.append(&[event]).unwrap();
        }

        // We should have multiple segment files.
        let segments = list_segments(dir.path()).unwrap();
        assert!(
            segments.len() > 1,
            "expected segment rotation, got {} segments",
            segments.len()
        );

        // Recovery should still find the correct head.
        let head = writer.head();
        drop(writer);

        let recovered = SegmentWriter::open(dir.path(), tiny_segment).unwrap();
        assert_eq!(recovered.head(), head);
    }
}
