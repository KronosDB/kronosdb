use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::error::Error;
use crate::event::{AppendEvent, Position, StoredEvent};

use crate::segment::format::EventIndexFields;
use crate::segment::segment_index::SegmentIndex;
use crate::segment::{
    RECORD_HEADER_SIZE, SEGMENT_HEADER_SIZE, SEGMENT_MAGIC, SEGMENT_VERSION, flags, format, record,
    segment_path,
};

#[cfg(feature = "bench-instrumentation")]
use crate::raft::bench_instrumentation::{self as bi, Region, Timer};

/// Result of applying a frame of raw replicated segment records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawAppendResult {
    /// First event position expected before the frame was applied.
    pub first_position: Position,
    /// Number of event records in the frame (control/marker records excluded).
    pub event_count: u32,
    /// Next-exclusive local cursor after the frame.
    pub durable_position: Position,
    /// Raw byte range appended within the active segment.
    pub byte_start: u64,
    pub byte_end: u64,
    /// Index projections for the applied events. The engine feeds these into
    /// its active `TagIndex`; payloads are absent by design.
    pub events: Vec<EventIndexFields>,
}

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
    /// Index of the active segment, maintained incrementally as events are
    /// written. Sealing serializes this instead of re-reading the segment
    /// file. Shared (behind RwLock) so reads can direct-seek into the active
    /// segment without taking the writer lock.
    active_index: Arc<RwLock<SegmentIndex>>,
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
            active_index: Arc::new(RwLock::new(SegmentIndex::new(base_position))),
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

        // Rebuild the active segment's in-memory index from its recovered
        // records (one-time boot cost, bounded by max_segment_size). The
        // truncate + preallocate above guarantees iteration stops at the
        // recovered tail (zeroed record header).
        let active_index = SegmentIndex::build_from_segment(&path)?;

        Ok(Self {
            dir: dir.to_path_buf(),
            max_segment_size,
            active_file: file,
            active_base_position: latest_base,
            write_offset,
            next_position,
            serialize_buf: Vec::with_capacity(4096),
            record_buf: Vec::with_capacity(4096),
            active_index: Arc::new(RwLock::new(active_index)),
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

    /// Writes a native control record without consuming an event position or
    /// fsyncing. The caller commits it through the normal group-commit wave.
    pub fn write_control(&mut self, control: &format::ControlRecord) -> Result<(), Error> {
        self.serialize_buf.clear();
        let flags_byte = format::serialize_control(control, &mut self.serialize_buf);
        let payload = std::mem::take(&mut self.serialize_buf);
        self.write_record(flags_byte, &payload)?;
        self.serialize_buf = payload;
        Ok(())
    }

    /// Applies already-framed segment records received from the leader.
    ///
    /// The bytes are CRC-checked and event positions are verified before any
    /// write happens, then written verbatim in one `write_all`. Event payloads
    /// are never materialized: only position, name, and tags are partially
    /// decoded to maintain the active index. Rotation is leader-decided and
    /// must arrive separately via `rotate_replicated`; a frame that does not
    /// fit the active segment is rejected rather than silently rotating and
    /// breaking byte equality.
    pub fn append_raw_replicated(
        &mut self,
        bytes: &[u8],
        first_position: Position,
    ) -> Result<RawAppendResult, Error> {
        if first_position != self.next_position {
            return Err(Error::Corrupted {
                message: format!(
                    "replicated frame starts at position {}, local tail is {}",
                    first_position.0, self.next_position.0
                ),
            });
        }
        if self.write_offset + bytes.len() as u64 > self.max_segment_size {
            return Err(Error::Corrupted {
                message: format!(
                    "replicated frame exceeds active segment {} without Rotate",
                    self.active_base_position
                ),
            });
        }

        // Validate the entire frame before mutating disk or indexes. Store the
        // tiny index projections and record-relative offsets; payload bytes
        // remain borrowed from the frame and are never copied.
        let mut cursor = 0usize;
        let mut expected = first_position;
        let mut events: Vec<(usize, EventIndexFields)> = Vec::new();
        while cursor < bytes.len() {
            if bytes.len() - cursor < RECORD_HEADER_SIZE {
                return Err(Error::Corrupted {
                    message: "replicated frame ends inside a record header".into(),
                });
            }
            let record_start = cursor;
            let stored_crc = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap());
            let record_len =
                u32::from_le_bytes(bytes[cursor + 4..cursor + 8].try_into().unwrap()) as usize;
            if record_len < 1 {
                return Err(Error::Corrupted {
                    message: "replicated record has zero length".into(),
                });
            }
            let record_end = cursor
                .checked_add(8)
                .and_then(|v| v.checked_add(record_len))
                .ok_or_else(|| Error::Corrupted {
                    message: "replicated record length overflow".into(),
                })?;
            if record_end > bytes.len() {
                return Err(Error::Corrupted {
                    message: "replicated frame ends inside a record payload".into(),
                });
            }

            let flags_byte = bytes[cursor + 8];
            let payload = &bytes[cursor + RECORD_HEADER_SIZE..record_end];
            let computed_crc = crc32c::crc32c_append(crc32c::crc32c(&[flags_byte]), payload);
            if computed_crc != stored_crc {
                return Err(Error::Corrupted {
                    message: format!("CRC mismatch in replicated record at frame byte {cursor}"),
                });
            }

            if flags::is_event(flags_byte) {
                let (fields, consumed) = format::deserialize_event_index_fields(payload)?;
                if consumed != payload.len() {
                    return Err(Error::Corrupted {
                        message: "replicated event has trailing bytes".into(),
                    });
                }
                if fields.position != expected {
                    return Err(Error::Corrupted {
                        message: format!(
                            "replicated event position {}, expected {}",
                            fields.position.0, expected.0
                        ),
                    });
                }
                expected = expected.next();
                events.push((record_start, fields));
            }
            cursor = record_end;
        }

        let byte_start = self.write_offset;
        self.active_file.write_all(bytes)?;
        self.write_offset += bytes.len() as u64;

        // Publish index entries only after the full frame write succeeds, so
        // a concurrent reader can never seek to an incomplete record.
        {
            let mut index = self.active_index.write();
            for (relative_offset, fields) in &events {
                index.insert_event(
                    fields.position.0,
                    byte_start + *relative_offset as u64,
                    &fields.name,
                    &fields.tags,
                );
            }
        }
        self.next_position = expected;

        let projections = events.into_iter().map(|(_, fields)| fields).collect();
        Ok(RawAppendResult {
            first_position,
            event_count: (expected.0 - first_position.0) as u32,
            durable_position: expected,
            byte_start,
            byte_end: self.write_offset,
            events: projections,
        })
    }

    /// Mirrors a leader-decided segment rotation. The current segment is
    /// synced, truncated to its exact byte length, and indexed before the new
    /// segment is created. `new_base` must equal the follower's local tail.
    pub fn rotate_replicated(&mut self, new_base: Position) -> Result<(), Error> {
        if new_base != self.next_position {
            return Err(Error::Corrupted {
                message: format!(
                    "replicated Rotate base {}, local tail is {}",
                    new_base.0, self.next_position.0
                ),
            });
        }
        self.rotate_segment()
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
        let record_offset = self.write_record(flags::EVENT, &payload)?;
        self.serialize_buf = payload;

        // Index the event AFTER its record is fully written, so a concurrent
        // reader that sees the index entry can always read the record bytes.
        self.active_index.write().insert_event(
            self.next_position.0,
            record_offset,
            &event.name,
            &event.tags,
        );

        self.next_position = self.next_position.next();
        Ok(())
    }

    /// Low-level: writes a single record to the active segment.
    /// Rotates the segment if the record doesn't fit.
    /// Returns the byte offset of the record header within the (possibly
    /// freshly rotated) active segment.
    fn write_record(&mut self, flags_byte: u8, payload: &[u8]) -> Result<u64, Error> {
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

        let record_offset = self.write_offset;
        self.active_file.write_all(&self.record_buf)?;
        self.write_offset += total_record_size as u64;
        Ok(record_offset)
    }

    /// Fsyncs the active segment to disk, making all written events durable.
    pub fn sync(&mut self) -> Result<(), Error> {
        fdatasync(&self.active_file)?;
        #[cfg(feature = "bench-instrumentation")]
        bi::bump_fsync();
        Ok(())
    }

    /// Clones the active segment's file handle so a group-commit thread can
    /// fsync it WITHOUT holding the writer lock. The clone shares the open
    /// file description; fsyncing it covers every byte written to the file
    /// before the fsync, regardless of which handle wrote them.
    pub fn active_file_handle(&self) -> Result<File, Error> {
        Ok(self.active_file.try_clone()?)
    }

    /// Returns the current head position (next position to be assigned).
    pub fn head(&self) -> Position {
        self.next_position
    }

    /// Returns the current write offset within the active segment. A wave
    /// descriptor snapshots this under the writer lock so its raw byte range
    /// is ordered against the seal barrier.
    pub fn write_offset(&self) -> u64 {
        self.write_offset
    }

    pub fn has_records(&self) -> bool {
        self.write_offset > SEGMENT_HEADER_SIZE as u64
    }

    /// Reopens `base` as the active segment after an engine-level suffix
    /// truncation. Later segment files have already been deleted by the
    /// caller. Preserves the shared active-index handle used by readers.
    pub fn reopen_truncated(
        &mut self,
        base: u64,
        write_offset: u64,
        next_position: Position,
    ) -> Result<(), Error> {
        use std::io::{Seek, SeekFrom};

        let path = segment_path(&self.dir, base);
        let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
        file.set_len(write_offset)?;
        file.sync_data()?;
        preallocate(&file, self.max_segment_size);
        file.seek(SeekFrom::Start(write_offset))?;

        let rebuilt = SegmentIndex::build_from_segment(&path)?;
        *self.active_index.write() = rebuilt;
        self.active_file = file;
        self.active_base_position = base;
        self.write_offset = write_offset;
        self.next_position = next_position;
        Ok(())
    }

    /// Returns the base position of the currently active segment.
    pub fn active_base_position(&self) -> u64 {
        self.active_base_position
    }

    /// Returns the path to the currently active segment file.
    pub fn active_segment_path(&self) -> PathBuf {
        segment_path(&self.dir, self.active_base_position)
    }

    /// Shared handle to the active segment's in-memory index, for readers
    /// that direct-seek into the active segment without the writer lock.
    pub fn active_index_handle(&self) -> Arc<RwLock<SegmentIndex>> {
        Arc::clone(&self.active_index)
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

        // New segment starts at the current next_position.
        let new_base = self.next_position.0;

        // Seal: swap in a fresh index for the new segment and persist the
        // old one. The index was built incrementally at append time, so this
        // is serialize-and-write only — no re-read of the segment file.
        let sealed_path = segment_path(&self.dir, self.active_base_position);
        let sealed_index = {
            let mut idx = self.active_index.write();
            std::mem::replace(&mut *idx, SegmentIndex::new(new_base))
        };
        sealed_index.write_to_disk(&sealed_path)?;
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

fn create_segment_file(path: &Path) -> Result<File, io::Error> {
    let file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)?;
    // Make the new directory entry durable. `fdatasync` on the file persists
    // its CONTENTS but not the filename: on some filesystems a crash right
    // after rotation could lose the freshly created segment file entirely,
    // even though its records were "synced". One dir fsync per segment
    // creation (every ~256MB) is noise. Same pattern as raft/snapshot_store.
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(file)
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

/// Scans an active segment to its last complete CRC-valid native record.
/// Events must be position-contiguous; control records consume no position.
/// The first torn, malformed, unknown, or out-of-sequence record terminates
/// recovery and the caller truncates the physical suffix.
///
/// Returns `(write_offset, next_position)` — the offset to resume writing and
/// the next position to assign.
fn recover_segment(file: &mut File) -> Result<(u64, Position), Error> {
    use std::io::{Read, Seek, SeekFrom};

    file.seek(SeekFrom::Start(0))?;
    let header = read_segment_header(file)?;
    let file_len = file.seek(SeekFrom::End(0))?;
    let mut offset = SEGMENT_HEADER_SIZE as u64;
    let mut valid_offset = offset;
    let mut next_position = Position(header.base_position);
    file.seek(SeekFrom::Start(offset))?;

    while offset + RECORD_HEADER_SIZE as u64 <= file_len {
        let mut header_buf = [0u8; RECORD_HEADER_SIZE];
        if file.read_exact(&mut header_buf).is_err() {
            break;
        }

        let header = match record::parse_header(&header_buf)? {
            Some(header) => header,
            None => break,
        };
        if offset + header.total_len() as u64 > file_len {
            break;
        }

        let mut payload = vec![0u8; header.payload_len];
        if file.read_exact(&mut payload).is_err() || !record::validate_crc(header, &payload) {
            break;
        }

        match record::decode_native(header, &payload) {
            Ok(record::NativeRecord::Event { position }) if position == next_position.0 => {
                next_position = next_position.next();
            }
            Ok(record::NativeRecord::Control(_)) => {}
            _ => break,
        }

        offset += header.total_len() as u64;
        valid_offset = offset;
    }

    Ok((valid_offset, next_position))
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
/// Fsyncs a standalone file handle (see `active_file_handle`) with the same
/// platform semantics as `SegmentWriter::sync`.
pub(crate) fn sync_file(file: &File) -> Result<(), crate::error::Error> {
    fdatasync(file)?;
    #[cfg(feature = "bench-instrumentation")]
    bi::bump_fsync();
    Ok(())
}

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
            let event = make_event(&format!("Event{i}"), &[0u8; 50]);
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
