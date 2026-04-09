use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::error::Error;
use crate::event::{AppendEvent, Position, StoredEvent};

use crate::segment::{
    format, segment_path, flags, RECORD_HEADER_SIZE, SEGMENT_HEADER_SIZE, SEGMENT_MAGIC,
    SEGMENT_VERSION,
};

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
    /// For a new database this is Position(1). For recovery, it's the position
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

            let payload_with_flags_len = 1 + self.serialize_buf.len();
            let total_record_size = RECORD_HEADER_SIZE + self.serialize_buf.len();

            if self.write_offset + total_record_size as u64 > self.max_segment_size {
                self.rotate_segment()?;
            }

            let crc = {
                let mut digest = crc32c::crc32c(&[flags::NONE]);
                digest = crc32c::crc32c_append(digest, &self.serialize_buf);
                digest
            };

            self.record_buf.clear();
            self.record_buf.extend_from_slice(&crc.to_le_bytes());
            self.record_buf
                .extend_from_slice(&(payload_with_flags_len as u32).to_le_bytes());
            self.record_buf.push(flags::NONE);
            self.record_buf.extend_from_slice(&self.serialize_buf);

            self.active_file.write_all(&self.record_buf)?;
            self.write_offset += total_record_size as u64;
            self.next_position = self.next_position.next();
        }

        Ok((first_position, events.len() as u32))
    }

    /// Fsyncs the active segment to disk, making all written events durable.
    pub fn sync(&mut self) -> Result<(), Error> {
        fdatasync(&self.active_file)?;
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
    file.read_exact(&mut header_buf).map_err(|_| Error::Corrupted {
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

/// Scans the segment from after the header, validating CRCs.
/// Returns (write_offset, next_position) — the offset to resume writing
/// and the next position to assign.
fn recover_segment(file: &mut File) -> Result<(u64, Position), Error> {
    use std::io::{Read, Seek, SeekFrom};

    let file_len = file.seek(SeekFrom::End(0))?;
    file.seek(SeekFrom::Start(SEGMENT_HEADER_SIZE as u64))?;

    let mut offset = SEGMENT_HEADER_SIZE as u64;
    let mut last_position: Option<Position> = None;

    while offset + RECORD_HEADER_SIZE as u64 <= file_len {
        // Read the record header.
        let mut header_buf = [0u8; RECORD_HEADER_SIZE];
        if file.read_exact(&mut header_buf).is_err() {
            break;
        }

        let stored_crc = u32::from_le_bytes(header_buf[0..4].try_into().unwrap());
        let record_len = u32::from_le_bytes(header_buf[4..8].try_into().unwrap()) as usize;
        let _flags = header_buf[8];

        // Sanity check record length.
        if record_len < 1 || offset + RECORD_HEADER_SIZE as u64 + (record_len - 1) as u64 > file_len {
            break; // Torn write — stop here.
        }

        // Read flags + payload (record_len includes the flags byte we already have in header).
        let payload_len = record_len - 1; // subtract the flags byte
        let mut payload_buf = vec![0u8; payload_len];
        if file.read_exact(&mut payload_buf).is_err() {
            break;
        }

        // Verify CRC over flags + payload.
        let computed_crc = {
            let mut digest = crc32c::crc32c(&[_flags]);
            digest = crc32c::crc32c_append(digest, &payload_buf);
            digest
        };

        if computed_crc != stored_crc {
            break; // Corruption or torn write — stop here.
        }

        // Extract the position from the payload (first 8 bytes of the serialized event).
        if payload_buf.len() >= 8 {
            let position = u64::from_le_bytes(payload_buf[0..8].try_into().unwrap());
            last_position = Some(Position(position));
        }

        offset += RECORD_HEADER_SIZE as u64 + payload_len as u64;
    }

    let next_position = match last_position {
        Some(pos) => pos.next(),
        None => {
            // No valid records in this segment. Read the base position from the header.
            file.seek(SeekFrom::Start(0))?;
            let header = read_segment_header(file)?;
            Position(header.base_position)
        }
    };

    Ok((offset, next_position))
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
    use crate::segment::DEFAULT_SEGMENT_SIZE;
    use crate::event::Tag;

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
        let mut writer = SegmentWriter::new(dir.path(), Position(1), DEFAULT_SEGMENT_SIZE).unwrap();

        let events = vec![
            make_event("OrderPlaced", b"order-1"),
            make_event("PaymentReceived", b"payment-1"),
            make_event("OrderShipped", b"ship-1"),
        ];

        let (first_pos, count) = writer.append(&events).unwrap();
        assert_eq!(first_pos, Position(1));
        assert_eq!(count, 3);
        assert_eq!(writer.head(), Position(4));

        // Drop the writer and reopen via recovery.
        drop(writer);

        let recovered = SegmentWriter::open(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap();
        assert_eq!(recovered.head(), Position(4));
    }

    #[test]
    fn empty_append() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::new(dir.path(), Position(1), DEFAULT_SEGMENT_SIZE).unwrap();

        let (first_pos, count) = writer.append(&[]).unwrap();
        assert_eq!(first_pos, Position(1));
        assert_eq!(count, 0);
        assert_eq!(writer.head(), Position(1));
    }

    #[test]
    fn segment_rotation() {
        let dir = tempfile::tempdir().unwrap();

        // Use a tiny segment size to force rotation.
        let tiny_segment = SEGMENT_HEADER_SIZE as u64 + 200;
        let mut writer = SegmentWriter::new(dir.path(), Position(1), tiny_segment).unwrap();

        // Write events until we get a rotation.
        for i in 0..5 {
            let event = make_event(&format!("Event{i}"), &vec![0u8; 50]);
            writer.append(&[event]).unwrap();
        }

        // We should have multiple segment files.
        let segments = list_segments(dir.path()).unwrap();
        assert!(segments.len() > 1, "expected segment rotation, got {} segments", segments.len());

        // Recovery should still find the correct head.
        let head = writer.head();
        drop(writer);

        let recovered = SegmentWriter::open(dir.path(), tiny_segment).unwrap();
        assert_eq!(recovered.head(), head);
    }
}
