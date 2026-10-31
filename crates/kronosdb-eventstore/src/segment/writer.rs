use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use parking_lot::RwLock;

use crate::append::RecordBatch;
use crate::cache::IndexCache;
use crate::error::Error;
use crate::event::{AppendEvent, Position};

use crate::replication::epochs::EpochStart;
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
    /// The `EpochChange` records in the frame, in order.
    pub epoch_changes: Vec<EpochStart>,
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
    /// Segments rotated away from since the group-commit thread last looked.
    /// Rotation itself only swaps files and indexes under the writer lock;
    /// everything that touches the disk for the sealed segment — its final
    /// fdatasync, the directory sync that makes the new file's name durable,
    /// and the `.idx`/`.bloom` companions — is the sync thread's, off the lock.
    pending_seals: Vec<PendingSeal>,
    /// Where a rotated-away segment's index is published for readers until
    /// its companions are on disk. Set by the engine; a standalone writer
    /// has no readers to publish to.
    index_cache: Option<Arc<IndexCache>>,
    /// Bytes at the end of `record_buf` that `write_batch_deferred` has
    /// accounted for (offsets, positions, index entries) but not yet written
    /// to the file. Zero outside a deferred group; see `flush_deferred`.
    deferred: usize,
    /// A deferred write failed: the file no longer matches the writer's
    /// offsets and positions, so nothing more may be written through it.
    broken: bool,
    /// Test hook: the next flush of deferred bytes fails.
    #[cfg(test)]
    fail_next_flush: bool,
}

/// A segment the writer has rotated away from but whose seal is not yet on
/// disk. Handed to the group-commit thread at its next barrier.
pub struct PendingSeal {
    /// Base position of the sealed segment.
    pub base: u64,
    /// The sealed file, already truncated to `len`. fdatasync'd by the
    /// wave that takes the seal, so every record in it is durable before
    /// the wave's writers are released.
    pub file: File,
    /// Exact byte length of the sealed file.
    pub len: u64,
    /// The segment's complete index. Reads resolve the segment through this
    /// (pinned in the cache) until the companion files are written.
    pub index: Arc<SegmentIndex>,
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
        preallocate(&file, max_segment_size)?;
        sync_dir(dir)?;

        Ok(Self {
            dir: dir.to_path_buf(),
            max_segment_size,
            active_file: file,
            active_base_position: base_position,
            write_offset: SEGMENT_HEADER_SIZE as u64,
            next_position: start_position,
            serialize_buf: Vec::with_capacity(4096),
            record_buf: Vec::with_capacity(64 * 1024),
            active_index: Arc::new(RwLock::new(SegmentIndex::new(base_position))),
            pending_seals: Vec::new(),
            index_cache: None,
            deferred: 0,
            broken: false,
            #[cfg(test)]
            fail_next_flush: false,
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
        preallocate(&file, max_segment_size)?;

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
            record_buf: Vec::with_capacity(64 * 1024),
            active_index: Arc::new(RwLock::new(active_index)),
            pending_seals: Vec::new(),
            index_cache: None,
            deferred: 0,
            broken: false,
            #[cfg(test)]
            fail_next_flush: false,
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
        self.write_batch(&RecordBatch::from_events(events))
    }

    /// Writes a batch of event records WITHOUT fsyncing: each record is
    /// copied behind its CRC with its position set, and its type and tags are
    /// indexed by reading them in place. [`write_events`](Self::write_events)
    /// without the serialization.
    pub fn write_batch(&mut self, batch: &RecordBatch) -> Result<(Position, u32), Error> {
        self.write_batch_inner(batch, false)
    }

    /// [`write_batch`](Self::write_batch) that leaves the bytes in memory,
    /// for a caller that stages several appends under one hold of the writer
    /// lock and wants them in the file with one syscall: positions, offsets
    /// and index entries move on as usual, so the next append sees this one,
    /// but the file has nothing of it until
    /// [`flush_deferred`](Self::flush_deferred). The caller must flush before
    /// it lets go of the writer (anyone who then takes the lock — the wave
    /// barrier above all — expects every byte below `write_offset` to be in
    /// the file) and must not publish a position to readers before it has.
    /// Fails like `write_batch` does; and a flush that fails leaves the
    /// writer unusable, which the caller answers by poisoning the engine.
    pub fn write_batch_deferred(&mut self, batch: &RecordBatch) -> Result<(Position, u32), Error> {
        self.write_batch_inner(batch, true)
    }

    /// Writes the bytes `write_batch_deferred` left in memory, in one
    /// syscall. A no-op when there are none.
    pub fn flush_deferred(&mut self) -> Result<(), Error> {
        if self.deferred == 0 {
            return Ok(());
        }
        #[cfg(test)]
        let injected = std::mem::take(&mut self.fail_next_flush);
        #[cfg(not(test))]
        let injected = false;
        let written = if injected {
            Err(io::Error::other("injected write failure"))
        } else {
            self.active_file.write_all(&self.record_buf)
        };
        self.record_buf.clear();
        self.deferred = 0;
        if let Err(error) = written {
            self.broken = true;
            return Err(Error::Io(error));
        }
        Ok(())
    }

    /// A deferred write failed: the writer's books and the file disagree.
    pub fn is_broken(&self) -> bool {
        self.broken
    }

    /// Bytes waiting for `flush_deferred`.
    pub fn deferred_len(&self) -> usize {
        self.deferred
    }

    #[cfg(test)]
    pub(crate) fn fail_next_flush(&mut self) {
        self.fail_next_flush = true;
    }

    fn check_writable(&self) -> Result<(), Error> {
        if self.broken {
            return Err(Error::Io(io::Error::other(
                "the segment writer is unusable after a failed write",
            )));
        }
        Ok(())
    }

    fn write_batch_inner(
        &mut self,
        batch: &RecordBatch,
        defer: bool,
    ) -> Result<(Position, u32), Error> {
        if batch.is_empty() {
            return Ok((self.next_position, 0));
        }
        self.check_writable()?;
        if !defer {
            // Whatever a deferred group left is older than this write and
            // goes to the file first.
            self.flush_deferred()?;
        }

        let first_position = self.next_position;
        let records = batch.bytes();

        // A record larger than a whole segment is refused up front, rather
        // than after the records before it were written.
        let mut at = 0;
        while at < records.len() {
            let wire_len = wire_record_len(records, at);
            let needed = (4 + wire_len) as u64;
            if SEGMENT_HEADER_SIZE as u64 + needed > self.max_segment_size {
                return Err(Error::Io(io::Error::other(format!(
                    "event record of {needed} bytes exceeds the segment size"
                ))));
            }
            at += wire_len;
        }

        // The batch is framed into one buffer and written with one syscall
        // per segment it lands in: a 1000-event append is one `write_all`,
        // not a thousand. Index entries are published after the bytes are
        // in the file, under one index lock per run, so a reader that sees
        // an entry can always read its record. (A deferred batch publishes
        // its entries before its bytes are in the file; see
        // `write_batch_deferred` for why that is safe.)
        let mut run_start = 0;
        while run_start < records.len() {
            if self.deferred == 0 {
                self.record_buf.clear();
            }
            let mut position = self.next_position;
            let mut offset = self.write_offset;
            let mut cursor = run_start;
            while cursor < records.len() {
                let wire_len = wire_record_len(records, cursor);
                let stored = 4 + wire_len;
                if offset + stored as u64 > self.max_segment_size {
                    break;
                }
                // CRC (filled in below), then the record as it arrived, then
                // the position the client left for the server to set.
                let start = self.record_buf.len();
                self.record_buf.extend_from_slice(&[0; 4]);
                self.record_buf
                    .extend_from_slice(&records[cursor..cursor + wire_len]);
                self.record_buf[start + RECORD_HEADER_SIZE..start + RECORD_HEADER_SIZE + 8]
                    .copy_from_slice(&position.0.to_le_bytes());
                // flags and body: everything after the length.
                let crc = crc32c::crc32c(&self.record_buf[start + 8..]);
                self.record_buf[start..start + 4].copy_from_slice(&crc.to_le_bytes());
                offset += stored as u64;
                position = position.next();
                cursor += wire_len;
            }

            if cursor == run_start {
                // The next record does not fit: seal this segment and retry
                // it in the fresh one.
                self.rotate_segment()?;
                continue;
            }

            if defer {
                self.deferred = self.record_buf.len();
            } else {
                self.active_file.write_all(&self.record_buf)?;
            }
            let mut record_offset = self.write_offset;
            let mut indexed_position = self.next_position.0;
            self.write_offset = offset;
            self.next_position = position;
            {
                let mut index = self.active_index.write();
                let mut at = run_start;
                while at < cursor {
                    let wire_len = wire_record_len(records, at);
                    let scan = format::scan(&records[at + 5..at + wire_len])
                        .expect("the batch's records were checked when it was built");
                    index.insert_scanned(indexed_position, record_offset, &scan);
                    record_offset += 4 + wire_len as u64;
                    indexed_position += 1;
                    at += wire_len;
                }
            }
            run_start = cursor;
        }

        Ok((first_position, batch.len()))
    }

    /// Whether `batch` fits in the active segment as it stands. A batch that
    /// does not fit rotates the segment mid-write, which creates and
    /// preallocates a file: work for a thread that may block, not for an
    /// async worker.
    pub fn fits_without_rotation(&self, batch: &RecordBatch) -> bool {
        self.write_offset + batch.stored_len() <= self.max_segment_size
    }

    /// Segments rotated away from since the last barrier, with the sealed
    /// file handles. The group-commit thread takes these under the writer
    /// lock and finishes the seals off it.
    pub fn take_pending_seals(&mut self) -> Vec<PendingSeal> {
        std::mem::take(&mut self.pending_seals)
    }

    /// Seals not yet taken by the group-commit thread.
    pub fn pending_seals(&self) -> &[PendingSeal] {
        &self.pending_seals
    }

    /// Drops pending seals at or past `base` — their files are being
    /// truncated away or reopened, so no companion may be written for them.
    pub fn discard_pending_seals_from(&mut self, base: u64) {
        self.pending_seals.retain(|seal| seal.base < base);
    }

    /// Writes a native control record without consuming an event position or
    /// fsyncing. The caller commits it through the normal group-commit wave.
    pub fn write_control(&mut self, control: &format::ControlRecord) -> Result<(), Error> {
        self.serialize_buf.clear();
        let flags_byte = format::serialize_control(control, &mut self.serialize_buf);
        let payload = std::mem::take(&mut self.serialize_buf);
        let written = self.write_record(flags_byte, &payload);
        self.serialize_buf = payload;
        written?;
        // After the write, which may have rotated: the record sits in the
        // segment that is active now, and its sealed index will carry it.
        self.active_index.write().insert_control(*control);
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
        self.check_writable()?;
        self.flush_deferred()?;
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
        // Control records, each with how many of the frame's events precede
        // it, so the index records them where they sit.
        let mut controls: Vec<(usize, format::ControlRecord)> = Vec::new();
        let mut epoch_changes = Vec::new();
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
            } else if flags::is_control(flags_byte) {
                let control = format::deserialize_control(flags_byte, payload)?;
                if let format::ControlRecord::EpochChange {
                    epoch,
                    start_position,
                    ..
                } = control
                {
                    epoch_changes.push(EpochStart {
                        epoch,
                        start_position,
                    });
                }
                controls.push((events.len(), control));
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
            let mut controls = controls.iter().peekable();
            for (i, (relative_offset, fields)) in events.iter().enumerate() {
                while let Some((_, control)) = controls.next_if(|(before, _)| *before == i) {
                    index.insert_control(*control);
                }
                index.insert_event(
                    fields.position.0,
                    byte_start + *relative_offset as u64,
                    &fields.name,
                    &fields.tags,
                );
            }
            for (_, control) in controls {
                index.insert_control(*control);
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
            epoch_changes,
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

    /// Low-level: writes a single record to the active segment.
    /// Rotates the segment if the record doesn't fit.
    /// Returns the byte offset of the record header within the (possibly
    /// freshly rotated) active segment.
    fn write_record(&mut self, flags_byte: u8, payload: &[u8]) -> Result<u64, Error> {
        self.check_writable()?;
        self.flush_deferred()?;
        let total_record_size = RECORD_HEADER_SIZE + payload.len();

        if self.write_offset + total_record_size as u64 > self.max_segment_size {
            self.rotate_segment()?;
        }

        self.record_buf.clear();
        frame_record(flags_byte, payload, &mut self.record_buf);

        let record_offset = self.write_offset;
        self.active_file.write_all(&self.record_buf)?;
        self.write_offset += total_record_size as u64;
        Ok(record_offset)
    }

    /// Fsyncs the active segment — and any segment rotated away from since
    /// the last sync — making all written events durable.
    pub fn sync(&mut self) -> Result<(), Error> {
        for seal in &self.pending_seals {
            fdatasync(&seal.file)?;
        }
        if !self.pending_seals.is_empty() {
            sync_dir(&self.dir)?;
        }
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
        preallocate(&file, self.max_segment_size)?;
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

    /// Publishes every future rotation's sealed index to `cache` (see
    /// `rotate_segment`). The engine attaches its cache before any reader
    /// exists.
    pub fn attach_index_cache(&mut self, cache: Arc<IndexCache>) {
        self.index_cache = Some(cache);
    }

    /// Rotates to a new segment file.
    ///
    /// Under the writer lock this is metadata only: create and preallocate
    /// the new file, trim the old one to its data length, swap in a fresh
    /// index. Every appender waits on this lock, so nothing here may wait on
    /// the disk. The sealed segment's fdatasync, the directory sync for the
    /// new file's name, and its `.idx`/`.bloom` companions are left as a
    /// `PendingSeal` for the group-commit thread, which does them off the
    /// lock in the wave that carries the rotation.
    fn rotate_segment(&mut self) -> Result<(), Error> {
        // What deferred appends left waiting belongs in the file being sealed.
        self.check_writable()?;
        self.flush_deferred()?;

        // New segment starts at the current next_position.
        let new_base = self.next_position.0;

        // Everything that can fail runs before any state changes, so a
        // failed rotation leaves the writer on the old segment, intact.
        let path = segment_path(&self.dir, new_base);
        let mut file = create_segment_file(&path)?;
        let prepared = (|| -> Result<(), Error> {
            write_segment_header(&mut file, new_base)?;
            preallocate(&file, self.max_segment_size)?;
            // Truncate the sealed segment to its actual data size. It was
            // preallocated to max_segment_size; a length change is a
            // metadata op, not a flush.
            self.active_file.set_len(self.write_offset)?;
            Ok(())
        })();
        if let Err(error) = prepared {
            // A half-made file would fail the next attempt's create_new.
            let _ = std::fs::remove_file(&path);
            return Err(error);
        }

        // Swap in a fresh index for the new segment. The old one was built
        // incrementally at append time and is complete. Readers do not take
        // the writer lock: one that sees the new active base resolves the old
        // segment through the cache, and its `.idx` is not on disk until the
        // background seal writes it. So the sealed index is published before
        // the guard is released — whoever sees the swap also finds it.
        let sealed_index = {
            let mut active = self.active_index.write();
            let sealed = Arc::new(std::mem::replace(&mut *active, SegmentIndex::new(new_base)));
            if let Some(cache) = &self.index_cache {
                cache.pin_sealed(self.active_base_position, Arc::clone(&sealed));
            }
            sealed
        };

        let sealed_file = std::mem::replace(&mut self.active_file, file);
        self.pending_seals.push(PendingSeal {
            base: self.active_base_position,
            file: sealed_file,
            len: self.write_offset,
            index: sealed_index,
        });
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
    Ok(file)
}

/// Makes the directory's entries durable. `fdatasync` on a file persists
/// its CONTENTS but not its name: without this, a crash after rotation
/// could lose a freshly created segment file entirely even though its
/// records were synced. Called by the group-commit thread in the wave that
/// carries a rotation, never under the writer lock.
pub(crate) fn sync_dir(dir: &Path) -> Result<(), io::Error> {
    File::open(dir)?.sync_all()
}

/// Length of the record at `at` in a batch, its length field included.
fn wire_record_len(records: &[u8], at: usize) -> usize {
    4 + u32::from_le_bytes(records[at..at + 4].try_into().unwrap()) as usize
}

/// Frames one record — CRC, length, flags, payload — onto `out`.
fn frame_record(flags_byte: u8, payload: &[u8], out: &mut Vec<u8>) {
    let crc = crc32c::crc32c_append(crc32c::crc32c(&[flags_byte]), payload);
    out.extend_from_slice(&crc.to_le_bytes());
    out.extend_from_slice(&((1 + payload.len()) as u32).to_le_bytes());
    out.push(flags_byte);
    out.extend_from_slice(payload);
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

/// Grows the active segment file to its full size up front.
///
/// On Linux this is `fallocate`, which reserves contiguous blocks without
/// writing zeros. Filesystems that do not implement it — ZFS, NFS, and the
/// FUSE/virtiofs mounts behind a bind-mounted container volume — fail the
/// syscall, so we fall back to extending the file logically with `set_len`.
///
/// The contiguous blocks and the skipped metadata update on each fdatasync
/// are the optimization. The file *length* is not: readers mmap the active
/// segment once and keep that mapping for the segment's whole lifetime, so
/// a file that is not full-length maps short and every later append lands
/// outside the mapping. That is why a failure here is an error rather than
/// something to shrug off.
pub(crate) fn preallocate(file: &File, size: u64) -> Result<(), Error> {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::io::AsRawFd;
        // SAFETY: `fallocate` is a plain syscall and the fd is valid for the
        // lifetime of `file`.
        if unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, size as i64) } == 0 {
            return Ok(());
        }
    }
    file.set_len(size)?;
    Ok(())
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

pub(crate) fn fdatasync(file: &File) -> Result<(), io::Error> {
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

    /// Deferred batches are in the writer's books at once and in the file
    /// only at the flush, which writes what the same batches would have
    /// written one by one — across a rotation too.
    #[test]
    fn deferred_batches_reach_the_file_at_the_flush_and_match_immediate_writes() {
        let max = SEGMENT_HEADER_SIZE as u64 + 1100;
        let batches: Vec<RecordBatch> = (0..8)
            .map(|i| {
                RecordBatch::from_events(&[
                    make_event(&format!("E{i}"), &[i as u8; 60]),
                    make_event(&format!("F{i}"), &[i as u8; 90]),
                ])
            })
            .collect();

        let immediate_dir = tempfile::tempdir().unwrap();
        let mut immediate = SegmentWriter::new(immediate_dir.path(), Position(0), max).unwrap();
        for batch in &batches {
            immediate.write_batch(batch).unwrap();
        }

        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::new(dir.path(), Position(0), max).unwrap();
        // Three batches fit in the first segment: nothing of them is in the file.
        for batch in &batches[..3] {
            writer.write_batch_deferred(batch).unwrap();
        }
        assert_eq!(writer.head(), Position(6));
        assert!(writer.deferred_len() > 0);
        let file = std::fs::read(segment_path(dir.path(), 0)).unwrap();
        assert!(file[SEGMENT_HEADER_SIZE..].iter().all(|&b| b == 0));
        // The rest rotate, which puts the waiting bytes in the segment they
        // belong to before the next one starts.
        for batch in &batches[3..] {
            writer.write_batch_deferred(batch).unwrap();
        }
        let sealed = std::fs::read(segment_path(dir.path(), 0)).unwrap();
        assert_eq!(
            sealed,
            std::fs::read(segment_path(immediate_dir.path(), 0)).unwrap()
        );
        writer.flush_deferred().unwrap();
        assert_eq!(writer.deferred_len(), 0);
        assert_eq!(writer.head(), immediate.head());
        let (a, b) = (
            list_segments(dir.path()),
            list_segments(immediate_dir.path()),
        );
        let (mut a, mut b) = (a.unwrap(), b.unwrap());
        a.sort();
        b.sort();
        assert_eq!(a, b);
        assert!(a.len() >= 2);
        for base in a {
            assert!(
                std::fs::read(segment_path(dir.path(), base)).unwrap()
                    == std::fs::read(segment_path(immediate_dir.path(), base)).unwrap(),
                "segment {base} differs"
            );
        }
    }

    #[test]
    fn a_failed_flush_makes_the_writer_unusable() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::new(dir.path(), Position(0), DEFAULT_SEGMENT_SIZE).unwrap();
        writer
            .write_batch_deferred(&RecordBatch::from_events(&[make_event("A", b"x")]))
            .unwrap();
        writer.fail_next_flush();
        assert!(writer.flush_deferred().is_err());
        let batch = RecordBatch::from_events(&[make_event("B", b"y")]);
        assert!(writer.write_batch(&batch).is_err());
        assert!(writer.write_batch_deferred(&batch).is_err());
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
