use std::path::Path;
use std::sync::Arc;

use memmap2::Mmap;

use crate::error::Error;
use crate::event::{Position, StoredEvent};

use crate::segment::{
    RECORD_HEADER_SIZE, SEGMENT_HEADER_SIZE, SEGMENT_MAGIC, SEGMENT_VERSION, flags, format,
};

/// Reads events from a segment file using memory-mapped I/O.
///
/// This is designed for sealed (immutable) segments. For the active segment,
/// the caller should limit reads to the committed position.
///
/// The mmap is Arc-wrapped so sealed segments can share a single mapping
/// via the cache, avoiding repeated open()/mmap() syscalls.
pub struct SegmentReader {
    mmap: Arc<Mmap>,
    base_position: u64,
}

impl SegmentReader {
    /// Opens a segment file for reading via mmap.
    pub fn open(path: &Path) -> Result<Self, Error> {
        let file = std::fs::File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        Self::from_mmap(Arc::new(mmap))
    }

    /// Creates a reader from a shared mmap handle (e.g., from the cache).
    /// Validates the segment header.
    pub fn from_shared_mmap(mmap: Arc<Mmap>) -> Result<Self, Error> {
        Self::from_mmap(mmap)
    }

    fn from_mmap(mmap: Arc<Mmap>) -> Result<Self, Error> {
        if mmap.len() < SEGMENT_HEADER_SIZE {
            return Err(Error::Corrupted {
                message: "segment file too small for header".into(),
            });
        }

        // Validate header.
        if mmap[0..4] != SEGMENT_MAGIC {
            return Err(Error::Corrupted {
                message: "invalid segment magic bytes".into(),
            });
        }
        if mmap[4] != SEGMENT_VERSION {
            return Err(Error::Corrupted {
                message: format!("unsupported segment version: {}", mmap[4]),
            });
        }
        let base_position = u64::from_le_bytes(mmap[5..13].try_into().unwrap());

        Ok(Self {
            mmap,
            base_position,
        })
    }

    /// Returns the base position of this segment.
    pub fn base_position(&self) -> u64 {
        self.base_position
    }

    /// Returns an iterator over all valid events in the segment.
    ///
    /// `up_to` limits the read to events with position < up_to.
    /// Pass `None` to read all events.
    pub fn iter(&self, up_to: Option<Position>) -> SegmentIterator<'_> {
        SegmentIterator {
            data: &self.mmap,
            offset: SEGMENT_HEADER_SIZE,
            up_to,
        }
    }

    /// Returns an iterator that also yields the byte offset of each record.
    /// Used during index building to populate the position→offset table.
    pub fn iter_with_offsets(&self, up_to: Option<Position>) -> OffsetTrackingIterator<'_> {
        OffsetTrackingIterator {
            data: &self.mmap,
            offset: SEGMENT_HEADER_SIZE,
            up_to,
        }
    }

    /// Reads a single event at the given byte offset within the segment.
    /// Used for direct seeks when the offset table is available.
    ///
    /// This skips all events between the segment header and the target offset,
    /// reading only the one event at the specified location.
    pub fn read_event_at(&self, byte_offset: usize) -> Result<StoredEvent, Error> {
        let data = &*self.mmap;

        if byte_offset + RECORD_HEADER_SIZE > data.len() {
            return Err(Error::Corrupted {
                message: format!("offset {byte_offset} beyond segment bounds"),
            });
        }

        // Read record header.
        let stored_crc = u32::from_le_bytes(data[byte_offset..byte_offset + 4].try_into().unwrap());
        let record_len =
            u32::from_le_bytes(data[byte_offset + 4..byte_offset + 8].try_into().unwrap()) as usize;
        let flags_byte = data[byte_offset + 8];

        if record_len == 0 {
            return Err(Error::Corrupted {
                message: format!("zero-length record at offset {byte_offset}"),
            });
        }

        let payload_len = record_len - 1;
        let payload_start = byte_offset + RECORD_HEADER_SIZE;
        let payload_end = payload_start + payload_len;

        if payload_end > data.len() {
            return Err(Error::Corrupted {
                message: format!("record at offset {byte_offset} extends beyond segment"),
            });
        }

        let payload = &data[payload_start..payload_end];

        // Verify CRC.
        let computed_crc = {
            let digest = crc32c::crc32c(&[flags_byte]);
            crc32c::crc32c_append(digest, payload)
        };

        if computed_crc != stored_crc {
            return Err(Error::Corrupted {
                message: format!("CRC mismatch at offset {byte_offset}"),
            });
        }

        let (event, _) = format::deserialize_event(payload)?;
        Ok(event)
    }
}

/// Iterates over event records in a segment.
pub struct SegmentIterator<'a> {
    data: &'a [u8],
    offset: usize,
    up_to: Option<Position>,
}

impl<'a> Iterator for SegmentIterator<'a> {
    type Item = Result<StoredEvent, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            // Check if we have enough bytes for a record header.
            if self.offset + RECORD_HEADER_SIZE > self.data.len() {
                return None;
            }

            let header_start = self.offset;

            // Read record header.
            let stored_crc = u32::from_le_bytes(
                self.data[header_start..header_start + 4]
                    .try_into()
                    .unwrap(),
            );
            let record_len = u32::from_le_bytes(
                self.data[header_start + 4..header_start + 8]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let flags_byte = self.data[header_start + 8];

            // Zero record_len means we've hit unwritten space.
            if record_len == 0 {
                return None;
            }

            let payload_len = record_len - 1; // subtract flags byte
            let payload_start = header_start + RECORD_HEADER_SIZE;
            let payload_end = payload_start + payload_len;

            // Check bounds.
            if payload_end > self.data.len() {
                return None; // Partial record at end of file.
            }

            let payload = &self.data[payload_start..payload_end];

            // Check the position bound BEFORE the CRC. The active segment is
            // preallocated and mmap'd at full length, so an iterator can
            // catch a record mid-write; a record at or past the committed
            // bound must read as end-of-data, not as corruption. The
            // position is the first field of an event payload and is written
            // in the same buffered write as the rest of the record.
            if let Some(up_to) = self.up_to
                && flags::is_event(flags_byte)
                && payload.len() >= 8
            {
                let position = u64::from_le_bytes(payload[0..8].try_into().unwrap());
                if position >= up_to.0 {
                    return None;
                }
            }

            // Verify CRC.
            let computed_crc = {
                let digest = crc32c::crc32c(&[flags_byte]);
                crc32c::crc32c_append(digest, payload)
            };

            if computed_crc != stored_crc {
                return Some(Err(Error::Corrupted {
                    message: format!("CRC mismatch at offset {header_start}"),
                }));
            }

            // Advance past this record.
            self.offset = payload_end;

            // Skip non-event records (Raft markers, future record types).
            // The loop continues to the next record.
            if !flags::is_event(flags_byte) {
                continue;
            }

            // Deserialize the event.
            match format::deserialize_event(payload) {
                Ok((event, _)) => {
                    // Check position limit (redundant with the pre-CRC check
                    // above, kept as the authoritative post-deserialize bound).
                    if let Some(up_to) = self.up_to
                        && event.position >= up_to
                    {
                        return None;
                    }
                    return Some(Ok(event));
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

/// Like SegmentIterator but also yields the byte offset of each event record's header.
/// Non-event records (Raft markers, etc.) are skipped; the offsets returned always
/// refer to event records specifically. Used during index building.
pub struct OffsetTrackingIterator<'a> {
    data: &'a [u8],
    offset: usize,
    up_to: Option<Position>,
}

impl<'a> Iterator for OffsetTrackingIterator<'a> {
    /// (byte_offset_of_event_record_header, Result<StoredEvent>)
    type Item = (usize, Result<StoredEvent, Error>);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.offset + RECORD_HEADER_SIZE > self.data.len() {
                return None;
            }

            let header_start = self.offset;
            let stored_crc = u32::from_le_bytes(
                self.data[header_start..header_start + 4]
                    .try_into()
                    .unwrap(),
            );
            let record_len = u32::from_le_bytes(
                self.data[header_start + 4..header_start + 8]
                    .try_into()
                    .unwrap(),
            ) as usize;
            let flags_byte = self.data[header_start + 8];

            if record_len == 0 {
                return None;
            }

            let payload_len = record_len - 1;
            let payload_start = header_start + RECORD_HEADER_SIZE;
            let payload_end = payload_start + payload_len;

            if payload_end > self.data.len() {
                return None;
            }

            let payload = &self.data[payload_start..payload_end];

            let computed_crc = {
                let digest = crc32c::crc32c(&[flags_byte]);
                crc32c::crc32c_append(digest, payload)
            };
            if computed_crc != stored_crc {
                return Some((
                    header_start,
                    Err(Error::Corrupted {
                        message: format!("CRC mismatch at offset {header_start}"),
                    }),
                ));
            }

            self.offset = payload_end;

            if !flags::is_event(flags_byte) {
                continue;
            }

            match format::deserialize_event(payload) {
                Ok((event, _)) => {
                    if let Some(up_to) = self.up_to
                        && event.position >= up_to
                    {
                        return None;
                    }
                    return Some((header_start, Ok(event)));
                }
                Err(e) => return Some((header_start, Err(e))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::AppendEvent;
    use crate::event::Tag;
    use crate::segment::DEFAULT_SEGMENT_SIZE;
    use crate::segment::writer::SegmentWriter;

    fn make_event(name: &str, payload: &[u8]) -> AppendEvent {
        AppendEvent {
            identifier: format!("id-{name}"),
            name: name.into(),
            version: "1.0".into(),
            timestamp: 1712345678000,
            payload: payload.to_vec(),
            metadata: vec![("key".into(), "value".into())],
            tags: vec![Tag::from_str("test", "true")],
        }
    }

    #[test]
    fn write_then_read() {
        let dir = tempfile::tempdir().unwrap();

        // Write events.
        let mut writer = SegmentWriter::new(dir.path(), Position(0), DEFAULT_SEGMENT_SIZE).unwrap();
        let events = vec![
            make_event("OrderPlaced", b"order-data"),
            make_event("PaymentReceived", b"payment-data"),
        ];
        writer.append(&events).unwrap();
        let seg_path = writer.active_segment_path();
        drop(writer);

        // Read them back.
        let reader = SegmentReader::open(&seg_path).unwrap();
        let read_events: Vec<_> = reader.iter(None).collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(read_events.len(), 2);
        assert_eq!(read_events[0].position, Position(0));
        assert_eq!(read_events[0].name, "OrderPlaced");
        assert_eq!(read_events[0].payload, b"order-data");
        assert_eq!(read_events[1].position, Position(1));
        assert_eq!(read_events[1].name, "PaymentReceived");
        assert_eq!(
            read_events[1].metadata,
            vec![("key".into(), "value".into())]
        );
    }

    #[test]
    fn read_with_position_limit() {
        let dir = tempfile::tempdir().unwrap();

        let mut writer = SegmentWriter::new(dir.path(), Position(0), DEFAULT_SEGMENT_SIZE).unwrap();
        for i in 0..5 {
            let event = make_event(&format!("Event{i}"), b"data");
            writer.append(&[event]).unwrap();
        }
        let seg_path = writer.active_segment_path();
        drop(writer);

        let reader = SegmentReader::open(&seg_path).unwrap();

        // Read only events with position < 2 (so positions 0 and 1).
        let read_events: Vec<_> = reader
            .iter(Some(Position(2)))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(read_events.len(), 2);
        assert_eq!(read_events[0].position, Position(0));
        assert_eq!(read_events[1].position, Position(1));
    }
}
