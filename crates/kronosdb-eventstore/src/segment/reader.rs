use std::path::Path;
use std::sync::Arc;

use memmap2::Mmap;

use crate::error::Error;
use crate::event::{Position, StoredEvent};

use crate::segment::{
    format, RECORD_HEADER_SIZE, SEGMENT_HEADER_SIZE, SEGMENT_MAGIC, SEGMENT_VERSION,
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
            let stored_crc =
                u32::from_le_bytes(self.data[header_start..header_start + 4].try_into().unwrap());
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

            // Deserialize the event.
            match format::deserialize_event(payload) {
                Ok((event, _)) => {
                    // Check position limit.
                    if let Some(up_to) = self.up_to {
                        if event.position >= up_to {
                            return None;
                        }
                    }
                    return Some(Ok(event));
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::writer::SegmentWriter;
    use crate::segment::DEFAULT_SEGMENT_SIZE;
    use crate::event::AppendEvent;
    use crate::event::Tag;

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
        let mut writer =
            SegmentWriter::new(dir.path(), Position(1), DEFAULT_SEGMENT_SIZE).unwrap();
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
        assert_eq!(read_events[0].position, Position(1));
        assert_eq!(read_events[0].name, "OrderPlaced");
        assert_eq!(read_events[0].payload, b"order-data");
        assert_eq!(read_events[1].position, Position(2));
        assert_eq!(read_events[1].name, "PaymentReceived");
        assert_eq!(read_events[1].metadata, vec![("key".into(), "value".into())]);
    }

    #[test]
    fn read_with_position_limit() {
        let dir = tempfile::tempdir().unwrap();

        let mut writer =
            SegmentWriter::new(dir.path(), Position(1), DEFAULT_SEGMENT_SIZE).unwrap();
        for i in 0..5 {
            let event = make_event(&format!("Event{i}"), b"data");
            writer.append(&[event]).unwrap();
        }
        let seg_path = writer.active_segment_path();
        drop(writer);

        let reader = SegmentReader::open(&seg_path).unwrap();

        // Read only events with position < 3 (so positions 1 and 2).
        let read_events: Vec<_> = reader
            .iter(Some(Position(3)))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(read_events.len(), 2);
        assert_eq!(read_events[0].position, Position(1));
        assert_eq!(read_events[1].position, Position(2));
    }
}
