use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use growable_bloom_filter::GrowableBloom;
use roaring::RoaringTreemap;

use crate::criteria::{Criterion, SourcingCondition};
use crate::error::Error;
use crate::event::Position;
use crate::segment::reader::SegmentReader;
/// Reserved tag key for event type names (same as in tag_index.rs).
const EVENT_TYPE_TAG_KEY: &[u8] = b"__kronosdb_event_type__";

/// Magic bytes for `.idx` files.
const IDX_MAGIC: [u8; 4] = *b"KIDX";
const IDX_VERSION: u8 = 1;

/// Magic bytes for `.bloom` files.
const BLOOM_MAGIC: [u8; 4] = *b"KBLM";
const BLOOM_VERSION: u8 = 1;

/// Desired bloom filter false positive rate.
const BLOOM_FPR: f64 = 0.01;

/// A per-segment tag index with companion bloom filter and offset table.
///
/// Built when a segment is sealed, written to disk as `.idx` and `.bloom` files.
/// Loaded on demand for Source queries on sealed segments.
pub struct SegmentIndex {
    /// Forward index: tag forward key → roaring bitmap of positions.
    bitmaps: HashMap<Vec<u8>, RoaringTreemap>,
    /// Bloom filter of all tag forward keys in this segment.
    bloom: GrowableBloom,
    /// Bitmap of all positions in this segment.
    all_positions: RoaringTreemap,
    /// Base position of this segment (position of the first event).
    base_position: u64,
    /// Dense position→byte offset table. `offsets[pos - base_position]` gives
    /// the byte offset of the record header for that event within the segment file.
    offsets: Vec<u64>,
}

impl SegmentIndex {
    /// Builds a segment index by reading all events from a segment file.
    pub fn build_from_segment(segment_path: &Path) -> Result<Self, Error> {
        let reader = SegmentReader::open(segment_path)?;
        let base_position = reader.base_position();

        let mut bitmaps: HashMap<Vec<u8>, RoaringTreemap> = HashMap::new();
        let mut bloom = GrowableBloom::new(BLOOM_FPR, 1000);
        let mut all_positions = RoaringTreemap::new();
        let mut offsets: Vec<u64> = Vec::new();

        // Use the offset-tracking iterator to capture byte positions.
        for (record_offset, result) in reader.iter_with_offsets(None) {
            let event = result?;
            let pos = event.position.0;
            all_positions.insert(pos);

            // Dense offset table: offsets[pos - base] = byte offset.
            let relative = (pos - base_position) as usize;
            if offsets.len() <= relative {
                offsets.resize(relative + 1, 0);
            }
            offsets[relative] = record_offset as u64;

            // Index event type name.
            let type_key = make_forward_key(EVENT_TYPE_TAG_KEY, event.name.as_bytes());
            bloom.insert(&type_key);
            bitmaps
                .entry(type_key)
                .or_insert_with(RoaringTreemap::new)
                .insert(pos);

            // Index each tag.
            for tag in &event.tags {
                let key = make_forward_key(&tag.key, &tag.value);
                bloom.insert(&key);
                bitmaps
                    .entry(key)
                    .or_insert_with(RoaringTreemap::new)
                    .insert(pos);
            }
        }

        Ok(Self {
            bitmaps,
            bloom,
            all_positions,
            base_position,
            offsets,
        })
    }

    /// Returns the byte offset for a given position within this segment.
    pub fn get_offset(&self, position: u64) -> Option<u64> {
        let relative = position.checked_sub(self.base_position)? as usize;
        self.offsets.get(relative).copied()
    }

    /// Checks if a tag *might* exist in this segment (bloom filter check).
    /// False positives are possible, false negatives are not.
    pub fn might_contain_tag(&self, key: &[u8], value: &[u8]) -> bool {
        let forward_key = make_forward_key(key, value);
        self.bloom.contains(&forward_key)
    }

    /// Checks if an event type name might exist in this segment.
    pub fn might_contain_event_type(&self, name: &str) -> bool {
        let forward_key = make_forward_key(EVENT_TYPE_TAG_KEY, name.as_bytes());
        self.bloom.contains(&forward_key)
    }

    /// Resolves a query against this segment's index.
    /// Returns a bitmap of matching positions, or None if no matches.
    pub fn matching(&self, condition: &SourcingCondition) -> Option<RoaringTreemap> {
        let mut result: Option<RoaringTreemap> = None;

        for criterion in &condition.criteria {
            if let Some(bitmap) = self.resolve_criterion(criterion) {
                match &mut result {
                    Some(existing) => *existing |= &bitmap,
                    None => result = Some(bitmap),
                }
            }
        }

        result
    }

    /// Checks if any event matching the query exists after the given position.
    pub fn has_match_after(&self, condition: &SourcingCondition, after: u64) -> Option<Position> {
        let bitmap = self.matching(condition)?;
        bitmap.iter().find(|&pos| pos > after).map(Position)
    }

    fn resolve_criterion(&self, criterion: &Criterion) -> Option<RoaringTreemap> {
        let mut parts: Vec<&RoaringTreemap> = Vec::new();

        for tag in &criterion.tags {
            let key = make_forward_key(&tag.key, &tag.value);
            match self.bitmaps.get(&key) {
                Some(bitmap) => parts.push(bitmap),
                None => return None,
            }
        }

        let name_bitmap;
        if !criterion.names.is_empty() {
            let mut names_combined: Option<RoaringTreemap> = None;
            for name in &criterion.names {
                let key = make_forward_key(EVENT_TYPE_TAG_KEY, name.as_bytes());
                if let Some(bitmap) = self.bitmaps.get(&key) {
                    match &mut names_combined {
                        Some(existing) => *existing |= bitmap,
                        None => names_combined = Some(bitmap.clone()),
                    }
                }
            }
            match names_combined {
                Some(bitmap) => {
                    name_bitmap = bitmap;
                    parts.push(&name_bitmap);
                }
                None => return None,
            }
        }

        if parts.is_empty() {
            return Some(self.all_positions.clone());
        }

        let mut result = parts[0].clone();
        for part in &parts[1..] {
            result &= *part;
        }

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    /// Writes the index to an `.idx` file.
    pub fn write_idx(&self, path: &Path) -> Result<(), Error> {
        let tmp_path = path.with_extension("idx.tmp");
        let mut file = File::create(&tmp_path)?;

        // Header.
        file.write_all(&IDX_MAGIC)?;
        file.write_all(&[IDX_VERSION])?;

        // Number of bitmap entries.
        let count = self.bitmaps.len() as u32;
        file.write_all(&count.to_le_bytes())?;

        // Each entry: key_len + key + bitmap.
        for (key, bitmap) in &self.bitmaps {
            file.write_all(&(key.len() as u32).to_le_bytes())?;
            file.write_all(key)?;

            let bitmap_size = bitmap.serialized_size();
            file.write_all(&(bitmap_size as u32).to_le_bytes())?;
            let mut bitmap_buf = Vec::with_capacity(bitmap_size);
            bitmap.serialize_into(&mut bitmap_buf)?;
            file.write_all(&bitmap_buf)?;
        }

        // Write all_positions bitmap.
        let all_size = self.all_positions.serialized_size();
        file.write_all(&(all_size as u32).to_le_bytes())?;
        let mut all_buf = Vec::with_capacity(all_size);
        self.all_positions.serialize_into(&mut all_buf)?;
        file.write_all(&all_buf)?;

        // v2: Write position→offset table.
        file.write_all(&self.base_position.to_le_bytes())?;
        let offset_count = self.offsets.len() as u32;
        file.write_all(&offset_count.to_le_bytes())?;
        for &offset in &self.offsets {
            file.write_all(&offset.to_le_bytes())?;
        }

        file.sync_all()?;
        fs::rename(&tmp_path, path)?;
        Ok(())
    }

    /// Reads an index from an `.idx` file.
    ///
    /// The bloom filter is NOT reconstructed here — bloom checks go through
    /// the separate `.bloom` file via the cache.
    pub fn read_idx(path: &Path) -> Result<Self, Error> {
        let data = fs::read(path)?;

        // Header.
        if data.len() < 9 || data[0..4] != IDX_MAGIC {
            return Err(Error::Corrupted {
                message: "invalid .idx magic bytes".into(),
            });
        }
        if data[4] != IDX_VERSION {
            return Err(Error::Corrupted {
                message: format!("unsupported .idx version: {}", data[4]),
            });
        }
        let mut cursor = 5;

        let count = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;

        let mut bitmaps = HashMap::with_capacity(count);

        for _ in 0..count {
            let key_len = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            let key = data[cursor..cursor + key_len].to_vec();
            cursor += key_len;

            let bitmap_size =
                u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            let bitmap = RoaringTreemap::deserialize_from(&data[cursor..cursor + bitmap_size])
                .map_err(|e| Error::Corrupted {
                    message: format!("failed to deserialize bitmap: {e}"),
                })?;
            cursor += bitmap_size;

            bitmaps.insert(key, bitmap);
        }

        // Read all_positions bitmap.
        let all_size = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let all_positions = RoaringTreemap::deserialize_from(&data[cursor..cursor + all_size])
            .map_err(|e| Error::Corrupted {
                message: format!("failed to deserialize all_positions bitmap: {e}"),
            })?;
        cursor += all_size;

        // Read position→offset table.
        let base_position = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        let offset_count =
            u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;

        let mut offsets = Vec::with_capacity(offset_count);
        for _ in 0..offset_count {
            let offset = u64::from_le_bytes(data[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            offsets.push(offset);
        }

        // Empty bloom — the .bloom file is loaded separately via the cache.
        let bloom = GrowableBloom::new(BLOOM_FPR, 1);

        Ok(Self {
            bitmaps,
            bloom,
            all_positions,
            base_position,
            offsets,
        })
    }

    /// Writes the bloom filter to a `.bloom` file.
    pub fn write_bloom(&self, path: &Path) -> Result<(), Error> {
        let tmp_path = path.with_extension("bloom.tmp");
        let mut file = File::create(&tmp_path)?;

        file.write_all(&BLOOM_MAGIC)?;
        file.write_all(&[BLOOM_VERSION])?;

        let encoded = bincode::serialize(&self.bloom).map_err(|e| Error::Corrupted {
            message: format!("failed to serialize bloom filter: {e}"),
        })?;
        file.write_all(&(encoded.len() as u32).to_le_bytes())?;
        file.write_all(&encoded)?;

        file.sync_all()?;
        fs::rename(&tmp_path, path)?;
        Ok(())
    }

    /// Reads a bloom filter from a `.bloom` file.
    /// Returns just the bloom filter (lightweight, for keeping in memory).
    pub fn read_bloom(path: &Path) -> Result<GrowableBloom, Error> {
        let data = fs::read(path)?;

        if data.len() < 9 || data[0..4] != BLOOM_MAGIC {
            return Err(Error::Corrupted {
                message: "invalid .bloom magic bytes".into(),
            });
        }
        if data[4] != BLOOM_VERSION {
            return Err(Error::Corrupted {
                message: format!("unsupported .bloom version: {}", data[4]),
            });
        }

        let payload_len = u32::from_le_bytes(data[5..9].try_into().unwrap()) as usize;
        let bloom: GrowableBloom =
            bincode::deserialize(&data[9..9 + payload_len]).map_err(|e| Error::Corrupted {
                message: format!("failed to deserialize bloom filter: {e}"),
            })?;

        Ok(bloom)
    }

    /// Writes both `.idx` and `.bloom` companion files for a segment.
    pub fn write_to_disk(&self, segment_path: &Path) -> Result<(), Error> {
        let idx_path = segment_path.with_extension("idx");
        let bloom_path = segment_path.with_extension("bloom");
        self.write_idx(&idx_path)?;
        self.write_bloom(&bloom_path)?;
        Ok(())
    }

    /// Checks if a segment has companion index files.
    pub fn has_companion_files(segment_path: &Path) -> bool {
        let idx_path = segment_path.with_extension("idx");
        let bloom_path = segment_path.with_extension("bloom");
        idx_path.exists() && bloom_path.exists()
    }
}

/// Creates the forward index map key (same as tag_index.rs).
fn make_forward_key(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + key.len() + value.len());
    k.extend_from_slice(&(key.len() as u32).to_le_bytes());
    k.extend_from_slice(key);
    k.extend_from_slice(value);
    k
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Tag;
    use crate::event::{AppendEvent, Position};
    use crate::segment::DEFAULT_SEGMENT_SIZE;
    use crate::segment::writer::SegmentWriter;

    fn tag(key: &str, value: &str) -> Tag {
        Tag::from_str(key, value)
    }

    fn make_event(name: &str, tags: Vec<Tag>) -> AppendEvent {
        AppendEvent {
            identifier: format!("id-{name}"),
            name: name.into(),
            version: "1.0".into(),
            timestamp: 1712345678000,
            payload: b"data".to_vec(),
            metadata: vec![],
            tags,
        }
    }

    #[test]
    fn build_and_query_segment_index() {
        let dir = tempfile::tempdir().unwrap();

        let mut writer = SegmentWriter::new(dir.path(), Position(1), DEFAULT_SEGMENT_SIZE).unwrap();
        writer
            .append(&[
                make_event(
                    "OrderPlaced",
                    vec![tag("orderId", "A"), tag("region", "EU")],
                ),
                make_event(
                    "OrderPlaced",
                    vec![tag("orderId", "B"), tag("region", "US")],
                ),
                make_event("PaymentReceived", vec![tag("orderId", "A")]),
            ])
            .unwrap();
        let seg_path = writer.active_segment_path();
        drop(writer);

        let index = SegmentIndex::build_from_segment(&seg_path).unwrap();

        // Query: orderId=A
        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("orderId", "A")],
            }],
        };
        let result = index.matching(&cond).unwrap();
        let positions: Vec<u64> = result.iter().collect();
        assert_eq!(positions, vec![1, 3]);

        // Query: orderId=A AND region=EU
        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("orderId", "A"), tag("region", "EU")],
            }],
        };
        let result = index.matching(&cond).unwrap();
        let positions: Vec<u64> = result.iter().collect();
        assert_eq!(positions, vec![1]);

        // Bloom filter checks.
        assert!(index.might_contain_tag(b"orderId", b"A"));
        assert!(index.might_contain_tag(b"orderId", b"B"));
        assert!(!index.might_contain_tag(b"orderId", b"NONEXISTENT"));
        assert!(index.might_contain_event_type("OrderPlaced"));

        // Offset table checks.
        assert!(index.get_offset(1).is_some());
        assert!(index.get_offset(1).is_some());
        assert!(index.get_offset(2).is_some());
        assert!(index.get_offset(3).is_some());
    }

    #[test]
    fn write_and_read_idx() {
        let dir = tempfile::tempdir().unwrap();

        let mut writer = SegmentWriter::new(dir.path(), Position(1), DEFAULT_SEGMENT_SIZE).unwrap();
        writer
            .append(&[
                make_event("OrderPlaced", vec![tag("orderId", "A")]),
                make_event("PaymentReceived", vec![tag("orderId", "A")]),
            ])
            .unwrap();
        let seg_path = writer.active_segment_path();
        drop(writer);

        let index = SegmentIndex::build_from_segment(&seg_path).unwrap();
        index.write_to_disk(&seg_path).unwrap();

        assert!(SegmentIndex::has_companion_files(&seg_path));

        // Read back and query.
        let idx_path = seg_path.with_extension("idx");
        let loaded = SegmentIndex::read_idx(&idx_path).unwrap();

        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("orderId", "A")],
            }],
        };
        let result = loaded.matching(&cond).unwrap();
        let positions: Vec<u64> = result.iter().collect();
        assert_eq!(positions, vec![1, 2]);

        // Offset table survives round-trip.
        assert!(loaded.get_offset(1).is_some());
        let off1 = loaded.get_offset(1).unwrap();
        let off2 = loaded.get_offset(2).unwrap();
        assert!(
            off2 > off1,
            "second event should be at a higher byte offset"
        );
    }

    #[test]
    fn write_and_read_bloom() {
        let dir = tempfile::tempdir().unwrap();

        let mut writer = SegmentWriter::new(dir.path(), Position(1), DEFAULT_SEGMENT_SIZE).unwrap();
        writer
            .append(&[make_event("OrderPlaced", vec![tag("orderId", "A")])])
            .unwrap();
        let seg_path = writer.active_segment_path();
        drop(writer);

        let index = SegmentIndex::build_from_segment(&seg_path).unwrap();
        let bloom_path = seg_path.with_extension("bloom");
        index.write_bloom(&bloom_path).unwrap();

        let bloom = SegmentIndex::read_bloom(&bloom_path).unwrap();
        let key = make_forward_key(b"orderId", b"A");
        assert!(bloom.contains(&key));

        let missing_key = make_forward_key(b"orderId", b"NONEXISTENT");
        assert!(!bloom.contains(&missing_key));
    }

    #[test]
    fn has_match_after() {
        let dir = tempfile::tempdir().unwrap();

        let mut writer = SegmentWriter::new(dir.path(), Position(1), DEFAULT_SEGMENT_SIZE).unwrap();
        writer
            .append(&[
                make_event("OrderPlaced", vec![tag("orderId", "A")]),
                make_event("PaymentReceived", vec![tag("orderId", "B")]),
                make_event("OrderShipped", vec![tag("orderId", "A")]),
            ])
            .unwrap();
        let seg_path = writer.active_segment_path();
        drop(writer);

        let index = SegmentIndex::build_from_segment(&seg_path).unwrap();

        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("orderId", "A")],
            }],
        };

        assert_eq!(index.has_match_after(&cond, 0), Some(Position(1)));
        assert_eq!(index.has_match_after(&cond, 1), Some(Position(3)));
        assert_eq!(index.has_match_after(&cond, 3), None);
    }

    #[test]
    fn direct_offset_reads_match_sequential() {
        let dir = tempfile::tempdir().unwrap();

        let mut writer = SegmentWriter::new(dir.path(), Position(1), DEFAULT_SEGMENT_SIZE).unwrap();
        writer
            .append(&[
                make_event("OrderPlaced", vec![tag("orderId", "A")]),
                make_event("PaymentReceived", vec![tag("orderId", "B")]),
                make_event("OrderShipped", vec![tag("orderId", "A")]),
            ])
            .unwrap();
        let seg_path = writer.active_segment_path();
        drop(writer);

        let index = SegmentIndex::build_from_segment(&seg_path).unwrap();
        let reader = SegmentReader::open(&seg_path).unwrap();

        // Read all events sequentially.
        let sequential: Vec<_> = reader.iter(None).collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(sequential.len(), 3);

        // Read positions 1 and 3 via direct offset.
        let off1 = index.get_offset(1).unwrap();
        let off3 = index.get_offset(3).unwrap();

        let evt1 = reader.read_event_at(off1 as usize).unwrap();
        let evt3 = reader.read_event_at(off3 as usize).unwrap();

        assert_eq!(evt1.position, sequential[0].position);
        assert_eq!(evt1.name, sequential[0].name);
        assert_eq!(evt1.payload, sequential[0].payload);

        assert_eq!(evt3.position, sequential[2].position);
        assert_eq!(evt3.name, sequential[2].name);
    }
}
