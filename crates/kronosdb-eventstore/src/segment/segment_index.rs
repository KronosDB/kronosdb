use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;

use growable_bloom_filter::GrowableBloom;
use roaring::RoaringTreemap;

use crate::error::Error;
use crate::event::Position;
use crate::criteria::{Criterion, SourcingCondition};
use crate::segment::reader::SegmentReader;
/// Reserved tag key for event type names (same as in tag_index.rs).
const EVENT_TYPE_TAG_KEY: &[u8] = b"__kronosdb_event_type__";

/// Magic bytes for `.idx` files.
const IDX_MAGIC: [u8; 4] = *b"KIDX";
const IDX_VERSION: u8 = 1;

/// Magic bytes for `.bloom` files.
const BLOOM_MAGIC: [u8; 4] = *b"KBLM";
const BLOOM_VERSION: u8 = 2;

/// Desired bloom filter false positive rate.
const BLOOM_FPR: f64 = 0.01;

/// A per-segment tag index with companion bloom filter.
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
}

impl SegmentIndex {
    /// Builds a segment index by reading all events from a segment file.
    pub fn build_from_segment(segment_path: &Path) -> Result<Self, Error> {
        let reader = SegmentReader::open(segment_path)?;

        let mut bitmaps: HashMap<Vec<u8>, RoaringTreemap> = HashMap::new();
        let mut bloom = GrowableBloom::new(BLOOM_FPR, 1000);
        let mut all_positions = RoaringTreemap::new();

        for result in reader.iter(None) {
            let event = result?;
            let pos = event.position.0;
            all_positions.insert(pos);

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
        })
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

        // Number of entries.
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

        file.sync_all()?;
        fs::rename(&tmp_path, path)?;
        Ok(())
    }

    /// Reads an index from an `.idx` file.
    ///
    /// The bloom filter is NOT reconstructed here — bloom checks go through
    /// the separate `.bloom` file via the cache. This avoids wasting CPU
    /// reinserting every key into a bloom filter that's never queried.
    pub fn read_idx(path: &Path) -> Result<Self, Error> {
        let data = fs::read(path)?;
        let mut cursor;

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
        cursor = 5;

        let count = u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;

        let mut bitmaps = HashMap::with_capacity(count);

        for _ in 0..count {
            let key_len =
                u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            let key = data[cursor..cursor + key_len].to_vec();
            cursor += key_len;

            let bitmap_size =
                u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            let bitmap =
                RoaringTreemap::deserialize_from(&data[cursor..cursor + bitmap_size])
                    .map_err(|e| Error::Corrupted {
                        message: format!("failed to deserialize bitmap: {e}"),
                    })?;
            cursor += bitmap_size;

            bitmaps.insert(key, bitmap);
        }

        // Read all_positions bitmap.
        let all_size =
            u32::from_le_bytes(data[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4;
        let all_positions =
            RoaringTreemap::deserialize_from(&data[cursor..cursor + all_size])
                .map_err(|e| Error::Corrupted {
                    message: format!("failed to deserialize all_positions bitmap: {e}"),
                })?;

        // Empty bloom — the .bloom file is loaded separately via the cache.
        let bloom = GrowableBloom::new(BLOOM_FPR, 1);

        Ok(Self {
            bitmaps,
            bloom,
            all_positions,
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

        let payload_len =
            u32::from_le_bytes(data[5..9].try_into().unwrap()) as usize;
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
    use crate::event::{AppendEvent, Position};
    use crate::segment::writer::SegmentWriter;
    use crate::segment::DEFAULT_SEGMENT_SIZE;
    use crate::event::Tag;

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

        // Write some events.
        let mut writer =
            SegmentWriter::new(dir.path(), Position(1), DEFAULT_SEGMENT_SIZE).unwrap();
        writer
            .append(&[
                make_event("OrderPlaced", vec![tag("orderId", "A"), tag("region", "EU")]),
                make_event("OrderPlaced", vec![tag("orderId", "B"), tag("region", "US")]),
                make_event("PaymentReceived", vec![tag("orderId", "A")]),
            ])
            .unwrap();
        let seg_path = writer.active_segment_path();
        drop(writer);

        // Build index.
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
    }

    #[test]
    fn write_and_read_idx() {
        let dir = tempfile::tempdir().unwrap();

        let mut writer =
            SegmentWriter::new(dir.path(), Position(1), DEFAULT_SEGMENT_SIZE).unwrap();
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

        // Verify companion files exist.
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
    }

    #[test]
    fn write_and_read_bloom() {
        let dir = tempfile::tempdir().unwrap();

        let mut writer =
            SegmentWriter::new(dir.path(), Position(1), DEFAULT_SEGMENT_SIZE).unwrap();
        writer
            .append(&[make_event("OrderPlaced", vec![tag("orderId", "A")])])
            .unwrap();
        let seg_path = writer.active_segment_path();
        drop(writer);

        let index = SegmentIndex::build_from_segment(&seg_path).unwrap();
        let bloom_path = seg_path.with_extension("bloom");
        index.write_bloom(&bloom_path).unwrap();

        // Read bloom and check.
        let bloom = SegmentIndex::read_bloom(&bloom_path).unwrap();
        let key = make_forward_key(b"orderId", b"A");
        assert!(bloom.contains(&key));

        let missing_key = make_forward_key(b"orderId", b"NONEXISTENT");
        assert!(!bloom.contains(&missing_key));
    }

    #[test]
    fn has_match_after() {
        let dir = tempfile::tempdir().unwrap();

        let mut writer =
            SegmentWriter::new(dir.path(), Position(1), DEFAULT_SEGMENT_SIZE).unwrap();
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

        // After position 0 → first match at 1.
        assert_eq!(index.has_match_after(&cond, 0), Some(Position(1)));
        // After position 1 → next match at 3.
        assert_eq!(index.has_match_after(&cond, 1), Some(Position(3)));
        // After position 3 → no more matches.
        assert_eq!(index.has_match_after(&cond, 3), None);
    }
}
