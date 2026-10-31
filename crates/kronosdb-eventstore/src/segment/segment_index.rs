use std::collections::HashMap;
use std::path::Path;

use roaring::RoaringBitmap;

use crate::error::Error;
use crate::event::{Position, Tag};
use crate::segment::format::{self, ControlRecord};
use crate::segment::key_table::{self, TableInfo};
use crate::segment::query::{
    EVENT_TYPE_TAG_KEY, KeySource, Locator, PositionSet, Positions, Probe, ProbeKey, Resolved,
    write_forward_key,
};
use crate::segment::record;
use crate::segment::sealed_index::{self, LocatorParts, LoggedControl};
use crate::segment::{RECORD_HEADER_SIZE, SEGMENT_HEADER_SIZE, flags};

/// Forward key to positions, relative to the segment's base. Keys are
/// client-supplied tag values, so the hasher is seeded per map (foldhash's
/// `fast::RandomState`): several times cheaper than SipHash on short keys,
/// and still not predictable from outside.
type KeyMap = HashMap<Vec<u8>, RoaringBitmap, foldhash::fast::RandomState>;

/// The index of a segment held in memory: the active segment's, maintained
/// as records are written, and a freshly sealed segment's until its files
/// are on disk. A sealed segment is otherwise served from its mapped `.idx`
/// (`SealedIndex`, where its events are) and its `.keys` or a merged table
/// (`KeyTableFile`, which events hold which keys), both of which this writes.
pub struct SegmentIndex {
    /// Forward key → positions holding it, relative to `base_position`.
    bitmaps: KeyMap,
    /// Base position of this segment (position of the first event).
    base_position: u64,
    /// Dense position→byte offset table: `offsets[pos - base_position]` is
    /// the byte offset of that event's record within the segment file.
    offsets: Vec<u64>,
    /// The segment's control records, in log order.
    controls: Vec<LoggedControl>,
    /// Where the segment's data ends, once it is sealed (or was read back).
    data_len: Option<u64>,
    /// Scratch for building a forward key on insert, so a key that is already
    /// indexed costs no allocation.
    key_scratch: Vec<u8>,
}

impl SegmentIndex {
    /// Creates an empty index for an in-progress (active) segment. Populated
    /// incrementally as records are written, so sealing never has to re-read
    /// the segment file.
    pub fn new(base_position: u64) -> Self {
        Self {
            bitmaps: KeyMap::default(),
            base_position,
            offsets: Vec::new(),
            controls: Vec::new(),
            data_len: None,
            key_scratch: Vec::new(),
        }
    }

    /// Returns the base position of the segment this index covers.
    pub fn base_position(&self) -> u64 {
        self.base_position
    }

    /// Indexes one event written at `record_offset` within the segment file.
    /// Positions within a segment are dense and ascending.
    pub fn insert_event(&mut self, position: u64, record_offset: u64, name: &str, tags: &[Tag]) {
        self.insert_fields(
            position,
            record_offset,
            name.as_bytes(),
            tags.iter().map(|tag| (&tag.key[..], &tag.value[..])),
        );
    }

    /// [`insert_event`](Self::insert_event) for an event read in place from
    /// its record: nothing was copied to describe it.
    pub fn insert_scanned(&mut self, position: u64, record_offset: u64, scan: &format::Scan<'_>) {
        self.insert_fields(position, record_offset, scan.name, scan.tags());
    }

    /// Notes a control record written after every event indexed so far.
    pub fn insert_control(&mut self, record: ControlRecord) {
        self.controls.push(LoggedControl {
            at: self.base_position + self.offsets.len() as u64,
            record,
        });
    }

    fn insert_fields<'t>(
        &mut self,
        position: u64,
        record_offset: u64,
        name: &[u8],
        tags: impl Iterator<Item = (&'t [u8], &'t [u8])>,
    ) {
        let relative = (position - self.base_position) as usize;
        if self.offsets.len() <= relative {
            self.offsets.resize(relative + 1, 0);
        }
        self.offsets[relative] = record_offset;

        let mut key = std::mem::take(&mut self.key_scratch);
        self.index_key(&mut key, EVENT_TYPE_TAG_KEY, name, relative as u32);
        for (tag_key, tag_value) in tags {
            self.index_key(&mut key, tag_key, tag_value, relative as u32);
        }
        self.key_scratch = key;
    }

    /// Adds `relative` under the forward key of `(key, value)`. Only a new
    /// key pays for an owned copy of itself.
    fn index_key(&mut self, scratch: &mut Vec<u8>, key: &[u8], value: &[u8], relative: u32) {
        write_forward_key(scratch, key, value);
        if let Some(bitmap) = self.bitmaps.get_mut(scratch.as_slice()) {
            bitmap.insert(relative);
            return;
        }
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(relative);
        self.bitmaps.insert(scratch.clone(), bitmap);
    }

    /// Builds a segment's index by reading its records in place: every event
    /// and control record up to the end of the written data (a zero record
    /// length, or a record the file cannot hold). A record whose checksum
    /// fails, or an event out of sequence, is `Error::Corrupted`.
    pub fn build_from_segment(segment_path: &Path) -> Result<Self, Error> {
        let file = std::fs::File::open(segment_path)?;
        // SAFETY: read-only mapping of a segment file; the bytes read are
        // CRC-checked before they are trusted.
        let map = unsafe { memmap2::Mmap::map(&file)? };
        let data = &map[..];
        if data.len() < SEGMENT_HEADER_SIZE
            || data[0..4] != crate::segment::SEGMENT_MAGIC
            || data[4] != crate::segment::SEGMENT_VERSION
        {
            return Err(Error::Corrupted {
                message: format!("{} has no valid segment header", segment_path.display()),
            });
        }
        let base = u64::from_le_bytes(data[5..13].try_into().unwrap());
        let mut index = Self::new(base);
        let mut offset = SEGMENT_HEADER_SIZE;
        while offset + RECORD_HEADER_SIZE <= data.len() {
            let header: &[u8; RECORD_HEADER_SIZE] = data[offset..offset + RECORD_HEADER_SIZE]
                .try_into()
                .unwrap();
            let Some(header) = record::parse_header(header)? else {
                break;
            };
            let end = offset + header.total_len();
            if end > data.len() {
                break;
            }
            let payload = &data[offset + RECORD_HEADER_SIZE..end];
            if !record::validate_crc(header, payload) {
                return Err(Error::Corrupted {
                    message: format!(
                        "CRC mismatch at offset {offset} of {}",
                        segment_path.display()
                    ),
                });
            }
            if flags::is_event(header.flags) {
                let expected = base + index.offsets.len() as u64;
                let position = payload
                    .get(..8)
                    .map(|p| u64::from_le_bytes(p.try_into().unwrap()));
                if position != Some(expected) {
                    return Err(Error::Corrupted {
                        message: format!(
                            "event at offset {offset} of {} is out of sequence (expected position {expected})",
                            segment_path.display()
                        ),
                    });
                }
                index.insert_scanned(expected, offset as u64, &format::scan(payload)?);
            } else if flags::is_control(header.flags) {
                index.insert_control(format::deserialize_control(header.flags, payload)?);
            }
            offset = end;
        }
        index.data_len = Some(offset as u64);
        Ok(index)
    }

    /// The positions this segment holds: dense from its base.
    pub fn position_range(&self) -> std::ops::Range<u64> {
        self.base_position..self.base_position + self.offsets.len() as u64
    }

    /// The dense position→offset table: `offsets()[pos - base_position]` is
    /// the byte offset of that event's record header. Ascending.
    pub fn offsets(&self) -> &[u64] {
        &self.offsets
    }

    /// Returns the byte offset for a given position within this segment.
    pub fn get_offset(&self, position: u64) -> Option<u64> {
        let relative = position.checked_sub(self.base_position)? as usize;
        self.offsets.get(relative).copied()
    }

    /// The segment's control records, in log order.
    pub fn controls(&self) -> &[LoggedControl] {
        &self.controls
    }

    /// Number of distinct keys (tags and event types).
    pub fn key_count(&self) -> usize {
        self.bitmaps.len()
    }

    /// Positions matching `condition`. A convenience for callers with no
    /// prepared probe.
    pub fn matching_condition(
        &self,
        condition: &crate::criteria::SourcingCondition,
    ) -> Option<PositionSet> {
        Resolved::new(self, &Probe::new(condition))
            .and_then(|resolved| resolved.matching_in(0, u64::MAX))
            .expect("an in-memory index cannot fail")
    }

    /// The first position at or after `after` matching `condition`: the DCB
    /// check.
    pub fn has_match_after(
        &self,
        condition: &crate::criteria::SourcingCondition,
        after: u64,
    ) -> Option<Position> {
        Resolved::new(self, &Probe::new(condition))
            .and_then(|resolved| resolved.first_at_or_after(after))
            .expect("an in-memory index cannot fail")
            .map(Position)
    }

    /// Records where the segment's data ends: it is sealed.
    pub fn set_data_len(&mut self, data_len: u64) {
        self.data_len = Some(data_len);
    }

    fn locator_parts(&self) -> Result<LocatorParts<'_>, Error> {
        Ok(LocatorParts {
            base: self.base_position,
            offsets: &self.offsets,
            controls: &self.controls,
            data_len: self.data_len.ok_or_else(|| Error::Internal {
                message: format!(
                    "segment {} is not sealed: its data length is unknown",
                    self.base_position
                ),
            })?,
        })
    }

    /// The segment's `.idx` as bytes. Deterministic: the same segment gives
    /// the same bytes however its index was built.
    pub fn encode_locator(&self) -> Result<Vec<u8>, Error> {
        sealed_index::encode(self.locator_parts()?)
    }

    /// Writes the segment's `.idx` next to it.
    pub fn write_locator(&self, segment_path: &Path) -> Result<(), Error> {
        sealed_index::write(&segment_path.with_extension("idx"), self.locator_parts()?)
    }

    /// Writes the segment's `.keys` next to it: its key table.
    pub fn write_keys(&self, segment_path: &Path) -> Result<TableInfo, Error> {
        key_table::write_table(
            &segment_path.with_extension("keys"),
            self.base_position,
            self.offsets.len() as u64,
            self.bitmaps
                .iter()
                .map(|(key, bitmap)| (key.as_slice(), bitmap)),
        )
    }

    /// Writes both of the segment's index files: `.keys`, then `.idx`.
    pub fn write_to_disk(&self, segment_path: &Path) -> Result<(), Error> {
        self.write_keys(segment_path)?;
        self.write_locator(segment_path)
    }
}

impl KeySource for SegmentIndex {
    fn base(&self) -> u64 {
        self.base_position
    }

    fn span(&self) -> u64 {
        self.offsets.len() as u64
    }

    fn positions(&self, key: &ProbeKey) -> Result<Option<Positions<'_>>, Error> {
        Ok(self.bitmaps.get(&key.bytes).map(Positions::Memory))
    }
}

impl Locator for SegmentIndex {
    fn base_position(&self) -> u64 {
        self.base_position
    }

    fn event_count(&self) -> u64 {
        self.offsets.len() as u64
    }

    fn offset_of(&self, position: u64) -> Result<Option<u64>, Error> {
        Ok(self.get_offset(position))
    }

    fn record_len(&self, position: u64) -> Result<Option<u32>, Error> {
        let Some(offset) = self.get_offset(position) else {
            return Ok(None);
        };
        let next = match self.get_offset(position + 1) {
            Some(next) => next,
            None => match self.data_len {
                Some(end) => end,
                None => return Ok(None),
            },
        };
        sealed_index::record_len_before(offset, next, &self.controls, position + 1).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::criteria::{Criterion, SourcingCondition};
    use crate::event::AppendEvent;
    use crate::segment::DEFAULT_SEGMENT_SIZE;
    use crate::segment::key_table::KeyTableFile;
    use crate::segment::reader::SegmentReader;
    use crate::segment::sealed_index::SealedIndex;
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

    fn by_tags(tags: Vec<Tag>) -> SourcingCondition {
        SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags,
            }],
        }
    }

    fn positions(set: Option<PositionSet>) -> Vec<u64> {
        set.map(|s| s.iter().collect()).unwrap_or_default()
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
        let cond = by_tags(vec![tag("orderId", "A")]);
        assert_eq!(positions(index.matching_condition(&cond)), vec![1, 3]);
        let cond = by_tags(vec![tag("orderId", "A"), tag("region", "EU")]);
        assert_eq!(positions(index.matching_condition(&cond)), vec![1]);
        assert!(index.get_offset(1).is_some());
        assert!(index.get_offset(3).is_some());
        assert!(index.get_offset(4).is_none());
    }

    #[test]
    fn write_and_map_idx() {
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
        let mapped = SealedIndex::open(&seg_path.with_extension("idx"), 1).unwrap();
        let keys = KeyTableFile::open(&seg_path.with_extension("keys")).unwrap();
        let probe = Probe::new(&by_tags(vec![tag("orderId", "A")]));
        let resolved = Resolved::new(&keys, &probe).unwrap();
        assert_eq!(
            positions(resolved.matching_in(0, u64::MAX).unwrap()),
            vec![1, 2]
        );
        let (o1, o2) = (
            mapped.offset_of(1).unwrap().unwrap(),
            mapped.offset_of(2).unwrap().unwrap(),
        );
        assert!(o2 > o1, "second event should be at a higher byte offset");
        assert_eq!(Some(o1), index.get_offset(1));
        assert!(mapped.offset_of(3).unwrap().is_none());
        assert!(mapped.offset_of(0).unwrap().is_none());
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
        let cond = by_tags(vec![tag("orderId", "A")]);
        // `after` is the next-exclusive marker: returns first matching pos >= after.
        assert_eq!(index.has_match_after(&cond, 0), Some(Position(1)));
        assert_eq!(index.has_match_after(&cond, 1), Some(Position(1)));
        assert_eq!(index.has_match_after(&cond, 2), Some(Position(3)));
        assert_eq!(index.has_match_after(&cond, 3), Some(Position(3)));
        assert_eq!(index.has_match_after(&cond, 4), None);
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
        let sequential: Vec<_> = reader.iter(None).collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(sequential.len(), 3);
        let evt1 = reader
            .read_event_at(index.get_offset(1).unwrap() as usize)
            .unwrap();
        let evt3 = reader
            .read_event_at(index.get_offset(3).unwrap() as usize)
            .unwrap();
        assert_eq!(evt1.position, sequential[0].position);
        assert_eq!(evt1.payload, sequential[0].payload);
        assert_eq!(evt3.position, sequential[2].position);
        assert_eq!(evt3.name, sequential[2].name);
    }

    #[test]
    fn control_records_are_indexed_where_they_sit() {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::new(dir.path(), Position(5), DEFAULT_SEGMENT_SIZE).unwrap();
        let epoch = ControlRecord::EpochChange {
            epoch: 3,
            leader_id: 7,
            start_position: 5,
        };
        writer.write_control(&epoch).unwrap();
        writer
            .append(&[make_event("A", vec![]), make_event("B", vec![])])
            .unwrap();
        let checkpoint = ControlRecord::WatermarkCheckpoint {
            epoch: 3,
            position: 6,
        };
        writer.write_control(&checkpoint).unwrap();
        let live: Vec<LoggedControl> = writer.active_index_handle().read().controls().to_vec();
        let seg_path = writer.active_segment_path();
        drop(writer);

        let expected = vec![
            LoggedControl {
                at: 5,
                record: epoch,
            },
            LoggedControl {
                at: 7,
                record: checkpoint,
            },
        ];
        assert_eq!(live, expected);
        let built = SegmentIndex::build_from_segment(&seg_path).unwrap();
        assert_eq!(built.controls(), expected.as_slice());
        built.write_to_disk(&seg_path).unwrap();
        let mapped = SealedIndex::open(&seg_path.with_extension("idx"), 5).unwrap();
        assert_eq!(mapped.controls(), expected.as_slice());
        assert_eq!(mapped.event_count(), 2);
    }
}
