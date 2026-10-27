use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;

use growable_bloom_filter::GrowableBloom;
use roaring::RoaringTreemap;

use crate::criteria::{Criterion, SourcingCondition};
use crate::error::Error;
use crate::event::{Position, Tag};
use crate::segment::reader::SegmentReader;
/// Reserved tag key for event type names.
const EVENT_TYPE_TAG_KEY: &[u8] = b"__kronosdb_event_type__";

/// Magic bytes for `.idx` files.
const IDX_MAGIC: [u8; 4] = *b"KIDX";
///
/// Version 1 files carry no checksum. Version 2 ends with the CRC32C of
/// everything before it, so a damaged index is refused instead of answering
/// queries wrongly: a flipped bit in a bitmap hides events, one in an offset
/// points a read at the wrong record.
const IDX_VERSION: u8 = 2;
const IDX_LEGACY_VERSION: u8 = 1;

/// Magic bytes for `.bloom` files.
const BLOOM_MAGIC: [u8; 4] = *b"KBLM";
/// Version 2 ends with a CRC32C of everything before it (see `IDX_VERSION`);
/// a bit flipped in a filter can answer "definitely absent" for a tag that
/// is present, which a consistency check would take for no conflict.
const BLOOM_VERSION: u8 = 2;
const BLOOM_LEGACY_VERSION: u8 = 1;

/// Desired bloom filter false positive rate.
const BLOOM_FPR: f64 = 0.01;

/// Forward-key to positions. Keys are client-supplied tag values, so the
/// hasher is seeded per map (foldhash's `fast::RandomState`): several times
/// cheaper than SipHash on short keys, and still not predictable from outside.
type KeyMap = HashMap<Vec<u8>, RoaringTreemap, foldhash::fast::RandomState>;

/// Writes through to `inner` while keeping the CRC32C of what passed.
struct CrcWriter<W: Write> {
    inner: W,
    crc: u32,
}

impl<W: Write> CrcWriter<W> {
    fn new(inner: W) -> Self {
        Self { inner, crc: 0 }
    }

    /// Appends the checksum of everything written so far.
    fn finish(mut self) -> std::io::Result<W> {
        let crc = self.crc;
        self.inner.write_all(&crc.to_le_bytes())?;
        Ok(self.inner)
    }
}

impl<W: Write> Write for CrcWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.crc = crc32c::crc32c_append(self.crc, &buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Splits the checksum off a version-2 companion file and checks it.
fn verified_body<'a>(data: &'a [u8], what: &str) -> Result<&'a [u8], Error> {
    let Some((body, trailer)) = data.len().checked_sub(4).map(|at| data.split_at(at)) else {
        return Err(Error::Corrupted {
            message: format!("{what} file too small for its checksum"),
        });
    };
    let stored = u32::from_le_bytes(trailer.try_into().unwrap());
    if crc32c::crc32c(body) != stored {
        return Err(Error::Corrupted {
            message: format!("{what} checksum mismatch"),
        });
    }
    Ok(body)
}

/// A per-segment tag index with companion bloom filter and offset table.
///
/// Built when a segment is sealed, written to disk as `.idx` and `.bloom` files.
/// Loaded on demand for Source queries on sealed segments.
pub struct SegmentIndex {
    /// Forward index: tag forward key → roaring bitmap of positions.
    bitmaps: KeyMap,
    /// Bloom filter of all tag forward keys in this segment.
    bloom: GrowableBloom,
    /// Bitmap of all positions in this segment.
    all_positions: RoaringTreemap,
    /// Base position of this segment (position of the first event).
    base_position: u64,
    /// Dense position→byte offset table. `offsets[pos - base_position]` gives
    /// the byte offset of the record header for that event within the segment file.
    offsets: Vec<u64>,
    /// Scratch for building a forward key on insert, so a key that is already
    /// indexed costs no allocation.
    key_scratch: Vec<u8>,
    /// `heap_bytes` as worked out while loading the file, where the sizes
    /// are at hand; 0 for an index built in memory.
    loaded_heap_bytes: usize,
    /// Loaded without its tag bitmaps (`read_idx_offsets`): it knows where
    /// every position lives and nothing about what the events are.
    positions_only: bool,
}

/// What one bitmap costs on the heap besides its payload: the key's
/// allocation, the treemap's and the containers' (calibrated, see
/// `SegmentIndex::heap_bytes`).
const HEAP_PER_KEY: usize = 448;

fn bitmap_heap_bytes(key_capacity: usize, serialized_size: usize) -> usize {
    HEAP_PER_KEY + key_capacity + serialized_size * 3 / 2
}

impl SegmentIndex {
    /// Creates an empty index for an in-progress (active) segment. Populated
    /// incrementally via `insert_event` as records are written, so sealing
    /// never has to re-read the segment file.
    pub fn new(base_position: u64) -> Self {
        Self {
            bitmaps: KeyMap::default(),
            bloom: GrowableBloom::new(BLOOM_FPR, 1000),
            all_positions: RoaringTreemap::new(),
            base_position,
            offsets: Vec::new(),
            key_scratch: Vec::new(),
            loaded_heap_bytes: 0,
            positions_only: false,
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
    pub fn insert_scanned(
        &mut self,
        position: u64,
        record_offset: u64,
        scan: &crate::segment::format::Scan<'_>,
    ) {
        self.insert_fields(position, record_offset, scan.name, scan.tags());
    }

    fn insert_fields<'t>(
        &mut self,
        position: u64,
        record_offset: u64,
        name: &[u8],
        tags: impl Iterator<Item = (&'t [u8], &'t [u8])>,
    ) {
        self.all_positions.insert(position);

        let relative = (position - self.base_position) as usize;
        if self.offsets.len() <= relative {
            self.offsets.resize(relative + 1, 0);
        }
        self.offsets[relative] = record_offset;

        let mut key = std::mem::take(&mut self.key_scratch);
        self.index_key(&mut key, EVENT_TYPE_TAG_KEY, name, position);
        for (tag_key, tag_value) in tags {
            self.index_key(&mut key, tag_key, tag_value, position);
        }
        self.key_scratch = key;
    }

    /// Adds `position` under the forward key of `(key, value)`. A key already
    /// in the map is already in the bloom filter, so only a new key pays for
    /// bloom hashing and an owned copy of the key.
    fn index_key(&mut self, scratch: &mut Vec<u8>, key: &[u8], value: &[u8], position: u64) {
        scratch.clear();
        scratch.extend_from_slice(&(key.len() as u32).to_le_bytes());
        scratch.extend_from_slice(key);
        scratch.extend_from_slice(value);
        if let Some(bitmap) = self.bitmaps.get_mut(scratch.as_slice()) {
            bitmap.insert(position);
            return;
        }
        self.bloom.insert(&*scratch);
        let mut bitmap = RoaringTreemap::new();
        bitmap.insert(position);
        self.bitmaps.insert(scratch.clone(), bitmap);
    }

    /// Builds a segment index by reading all events from a segment file.
    pub fn build_from_segment(segment_path: &Path) -> Result<Self, Error> {
        let reader = SegmentReader::open(segment_path)?;
        let mut index = Self::new(reader.base_position());

        // Use the offset-tracking iterator to capture byte positions.
        for (record_offset, result) in reader.iter_with_offsets(None) {
            let event = result?;
            index.insert_event(
                event.position.0,
                record_offset as u64,
                &event.name,
                &event.tags,
            );
        }

        Ok(index)
    }

    /// Estimated heap bytes this index holds in memory: the offset table,
    /// every bitmap and its key, and the hash map around them. The bloom
    /// filter is cached separately and not counted.
    ///
    /// Most of it is per-key overhead, not bitmap payload: a key is a heap
    /// allocation, its treemap another, and each bitmap's containers a third.
    /// The constants were calibrated against a counting allocator on indexes
    /// of 32 MB and 256 MB segments (`examples/index_mem.rs`) and are good to
    /// about 20%, which is all a cache budget needs.
    pub fn heap_bytes(&self) -> usize {
        if self.loaded_heap_bytes != 0 {
            return self.loaded_heap_bytes;
        }
        self.compute_heap_bytes()
    }

    fn compute_heap_bytes(&self) -> usize {
        let bitmaps: usize = self
            .bitmaps
            .iter()
            .map(|(key, bitmap)| bitmap_heap_bytes(key.capacity(), bitmap.serialized_size()))
            .sum();
        self.offsets.capacity() * 8 + bitmaps + self.all_positions.serialized_size()
    }

    /// Whether this index came from `read_idx_offsets`: it answers position
    /// to offset questions and the match-everything criterion, and must not
    /// be asked about tags or event types.
    pub fn is_positions_only(&self) -> bool {
        self.positions_only
    }

    /// The segment's bloom filter over tag and event-type keys.
    pub fn bloom(&self) -> &GrowableBloom {
        &self.bloom
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

    /// Checks if a tag *might* exist in this segment (bloom filter check).
    /// False positives are possible, false negatives are not. Test-only —
    /// the production bloom path goes through `cache::condition_might_match_bloom`.
    #[cfg(test)]
    pub fn might_contain_tag(&self, key: &[u8], value: &[u8]) -> bool {
        let forward_key = make_forward_key(key, value);
        self.bloom.contains(&forward_key)
    }

    /// Checks if an event type name might exist in this segment. Test-only.
    #[cfg(test)]
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

    /// Whether any event in `[from, to)` could match `condition`, decided
    /// from ranks alone: no bitmap is cloned or intersected. A criterion is
    /// possible when each of its tags has a position in the range and, if it
    /// names event types, at least one of them does. Never a false negative;
    /// a true answer still needs `matching` to confirm. This is what lets a
    /// subscriber woken by a commit decide, in a few lookups, that the
    /// commit holds nothing for it.
    pub fn any_match_in(&self, condition: &SourcingCondition, from: u64, to: u64) -> bool {
        if from >= to {
            return false;
        }
        let in_range =
            |bitmap: &RoaringTreemap| first_at_or_after(bitmap, from).is_some_and(|p| p < to);
        condition.criteria.iter().any(|criterion| {
            let tags_possible = criterion.tags.iter().all(|tag| {
                self.bitmaps
                    .get(&make_forward_key(&tag.key, &tag.value))
                    .is_some_and(in_range)
            });
            if !tags_possible {
                return false;
            }
            if criterion.names.is_empty() {
                return if criterion.tags.is_empty() {
                    in_range(&self.all_positions)
                } else {
                    true
                };
            }
            criterion.names.iter().any(|name| {
                self.bitmaps
                    .get(&make_forward_key(EVENT_TYPE_TAG_KEY, name.as_bytes()))
                    .is_some_and(in_range)
            })
        })
    }

    /// Checks if any event matching the query exists at or after the given position.
    /// `after` is the next-exclusive head the caller validated against (DCB marker).
    ///
    /// The answer `first_at_or_after(matching(condition), after)` would give,
    /// without building the matching set: this runs under the writer lock on
    /// every conditional append, and the type bitmap of a busy segment holds
    /// a large share of its events.
    pub fn has_match_after(&self, condition: &SourcingCondition, after: u64) -> Option<Position> {
        condition
            .criteria
            .iter()
            .filter_map(|criterion| self.first_match_in(criterion, after))
            .min()
            .map(Position)
    }

    /// The first position at or after `after` that satisfies `criterion`:
    /// every tag, and one of its event types if it names any. A leapfrog
    /// over the member bitmaps, each probed by rank and select, so the cost
    /// follows the matches at or after `after` (usually none), not the
    /// size of the bitmaps.
    fn first_match_in(&self, criterion: &Criterion, after: u64) -> Option<u64> {
        let mut tags: Vec<&RoaringTreemap> = Vec::with_capacity(criterion.tags.len());
        for tag in &criterion.tags {
            tags.push(self.bitmaps.get(&make_forward_key(&tag.key, &tag.value))?);
        }
        let mut types: Vec<&RoaringTreemap> = Vec::with_capacity(criterion.names.len());
        for name in &criterion.names {
            if let Some(bitmap) = self
                .bitmaps
                .get(&make_forward_key(EVENT_TYPE_TAG_KEY, name.as_bytes()))
            {
                types.push(bitmap);
            }
        }
        if !criterion.names.is_empty() && types.is_empty() {
            return None;
        }
        if tags.is_empty() && types.is_empty() {
            return first_at_or_after(&self.all_positions, after);
        }

        let mut cursor = after;
        loop {
            let mut highest = cursor;
            let mut agreed = true;
            let mut first_seen: Option<u64> = None;
            let mut see = |found: u64| {
                if first_seen.is_some_and(|seen| seen != found) {
                    agreed = false;
                }
                first_seen = Some(found);
                highest = highest.max(found);
            };
            for bitmap in &tags {
                see(first_at_or_after(bitmap, cursor)?);
            }
            if !types.is_empty() {
                see(types
                    .iter()
                    .filter_map(|bitmap| first_at_or_after(bitmap, cursor))
                    .min()?);
            }
            if agreed {
                return Some(highest);
            }
            cursor = highest;
        }
    }

    fn resolve_criterion(&self, criterion: &Criterion) -> Option<RoaringTreemap> {
        let mut parts: Vec<&RoaringTreemap> = Vec::new();

        for tag in &criterion.tags {
            let key = make_forward_key(&tag.key, &tag.value);
            parts.push(self.bitmaps.get(&key)?);
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
            name_bitmap = names_combined?;
            parts.push(&name_bitmap);
        }

        if parts.is_empty() {
            return Some(self.all_positions.clone());
        }

        // Start from the smallest set: the clone is of the most selective
        // tag, and every AND after it can only shrink it.
        parts.sort_by_key(|bitmap| bitmap.len());
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
        // Buffered: the offset table alone is one 8-byte write per event —
        // unbuffered that is ~1M syscalls per seal.
        let mut file = CrcWriter::new(BufWriter::new(File::create(&tmp_path)?));

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

        let mut file = file.finish()?;
        file.flush()?;
        file.get_ref().sync_all()?;
        fs::rename(&tmp_path, path)?;
        Ok(())
    }

    /// Reads an index from an `.idx` file.
    ///
    /// The bloom filter is NOT reconstructed here — bloom checks go through
    /// the separate `.bloom` file via the cache.
    ///
    /// Every read is bounds-checked: a truncated or corrupt file returns
    /// `Error::Corrupted` instead of panicking on an out-of-range slice.
    pub fn read_idx(path: &Path) -> Result<Self, Error> {
        let data = fs::read(path)?;
        let mut reader = ByteReader::new(idx_body(&data)?, 5);

        let count = reader.read_u32(".idx tag count")? as usize;

        let mut bitmaps = KeyMap::with_capacity_and_hasher(count, Default::default());
        let mut heap = 0usize;

        for _ in 0..count {
            let key_len = reader.read_u32(".idx key length")? as usize;
            let key = reader.take(key_len, ".idx key")?.to_vec();

            let bitmap_size = reader.read_u32(".idx bitmap size")? as usize;
            let bitmap = RoaringTreemap::deserialize_from(reader.take(bitmap_size, ".idx bitmap")?)
                .map_err(|e| Error::Corrupted {
                    message: format!("failed to deserialize bitmap: {e}"),
                })?;

            // The key was copied to its exact length and the file says how
            // big the bitmap serializes: no need to ask the bitmap again.
            heap += bitmap_heap_bytes(key.capacity(), bitmap_size);
            bitmaps.insert(key, bitmap);
        }

        // Read all_positions bitmap.
        let all_size = reader.read_u32(".idx all_positions size")? as usize;
        let all_positions =
            RoaringTreemap::deserialize_from(reader.take(all_size, ".idx all_positions")?)
                .map_err(|e| Error::Corrupted {
                    message: format!("failed to deserialize all_positions bitmap: {e}"),
                })?;

        let (base_position, offsets) = read_offset_table(&mut reader)?;
        heap += offsets.capacity() * 8 + all_size;

        // Empty bloom — the .bloom file is loaded separately via the cache.
        let bloom = GrowableBloom::new(BLOOM_FPR, 1);

        Ok(Self {
            bitmaps,
            bloom,
            all_positions,
            base_position,
            offsets,
            key_scratch: Vec::new(),
            loaded_heap_bytes: heap,
            positions_only: false,
        })
    }

    /// Reads only the position→offset table of an `.idx` file: what a read
    /// that takes every event needs (replay, a subscriber catching up, a
    /// point read by position), at a small fraction of the cost of
    /// `read_idx` and of its memory. The tag section is stepped over by its
    /// length prefixes, not deserialized; the file's checksum is still
    /// verified. The result has no tag bitmaps (see `is_positions_only`) and
    /// holds every position of the segment as one range.
    pub fn read_idx_offsets(path: &Path) -> Result<Self, Error> {
        let data = fs::read(path)?;
        let mut reader = ByteReader::new(idx_body(&data)?, 5);

        let count = reader.read_u32(".idx tag count")? as usize;
        for _ in 0..count {
            let key_len = reader.read_u32(".idx key length")? as usize;
            reader.take(key_len, ".idx key")?;
            let bitmap_size = reader.read_u32(".idx bitmap size")? as usize;
            reader.take(bitmap_size, ".idx bitmap")?;
        }
        let all_size = reader.read_u32(".idx all_positions size")? as usize;
        reader.take(all_size, ".idx all_positions")?;

        let (base_position, offsets) = read_offset_table(&mut reader)?;
        let mut all_positions = RoaringTreemap::new();
        all_positions.insert_range(base_position..base_position + offsets.len() as u64);
        let heap = offsets.capacity() * 8 + all_positions.serialized_size();

        Ok(Self {
            bitmaps: KeyMap::default(),
            bloom: GrowableBloom::new(BLOOM_FPR, 1),
            all_positions,
            base_position,
            offsets,
            key_scratch: Vec::new(),
            loaded_heap_bytes: heap,
            positions_only: true,
        })
    }

    /// Writes the bloom filter to a `.bloom` file.
    pub fn write_bloom(&self, path: &Path) -> Result<(), Error> {
        let tmp_path = path.with_extension("bloom.tmp");
        let mut file = CrcWriter::new(BufWriter::new(File::create(&tmp_path)?));

        file.write_all(&BLOOM_MAGIC)?;
        file.write_all(&[BLOOM_VERSION])?;

        let encoded = bincode::serialize(&self.bloom).map_err(|e| Error::Corrupted {
            message: format!("failed to serialize bloom filter: {e}"),
        })?;
        file.write_all(&(encoded.len() as u32).to_le_bytes())?;
        file.write_all(&encoded)?;

        let mut file = file.finish()?;
        file.flush()?;
        file.get_ref().sync_all()?;
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
        let data: &[u8] = match data[4] {
            BLOOM_VERSION => verified_body(&data, ".bloom")?,
            BLOOM_LEGACY_VERSION => &data,
            version => {
                return Err(Error::Corrupted {
                    message: format!("unsupported .bloom version: {version}"),
                });
            }
        };

        let payload_len = u32::from_le_bytes(data[5..9].try_into().unwrap()) as usize;
        let mut reader = ByteReader::new(data, 9);
        let bloom: GrowableBloom =
            bincode::deserialize(reader.take(payload_len, ".bloom payload")?).map_err(|e| {
                Error::Corrupted {
                    message: format!("failed to deserialize bloom filter: {e}"),
                }
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

/// The smallest member of `bitmap` at or after `after`, without walking the
/// members below it: `rank` counts them, `select` skips them.
pub fn first_at_or_after(bitmap: &RoaringTreemap, after: u64) -> Option<u64> {
    if after == 0 {
        return bitmap.min();
    }
    bitmap.select(bitmap.rank(after - 1))
}

/// Creates the forward index map key.
fn make_forward_key(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + key.len() + value.len());
    k.extend_from_slice(&(key.len() as u32).to_le_bytes());
    k.extend_from_slice(key);
    k.extend_from_slice(value);
    k
}

/// Cursor over a fully-read file buffer with bounds-checked reads: a
/// truncated or corrupt file surfaces as `Error::Corrupted` instead of an
/// out-of-range slice panic.
/// The checksummed body of an `.idx` file, after validating its header.
fn idx_body(data: &[u8]) -> Result<&[u8], Error> {
    // Header.
    if data.len() < 9 || data[0..4] != IDX_MAGIC {
        return Err(Error::Corrupted {
            message: "invalid .idx magic bytes".into(),
        });
    }
    match data[4] {
        IDX_VERSION => verified_body(data, ".idx"),
        IDX_LEGACY_VERSION => Ok(data),
        version => Err(Error::Corrupted {
            message: format!("unsupported .idx version: {version}"),
        }),
    }
}

/// The base position and position→offset table that close an `.idx` body.
fn read_offset_table(reader: &mut ByteReader<'_>) -> Result<(u64, Vec<u64>), Error> {
    let base_position = reader.read_u64(".idx base position")?;
    let offset_count = reader.read_u32(".idx offset count")? as usize;

    // The whole table is taken in one bounds check, so a count that the file
    // cannot back is refused before anything is allocated for it.
    let table = reader.take(
        offset_count
            .checked_mul(8)
            .ok_or_else(|| Error::Corrupted {
                message: "offset count overflows".into(),
            })?,
        ".idx offset table",
    )?;
    let offsets: Vec<u64> = table
        .as_chunks::<8>()
        .0
        .iter()
        .map(|entry| u64::from_le_bytes(*entry))
        .collect();
    Ok((base_position, offsets))
}

struct ByteReader<'a> {
    data: &'a [u8],
    cursor: usize,
}

impl<'a> ByteReader<'a> {
    fn new(data: &'a [u8], cursor: usize) -> Self {
        Self { data, cursor }
    }

    fn take(&mut self, len: usize, what: &str) -> Result<&'a [u8], Error> {
        let end = self
            .cursor
            .checked_add(len)
            .filter(|&e| e <= self.data.len());
        match end {
            Some(end) => {
                let slice = &self.data[self.cursor..end];
                self.cursor = end;
                Ok(slice)
            }
            None => Err(Error::Corrupted {
                message: format!(
                    "truncated file reading {what}: need {len} bytes at offset {}, file is {} bytes",
                    self.cursor,
                    self.data.len()
                ),
            }),
        }
    }

    fn read_u32(&mut self, what: &str) -> Result<u32, Error> {
        Ok(u32::from_le_bytes(self.take(4, what)?.try_into().unwrap()))
    }

    fn read_u64(&mut self, what: &str) -> Result<u64, Error> {
        Ok(u64::from_le_bytes(self.take(8, what)?.try_into().unwrap()))
    }
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

        // `after` is the next-exclusive marker: returns first matching pos >= after.
        assert_eq!(index.has_match_after(&cond, 0), Some(Position(1)));
        assert_eq!(index.has_match_after(&cond, 1), Some(Position(1)));
        assert_eq!(index.has_match_after(&cond, 2), Some(Position(3)));
        assert_eq!(index.has_match_after(&cond, 3), Some(Position(3)));
        assert_eq!(index.has_match_after(&cond, 4), None);
    }

    /// The leapfrog check must answer exactly what materializing the
    /// matching set and taking its first member at or after the marker does.
    #[test]
    fn has_match_after_agrees_with_the_matching_set() {
        let mut index = SegmentIndex::new(10);
        let names = ["A", "B", "C"];
        let mut state = 0x2545F4914F6CDD1Du64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        for i in 0..400u64 {
            let name = names[(next() % 3) as usize];
            let tags = vec![
                tag("k", &format!("{}", next() % 7)),
                tag("j", &format!("{}", next() % 3)),
            ];
            index.insert_event(10 + i, 13 + i * 100, name, &tags);
        }
        let mut cases = 0;
        for _ in 0..600 {
            let mut criteria = Vec::new();
            for _ in 0..(1 + next() % 2) {
                let mut tags = Vec::new();
                if next() % 4 != 0 {
                    tags.push(tag("k", &format!("{}", next() % 9)));
                }
                if next() % 3 == 0 {
                    tags.push(tag("j", &format!("{}", next() % 4)));
                }
                let mut crit_names = Vec::new();
                for name in ["A", "B", "Z"] {
                    if next() % 3 == 0 {
                        crit_names.push(name.to_string());
                    }
                }
                criteria.push(Criterion {
                    names: crit_names,
                    tags,
                });
            }
            let condition = SourcingCondition { criteria };
            let after = next() % 460;
            let expected = index
                .matching(&condition)
                .and_then(|set| first_at_or_after(&set, after))
                .map(Position);
            assert_eq!(
                index.has_match_after(&condition, after),
                expected,
                "after {after}"
            );
            cases += expected.is_some() as u32;
        }
        assert!(cases > 100, "the generator must produce real matches");
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

    fn sealed_index(dir: &Path) -> (SegmentIndex, std::path::PathBuf) {
        let mut writer = SegmentWriter::new(dir, Position(0), DEFAULT_SEGMENT_SIZE).unwrap();
        writer
            .append(&[
                make_event("OrderPlaced", vec![tag("orderId", "A")]),
                make_event("PaymentReceived", vec![tag("orderId", "B")]),
            ])
            .unwrap();
        let path = writer.active_segment_path();
        drop(writer);
        let index = SegmentIndex::build_from_segment(&path).unwrap();
        index.write_to_disk(&path).unwrap();
        (index, path)
    }

    #[test]
    fn companion_files_carry_a_checksum_that_catches_a_flipped_bit() {
        let dir = tempfile::tempdir().unwrap();
        let (index, path) = sealed_index(dir.path());
        let idx_path = path.with_extension("idx");
        let bloom_path = path.with_extension("bloom");

        // Intact files load.
        let loaded = SegmentIndex::read_idx(&idx_path).unwrap();
        assert_eq!(loaded.get_offset(1), index.get_offset(1));
        SegmentIndex::read_bloom(&bloom_path).unwrap();

        // One bit anywhere in either file is refused.
        for (path, is_idx) in [(&idx_path, true), (&bloom_path, false)] {
            let intact = std::fs::read(path).unwrap();
            for at in [5, intact.len() / 2, intact.len() - 5] {
                let mut damaged = intact.clone();
                damaged[at] ^= 0x01;
                std::fs::write(path, &damaged).unwrap();
                let error = if is_idx {
                    SegmentIndex::read_idx(path).map(|_| ()).unwrap_err()
                } else {
                    SegmentIndex::read_bloom(path).map(|_| ()).unwrap_err()
                };
                assert!(
                    matches!(error, Error::Corrupted { .. }),
                    "byte {at}: {error:?}"
                );
            }
            std::fs::write(path, &intact).unwrap();
        }
    }

    /// Files written before the checksum existed still load.
    #[test]
    fn companion_files_without_a_checksum_still_load() {
        let dir = tempfile::tempdir().unwrap();
        let (index, path) = sealed_index(dir.path());
        for ext in ["idx", "bloom"] {
            let file = path.with_extension(ext);
            let mut bytes = std::fs::read(&file).unwrap();
            bytes.truncate(bytes.len() - 4);
            bytes[4] = 1;
            std::fs::write(&file, &bytes).unwrap();
        }
        let loaded = SegmentIndex::read_idx(&path.with_extension("idx")).unwrap();
        assert_eq!(loaded.get_offset(1), index.get_offset(1));
        SegmentIndex::read_bloom(&path.with_extension("bloom")).unwrap();
    }

    #[test]
    fn offsets_only_load_agrees_with_the_full_load_on_positions() {
        let dir = tempfile::tempdir().unwrap();
        let (_, path) = sealed_index(dir.path());
        let idx_path = path.with_extension("idx");
        let full = SegmentIndex::read_idx(&idx_path).unwrap();
        let lean = SegmentIndex::read_idx_offsets(&idx_path).unwrap();

        assert!(!full.is_positions_only());
        assert!(lean.is_positions_only());
        assert_eq!(lean.base_position(), full.base_position());
        assert_eq!(lean.offsets(), full.offsets());
        assert_eq!(lean.position_range(), full.position_range());
        for position in full.position_range() {
            assert_eq!(lean.get_offset(position), full.get_offset(position));
        }
        // The match-everything criterion, the one a positions-only index
        // answers, gives the same verdict from any starting point.
        let everything = SourcingCondition {
            criteria: vec![crate::criteria::Criterion {
                names: vec![],
                tags: vec![],
            }],
        };
        let range = full.position_range();
        for from in [0, range.start, range.end - 1, range.end, range.end + 10] {
            assert_eq!(
                lean.any_match_in(&everything, from, u64::MAX),
                full.any_match_in(&everything, from, u64::MAX),
                "from {from}"
            );
        }
        assert!(lean.heap_bytes() < full.heap_bytes());
    }

    #[test]
    fn offsets_only_load_still_verifies_the_checksum() {
        let dir = tempfile::tempdir().unwrap();
        let (_, path) = sealed_index(dir.path());
        let idx_path = path.with_extension("idx");
        let intact = std::fs::read(&idx_path).unwrap();
        // Inside the tag section, which this load steps over without
        // reading: only the checksum can notice.
        let mut damaged = intact.clone();
        damaged[12] ^= 0x01;
        std::fs::write(&idx_path, &damaged).unwrap();
        assert!(matches!(
            SegmentIndex::read_idx_offsets(&idx_path),
            Err(Error::Corrupted { .. })
        ));
        // And a file cut short is refused, not panicked on.
        let mut short = intact.clone();
        short.truncate(short.len() - 20);
        std::fs::write(&idx_path, &short).unwrap();
        assert!(SegmentIndex::read_idx_offsets(&idx_path).is_err());
        assert!(SegmentIndex::read_idx(&idx_path).is_err());
    }

    /// The weight taken from the file's own sizes while loading is the
    /// weight `heap_bytes` works out from the finished index.
    #[test]
    fn heap_bytes_from_loading_matches_heap_bytes_from_counting() {
        let dir = tempfile::tempdir().unwrap();
        let (built, path) = sealed_index(dir.path());
        let loaded = SegmentIndex::read_idx(&path.with_extension("idx")).unwrap();
        assert_ne!(loaded.loaded_heap_bytes, 0);
        assert_eq!(loaded.heap_bytes(), loaded.compute_heap_bytes());
        // The built one has no load-time figure and counts.
        assert_eq!(built.loaded_heap_bytes, 0);
        assert!(built.heap_bytes() > 0);
    }
}
