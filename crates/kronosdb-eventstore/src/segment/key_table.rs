//! A sorted table of keys, each with the set of positions that hold it.
//!
//! The table covers a span of positions from a base; every position set is
//! a 32-bit roaring bitmap (portable format) of positions relative to that
//! base. A sealed segment's `.idx` holds one, for its own events. The layout
//! is deliberately a plain sorted run so that many tables can later be
//! merged into one larger one (a log-wide index) by walking them in key
//! order and re-basing their sets; nothing in it is specific to a segment.
//!
//! Four byte regions, each read in place:
//!
//! ```text
//! directory  one 32-byte entry per key, sorted by key bytes:
//!              key_offset u64 | bitmap_offset u64 | key_len u32 |
//!              bitmap_len u32 | count u32 | bitmap_crc u32
//! keys       the key bytes, in directory order, back to back
//! bitmaps    each key's serialized position set
//! checks     CRC32C of each block of 128 directory entries together with
//!            the bytes of their keys (which are contiguous)
//! ```
//!
//! Integrity is checked where the bytes are used, at a cost that follows
//! the lookup rather than the table: a directory block (4 KiB of entries and
//! their keys) is verified the first time a lookup touches it and remembered
//! as verified; a position set is verified against its own checksum every
//! time it is read, which is one pass over bytes the read deserializes
//! anyway. A failed check is `Error::Corrupted`.

use std::sync::atomic::{AtomicU64, Ordering};

use roaring::RoaringBitmap;

use crate::error::Error;
use crate::segment::query::PositionSet;

/// Bytes of one directory entry.
pub(crate) const ENTRY_LEN: usize = 32;
/// Directory entries covered by one check.
pub(crate) const BLOCK_ENTRIES: usize = 128;

/// Which blocks of a region have passed their check, remembered so each is
/// verified once per mapping.
pub(crate) struct VerifiedBlocks(Vec<AtomicU64>);

impl VerifiedBlocks {
    pub(crate) fn new(blocks: usize) -> Self {
        Self(
            (0..blocks.div_ceil(64))
                .map(|_| AtomicU64::new(0))
                .collect(),
        )
    }

    fn is_set(&self, block: usize) -> bool {
        self.0[block / 64].load(Ordering::Relaxed) & (1 << (block % 64)) != 0
    }

    fn set(&self, block: usize) {
        self.0[block / 64].fetch_or(1 << (block % 64), Ordering::Relaxed);
    }

    /// Runs `verify` for `block` unless it already passed.
    pub(crate) fn ensure(
        &self,
        block: usize,
        verify: impl FnOnce() -> Result<(), Error>,
    ) -> Result<(), Error> {
        if self.is_set(block) {
            return Ok(());
        }
        verify()?;
        self.set(block);
        Ok(())
    }
}

/// The regions of a built table, ready to be written.
pub(crate) struct BuiltTable {
    pub(crate) directory: Vec<u8>,
    pub(crate) keys: Vec<u8>,
    pub(crate) bitmaps: Vec<u8>,
    pub(crate) checks: Vec<u8>,
}

/// Builds a table from `(key, positions)` pairs in ascending key order, the
/// positions relative to the table's base.
pub(crate) fn build<'a>(
    entries: impl ExactSizeIterator<Item = (&'a [u8], &'a RoaringBitmap)>,
) -> Result<BuiltTable, Error> {
    let mut table = BuiltTable {
        directory: Vec::with_capacity(entries.len() * ENTRY_LEN),
        keys: Vec::new(),
        bitmaps: Vec::new(),
        checks: Vec::with_capacity(entries.len().div_ceil(BLOCK_ENTRIES) * 4),
    };
    let mut previous: Option<&[u8]> = None;
    for (key, bitmap) in entries {
        if previous.is_some_and(|previous| previous >= key) {
            return Err(Error::Internal {
                message: "key table entries must be distinct and in ascending order".into(),
            });
        }
        previous = Some(key);
        let key_len = u32::try_from(key.len()).map_err(|_| Error::Internal {
            message: format!("key of {} bytes is too long to index", key.len()),
        })?;
        let count = u32::try_from(bitmap.len()).map_err(|_| Error::Internal {
            message: "position set larger than a table's span".into(),
        })?;
        let bitmap_offset = table.bitmaps.len();
        canonical_serialize(bitmap, &mut table.bitmaps);
        let serialized = &table.bitmaps[bitmap_offset..];
        let bitmap_len = u32::try_from(serialized.len()).map_err(|_| Error::Internal {
            message: "position set too large to index".into(),
        })?;
        let bitmap_crc = crc32c::crc32c(serialized);

        let entry = &mut table.directory;
        entry.extend_from_slice(&(table.keys.len() as u64).to_le_bytes());
        entry.extend_from_slice(&(bitmap_offset as u64).to_le_bytes());
        entry.extend_from_slice(&key_len.to_le_bytes());
        entry.extend_from_slice(&bitmap_len.to_le_bytes());
        entry.extend_from_slice(&count.to_le_bytes());
        entry.extend_from_slice(&bitmap_crc.to_le_bytes());
        table.keys.extend_from_slice(key);
    }
    let entries = table.directory.len() / ENTRY_LEN;
    for block in 0..entries.div_ceil(BLOCK_ENTRIES) {
        let (dir, keys) = block_bytes(&table.directory, &table.keys, block)
            .expect("a table just built is consistent");
        let crc = crc32c::crc32c_append(crc32c::crc32c(dir), keys);
        table.checks.extend_from_slice(&crc.to_le_bytes());
    }
    Ok(table)
}

/// Cookie of the portable roaring format without run containers.
const ROARING_COOKIE: u32 = 12346;
/// Most values a container holds as a sorted array; more are a bitmap.
const ARRAY_LIMIT: usize = 4096;

/// Appends `bitmap` in the portable roaring format (the one the C, Java and
/// Go implementations read), in its one canonical form: containers in key
/// order, each an array of its values when it holds at most 4096 of them
/// and a 65536-bit bitmap otherwise, no run containers. The bytes follow
/// from the set alone, never from how it was built, which is what lets two
/// nodes (or two rebuilds) write the same index for the same segment.
pub(crate) fn canonical_serialize(bitmap: &RoaringBitmap, out: &mut Vec<u8>) {
    let values: Vec<u32> = bitmap.iter().collect();
    // (key, first value, end) of each container's run of values.
    let mut containers: Vec<(u16, usize, usize)> = Vec::new();
    let mut start = 0;
    while start < values.len() {
        let key = (values[start] >> 16) as u16;
        let end = start + values[start..].partition_point(|&v| (v >> 16) as u16 == key);
        containers.push((key, start, end));
        start = end;
    }
    out.extend_from_slice(&ROARING_COOKIE.to_le_bytes());
    out.extend_from_slice(&(containers.len() as u32).to_le_bytes());
    for &(key, start, end) in &containers {
        out.extend_from_slice(&key.to_le_bytes());
        out.extend_from_slice(&((end - start - 1) as u16).to_le_bytes());
    }
    let size = |len: usize| {
        if len > ARRAY_LIMIT {
            8192
        } else {
            2 * len as u32
        }
    };
    let mut offset = 8 + 8 * containers.len() as u32;
    for &(_, start, end) in &containers {
        out.extend_from_slice(&offset.to_le_bytes());
        offset += size(end - start);
    }
    for &(_, start, end) in &containers {
        let low = values[start..end].iter().map(|&v| v as u16);
        if end - start > ARRAY_LIMIT {
            let mut words = [0u64; 1024];
            for v in low {
                words[usize::from(v) / 64] |= 1 << (v % 64);
            }
            for word in words {
                out.extend_from_slice(&word.to_le_bytes());
            }
        } else {
            for v in low {
                out.extend_from_slice(&v.to_le_bytes());
            }
        }
    }
}

/// The entries of directory block `block` and the bytes of their keys.
/// `None` when the entries point outside the key region.
fn block_bytes<'a>(
    directory: &'a [u8],
    keys: &'a [u8],
    block: usize,
) -> Option<(&'a [u8], &'a [u8])> {
    let entries = directory.len() / ENTRY_LEN;
    let first = block * BLOCK_ENTRIES;
    let last = (first + BLOCK_ENTRIES).min(entries).checked_sub(1)?;
    let dir = &directory[first * ENTRY_LEN..(last + 1) * ENTRY_LEN];
    let start = RawEntry::at(directory, first).key_offset;
    let tail = RawEntry::at(directory, last);
    let end = tail.key_offset.checked_add(u64::from(tail.key_len))?;
    let keys = keys.get(usize::try_from(start).ok()?..usize::try_from(end).ok()?)?;
    Some((dir, keys))
}

/// A directory entry as stored, not yet checked.
#[derive(Clone, Copy)]
struct RawEntry {
    key_offset: u64,
    bitmap_offset: u64,
    key_len: u32,
    bitmap_len: u32,
    count: u32,
    bitmap_crc: u32,
}

impl RawEntry {
    fn at(directory: &[u8], index: usize) -> Self {
        let e = &directory[index * ENTRY_LEN..(index + 1) * ENTRY_LEN];
        let u64_at = |at: usize| u64::from_le_bytes(e[at..at + 8].try_into().unwrap());
        let u32_at = |at: usize| u32::from_le_bytes(e[at..at + 4].try_into().unwrap());
        Self {
            key_offset: u64_at(0),
            bitmap_offset: u64_at(8),
            key_len: u32_at(16),
            bitmap_len: u32_at(20),
            count: u32_at(24),
            bitmap_crc: u32_at(28),
        }
    }
}

/// A key table read in place. Cheap to make: it borrows its regions and the
/// record of which blocks were verified.
#[derive(Clone, Copy)]
pub struct KeyTable<'a> {
    base: u64,
    span: u64,
    directory: &'a [u8],
    keys: &'a [u8],
    bitmaps: &'a [u8],
    checks: &'a [u8],
    verified: &'a VerifiedBlocks,
}

/// One key of a table and where its positions are.
#[derive(Clone, Copy)]
pub struct KeyEntry<'a> {
    pub key: &'a [u8],
    /// How many positions hold the key.
    pub count: u32,
    base: u64,
    span: u64,
    bitmap: &'a [u8],
    bitmap_crc: u32,
}

impl<'a> KeyTable<'a> {
    /// A view of a table whose regions have the right sizes (`checks` holds
    /// one check per directory block) and were mapped from a file whose
    /// header passed its own check. `verified` must be sized for the
    /// directory's blocks.
    pub(crate) fn new(
        base: u64,
        span: u64,
        directory: &'a [u8],
        keys: &'a [u8],
        bitmaps: &'a [u8],
        checks: &'a [u8],
        verified: &'a VerifiedBlocks,
    ) -> Result<Self, Error> {
        let entries = directory.len() / ENTRY_LEN;
        if !directory.len().is_multiple_of(ENTRY_LEN)
            || checks.len() != entries.div_ceil(BLOCK_ENTRIES) * 4
        {
            return Err(Error::Corrupted {
                message: "key table regions have inconsistent sizes".into(),
            });
        }
        Ok(Self {
            base,
            span,
            directory,
            keys,
            bitmaps,
            checks,
            verified,
        })
    }

    /// Number of keys.
    pub fn len(&self) -> usize {
        self.directory.len() / ENTRY_LEN
    }

    pub fn is_empty(&self) -> bool {
        self.directory.is_empty()
    }

    /// The first position the table covers; its sets are relative to it.
    pub fn base(&self) -> u64 {
        self.base
    }

    /// How many positions from the base the table covers.
    pub fn span(&self) -> u64 {
        self.span
    }

    fn verify_block(&self, block: usize) -> Result<(), Error> {
        self.verified.ensure(block, || {
            let stored =
                u32::from_le_bytes(self.checks[block * 4..block * 4 + 4].try_into().unwrap());
            let (dir, keys) =
                block_bytes(self.directory, self.keys, block).ok_or_else(|| Error::Corrupted {
                    message: format!("key table block {block} points outside its keys"),
                })?;
            if crc32c::crc32c_append(crc32c::crc32c(dir), keys) != stored {
                return Err(Error::Corrupted {
                    message: format!("key table block {block} checksum mismatch"),
                });
            }
            Ok(())
        })
    }

    /// Entry `index`, its block verified.
    pub fn entry(&self, index: usize) -> Result<KeyEntry<'a>, Error> {
        self.verify_block(index / BLOCK_ENTRIES)?;
        let raw = RawEntry::at(self.directory, index);
        let out_of_bounds = || Error::Corrupted {
            message: format!("key table entry {index} points outside its regions"),
        };
        let key_start = usize::try_from(raw.key_offset).map_err(|_| out_of_bounds())?;
        let key = self
            .keys
            .get(key_start..key_start + raw.key_len as usize)
            .ok_or_else(out_of_bounds)?;
        let bitmap_start = usize::try_from(raw.bitmap_offset).map_err(|_| out_of_bounds())?;
        let bitmap = self
            .bitmaps
            .get(bitmap_start..bitmap_start + raw.bitmap_len as usize)
            .ok_or_else(out_of_bounds)?;
        Ok(KeyEntry {
            key,
            count: raw.count,
            base: self.base,
            span: self.span,
            bitmap,
            bitmap_crc: raw.bitmap_crc,
        })
    }

    /// The entry for `key`, by binary search over the sorted directory.
    pub fn find(&self, key: &[u8]) -> Result<Option<KeyEntry<'a>>, Error> {
        let (mut lo, mut hi) = (0usize, self.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let entry = self.entry(mid)?;
            match entry.key.cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok(Some(entry)),
            }
        }
        Ok(None)
    }

    /// Every entry, in key order: what a merge of several tables walks.
    pub fn iter(&self) -> impl Iterator<Item = Result<KeyEntry<'a>, Error>> + '_ {
        (0..self.len()).map(|index| self.entry(index))
    }
}

impl KeyEntry<'_> {
    /// The positions holding the key, relative to the table's base, after
    /// checking them against their checksum and count.
    pub fn bitmap(&self) -> Result<RoaringBitmap, Error> {
        if crc32c::crc32c(self.bitmap) != self.bitmap_crc {
            return Err(Error::Corrupted {
                message: "key table position set checksum mismatch".into(),
            });
        }
        let bitmap =
            RoaringBitmap::deserialize_from(self.bitmap).map_err(|error| Error::Corrupted {
                message: format!("key table position set does not deserialize: {error}"),
            })?;
        if bitmap.len() != u64::from(self.count)
            || bitmap.max().is_some_and(|max| u64::from(max) >= self.span)
        {
            return Err(Error::Corrupted {
                message: "key table position set disagrees with its entry".into(),
            });
        }
        Ok(bitmap)
    }

    /// The positions holding the key.
    pub fn positions(&self) -> Result<PositionSet, Error> {
        Ok(PositionSet::new(self.base, self.bitmap()?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table_of(pairs: &[(&[u8], &[u32])]) -> (BuiltTable, Vec<RoaringBitmap>) {
        let bitmaps: Vec<RoaringBitmap> = pairs
            .iter()
            .map(|(_, positions)| positions.iter().copied().collect())
            .collect();
        let built = build(
            pairs
                .iter()
                .zip(&bitmaps)
                .map(|((key, _), bitmap)| (*key, bitmap)),
        )
        .unwrap();
        (built, bitmaps)
    }

    fn view<'a>(built: &'a BuiltTable, verified: &'a VerifiedBlocks) -> KeyTable<'a> {
        KeyTable::new(
            1_000,
            100,
            &built.directory,
            &built.keys,
            &built.bitmaps,
            &built.checks,
            verified,
        )
        .unwrap()
    }

    #[test]
    fn finds_every_key_and_nothing_else() {
        let keys: Vec<Vec<u8>> = (0..1000u32)
            .map(|k| format!("key-{k:05}").into_bytes())
            .collect();
        let positions: Vec<Vec<u32>> = (0..1000u32).map(|k| vec![k % 100, (k * 7) % 100]).collect();
        let pairs: Vec<(&[u8], &[u32])> = keys
            .iter()
            .zip(&positions)
            .map(|(k, p)| (k.as_slice(), p.as_slice()))
            .collect();
        let (built, bitmaps) = table_of(&pairs);
        let verified = VerifiedBlocks::new(built.checks.len() / 4);
        let table = view(&built, &verified);
        assert_eq!(table.len(), 1000);
        for (key, bitmap) in keys.iter().zip(&bitmaps) {
            let entry = table.find(key).unwrap().unwrap();
            assert_eq!(&entry.bitmap().unwrap(), bitmap);
            assert_eq!(entry.count as u64, bitmap.len());
            let positions = entry.positions().unwrap();
            assert!(positions.iter().all(|p| (1_000..1_100).contains(&p)));
        }
        for missing in [&b""[..], b"key-", b"key-99999", b"zzz", b"key-00000x"] {
            assert!(table.find(missing).unwrap().is_none());
        }
        let walked: Vec<Vec<u8>> = table.iter().map(|e| e.unwrap().key.to_vec()).collect();
        assert_eq!(walked, keys);
    }

    /// The canonical form is the portable format: roaring reads it back, and
    /// for a set built by inserting it is byte for byte what roaring writes.
    /// Built any other way (removals, a set operation) it still comes out
    /// the same for the same set.
    #[test]
    fn canonical_serialization_depends_only_on_the_set() {
        let sets: Vec<RoaringBitmap> = vec![
            RoaringBitmap::new(),
            [0u32].into_iter().collect(),
            (0..4096u32).collect(),
            (0..4097u32).collect(),
            (60_000..140_000u32).step_by(3).collect(),
            (0..300_000u32)
                .filter(|v| v % 17 == 0 || v % 5000 < 10)
                .collect(),
            [u32::MAX, 65_535, 65_536].into_iter().collect(),
        ];
        for set in &sets {
            let mut ours = Vec::new();
            canonical_serialize(set, &mut ours);
            let mut theirs = Vec::new();
            set.serialize_into(&mut theirs).unwrap();
            assert_eq!(ours, theirs, "{} values", set.len());
            assert_eq!(&RoaringBitmap::deserialize_from(&ours[..]).unwrap(), set);

            // The same set reached by removing from a bigger one.
            let mut shrunk: RoaringBitmap = (0..300_000u32).collect();
            shrunk -= &((0..300_000u32).collect::<RoaringBitmap>() - set);
            shrunk |= set;
            let mut again = Vec::new();
            canonical_serialize(&shrunk, &mut again);
            assert_eq!(again, ours);
        }
    }

    #[test]
    fn an_empty_table_answers_nothing() {
        let (built, _) = table_of(&[]);
        let verified = VerifiedBlocks::new(0);
        let table = view(&built, &verified);
        assert!(table.is_empty());
        assert!(table.find(b"anything").unwrap().is_none());
    }

    #[test]
    fn keys_must_arrive_sorted_and_distinct() {
        let a = RoaringBitmap::new();
        assert!(build([(&b"b"[..], &a), (&b"a"[..], &a)].into_iter()).is_err());
        assert!(build([(&b"a"[..], &a), (&b"a"[..], &a)].into_iter()).is_err());
    }

    #[test]
    fn a_flipped_bit_anywhere_is_refused() {
        let keys: Vec<Vec<u8>> = (0..300u32)
            .map(|k| format!("k{k:04}").into_bytes())
            .collect();
        let positions: Vec<Vec<u32>> = (0..300u32).map(|k| vec![k % 50]).collect();
        let pairs: Vec<(&[u8], &[u32])> = keys
            .iter()
            .zip(&positions)
            .map(|(k, p)| (k.as_slice(), p.as_slice()))
            .collect();
        let (built, _) = table_of(&pairs);
        // Flip one bit in each region in turn; every lookup that reaches the
        // damage must refuse, never answer differently.
        for region in 0..4 {
            for at in [0usize, 7, 100, 1000] {
                let mut damaged = BuiltTable {
                    directory: built.directory.clone(),
                    keys: built.keys.clone(),
                    bitmaps: built.bitmaps.clone(),
                    checks: built.checks.clone(),
                };
                let bytes = match region {
                    0 => &mut damaged.directory,
                    1 => &mut damaged.keys,
                    2 => &mut damaged.bitmaps,
                    _ => &mut damaged.checks,
                };
                let at = at % bytes.len();
                bytes[at] ^= 0x10;
                let verified = VerifiedBlocks::new(damaged.checks.len() / 4);
                let table = view(&damaged, &verified);
                let mut refused = false;
                for (key, expected) in keys.iter().zip(&positions) {
                    match table
                        .find(key)
                        .and_then(|e| e.map(|e| e.bitmap()).transpose())
                    {
                        Ok(Some(bitmap)) => {
                            let expected: RoaringBitmap = expected.iter().copied().collect();
                            assert_eq!(bitmap, expected, "region {region} byte {at}");
                        }
                        Ok(None) => panic!("region {region} byte {at}: key lost silently"),
                        Err(Error::Corrupted { .. }) => refused = true,
                        Err(other) => panic!("{other:?}"),
                    }
                }
                assert!(refused, "region {region} byte {at}: damage never noticed");
            }
        }
    }
}
