//! One key's positions, read in place from a key table's mapping.
//!
//! A set is stored in roaring's portable format without run containers (see
//! `key_table::canonical_serialize`):
//!
//! ```text
//! cookie u32 (12346) | containers u32 |
//! per container: key u16, cardinality - 1 u16 |
//! per container: byte offset u32 (from the start of the set) |
//! containers: a sorted array of u16 when they hold at most 4096 values,
//!             otherwise a 65536-bit bitmap (8 KiB)
//! ```
//!
//! A merged table's set can span billions of positions, so it is never
//! deserialized whole: the questions a read asks (the first member at or
//! after a position, the last one before it, the members in a range) find
//! the containers they need by binary search over the container keys and
//! read only those. Every byte is fetched through a [`Region`], which
//! verifies the 4 KiB chunks it lies in the first time they are touched.

use roaring::RoaringBitmap;

use crate::error::Error;
use crate::segment::key_table::VerifiedBlocks;

/// Cookie of the portable format without run containers.
pub(crate) const COOKIE: u32 = 12346;
/// Most values a container holds as a sorted array.
pub(crate) const ARRAY_LIMIT: usize = 4096;
/// Bytes of a bitmap container.
const BITMAP_BYTES: usize = 8192;
/// Bytes covered by one check of a bitmaps region.
pub(crate) const CHUNK: usize = 4096;

/// A region of a mapped file guarded by one CRC32C per [`CHUNK`] bytes.
#[derive(Clone, Copy)]
pub(crate) struct Region<'a> {
    pub(crate) bytes: &'a [u8],
    pub(crate) checks: &'a [u8],
    pub(crate) verified: &'a VerifiedBlocks,
}

impl<'a> Region<'a> {
    /// `len` bytes at `at`, after verifying the chunks they lie in.
    pub(crate) fn get(&self, at: usize, len: usize) -> Result<&'a [u8], Error> {
        let end = at
            .checked_add(len)
            .filter(|&end| end <= self.bytes.len())
            .ok_or_else(|| Error::Corrupted {
                message: format!("key table position set reaches past its region ({at}+{len})"),
            })?;
        if len > 0 {
            for chunk in at / CHUNK..=(end - 1) / CHUNK {
                self.verified.ensure(chunk, || {
                    let start = chunk * CHUNK;
                    let stop = (start + CHUNK).min(self.bytes.len());
                    let stored = self
                        .checks
                        .get(chunk * 4..chunk * 4 + 4)
                        .map(|b| u32::from_le_bytes(b.try_into().unwrap()));
                    if stored != Some(crc32c::crc32c(&self.bytes[start..stop])) {
                        return Err(Error::Corrupted {
                            message: format!(
                                "key table position sets: chunk {chunk} checksum mismatch"
                            ),
                        });
                    }
                    Ok(())
                })?;
            }
        }
        Ok(&self.bytes[at..end])
    }
}

/// The checks of a region: one CRC32C per [`CHUNK`] bytes.
#[cfg(test)]
pub(crate) fn chunk_checks(bytes: &[u8]) -> Vec<u8> {
    bytes
        .chunks(CHUNK)
        .flat_map(|chunk| crc32c::crc32c(chunk).to_le_bytes())
        .collect()
}

/// A set in place: where it sits in its region, how many members it has,
/// and the span its members must lie in.
#[derive(Clone, Copy)]
pub struct MappedSet<'a> {
    region: Region<'a>,
    at: usize,
    len: usize,
    count: u64,
    containers: usize,
}

/// One container, located.
#[derive(Clone, Copy)]
struct Container {
    key: u16,
    cardinality: usize,
    at: usize,
}

impl Container {
    fn is_bitmap(&self) -> bool {
        self.cardinality > ARRAY_LIMIT
    }

    fn bytes(&self) -> usize {
        if self.is_bitmap() {
            BITMAP_BYTES
        } else {
            2 * self.cardinality
        }
    }
}

fn corrupted(what: impl std::fmt::Display) -> Error {
    Error::Corrupted {
        message: format!("key table position set {what}"),
    }
}

impl<'a> MappedSet<'a> {
    /// The set of `len` bytes at `at` in `region`, holding `count` members.
    /// Checks the cookie and that the container directory fits; each
    /// container is checked against the set's bounds when it is read.
    pub(crate) fn new(
        region: Region<'a>,
        at: usize,
        len: usize,
        count: u64,
    ) -> Result<Self, Error> {
        let head = region.get(at, 8.min(len))?;
        if head.len() < 8 || u32::from_le_bytes(head[0..4].try_into().unwrap()) != COOKIE {
            return Err(corrupted("is not in the portable format"));
        }
        let containers = u32::from_le_bytes(head[4..8].try_into().unwrap()) as usize;
        if 8 + 8 * containers > len || (containers == 0) != (count == 0) {
            return Err(corrupted("has an inconsistent container directory"));
        }
        Ok(Self {
            region,
            at,
            len,
            count,
            containers,
        })
    }

    /// How many positions the set holds.
    pub fn len(&self) -> u64 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn container(&self, index: usize) -> Result<Container, Error> {
        let entry = self.region.get(self.at + 8 + 4 * index, 4)?;
        let offset = self
            .region
            .get(self.at + 8 + 4 * self.containers + 4 * index, 4)?;
        let container = Container {
            key: u16::from_le_bytes([entry[0], entry[1]]),
            cardinality: usize::from(u16::from_le_bytes([entry[2], entry[3]])) + 1,
            at: u32::from_le_bytes(offset.try_into().unwrap()) as usize,
        };
        if container.at < 8 + 8 * self.containers || container.at + container.bytes() > self.len {
            return Err(corrupted(format_args!(
                "container {index} lies outside the set"
            )));
        }
        Ok(container)
    }

    fn container_bytes(&self, container: &Container) -> Result<&'a [u8], Error> {
        self.region.get(self.at + container.at, container.bytes())
    }

    /// Index of the first container whose key is at least `key`.
    fn container_at_least(&self, key: u16) -> Result<usize, Error> {
        let (mut lo, mut hi) = (0, self.containers);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.container(mid)?.key < key {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    /// The smallest member at or after `value`.
    pub fn first_at_or_after(&self, value: u32) -> Result<Option<u32>, Error> {
        let high = (value >> 16) as u16;
        let mut index = self.container_at_least(high)?;
        while index < self.containers {
            let container = self.container(index)?;
            let low = if container.key == high {
                value as u16
            } else {
                0
            };
            let bytes = self.container_bytes(&container)?;
            if let Some(found) = first_in(&container, bytes, low) {
                return Ok(Some(u32::from(container.key) << 16 | u32::from(found)));
            }
            index += 1;
        }
        Ok(None)
    }

    /// The largest member below `bound` (which may be 2^32).
    pub fn last_before(&self, bound: u64) -> Result<Option<u32>, Error> {
        if bound == 0 {
            return Ok(None);
        }
        let last = (bound - 1).min(u64::from(u32::MAX)) as u32;
        let high = (last >> 16) as u16;
        // The container holding `high`, or the last one before it.
        let mut index = self.container_at_least(high)?;
        if index < self.containers && self.container(index)?.key == high {
            index += 1;
        }
        while index > 0 {
            index -= 1;
            let container = self.container(index)?;
            let low = if container.key == high {
                last as u16
            } else {
                u16::MAX
            };
            let bytes = self.container_bytes(&container)?;
            if let Some(found) = last_in(&container, bytes, low) {
                return Ok(Some(u32::from(container.key) << 16 | u32::from(found)));
            }
        }
        Ok(None)
    }

    /// The members in `[from, to)`, `to` up to 2^32. Only the containers
    /// that overlap the range are read; their bytes are copied as they are
    /// into a portable buffer and deserialized, so a dense range costs a
    /// copy, not a value-by-value rebuild.
    pub fn range(&self, from: u32, to: u64) -> Result<RoaringBitmap, Error> {
        if u64::from(from) >= to || self.containers == 0 {
            return Ok(RoaringBitmap::new());
        }
        let last = (to - 1).min(u64::from(u32::MAX)) as u32;
        let (first_key, last_key) = ((from >> 16) as u16, (last >> 16) as u16);
        let start = self.container_at_least(first_key)?;
        let mut picked = Vec::new();
        let mut index = start;
        while index < self.containers {
            let container = self.container(index)?;
            if container.key > last_key {
                break;
            }
            picked.push(container);
            index += 1;
        }
        if picked.is_empty() {
            return Ok(RoaringBitmap::new());
        }
        let mut buffer = Vec::with_capacity(
            8 + 8 * picked.len() + picked.iter().map(Container::bytes).sum::<usize>(),
        );
        buffer.extend_from_slice(&COOKIE.to_le_bytes());
        buffer.extend_from_slice(&(picked.len() as u32).to_le_bytes());
        for container in &picked {
            buffer.extend_from_slice(&container.key.to_le_bytes());
            buffer.extend_from_slice(&((container.cardinality - 1) as u16).to_le_bytes());
        }
        let mut offset = 8 + 8 * picked.len() as u32;
        for container in &picked {
            buffer.extend_from_slice(&offset.to_le_bytes());
            offset += container.bytes() as u32;
        }
        for container in &picked {
            buffer.extend_from_slice(self.container_bytes(container)?);
        }
        let mut set = RoaringBitmap::deserialize_from(&buffer[..])
            .map_err(|error| corrupted(format_args!("does not deserialize: {error}")))?;
        if from > 0 {
            set.remove_range(..from);
        }
        if to <= u64::from(u32::MAX) {
            set.remove_range(to as u32..);
        }
        Ok(set)
    }

    /// Every member.
    pub fn to_bitmap(&self) -> Result<RoaringBitmap, Error> {
        let bytes = self.region.get(self.at, self.len)?;
        let set = RoaringBitmap::deserialize_from(bytes)
            .map_err(|error| corrupted(format_args!("does not deserialize: {error}")))?;
        if set.len() != self.count {
            return Err(corrupted("disagrees with its entry's count"));
        }
        Ok(set)
    }
}

fn array_value(bytes: &[u8], index: usize) -> u16 {
    u16::from_le_bytes([bytes[2 * index], bytes[2 * index + 1]])
}

fn bitmap_word(bytes: &[u8], index: usize) -> u64 {
    u64::from_le_bytes(bytes[8 * index..8 * index + 8].try_into().unwrap())
}

/// The smallest value at or after `low` in a container.
fn first_in(container: &Container, bytes: &[u8], low: u16) -> Option<u16> {
    if container.is_bitmap() {
        let mut word = usize::from(low) / 64;
        let mut bits = bitmap_word(bytes, word) & (u64::MAX << (low % 64));
        loop {
            if bits != 0 {
                return Some((word * 64 + bits.trailing_zeros() as usize) as u16);
            }
            word += 1;
            if word == 1024 {
                return None;
            }
            bits = bitmap_word(bytes, word);
        }
    }
    let (mut lo, mut hi) = (0, container.cardinality);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if array_value(bytes, mid) < low {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    (lo < container.cardinality).then(|| array_value(bytes, lo))
}

/// The largest value at or below `high` in a container.
fn last_in(container: &Container, bytes: &[u8], high: u16) -> Option<u16> {
    if container.is_bitmap() {
        let mut word = usize::from(high) / 64;
        let keep = high % 64;
        let mut bits = bitmap_word(bytes, word)
            & if keep == 63 {
                u64::MAX
            } else {
                (1u64 << (keep + 1)) - 1
            };
        loop {
            if bits != 0 {
                return Some((word * 64 + 63 - bits.leading_zeros() as usize) as u16);
            }
            if word == 0 {
                return None;
            }
            word -= 1;
            bits = bitmap_word(bytes, word);
        }
    }
    // Count of values at or below `high`.
    let (mut lo, mut hi) = (0, container.cardinality);
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if array_value(bytes, mid) <= high {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    (lo > 0).then(|| array_value(bytes, lo - 1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::key_table::canonical_serialize;

    struct Owned {
        bytes: Vec<u8>,
        checks: Vec<u8>,
        verified: VerifiedBlocks,
        len: usize,
        count: u64,
    }

    /// A region holding `set`, behind some unrelated bytes so offsets are
    /// not all zero-based.
    fn owned(set: &RoaringBitmap) -> Owned {
        let mut bytes = vec![0xAB; 37];
        canonical_serialize(set, &mut bytes);
        let len = bytes.len() - 37;
        let checks = chunk_checks(&bytes);
        let verified = VerifiedBlocks::new(bytes.len().div_ceil(CHUNK));
        Owned {
            bytes,
            checks,
            verified,
            len,
            count: set.len(),
        }
    }

    fn view(owned: &Owned) -> MappedSet<'_> {
        MappedSet::new(
            Region {
                bytes: &owned.bytes,
                checks: &owned.checks,
                verified: &owned.verified,
            },
            37,
            owned.len,
            owned.count,
        )
        .unwrap()
    }

    fn sets() -> Vec<RoaringBitmap> {
        vec![
            RoaringBitmap::new(),
            [0u32].into_iter().collect(),
            [u32::MAX].into_iter().collect(),
            (0..4096u32).collect(),
            (0..4097u32).collect(),
            (60_000..400_000u32).step_by(3).collect(),
            (0..300_000u32)
                .filter(|v| v % 17 == 0 || v % 5000 < 10)
                .collect(),
            [5u32, 65_535, 65_536, 1 << 20, (1 << 20) + 1, u32::MAX - 1]
                .into_iter()
                .collect(),
        ]
    }

    /// Every question answered in place agrees with the deserialized set.
    #[test]
    fn answers_in_place_agree_with_the_whole_set() {
        let probes: Vec<u32> = vec![
            0,
            1,
            4,
            5,
            6,
            4095,
            4096,
            4097,
            59_999,
            60_000,
            60_001,
            65_535,
            65_536,
            65_537,
            131_071,
            299_999,
            300_000,
            399_999,
            1 << 20,
            u32::MAX - 1,
            u32::MAX,
        ];
        for set in sets() {
            let owned = owned(&set);
            let mapped = view(&owned);
            assert_eq!(mapped.len(), set.len());
            assert_eq!(mapped.to_bitmap().unwrap(), set);
            for &value in &probes {
                let expected = set.range(value..).next();
                assert_eq!(
                    mapped.first_at_or_after(value).unwrap(),
                    expected,
                    "first ≥ {value}"
                );
                let bound = u64::from(value);
                let expected = set.range(..value).next_back();
                assert_eq!(
                    mapped.last_before(bound).unwrap(),
                    expected,
                    "last < {value}"
                );
                for &to in &probes {
                    let expected: RoaringBitmap = if value < to {
                        set.range(value..to).collect()
                    } else {
                        RoaringBitmap::new()
                    };
                    assert_eq!(mapped.range(value, u64::from(to)).unwrap(), expected);
                }
                let expected: RoaringBitmap = set.range(value..).collect();
                assert_eq!(mapped.range(value, 1 << 32).unwrap(), expected);
            }
            assert_eq!(mapped.last_before(1 << 32).unwrap(), set.max());
        }
    }

    #[test]
    fn a_flipped_bit_is_refused_by_whatever_reads_it() {
        let set: RoaringBitmap = (0..200_000u32).filter(|v| v % 7 == 0).collect();
        let intact = owned(&set);
        for at in (37..intact.bytes.len()).step_by(997) {
            let mut damaged = owned(&set);
            damaged.bytes[at] ^= 0x04;
            let result = MappedSet::new(
                Region {
                    bytes: &damaged.bytes,
                    checks: &damaged.checks,
                    verified: &damaged.verified,
                },
                37,
                damaged.len,
                damaged.count,
            )
            .and_then(|mapped| mapped.to_bitmap());
            assert!(
                matches!(result, Err(Error::Corrupted { .. })),
                "byte {at}: {result:?}"
            );
        }
    }
}
