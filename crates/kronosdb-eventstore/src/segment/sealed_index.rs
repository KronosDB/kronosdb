//! A sealed segment's index: the `.idx` file, version 3, read through a
//! memory mapping and looked up in place, never loaded whole.
//!
//! ```text
//! header (192 bytes)
//!   0  magic "KIDX"            4  version 3
//!   5  flags: 1 = offsets are u64, 2 = has a key filter
//!   8  base position u64      16  event count u64
//!  24  key count u64          32  control record count u64
//!  40  eight sections, each (offset u64, length u64), in file order:
//!        filter, controls, directory, keys, bitmaps, offsets,
//!        directory checks, offset checks
//! 168  CRC32C of: filter, controls, directory checks, offset checks
//! 184  reserved (0)          188  CRC32C of bytes 0..188
//! sections, each starting on an 8-byte boundary, zero padding between
//!   filter          binary fuse filter of every key's hash (see `filter`)
//!   controls        the segment's control records, 40 bytes each:
//!                   flags u8 | 0 x7 | at u64 | epoch u64 | two u64 fields
//!   directory, keys, bitmaps, directory checks
//!                   the segment's key table (see `key_table`)
//!   offsets         byte offset of each event's record, by position - base
//!   offset checks   CRC32C of each run of 1024 offsets
//! ```
//!
//! Positions in a segment are dense: every event takes the next position and
//! control records take none, so `base..base + count` are exactly the
//! segment's events and the offset table has no holes.
//!
//! What is verified, and when, follows what a read touches. Opening checks
//! the header and the small sections every open needs (controls, the two
//! check arrays): a few hundred bytes to a few kilobytes. The filter is
//! verified whole the first time a probe asks it. A directory block or a run
//! of 1024 offsets is verified the first time it is used and remembered; a
//! position set every time it is read. So a lookup's first touch of a 256 MB
//! segment's index costs a few kilobytes of checksumming, not the whole
//! file. Every failed check is `Error::Corrupted`.
//!
//! The file is a function of the segment alone: keys in byte order, position
//! sets in roaring's canonical portable form, a filter whose construction is
//! deterministic, zeroed padding. The same segment gives the same bytes on
//! every node and in every rebuild.

use std::borrow::Cow;
use std::fs::{self, File};
use std::io::Write;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use memmap2::Mmap;
use roaring::RoaringBitmap;

use crate::error::Error;
use crate::event::Position;
use crate::segment::filter::{self, MappedFilter};
use crate::segment::format::ControlRecord;
use crate::segment::key_table::{self, KeyTable, VerifiedBlocks};
use crate::segment::query::{self, KeyBitmaps, PositionSet, Probe, ProbeKey, SegmentKeys};
use crate::segment::{SEGMENT_HEADER_SIZE, flags};

const MAGIC: [u8; 4] = *b"KIDX";
/// Version 1 and 2 files (a serialized hash map loaded whole, with a
/// separate `.bloom`) are not read: the heal path rebuilds them.
pub const VERSION: u8 = 3;
const HEADER_LEN: usize = 192;
const FLAG_OFFSETS_U64: u8 = 1;
const FLAG_FILTER: u8 = 2;
const CONTROL_LEN: usize = 40;
/// Offsets covered by one check.
const OFFSET_CHUNK: usize = 1024;

const FILTER: usize = 0;
const CONTROLS: usize = 1;
const DIRECTORY: usize = 2;
const KEYS: usize = 3;
const BITMAPS: usize = 4;
const OFFSETS: usize = 5;
const DIRECTORY_CHECKS: usize = 6;
const OFFSET_CHECKS: usize = 7;
const SECTIONS: usize = 8;

/// A control record and where it sits among the segment's events: `at` is
/// the position of the next event after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoggedControl {
    pub at: u64,
    pub record: ControlRecord,
}

fn encode_control(control: &LoggedControl, out: &mut Vec<u8>) {
    let (flags, epoch, a, b) = match control.record {
        ControlRecord::EpochChange {
            epoch,
            leader_id,
            start_position,
        } => (
            flags::CONTROL_EPOCH_CHANGE,
            epoch,
            leader_id,
            start_position,
        ),
        ControlRecord::WatermarkCheckpoint { epoch, position } => {
            (flags::CONTROL_WATERMARK_CHECKPOINT, epoch, position, 0)
        }
    };
    out.push(flags);
    out.extend_from_slice(&[0; 7]);
    for word in [control.at, epoch, a, b] {
        out.extend_from_slice(&word.to_le_bytes());
    }
}

fn decode_control(bytes: &[u8]) -> Result<LoggedControl, Error> {
    let word = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
    let record = match bytes[0] {
        flags::CONTROL_EPOCH_CHANGE => ControlRecord::EpochChange {
            epoch: word(16),
            leader_id: word(24),
            start_position: word(32),
        },
        flags::CONTROL_WATERMARK_CHECKPOINT => ControlRecord::WatermarkCheckpoint {
            epoch: word(16),
            position: word(24),
        },
        other => {
            return Err(Error::Corrupted {
                message: format!(".idx control summary holds unknown record kind {other:#04x}"),
            });
        }
    };
    Ok(LoggedControl {
        at: word(8),
        record,
    })
}

/// What a v3 file is written from.
pub(crate) struct SealedParts<'a> {
    pub base: u64,
    /// Byte offset of each event's record, by position - base.
    pub offsets: &'a [u64],
    /// Every key and its positions (relative to `base`), in any order.
    pub keys: Vec<(&'a [u8], &'a RoaringBitmap)>,
    pub controls: &'a [LoggedControl],
}

fn pad(out: &mut Vec<u8>) {
    out.resize(out.len().next_multiple_of(8), 0);
}

/// The bytes of a v3 file. Deterministic: a function of the parts' content.
pub(crate) fn encode(mut parts: SealedParts<'_>) -> Result<Vec<u8>, Error> {
    let count = parts.offsets.len() as u64;
    if parts
        .offsets
        .iter()
        .any(|&offset| offset < SEGMENT_HEADER_SIZE as u64)
    {
        return Err(Error::Internal {
            message: format!(
                "segment {} index has a hole in its offset table",
                parts.base
            ),
        });
    }
    if count > u64::from(u32::MAX) {
        return Err(Error::Internal {
            message: format!("segment {} holds too many events to index", parts.base),
        });
    }
    parts.keys.sort_unstable_by(|a, b| a.0.cmp(b.0));
    let hashes: Vec<u64> = parts
        .keys
        .iter()
        .map(|(key, _)| filter::key_hash(key))
        .collect();
    let filter = filter::build(hashes);
    let key_count = parts.keys.len() as u64;
    let table = key_table::build(parts.keys.into_iter())?;
    let wide = parts
        .offsets
        .last()
        .is_some_and(|&last| last > u64::from(u32::MAX));

    let mut controls = Vec::with_capacity(parts.controls.len() * CONTROL_LEN);
    for control in parts.controls {
        encode_control(control, &mut controls);
    }
    let width = if wide { 8 } else { 4 };
    let mut offsets = Vec::with_capacity(parts.offsets.len() * width);
    for &offset in parts.offsets {
        if wide {
            offsets.extend_from_slice(&offset.to_le_bytes());
        } else {
            offsets.extend_from_slice(&(offset as u32).to_le_bytes());
        }
    }
    let offset_checks: Vec<u8> = offsets
        .chunks(OFFSET_CHUNK * width)
        .flat_map(|chunk| crc32c::crc32c(chunk).to_le_bytes())
        .collect();

    let mut out = vec![0u8; HEADER_LEN];
    let mut sections = [(0u64, 0u64); SECTIONS];
    let contents: [&[u8]; SECTIONS] = [
        filter.as_deref().unwrap_or_default(),
        &controls,
        &table.directory,
        &table.keys,
        &table.bitmaps,
        &offsets,
        &table.checks,
        &offset_checks,
    ];
    for (index, bytes) in contents.iter().enumerate() {
        pad(&mut out);
        sections[index] = (out.len() as u64, bytes.len() as u64);
        out.extend_from_slice(bytes);
    }

    let crc_of = |index: usize| {
        let (at, len) = sections[index];
        crc32c::crc32c(&out[at as usize..(at + len) as usize])
    };
    let section_crcs = [
        crc_of(FILTER),
        crc_of(CONTROLS),
        crc_of(DIRECTORY_CHECKS),
        crc_of(OFFSET_CHECKS),
    ];
    let header = &mut out[..HEADER_LEN];
    header[0..4].copy_from_slice(&MAGIC);
    header[4] = VERSION;
    header[5] =
        if wide { FLAG_OFFSETS_U64 } else { 0 } | if filter.is_some() { FLAG_FILTER } else { 0 };
    header[8..16].copy_from_slice(&parts.base.to_le_bytes());
    header[16..24].copy_from_slice(&count.to_le_bytes());
    header[24..32].copy_from_slice(&key_count.to_le_bytes());
    header[32..40].copy_from_slice(&(parts.controls.len() as u64).to_le_bytes());
    for (i, (at, len)) in sections.iter().enumerate() {
        header[40 + i * 16..48 + i * 16].copy_from_slice(&at.to_le_bytes());
        header[48 + i * 16..56 + i * 16].copy_from_slice(&len.to_le_bytes());
    }
    for (i, crc) in section_crcs.iter().enumerate() {
        header[168 + i * 4..172 + i * 4].copy_from_slice(&crc.to_le_bytes());
    }
    let crc = crc32c::crc32c(&header[..188]);
    header[188..192].copy_from_slice(&crc.to_le_bytes());
    Ok(out)
}

/// Writes a v3 file at `path`: to a temporary name, synced, then renamed
/// over, so a reader finds the whole file or none.
pub(crate) fn write(path: &Path, parts: SealedParts<'_>) -> Result<(), Error> {
    let bytes = encode(parts)?;
    let tmp_path = path.with_extension("idx.tmp");
    let mut file = File::create(&tmp_path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&tmp_path, path)?;
    Ok(())
}

/// A mapped v3 `.idx`.
pub struct SealedIndex {
    map: Mmap,
    base: u64,
    count: u64,
    wide_offsets: bool,
    sections: [Range<usize>; SECTIONS],
    filter_crc: u32,
    has_filter: bool,
    filter_verified: AtomicBool,
    directory_verified: VerifiedBlocks,
    offsets_verified: VerifiedBlocks,
    controls: Vec<LoggedControl>,
}

fn corrupted(path: &Path, what: impl std::fmt::Display) -> Error {
    Error::Corrupted {
        message: format!("{}: {what}", path.display()),
    }
}

impl SealedIndex {
    /// Maps the index of the segment that starts at `base`. Checks the
    /// header and the sections every open uses; the rest is checked as it
    /// is read. A file that is not a v3 index of this segment, or fails a
    /// check, is `Error::Corrupted`.
    pub fn open(path: &Path, base: u64) -> Result<Self, Error> {
        let file = File::open(path)?;
        // SAFETY: index files are written whole under a temporary name and
        // renamed into place, never modified after; a mapping keeps the
        // inode it was made from even if a rebuild replaces the name.
        let map = unsafe { Mmap::map(&file)? };
        let bytes = &map[..];
        if bytes.len() < HEADER_LEN || bytes[0..4] != MAGIC {
            return Err(corrupted(path, "not a segment index"));
        }
        if bytes[4] != VERSION {
            return Err(corrupted(
                path,
                format_args!("segment index version {} (expected {VERSION})", bytes[4]),
            ));
        }
        let stored = u32::from_le_bytes(bytes[188..192].try_into().unwrap());
        if crc32c::crc32c(&bytes[..188]) != stored {
            return Err(corrupted(path, "segment index header checksum mismatch"));
        }
        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        let flags = bytes[5];
        let (header_base, count, key_count, control_count) =
            (u64_at(8), u64_at(16), u64_at(24), u64_at(32));
        if header_base != base {
            return Err(corrupted(
                path,
                format_args!("segment index is for base {header_base}, not {base}"),
            ));
        }
        let wide_offsets = flags & FLAG_OFFSETS_U64 != 0;
        let has_filter = flags & FLAG_FILTER != 0;

        let mut sections: [Range<usize>; SECTIONS] = Default::default();
        let mut end = HEADER_LEN as u64;
        for (i, section) in sections.iter_mut().enumerate() {
            let (at, len) = (u64_at(40 + i * 16), u64_at(48 + i * 16));
            let section_end = at.checked_add(len).filter(|&e| e <= bytes.len() as u64);
            match section_end {
                Some(section_end) if at >= end => {
                    *section = at as usize..section_end as usize;
                    end = section_end;
                }
                _ => return Err(corrupted(path, "segment index sections overlap or overrun")),
            }
        }
        let expect_len = |index: usize, len: Option<u64>| -> Result<(), Error> {
            if len != Some(sections[index].len() as u64) {
                return Err(corrupted(
                    path,
                    format_args!("segment index section {index} has the wrong length"),
                ));
            }
            Ok(())
        };
        let width = if wide_offsets { 8 } else { 4 };
        expect_len(CONTROLS, control_count.checked_mul(CONTROL_LEN as u64))?;
        expect_len(
            DIRECTORY,
            key_count.checked_mul(key_table::ENTRY_LEN as u64),
        )?;
        expect_len(OFFSETS, count.checked_mul(width))?;
        expect_len(
            DIRECTORY_CHECKS,
            Some(key_count.div_ceil(key_table::BLOCK_ENTRIES as u64) * 4),
        )?;
        expect_len(OFFSET_CHECKS, Some(count.div_ceil(OFFSET_CHUNK as u64) * 4))?;
        if has_filter == sections[FILTER].is_empty() || end != bytes.len() as u64 {
            return Err(corrupted(path, "segment index layout is inconsistent"));
        }
        for (index, at) in [
            (CONTROLS, 172),
            (DIRECTORY_CHECKS, 176),
            (OFFSET_CHECKS, 180),
        ] {
            if crc32c::crc32c(&bytes[sections[index].clone()]) != u32_at(at) {
                return Err(corrupted(
                    path,
                    format_args!("segment index section {index} checksum mismatch"),
                ));
            }
        }
        let controls = bytes[sections[CONTROLS].clone()]
            .chunks(CONTROL_LEN)
            .map(decode_control)
            .collect::<Result<Vec<_>, _>>()?;
        let filter_crc = u32_at(168);

        Ok(Self {
            base,
            count,
            wide_offsets,
            filter_crc,
            has_filter,
            filter_verified: AtomicBool::new(false),
            directory_verified: VerifiedBlocks::new(
                (key_count as usize).div_ceil(key_table::BLOCK_ENTRIES),
            ),
            offsets_verified: VerifiedBlocks::new((count as usize).div_ceil(OFFSET_CHUNK)),
            controls,
            sections,
            map,
        })
    }

    fn section(&self, index: usize) -> &[u8] {
        &self.map[self.sections[index].clone()]
    }

    /// The segment's control records, in log order.
    pub fn controls(&self) -> &[LoggedControl] {
        &self.controls
    }

    /// Number of distinct keys.
    pub fn key_count(&self) -> usize {
        self.sections[DIRECTORY].len() / key_table::ENTRY_LEN
    }

    /// The segment's key table.
    pub fn keys(&self) -> Result<KeyTable<'_>, Error> {
        KeyTable::new(
            self.base,
            self.count,
            self.section(DIRECTORY),
            self.section(KEYS),
            self.section(BITMAPS),
            self.section(DIRECTORY_CHECKS),
            &self.directory_verified,
        )
    }

    /// The key filter, verified the first time it is asked for. `None` when
    /// the segment has none (no keys, or construction failed at seal).
    fn filter(&self) -> Result<Option<MappedFilter<'_>>, Error> {
        if !self.has_filter {
            return Ok(None);
        }
        let section = self.section(FILTER);
        if !self.filter_verified.load(Ordering::Relaxed) {
            if crc32c::crc32c(section) != self.filter_crc {
                return Err(Error::Corrupted {
                    message: format!("segment {} index key filter checksum mismatch", self.base),
                });
            }
            self.filter_verified.store(true, Ordering::Relaxed);
        }
        MappedFilter::parse(section).map(Some)
    }

    /// Byte offset of the record of the event `index` places after the base.
    fn offset_at(&self, index: usize) -> Result<u64, Error> {
        let width = if self.wide_offsets { 8 } else { 4 };
        let table = self.section(OFFSETS);
        let chunk = index / OFFSET_CHUNK;
        self.offsets_verified.ensure(chunk, || {
            let start = chunk * OFFSET_CHUNK * width;
            let end = (start + OFFSET_CHUNK * width).min(table.len());
            let checks = self.section(OFFSET_CHECKS);
            let stored = u32::from_le_bytes(checks[chunk * 4..chunk * 4 + 4].try_into().unwrap());
            if crc32c::crc32c(&table[start..end]) != stored {
                return Err(Error::Corrupted {
                    message: format!(
                        "segment {} index offsets {}.. checksum mismatch",
                        self.base,
                        chunk * OFFSET_CHUNK
                    ),
                });
            }
            Ok(())
        })?;
        let at = index * width;
        Ok(if self.wide_offsets {
            u64::from_le_bytes(table[at..at + 8].try_into().unwrap())
        } else {
            u64::from(u32::from_le_bytes(table[at..at + 4].try_into().unwrap()))
        })
    }

    /// Bytes mapped.
    pub fn file_len(&self) -> usize {
        self.map.len()
    }
}

impl KeyBitmaps for SealedIndex {
    fn base(&self) -> u64 {
        self.base
    }

    fn count(&self) -> u64 {
        self.count
    }

    fn bitmap(&self, key: &ProbeKey) -> Result<Option<Cow<'_, RoaringBitmap>>, Error> {
        if let Some(filter) = self.filter()?
            && !filter.contains(key.hash)
        {
            return Ok(None);
        }
        match self.keys()?.find(&key.bytes)? {
            Some(entry) => Ok(Some(Cow::Owned(entry.bitmap()?))),
            None => Ok(None),
        }
    }
}

impl SegmentKeys for SealedIndex {
    fn base_position(&self) -> u64 {
        self.base
    }

    fn event_count(&self) -> u64 {
        self.count
    }

    fn offset_of(&self, position: u64) -> Result<Option<u64>, Error> {
        match position.checked_sub(self.base) {
            Some(index) if index < self.count => self.offset_at(index as usize).map(Some),
            _ => Ok(None),
        }
    }

    fn might_match(&self, probe: &Probe) -> Result<bool, Error> {
        Ok(match self.filter()? {
            Some(filter) => probe.might_match(|key| filter.contains(key.hash)),
            None => probe.might_match(|_| true),
        })
    }

    fn matching(&self, probe: &Probe) -> Result<Option<PositionSet>, Error> {
        query::matching(self, probe)
    }

    fn any_match_in(&self, probe: &Probe, from: u64, to: u64) -> Result<bool, Error> {
        query::any_match_in(self, probe, from, to)
    }

    fn first_match_after(&self, probe: &Probe, after: u64) -> Result<Option<Position>, Error> {
        query::first_match_after(self, probe, after)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::criteria::{Criterion, SourcingCondition};
    use crate::event::Tag;
    use crate::segment::segment_index::SegmentIndex;

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    const BASE: u64 = 70_000;

    /// An in-memory index of `events` random events: a few event types (so
    /// some sets outgrow roaring's array containers), keys from small pools
    /// and from one wide one (so the directory spans many check blocks), and
    /// control records here and there.
    fn random_index(seed: u64, events: u64) -> SegmentIndex {
        let mut rng = Rng(seed);
        let mut index = SegmentIndex::new(BASE);
        let mut offset = SEGMENT_HEADER_SIZE as u64;
        for i in 0..events {
            if rng.below(97) == 0 {
                index.insert_control(ControlRecord::WatermarkCheckpoint {
                    epoch: i,
                    position: BASE + i,
                });
            }
            let name = ["A", "B", "C"][rng.below(3) as usize];
            let mut tags = vec![Tag::from_str("k", &rng.below(9).to_string())];
            if rng.below(2) == 0 {
                tags.push(Tag::from_str("j", &rng.below(4).to_string()));
            }
            if rng.below(3) == 0 {
                tags.push(Tag::from_str("wide", &rng.below(3000).to_string()));
            }
            index.insert_event(BASE + i, offset, name, &tags);
            offset += 40 + rng.below(400);
        }
        index
    }

    fn random_probe(rng: &mut Rng) -> Probe {
        let mut criteria = Vec::new();
        for _ in 0..1 + rng.below(3) {
            let mut tags = Vec::new();
            if rng.below(4) != 0 {
                tags.push(Tag::from_str("k", &rng.below(11).to_string()));
            }
            if rng.below(3) == 0 {
                tags.push(Tag::from_str("j", &rng.below(5).to_string()));
            }
            if rng.below(4) == 0 {
                tags.push(Tag::from_str("wide", &rng.below(3200).to_string()));
            }
            let mut names = Vec::new();
            for name in ["A", "B", "Z"] {
                if rng.below(3) == 0 {
                    names.push(name.to_string());
                }
            }
            criteria.push(Criterion { names, tags });
        }
        Probe::new(&SourcingCondition { criteria })
    }

    fn mapped(index: &SegmentIndex, dir: &Path) -> SealedIndex {
        let path = dir.join("segment.idx");
        std::fs::write(&path, index.encode_sealed().unwrap()).unwrap();
        SealedIndex::open(&path, BASE).unwrap()
    }

    /// Every question a read asks gets the same answer from the mapped v3
    /// file as from the in-memory index it was written from.
    #[test]
    fn mapped_lookups_agree_with_the_in_memory_index() {
        let dir = tempfile::tempdir().unwrap();
        for (seed, events) in [(1, 0), (2, 1), (3, 700), (4, 20_000)] {
            let memory = random_index(0x9E37_79B9 + seed, events);
            let sealed = mapped(&memory, dir.path());
            assert_eq!(sealed.event_count(), events);
            assert_eq!(sealed.key_count(), memory.key_count());
            assert_eq!(sealed.controls(), memory.controls());
            for position in (BASE.saturating_sub(2)..BASE + events + 2).step_by(7) {
                assert_eq!(
                    sealed.offset_of(position).unwrap(),
                    memory.get_offset(position),
                    "offset of {position}"
                );
            }

            let mut rng = Rng(seed * 31 + 7);
            let mut matched = 0;
            for _ in 0..300 {
                let probe = random_probe(&mut rng);
                let expected = SegmentKeys::matching(&memory, &probe).unwrap();
                assert_eq!(sealed.matching(&probe).unwrap(), expected);
                if expected.is_some() {
                    matched += 1;
                    // The filter never rules out a segment holding a match.
                    assert!(sealed.might_match(&probe).unwrap());
                }
                for _ in 0..8 {
                    let span = events + 20;
                    let from = BASE + rng.below(span) - 10;
                    let to = from + rng.below(span / 2 + 1);
                    assert_eq!(
                        sealed.any_match_in(&probe, from, to).unwrap(),
                        memory.any_match_in(&probe, from, to).unwrap(),
                        "any_match_in {from}..{to}"
                    );
                    let after = BASE + rng.below(span) - 10;
                    let first = sealed.first_match_after(&probe, after).unwrap();
                    assert_eq!(first, memory.first_match_after(&probe, after).unwrap());
                    // And both are the first member of the matching set at
                    // or after the marker.
                    let from_set = expected
                        .as_ref()
                        .and_then(|set| set.iter().find(|&p| p >= after));
                    assert_eq!(first.map(|p| p.0), from_set, "after {after}");
                }
            }
            if events > 100 {
                assert!(matched > 50, "the generator must produce real matches");
            }
        }
    }

    /// A key the segment lacks is almost always ruled out by the filter
    /// alone, without a directory lookup.
    #[test]
    fn the_filter_rules_out_absent_keys() {
        let dir = tempfile::tempdir().unwrap();
        let sealed = mapped(&random_index(5, 5_000), dir.path());
        let ruled_out = (0..2_000)
            .filter(|k| {
                let probe = Probe::new(&SourcingCondition {
                    criteria: vec![Criterion {
                        names: vec![],
                        tags: vec![Tag::from_str("missing", &k.to_string())],
                    }],
                });
                !sealed.might_match(&probe).unwrap()
            })
            .count();
        assert!(ruled_out > 1_970, "{ruled_out} of 2000 ruled out");
    }

    /// One flipped bit anywhere in the file: either it is refused as
    /// corrupted (at open, or by the lookup that reaches it), or it sits in
    /// bytes no answer depends on. An answer is never different.
    #[test]
    fn a_damaged_index_is_refused_never_believed() {
        let dir = tempfile::tempdir().unwrap();
        let memory = random_index(11, 3_000);
        let intact = memory.encode_sealed().unwrap();
        let path = dir.path().join("segment.idx");
        let mut rng = Rng(77);
        let probes: Vec<Probe> = (0..40).map(|_| random_probe(&mut rng)).collect();
        let answers = |sealed: &SealedIndex| -> Result<Vec<String>, Error> {
            let mut out = Vec::new();
            for probe in &probes {
                out.push(format!(
                    "{:?} {:?} {:?} {}",
                    sealed.might_match(probe)?,
                    sealed.matching(probe)?,
                    sealed.first_match_after(probe, BASE + 1_000)?,
                    sealed.any_match_in(probe, BASE + 500, BASE + 900)?
                ));
            }
            for position in (BASE..BASE + 3_000).step_by(13) {
                out.push(format!("{:?}", sealed.offset_of(position)?));
            }
            Ok(out)
        };
        std::fs::write(&path, &intact).unwrap();
        let expected = answers(&SealedIndex::open(&path, BASE).unwrap()).unwrap();

        let (mut refused, mut harmless) = (0, 0);
        for at in (0..intact.len())
            .step_by(intact.len() / 400)
            .chain([4, 5, 9, 17, 41, 170, 189])
        {
            let mut damaged = intact.clone();
            damaged[at] ^= 0x08;
            std::fs::write(&path, &damaged).unwrap();
            match SealedIndex::open(&path, BASE).and_then(|sealed| answers(&sealed)) {
                Err(Error::Corrupted { .. }) => refused += 1,
                Err(other) => panic!("byte {at}: {other:?}"),
                Ok(got) => {
                    assert_eq!(got, expected, "byte {at} changed an answer");
                    harmless += 1;
                }
            }
        }
        assert!(refused > 300, "{refused} refused, {harmless} harmless");
        // A file cut short is refused too.
        std::fs::write(&path, &intact[..intact.len() - 3]).unwrap();
        assert!(matches!(
            SealedIndex::open(&path, BASE),
            Err(Error::Corrupted { .. })
        ));
        // As is an index of another segment.
        std::fs::write(&path, &intact).unwrap();
        assert!(matches!(
            SealedIndex::open(&path, BASE + 1),
            Err(Error::Corrupted { .. })
        ));
    }

    /// The bytes follow from the index's content only: two indexes of the
    /// same events (whose seeded hash maps iterate their keys in different
    /// orders) encode identically.
    #[test]
    fn encoding_is_deterministic() {
        let a = random_index(21, 4_000).encode_sealed().unwrap();
        let b = random_index(21, 4_000).encode_sealed().unwrap();
        assert_eq!(a, b);
        assert_ne!(a, random_index(22, 4_000).encode_sealed().unwrap());
    }
}
