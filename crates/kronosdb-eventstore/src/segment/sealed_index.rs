//! A sealed segment's locator: the `.idx` file, version 4, read through a
//! memory mapping. It says where each event of the segment is (the
//! position → byte offset table), what control records the segment holds,
//! and where its data ends. Which events hold which keys is the business of
//! key tables (`key_table`): the segment's own `.keys` until a merged table
//! covers it (ADR-0010). The locator is needed by every read and by startup
//! recovery and is never dropped.
//!
//! ```text
//! header (128 bytes)
//!   0  magic "KIDX"            4  version 4
//!   5  flags: 1 = offsets are u64        6..8 zero
//!   8  base position u64      16  event count u64
//!  24  data length u64        32  control record count u64
//!  40  three sections, each (offset u64, length u64), in file order:
//!        controls, offsets, offset checks
//!  88  CRC32C of controls     92  CRC32C of offset checks
//!  96  zero                  124  CRC32C of bytes 0..124
//! sections, each starting on an 8-byte boundary, zero padding between
//!   controls        the segment's control records, 40 bytes each:
//!                   flags u8 | 0 x7 | at u64 | epoch u64 | two u64 fields
//!   offsets         byte offset of each event's record, by position - base
//!   offset checks   CRC32C of each run of 1024 offsets
//! ```
//!
//! Positions in a segment are dense: every event takes the next position and
//! control records take none, so `base..base + count` are exactly the
//! segment's events and the offset table has no holes. Control records have
//! fixed sizes, so with the data length the table gives every event's exact
//! record length too ([`Locator::record_len`]).
//!
//! Opening checks the header and the small sections; a run of 1024 offsets
//! is checked the first time it is used and remembered. Every failed check
//! is `Error::Corrupted`. The file is a function of the segment alone: the
//! same segment gives the same bytes on every node and in every rebuild.

use std::fs::{self, File};
use std::io::Write;
use std::ops::Range;
use std::path::Path;

use memmap2::Mmap;

use crate::error::Error;
use crate::segment::format::ControlRecord;
use crate::segment::key_table::VerifiedBlocks;
use crate::segment::query::Locator;
use crate::segment::{RECORD_HEADER_SIZE, SEGMENT_HEADER_SIZE, flags};

const MAGIC: [u8; 4] = *b"KIDX";
/// Earlier versions (with the key table inside) are not read: they are
/// rebuilt from the segment at open.
pub const VERSION: u8 = 4;
const HEADER_LEN: usize = 128;
const FLAG_OFFSETS_U64: u8 = 1;
const CONTROL_LEN: usize = 40;
/// Offsets covered by one check.
const OFFSET_CHUNK: usize = 1024;

const CONTROLS: usize = 0;
const OFFSETS: usize = 1;
const OFFSET_CHECKS: usize = 2;
const SECTIONS: usize = 3;

/// A control record and where it sits among the segment's events: `at` is
/// the position of the next event after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LoggedControl {
    pub at: u64,
    pub record: ControlRecord,
}

impl LoggedControl {
    /// Bytes the record takes in the segment, header included.
    pub fn stored_len(&self) -> u64 {
        let payload = match self.record {
            ControlRecord::EpochChange { .. } => 24,
            ControlRecord::WatermarkCheckpoint { .. } => 16,
        };
        (RECORD_HEADER_SIZE + payload) as u64
    }
}

/// The exact length of the event record at `offset`, given where the next
/// event's record (or the segment's data) begins and the control records
/// between them.
pub(crate) fn record_len_before(
    offset: u64,
    next: u64,
    controls: &[LoggedControl],
    next_position: u64,
) -> Result<u32, Error> {
    let first = controls.partition_point(|control| control.at < next_position);
    let between: u64 = controls[first..]
        .iter()
        .take_while(|control| control.at == next_position)
        .map(LoggedControl::stored_len)
        .sum();
    next.checked_sub(offset)
        .and_then(|span| span.checked_sub(between))
        .filter(|&len| len > RECORD_HEADER_SIZE as u64)
        .and_then(|len| u32::try_from(len).ok())
        .ok_or_else(|| Error::Corrupted {
            message: format!("segment index puts the record at {offset} in no room before {next}"),
        })
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

/// What a locator is written from.
pub(crate) struct LocatorParts<'a> {
    pub base: u64,
    /// Byte offset of each event's record, by position - base.
    pub offsets: &'a [u64],
    pub controls: &'a [LoggedControl],
    /// Where the segment's data ends.
    pub data_len: u64,
}

fn pad(out: &mut Vec<u8>) {
    out.resize(out.len().next_multiple_of(8), 0);
}

/// The bytes of a locator. Deterministic: a function of the parts' content.
pub(crate) fn encode(parts: LocatorParts<'_>) -> Result<Vec<u8>, Error> {
    let count = parts.offsets.len() as u64;
    if parts
        .offsets
        .iter()
        .any(|&offset| offset < SEGMENT_HEADER_SIZE as u64 || offset >= parts.data_len)
    {
        return Err(Error::Internal {
            message: format!(
                "segment {} index has an offset outside its data",
                parts.base
            ),
        });
    }
    if count > u64::from(u32::MAX) {
        return Err(Error::Internal {
            message: format!("segment {} holds too many events to index", parts.base),
        });
    }
    let wide = parts.data_len > u64::from(u32::MAX);

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
    let contents: [&[u8]; SECTIONS] = [&controls, &offsets, &offset_checks];
    for (index, bytes) in contents.iter().enumerate() {
        pad(&mut out);
        sections[index] = (out.len() as u64, bytes.len() as u64);
        out.extend_from_slice(bytes);
    }

    let header = &mut out[..HEADER_LEN];
    header[0..4].copy_from_slice(&MAGIC);
    header[4] = VERSION;
    header[5] = if wide { FLAG_OFFSETS_U64 } else { 0 };
    header[8..16].copy_from_slice(&parts.base.to_le_bytes());
    header[16..24].copy_from_slice(&count.to_le_bytes());
    header[24..32].copy_from_slice(&parts.data_len.to_le_bytes());
    header[32..40].copy_from_slice(&(parts.controls.len() as u64).to_le_bytes());
    for (i, (at, len)) in sections.iter().enumerate() {
        header[40 + i * 16..48 + i * 16].copy_from_slice(&at.to_le_bytes());
        header[48 + i * 16..56 + i * 16].copy_from_slice(&len.to_le_bytes());
    }
    header[88..92].copy_from_slice(&crc32c::crc32c(&controls).to_le_bytes());
    header[92..96].copy_from_slice(&crc32c::crc32c(&offset_checks).to_le_bytes());
    let crc = crc32c::crc32c(&header[..124]);
    header[124..128].copy_from_slice(&crc.to_le_bytes());
    Ok(out)
}

/// Writes a locator at `path`: to a temporary name, synced, then renamed
/// over, so a reader finds the whole file or none.
pub(crate) fn write(path: &Path, parts: LocatorParts<'_>) -> Result<(), Error> {
    let bytes = encode(parts)?;
    let tmp_path = path.with_extension("idx.tmp");
    let mut file = File::create(&tmp_path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&tmp_path, path)?;
    Ok(())
}

/// A mapped locator.
pub struct SealedIndex {
    map: Mmap,
    base: u64,
    count: u64,
    data_len: u64,
    wide_offsets: bool,
    sections: [Range<usize>; SECTIONS],
    offsets_verified: VerifiedBlocks,
    controls: Vec<LoggedControl>,
}

fn corrupted(path: &Path, what: impl std::fmt::Display) -> Error {
    Error::Corrupted {
        message: format!("{}: {what}", path.display()),
    }
}

impl SealedIndex {
    /// Maps the locator of the segment that starts at `base`. Checks the
    /// header and the control summary; offsets are checked as they are
    /// read. A file that is not a version 4 locator of this segment, or
    /// fails a check, is `Error::Corrupted`.
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
        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        if crc32c::crc32c(&bytes[..124]) != u32_at(124) {
            return Err(corrupted(path, "segment index header checksum mismatch"));
        }
        let (header_base, count, data_len, control_count) =
            (u64_at(8), u64_at(16), u64_at(24), u64_at(32));
        if header_base != base {
            return Err(corrupted(
                path,
                format_args!("segment index is for base {header_base}, not {base}"),
            ));
        }
        let wide_offsets = bytes[5] & FLAG_OFFSETS_U64 != 0;

        let mut sections: [Range<usize>; SECTIONS] = Default::default();
        let mut end = HEADER_LEN as u64;
        for (i, section) in sections.iter_mut().enumerate() {
            let (at, len) = (u64_at(40 + i * 16), u64_at(48 + i * 16));
            match at.checked_add(len).filter(|&e| e <= bytes.len() as u64) {
                Some(section_end) if at >= end => {
                    *section = at as usize..section_end as usize;
                    end = section_end;
                }
                _ => return Err(corrupted(path, "segment index sections overlap or overrun")),
            }
        }
        let width = if wide_offsets { 8 } else { 4 };
        let len_of = |i: usize| Some(sections[i].len() as u64);
        if len_of(CONTROLS) != control_count.checked_mul(CONTROL_LEN as u64)
            || len_of(OFFSETS) != count.checked_mul(width)
            || len_of(OFFSET_CHECKS) != Some(count.div_ceil(OFFSET_CHUNK as u64) * 4)
            || end != bytes.len() as u64
        {
            return Err(corrupted(path, "segment index layout is inconsistent"));
        }
        for (index, at) in [(CONTROLS, 88), (OFFSET_CHECKS, 92)] {
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

        Ok(Self {
            base,
            count,
            data_len,
            wide_offsets,
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

    /// Where the segment's data ends.
    pub fn data_len(&self) -> u64 {
        self.data_len
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

impl Locator for SealedIndex {
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

    fn record_len(&self, position: u64) -> Result<Option<u32>, Error> {
        let Some(offset) = self.offset_of(position)? else {
            return Ok(None);
        };
        let index = position - self.base;
        let next = if index + 1 < self.count {
            self.offset_at(index as usize + 1)?
        } else {
            self.data_len
        };
        record_len_before(offset, next, &self.controls, position + 1).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
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

    /// An in-memory index of `events` events with made-up record lengths and
    /// control records here and there, and the exact length of each record.
    fn random_index(seed: u64, events: u64) -> (SegmentIndex, Vec<u32>) {
        let mut rng = Rng(seed);
        let mut index = SegmentIndex::new(BASE);
        let mut offset = SEGMENT_HEADER_SIZE as u64;
        let mut lens = Vec::new();
        for i in 0..events {
            if rng.below(97) == 0 {
                let control = ControlRecord::WatermarkCheckpoint {
                    epoch: i,
                    position: BASE + i,
                };
                index.insert_control(control);
                offset += (RECORD_HEADER_SIZE + 16) as u64;
            }
            index.insert_event(BASE + i, offset, "A", &[Tag::from_str("k", "v")]);
            let len = 40 + rng.below(400);
            lens.push(len as u32);
            offset += len;
            if rng.below(150) == 0 {
                index.insert_control(ControlRecord::EpochChange {
                    epoch: i,
                    leader_id: 2,
                    start_position: BASE + i + 1,
                });
                offset += (RECORD_HEADER_SIZE + 24) as u64;
            }
        }
        index.set_data_len(offset);
        (index, lens)
    }

    fn mapped(index: &SegmentIndex, dir: &Path) -> SealedIndex {
        let path = dir.join("segment.idx");
        std::fs::write(&path, index.encode_locator().unwrap()).unwrap();
        SealedIndex::open(&path, BASE).unwrap()
    }

    /// Offsets, record lengths and controls read from the mapped file agree
    /// with the in-memory index it was written from.
    #[test]
    fn mapped_answers_agree_with_the_in_memory_index() {
        let dir = tempfile::tempdir().unwrap();
        for (seed, events) in [(1, 0), (2, 1), (3, 700), (4, 20_000)] {
            let (memory, lens) = random_index(0x9E37_79B9 + seed, events);
            let sealed = mapped(&memory, dir.path());
            assert_eq!(sealed.event_count(), events);
            assert_eq!(sealed.controls(), memory.controls());
            for position in (BASE.saturating_sub(2)..BASE + events + 2).step_by(7) {
                assert_eq!(
                    sealed.offset_of(position).unwrap(),
                    memory.get_offset(position),
                    "offset of {position}"
                );
                let expected = position
                    .checked_sub(BASE)
                    .and_then(|i| lens.get(i as usize).copied());
                assert_eq!(
                    sealed.record_len(position).unwrap(),
                    expected,
                    "len of {position}"
                );
                assert_eq!(memory.record_len(position).unwrap(), expected);
            }
        }
    }

    /// One flipped bit anywhere in the file: either it is refused as
    /// corrupted (at open, or by the lookup that reaches it), or it sits in
    /// bytes no answer depends on. An answer is never different.
    #[test]
    fn a_damaged_index_is_refused_never_believed() {
        let dir = tempfile::tempdir().unwrap();
        let (memory, _) = random_index(11, 3_000);
        let intact = memory.encode_locator().unwrap();
        let path = dir.path().join("segment.idx");
        let answers = |sealed: &SealedIndex| -> Result<Vec<String>, Error> {
            let mut out = vec![format!("{:?}", sealed.controls())];
            for position in (BASE..BASE + 3_000).step_by(13) {
                out.push(format!(
                    "{:?} {:?}",
                    sealed.offset_of(position)?,
                    sealed.record_len(position)?
                ));
            }
            Ok(out)
        };
        std::fs::write(&path, &intact).unwrap();
        let expected = answers(&SealedIndex::open(&path, BASE).unwrap()).unwrap();

        let (mut refused, mut harmless) = (0, 0);
        for at in (0..intact.len())
            .step_by(intact.len() / 400)
            .chain([4, 5, 9, 17, 25, 41, 89, 125])
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
        std::fs::write(&path, &intact[..intact.len() - 3]).unwrap();
        assert!(matches!(
            SealedIndex::open(&path, BASE),
            Err(Error::Corrupted { .. })
        ));
        std::fs::write(&path, &intact).unwrap();
        assert!(matches!(
            SealedIndex::open(&path, BASE + 1),
            Err(Error::Corrupted { .. })
        ));
    }

    #[test]
    fn encoding_is_deterministic() {
        let a = random_index(21, 4_000).0.encode_locator().unwrap();
        let b = random_index(21, 4_000).0.encode_locator().unwrap();
        assert_eq!(a, b);
        assert_ne!(a, random_index(22, 4_000).0.encode_locator().unwrap());
    }
}
