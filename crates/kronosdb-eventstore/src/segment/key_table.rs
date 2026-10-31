//! Key tables: every key of a range of positions, with the positions holding
//! it. A sealed segment's `.keys` is one, over the segment's positions; a
//! merged table under `index/` is one over many segments' (ADR-0010). The
//! format is the same, so a merged table is just a table over a longer
//! range, and merging is a k-way merge of tables.
//!
//! ```text
//! header (128 bytes)
//!   0  magic "KTAB"       4  version 1        5..8 zero
//!   8  base u64          16  span u64         24  key count u64
//!  32  five sections, each (offset u64, length u64), in file order:
//!        bitmaps, keys, directory, directory checks, bitmap checks
//! 112  CRC32C of the directory checks, then of the bitmap checks
//! 120  zero              124  CRC32C of bytes 0..124
//! sections, each starting on an 8-byte boundary, zero padding between
//!   bitmaps           each key's positions, relative to base: roaring's
//!                     portable format in its one canonical form
//!   keys              the key bytes, in directory order, back to back
//!   directory         one 40-byte entry per key, sorted by (hash, key):
//!                       hash u64 | key_offset u64 | bitmap_offset u64 |
//!                       key_len u32 | bitmap_len u32 | count u32 | zero u32
//!   directory checks  CRC32C of each block of 128 entries and their keys
//!   bitmap checks     CRC32C of each 4 KiB of the bitmaps section
//! ```
//!
//! The directory is ordered by the key's hash ([`key_hash`]) and then by its
//! bytes. A lookup interpolates over the uniformly spread hashes, which finds
//! a key, or proves it absent, in two or three probes. That makes the
//! directory its own exact filter: no separate one is kept. It also lets a
//! table be written as it is produced, in order, with nothing held whole.
//!
//! A table spans at most `u32::MAX` positions, so positions are 32-bit
//! offsets from its base.
//!
//! Integrity is checked where the bytes are used. Opening checks the header
//! and the two check arrays. A directory block is verified the first time a
//! lookup touches it, a 4 KiB chunk of position sets the first time a read
//! touches it, and each is remembered. A failed check is `Error::Corrupted`.
//!
//! A table's bytes are a function of its keys and their positions alone:
//! the same range of the same log gives the same file on every node, however
//! it was built.

use std::fs::{self, File};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use memmap2::Mmap;
use roaring::RoaringBitmap;

use crate::error::Error;
use crate::segment::mapped_set::{self, MappedSet, Region};
use crate::segment::query::{KeySource, Positions, ProbeKey};

const MAGIC: [u8; 4] = *b"KTAB";
pub const VERSION: u8 = 1;
const HEADER_LEN: usize = 128;
/// Bytes of one directory entry.
pub(crate) const ENTRY_LEN: usize = 40;
/// Directory entries covered by one check.
pub(crate) const BLOCK_ENTRIES: usize = 128;
/// Most positions a table may span: its sets are 32-bit offsets.
pub const MAX_SPAN: u64 = u32::MAX as u64;

const BITMAPS: usize = 0;
const KEYS: usize = 1;
const DIRECTORY: usize = 2;
const DIRECTORY_CHECKS: usize = 3;
const BITMAP_CHECKS: usize = 4;
const SECTIONS: usize = 5;

/// Keys and directory are kept in memory while a table is written until
/// they pass this size, then spilled to a temporary file.
const SPILL_AFTER: usize = 64 << 20;

/// The stable 64-bit hash a key is ordered and found by: MurmurHash64A with
/// a fixed seed. Part of the format (tables are sorted by it), so it may
/// never change; a test freezes it.
pub fn key_hash(key: &[u8]) -> u64 {
    const M: u64 = 0xc6a4_a793_5bd1_e995;
    const R: u32 = 47;
    const SEED: u64 = 0x4b52_4f4e_4f53_4442; // "KRONOSDB"

    let mut h = SEED ^ (key.len() as u64).wrapping_mul(M);
    let (words, tail) = key.as_chunks::<8>();
    for word in words {
        let mut k = u64::from_le_bytes(*word);
        k = k.wrapping_mul(M);
        k ^= k >> R;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }
    if !tail.is_empty() {
        let mut last = [0u8; 8];
        last[..tail.len()].copy_from_slice(tail);
        h ^= u64::from_le_bytes(last);
        h = h.wrapping_mul(M);
    }
    h ^= h >> R;
    h = h.wrapping_mul(M);
    h ^= h >> R;
    h
}

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

/// Appends `bitmap` in the portable roaring format (the one the C, Java and
/// Go implementations read), in its one canonical form: containers in key
/// order, each an array of its values when it holds at most 4096 of them
/// and a 65536-bit bitmap otherwise, no run containers. The bytes follow
/// from the set alone, never from how it was built, which is what lets two
/// nodes (or two rebuilds) write the same table for the same positions.
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
    out.extend_from_slice(&mapped_set::COOKIE.to_le_bytes());
    out.extend_from_slice(&(containers.len() as u32).to_le_bytes());
    for &(key, start, end) in &containers {
        out.extend_from_slice(&key.to_le_bytes());
        out.extend_from_slice(&((end - start - 1) as u16).to_le_bytes());
    }
    let size = |len: usize| {
        if len > mapped_set::ARRAY_LIMIT {
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
        if end - start > mapped_set::ARRAY_LIMIT {
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

/// Bytes written so far, in memory or spilled to a temporary file.
enum Spill {
    Memory(Vec<u8>),
    File(BufWriter<File>, PathBuf),
}

impl Spill {
    fn write(&mut self, bytes: &[u8], spill_path: impl FnOnce() -> PathBuf) -> Result<(), Error> {
        match self {
            Spill::Memory(buffer) if buffer.len() + bytes.len() > SPILL_AFTER => {
                let path = spill_path();
                let mut file = BufWriter::with_capacity(1 << 20, File::create(&path)?);
                file.write_all(buffer)?;
                file.write_all(bytes)?;
                *self = Spill::File(file, path);
            }
            Spill::Memory(buffer) => buffer.extend_from_slice(bytes),
            Spill::File(file, _) => file.write_all(bytes)?,
        }
        Ok(())
    }

    /// Copies everything written into `out` and removes the spill file.
    fn drain_into(self, out: &mut impl Write) -> Result<(), Error> {
        match self {
            Spill::Memory(buffer) => out.write_all(&buffer)?,
            Spill::File(file, path) => {
                let file = file.into_inner().map_err(|error| error.into_error())?;
                drop(file);
                let mut reader = File::open(&path)?;
                std::io::copy(&mut reader, out)?;
                fs::remove_file(&path)?;
            }
        }
        Ok(())
    }

    fn discard(self) {
        if let Spill::File(_, path) = self {
            let _ = fs::remove_file(path);
        }
    }
}

/// What a finished table is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TableInfo {
    pub base: u64,
    pub span: u64,
    pub keys: u64,
    /// Bytes of the file.
    pub len: u64,
    /// The header's CRC32C, which covers the check arrays and through them
    /// every byte: a fingerprint of the table's content.
    pub header_crc: u32,
}

/// Writes a table in one pass, keys arriving in (hash, key) order. Position
/// sets go straight to the file; keys and directory entries are kept aside
/// (spilled past 64 MiB) and appended after them. The file is written under
/// a temporary name and renamed into place by [`finish`](Self::finish).
pub(crate) struct TableWriter {
    path: PathBuf,
    tmp: PathBuf,
    out: BufWriter<File>,
    base: u64,
    span: u64,
    bitmaps_len: u64,
    bitmap_checks: Vec<u8>,
    chunk_crc: u32,
    keys: Option<Spill>,
    keys_len: u64,
    directory: Option<Spill>,
    entries: u64,
    block_directory: Vec<u8>,
    block_keys: Vec<u8>,
    directory_checks: Vec<u8>,
    last: Option<(u64, Vec<u8>)>,
    scratch: Vec<u8>,
    done: bool,
}

impl TableWriter {
    /// Starts a table over `[base, base + span)` to be written at `path`.
    pub(crate) fn create(path: &Path, base: u64, span: u64) -> Result<Self, Error> {
        if span > MAX_SPAN {
            return Err(Error::Internal {
                message: format!("a key table may span at most {MAX_SPAN} positions, not {span}"),
            });
        }
        let tmp = tmp_path(path, "tmp");
        let mut out = BufWriter::with_capacity(1 << 20, File::create(&tmp)?);
        out.write_all(&[0u8; HEADER_LEN])?;
        Ok(Self {
            path: path.to_path_buf(),
            tmp,
            out,
            base,
            span,
            bitmaps_len: 0,
            bitmap_checks: Vec::new(),
            chunk_crc: 0,
            keys: Some(Spill::Memory(Vec::new())),
            keys_len: 0,
            directory: Some(Spill::Memory(Vec::new())),
            entries: 0,
            block_directory: Vec::with_capacity(BLOCK_ENTRIES * ENTRY_LEN),
            block_keys: Vec::new(),
            directory_checks: Vec::new(),
            last: None,
            scratch: Vec::new(),
            done: false,
        })
    }

    /// Bytes written so far, for throttling.
    pub(crate) fn written(&self) -> u64 {
        self.bitmaps_len + self.keys_len + self.entries * ENTRY_LEN as u64
    }

    fn write_bitmap_bytes(&mut self, mut bytes: &[u8]) -> Result<(), Error> {
        self.out.write_all(bytes)?;
        while !bytes.is_empty() {
            let fill = (self.bitmaps_len % mapped_set::CHUNK as u64) as usize;
            let take = bytes.len().min(mapped_set::CHUNK - fill);
            self.chunk_crc = crc32c::crc32c_append(self.chunk_crc, &bytes[..take]);
            self.bitmaps_len += take as u64;
            bytes = &bytes[take..];
            if self.bitmaps_len.is_multiple_of(mapped_set::CHUNK as u64) {
                self.bitmap_checks
                    .extend_from_slice(&self.chunk_crc.to_le_bytes());
                self.chunk_crc = 0;
            }
        }
        Ok(())
    }

    fn flush_block(&mut self) -> Result<(), Error> {
        if self.block_directory.is_empty() {
            return Ok(());
        }
        let crc = crc32c::crc32c_append(crc32c::crc32c(&self.block_directory), &self.block_keys);
        self.directory_checks.extend_from_slice(&crc.to_le_bytes());
        let (path, block) = (&self.path, std::mem::take(&mut self.block_directory));
        self.directory
            .as_mut()
            .unwrap()
            .write(&block, || tmp_path(path, "directory.tmp"))?;
        self.block_directory = block;
        self.block_directory.clear();
        self.block_keys.clear();
        Ok(())
    }

    /// Adds `key` holding `positions` (relative to the base). Keys must
    /// arrive in ascending (hash, key) order, each once.
    pub(crate) fn push(&mut self, key: &[u8], positions: &RoaringBitmap) -> Result<(), Error> {
        let hash = key_hash(key);
        if self
            .last
            .as_ref()
            .is_some_and(|(h, k)| (*h, k.as_slice()) >= (hash, key))
        {
            return Err(Error::Internal {
                message: "key table entries must be distinct and in (hash, key) order".into(),
            });
        }
        let last = self.last.get_or_insert_with(|| (0, Vec::new()));
        last.0 = hash;
        last.1.clear();
        last.1.extend_from_slice(key);

        if positions
            .max()
            .is_some_and(|max| u64::from(max) >= self.span)
        {
            return Err(Error::Internal {
                message: "a key's positions reach past its table's span".into(),
            });
        }
        let key_len = u32::try_from(key.len()).map_err(|_| Error::Internal {
            message: format!("key of {} bytes is too long to index", key.len()),
        })?;
        let mut scratch = std::mem::take(&mut self.scratch);
        scratch.clear();
        canonical_serialize(positions, &mut scratch);
        let bitmap_len = u32::try_from(scratch.len()).map_err(|_| Error::Internal {
            message: "position set too large to index".into(),
        })?;
        let bitmap_offset = self.bitmaps_len;
        self.write_bitmap_bytes(&scratch)?;
        self.scratch = scratch;

        let entry = &mut self.block_directory;
        entry.extend_from_slice(&hash.to_le_bytes());
        entry.extend_from_slice(&self.keys_len.to_le_bytes());
        entry.extend_from_slice(&bitmap_offset.to_le_bytes());
        entry.extend_from_slice(&key_len.to_le_bytes());
        entry.extend_from_slice(&bitmap_len.to_le_bytes());
        entry.extend_from_slice(&(positions.len() as u32).to_le_bytes());
        entry.extend_from_slice(&0u32.to_le_bytes());
        self.block_keys.extend_from_slice(key);
        let path = &self.path;
        self.keys
            .as_mut()
            .unwrap()
            .write(key, || tmp_path(path, "keys.tmp"))?;
        self.keys_len += key.len() as u64;
        self.entries += 1;
        if self.entries.is_multiple_of(BLOCK_ENTRIES as u64) {
            self.flush_block()?;
        }
        Ok(())
    }

    fn pad(&mut self, at: &mut u64) -> Result<(), Error> {
        let padded = at.next_multiple_of(8);
        self.out.write_all(&[0u8; 8][..(padded - *at) as usize])?;
        *at = padded;
        Ok(())
    }

    /// Writes the rest, syncs, and renames the file into place. The caller
    /// syncs the directory.
    pub(crate) fn finish(mut self) -> Result<TableInfo, Error> {
        self.flush_block()?;
        if !self.bitmaps_len.is_multiple_of(mapped_set::CHUNK as u64) {
            self.bitmap_checks
                .extend_from_slice(&self.chunk_crc.to_le_bytes());
        }
        let mut sections = [(0u64, 0u64); SECTIONS];
        let mut at = HEADER_LEN as u64;
        sections[BITMAPS] = (at, self.bitmaps_len);
        at += self.bitmaps_len;

        self.pad(&mut at)?;
        sections[KEYS] = (at, self.keys_len);
        self.keys.take().unwrap().drain_into(&mut self.out)?;
        at += self.keys_len;

        self.pad(&mut at)?;
        let directory_len = self.entries * ENTRY_LEN as u64;
        sections[DIRECTORY] = (at, directory_len);
        self.directory.take().unwrap().drain_into(&mut self.out)?;
        at += directory_len;

        self.pad(&mut at)?;
        sections[DIRECTORY_CHECKS] = (at, self.directory_checks.len() as u64);
        let directory_checks = std::mem::take(&mut self.directory_checks);
        self.out.write_all(&directory_checks)?;
        at += directory_checks.len() as u64;

        self.pad(&mut at)?;
        sections[BITMAP_CHECKS] = (at, self.bitmap_checks.len() as u64);
        let bitmap_checks = std::mem::take(&mut self.bitmap_checks);
        self.out.write_all(&bitmap_checks)?;
        at += bitmap_checks.len() as u64;

        let mut header = [0u8; HEADER_LEN];
        header[0..4].copy_from_slice(&MAGIC);
        header[4] = VERSION;
        header[8..16].copy_from_slice(&self.base.to_le_bytes());
        header[16..24].copy_from_slice(&self.span.to_le_bytes());
        header[24..32].copy_from_slice(&self.entries.to_le_bytes());
        for (i, (offset, len)) in sections.iter().enumerate() {
            header[32 + i * 16..40 + i * 16].copy_from_slice(&offset.to_le_bytes());
            header[40 + i * 16..48 + i * 16].copy_from_slice(&len.to_le_bytes());
        }
        header[112..116].copy_from_slice(&crc32c::crc32c(&directory_checks).to_le_bytes());
        header[116..120].copy_from_slice(&crc32c::crc32c(&bitmap_checks).to_le_bytes());
        let header_crc = crc32c::crc32c(&header[..124]);
        header[124..128].copy_from_slice(&header_crc.to_le_bytes());

        self.out.flush()?;
        let file = self.out.get_mut();
        file.seek(SeekFrom::Start(0))?;
        file.write_all(&header)?;
        file.sync_all()?;
        fs::rename(&self.tmp, &self.path)?;
        self.done = true;
        Ok(TableInfo {
            base: self.base,
            span: self.span,
            keys: self.entries,
            len: at,
            header_crc,
        })
    }
}

impl Drop for TableWriter {
    /// An abandoned table leaves no files behind.
    fn drop(&mut self) {
        if let Some(keys) = self.keys.take() {
            keys.discard();
        }
        if let Some(directory) = self.directory.take() {
            directory.discard();
        }
        if !self.done {
            let _ = fs::remove_file(&self.tmp);
        }
    }
}

/// `path` with `suffix` added to its extension, for files written beside it.
fn tmp_path(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".");
    name.push(suffix);
    PathBuf::from(name)
}

/// Writes a table of `(key, positions)` pairs in any order at `path`.
pub(crate) fn write_table<'a>(
    path: &Path,
    base: u64,
    span: u64,
    entries: impl IntoIterator<Item = (&'a [u8], &'a RoaringBitmap)>,
) -> Result<TableInfo, Error> {
    let mut sorted: Vec<(u64, &[u8], &RoaringBitmap)> = entries
        .into_iter()
        .map(|(key, positions)| (key_hash(key), key, positions))
        .collect();
    sorted.sort_unstable_by(|a, b| (a.0, a.1).cmp(&(b.0, b.1)));
    let mut writer = TableWriter::create(path, base, span)?;
    for (_, key, positions) in sorted {
        writer.push(key, positions)?;
    }
    writer.finish()
}

/// A directory entry as stored.
#[derive(Clone, Copy)]
struct RawEntry {
    hash: u64,
    key_offset: u64,
    bitmap_offset: u64,
    key_len: u32,
    bitmap_len: u32,
    count: u32,
}

impl RawEntry {
    fn at(directory: &[u8], index: usize) -> Self {
        let e = &directory[index * ENTRY_LEN..(index + 1) * ENTRY_LEN];
        let u64_at = |at: usize| u64::from_le_bytes(e[at..at + 8].try_into().unwrap());
        let u32_at = |at: usize| u32::from_le_bytes(e[at..at + 4].try_into().unwrap());
        Self {
            hash: u64_at(0),
            key_offset: u64_at(8),
            bitmap_offset: u64_at(16),
            key_len: u32_at(24),
            bitmap_len: u32_at(28),
            count: u32_at(32),
        }
    }
}

/// A mapped key table.
pub struct KeyTableFile {
    map: Mmap,
    base: u64,
    span: u64,
    keys: usize,
    sections: [std::ops::Range<usize>; SECTIONS],
    header_crc: u32,
    directory_verified: VerifiedBlocks,
    bitmaps_verified: VerifiedBlocks,
}

/// One key of a table and where its positions are.
#[derive(Clone, Copy)]
pub struct Entry<'a> {
    pub hash: u64,
    pub key: &'a [u8],
    /// How many positions hold the key.
    pub count: u32,
    table: &'a KeyTableFile,
    bitmap_offset: usize,
    bitmap_len: usize,
}

impl Entry<'_> {
    /// The key's positions, relative to the table's base, read in place.
    pub fn set(&self) -> Result<MappedSet<'_>, Error> {
        MappedSet::new(
            self.table.bitmap_region(),
            self.bitmap_offset,
            self.bitmap_len,
            u64::from(self.count),
        )
    }
}

fn corrupted(path: &Path, what: impl std::fmt::Display) -> Error {
    Error::Corrupted {
        message: format!("{}: {what}", path.display()),
    }
}

impl KeyTableFile {
    /// Maps the table at `path`. Checks the header and the check arrays;
    /// everything else is checked as it is read.
    pub fn open(path: &Path) -> Result<Self, Error> {
        let file = File::open(path)?;
        // SAFETY: tables are written whole under a temporary name and
        // renamed into place, never modified after; a mapping keeps the
        // inode it was made from even if the name is removed or replaced.
        let map = unsafe { Mmap::map(&file)? };
        let bytes = &map[..];
        if bytes.len() < HEADER_LEN || bytes[0..4] != MAGIC {
            return Err(corrupted(path, "not a key table"));
        }
        if bytes[4] != VERSION {
            return Err(corrupted(
                path,
                format_args!("key table version {} (expected {VERSION})", bytes[4]),
            ));
        }
        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        let header_crc = u32_at(124);
        if crc32c::crc32c(&bytes[..124]) != header_crc {
            return Err(corrupted(path, "key table header checksum mismatch"));
        }
        let (base, span, keys) = (u64_at(8), u64_at(16), u64_at(24));
        let mut sections: [std::ops::Range<usize>; SECTIONS] = Default::default();
        let mut end = HEADER_LEN as u64;
        for (i, section) in sections.iter_mut().enumerate() {
            let (at, len) = (u64_at(32 + i * 16), u64_at(40 + i * 16));
            match at.checked_add(len).filter(|&e| e <= bytes.len() as u64) {
                Some(section_end) if at >= end => {
                    *section = at as usize..section_end as usize;
                    end = section_end;
                }
                _ => return Err(corrupted(path, "key table sections overlap or overrun")),
            }
        }
        let len_of = |i: usize| sections[i].len() as u64;
        if span > MAX_SPAN
            || end != bytes.len() as u64
            || Some(len_of(DIRECTORY)) != keys.checked_mul(ENTRY_LEN as u64)
            || len_of(DIRECTORY_CHECKS) != keys.div_ceil(BLOCK_ENTRIES as u64) * 4
            || len_of(BITMAP_CHECKS) != len_of(BITMAPS).div_ceil(mapped_set::CHUNK as u64) * 4
        {
            return Err(corrupted(path, "key table layout is inconsistent"));
        }
        for (section, at) in [(DIRECTORY_CHECKS, 112), (BITMAP_CHECKS, 116)] {
            if crc32c::crc32c(&bytes[sections[section].clone()]) != u32_at(at) {
                return Err(corrupted(path, "key table check array checksum mismatch"));
            }
        }
        let keys = keys as usize;
        let bitmaps = sections[BITMAPS].len();
        Ok(Self {
            base,
            span,
            keys,
            header_crc,
            directory_verified: VerifiedBlocks::new(keys.div_ceil(BLOCK_ENTRIES)),
            bitmaps_verified: VerifiedBlocks::new(bitmaps.div_ceil(mapped_set::CHUNK)),
            sections,
            map,
        })
    }

    fn section(&self, index: usize) -> &[u8] {
        &self.map[self.sections[index].clone()]
    }

    fn bitmap_region(&self) -> Region<'_> {
        Region {
            bytes: self.section(BITMAPS),
            checks: self.section(BITMAP_CHECKS),
            verified: &self.bitmaps_verified,
        }
    }

    /// The first position the table covers; its sets are relative to it.
    pub fn base(&self) -> u64 {
        self.base
    }

    /// How many positions from the base the table covers.
    pub fn span(&self) -> u64 {
        self.span
    }

    /// The position after the last one the table covers.
    pub fn end(&self) -> u64 {
        self.base + self.span
    }

    /// Number of keys.
    pub fn len(&self) -> usize {
        self.keys
    }

    pub fn is_empty(&self) -> bool {
        self.keys == 0
    }

    /// Bytes mapped.
    pub fn file_len(&self) -> u64 {
        self.map.len() as u64
    }

    /// The header's checksum: a fingerprint of the whole table.
    pub fn header_crc(&self) -> u32 {
        self.header_crc
    }

    /// The entries of directory block `block` and the bytes of their keys.
    fn block_bytes(&self, block: usize) -> Option<(&[u8], &[u8])> {
        let directory = self.section(DIRECTORY);
        let first = block * BLOCK_ENTRIES;
        let last = (first + BLOCK_ENTRIES).min(self.keys).checked_sub(1)?;
        let entries = &directory[first * ENTRY_LEN..(last + 1) * ENTRY_LEN];
        let start = RawEntry::at(directory, first).key_offset;
        let tail = RawEntry::at(directory, last);
        let end = tail.key_offset.checked_add(u64::from(tail.key_len))?;
        let keys = self
            .section(KEYS)
            .get(usize::try_from(start).ok()?..usize::try_from(end).ok()?)?;
        Some((entries, keys))
    }

    fn verify_block(&self, block: usize) -> Result<(), Error> {
        self.directory_verified.ensure(block, || {
            let checks = self.section(DIRECTORY_CHECKS);
            let stored = u32::from_le_bytes(checks[block * 4..block * 4 + 4].try_into().unwrap());
            let (entries, keys) = self.block_bytes(block).ok_or_else(|| Error::Corrupted {
                message: format!("key table block {block} points outside its keys"),
            })?;
            if crc32c::crc32c_append(crc32c::crc32c(entries), keys) != stored {
                return Err(Error::Corrupted {
                    message: format!("key table block {block} checksum mismatch"),
                });
            }
            Ok(())
        })
    }

    fn raw(&self, index: usize) -> Result<RawEntry, Error> {
        self.verify_block(index / BLOCK_ENTRIES)?;
        Ok(RawEntry::at(self.section(DIRECTORY), index))
    }

    /// Entry `index` in directory order, its block verified.
    pub fn entry(&self, index: usize) -> Result<Entry<'_>, Error> {
        let raw = self.raw(index)?;
        let out_of_bounds = || Error::Corrupted {
            message: format!("key table entry {index} points outside its regions"),
        };
        let key_start = usize::try_from(raw.key_offset).map_err(|_| out_of_bounds())?;
        let key = self
            .section(KEYS)
            .get(key_start..key_start + raw.key_len as usize)
            .ok_or_else(out_of_bounds)?;
        let bitmap_offset = usize::try_from(raw.bitmap_offset).map_err(|_| out_of_bounds())?;
        let bitmap_len = raw.bitmap_len as usize;
        if bitmap_offset + bitmap_len > self.sections[BITMAPS].len() {
            return Err(out_of_bounds());
        }
        Ok(Entry {
            hash: raw.hash,
            key,
            count: raw.count,
            table: self,
            bitmap_offset,
            bitmap_len,
        })
    }

    /// The first index whose (hash, key) is not below `(hash, key)`.
    /// Interpolation over the hashes for up to four rounds, then binary
    /// search: two or three probes on uniform hashes, never worse than
    /// logarithmic on crafted ones.
    fn lower_bound(&self, hash: u64, key: &[u8]) -> Result<usize, Error> {
        let below = |index: usize| -> Result<(bool, u64), Error> {
            let entry = self.entry(index)?;
            Ok(((entry.hash, entry.key) < (hash, key), entry.hash))
        };
        // Invariant: entries before `lo` are below the target, entries from
        // `hi` on are not; `lo_hash`/`hi_hash` bound the hashes in between.
        let (mut lo, mut hi) = (0usize, self.keys);
        let (mut lo_hash, mut hi_hash) = (0u64, u64::MAX);
        for _ in 0..4 {
            if hi - lo <= 8 || hash < lo_hash || hash > hi_hash {
                break;
            }
            let width = u128::from(hi_hash - lo_hash) + 1;
            let offset = u128::from(hash - lo_hash) * (hi - lo) as u128 / width;
            let guess = lo + (offset as usize).min(hi - lo - 1);
            let (is_below, found) = below(guess)?;
            if is_below {
                lo = guess + 1;
                lo_hash = found;
            } else {
                hi = guess;
                hi_hash = found;
            }
        }
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if below(mid)?.0 {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    /// The entry for `key` (whose hash is `hash`).
    pub fn find(&self, key: &[u8], hash: u64) -> Result<Option<Entry<'_>>, Error> {
        let index = self.lower_bound(hash, key)?;
        if index == self.keys {
            return Ok(None);
        }
        let entry = self.entry(index)?;
        Ok((entry.hash == hash && entry.key == key).then_some(entry))
    }

    /// Every entry, in (hash, key) order: what a merge walks.
    pub fn iter(&self) -> impl Iterator<Item = Result<Entry<'_>, Error>> + '_ {
        (0..self.keys).map(|index| self.entry(index))
    }

    /// Reads every byte and checks it: the header, each directory block and
    /// each chunk of position sets, every entry against its set. What a
    /// table that is about to be trusted for archiving gets.
    pub fn verify_all(&self) -> Result<(), Error> {
        for block in 0..self.keys.div_ceil(BLOCK_ENTRIES) {
            self.verify_block(block)?;
        }
        let region = self.bitmap_region();
        region.get(0, region.bytes.len())?;
        let mut previous: Option<(u64, &[u8])> = None;
        for entry in self.iter() {
            let entry = entry?;
            if previous.is_some_and(|p| p >= (entry.hash, entry.key))
                || key_hash(entry.key) != entry.hash
            {
                return Err(Error::Corrupted {
                    message: "key table directory is out of order".into(),
                });
            }
            previous = Some((entry.hash, entry.key));
            let set = entry.set()?.to_bitmap()?;
            if set.max().is_some_and(|max| u64::from(max) >= self.span) {
                return Err(Error::Corrupted {
                    message: "key table position set reaches past the table's span".into(),
                });
            }
        }
        Ok(())
    }
}

impl KeySource for KeyTableFile {
    fn base(&self) -> u64 {
        self.base
    }

    fn span(&self) -> u64 {
        self.span
    }

    fn positions(&self, key: &ProbeKey) -> Result<Option<Positions<'_>>, Error> {
        match self.find(&key.bytes, key.hash)? {
            Some(entry) => Ok(Some(Positions::Mapped(MappedSet::new(
                self.bitmap_region(),
                entry.bitmap_offset,
                entry.bitmap_len,
                u64::from(entry.count),
            )?))),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_key_hash_is_frozen() {
        // Part of the format: tables are ordered by it.
        assert_eq!(key_hash(b""), 0x9146_2240_6682_3752);
        assert_eq!(key_hash(b"orderId"), 0xb67f_e671_ad93_6773);
        assert_eq!(
            key_hash(b"\x07\0\0\0orderIdo-123456789"),
            0x3761_69df_dc3e_c8c5
        );
    }

    fn table_of(dir: &Path, name: &str, pairs: &[(Vec<u8>, RoaringBitmap)]) -> KeyTableFile {
        let path = dir.join(name);
        write_table(
            &path,
            1_000,
            1 << 20,
            pairs.iter().map(|(k, b)| (k.as_slice(), b)),
        )
        .unwrap();
        KeyTableFile::open(&path).unwrap()
    }

    fn pairs(n: u32) -> Vec<(Vec<u8>, RoaringBitmap)> {
        (0..n)
            .map(|k| {
                let key = format!("key-{k:06}").into_bytes();
                let set: RoaringBitmap = [k % 1000, (k * 7) % 100_000, k * 3 + 500_000]
                    .into_iter()
                    .collect();
                (key, set)
            })
            .collect()
    }

    #[test]
    fn finds_every_key_and_nothing_else() {
        let dir = tempfile::tempdir().unwrap();
        for n in [0u32, 1, 2, 9, 128, 129, 5_000, 70_000] {
            let pairs = pairs(n);
            let table = table_of(dir.path(), &format!("t{n}"), &pairs);
            assert_eq!(table.len(), n as usize);
            assert_eq!((table.base(), table.span()), (1_000, 1 << 20));
            for (key, set) in &pairs {
                let entry = table.find(key, key_hash(key)).unwrap().unwrap();
                assert_eq!(entry.key, key.as_slice());
                assert_eq!(&entry.set().unwrap().to_bitmap().unwrap(), set);
            }
            for missing in [&b""[..], b"key-", b"key-999999", b"zzz", b"key-000000x"] {
                assert!(table.find(missing, key_hash(missing)).unwrap().is_none());
            }
            table.verify_all().unwrap();
            // The walk is in (hash, key) order and holds every key once.
            let walked: Vec<(u64, Vec<u8>)> = table
                .iter()
                .map(|e| e.map(|e| (e.hash, e.key.to_vec())).unwrap())
                .collect();
            let mut expected: Vec<(u64, Vec<u8>)> = pairs
                .iter()
                .map(|(k, _)| (key_hash(k), k.clone()))
                .collect();
            expected.sort();
            assert_eq!(walked, expected);
        }
    }

    /// Keys whose hashes collide, or crowd one corner of the hash space,
    /// are still all found: the search never assumes uniform hashes.
    #[test]
    fn crowded_hashes_are_still_found() {
        let dir = tempfile::tempdir().unwrap();
        // Pick keys whose hashes all fall in the lowest 1/4096 of the space.
        let crowded: Vec<(Vec<u8>, RoaringBitmap)> = (0u32..)
            .map(|k| format!("c{k}").into_bytes())
            .filter(|k| key_hash(k) >> 52 == 0)
            .take(300)
            .map(|k| (k, [1u32].into_iter().collect()))
            .chain(pairs(3_000))
            .collect();
        let table = table_of(dir.path(), "crowded", &crowded);
        for (key, _) in &crowded {
            assert!(table.find(key, key_hash(key)).unwrap().is_some());
        }
        // A key looked up under a hash that is not its own is not found:
        // equal hashes are told apart by their bytes.
        let (key, _) = &crowded[0];
        let (other, _) = &crowded[1];
        assert!(table.find(other, key_hash(key)).unwrap().is_none());
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

            let mut shrunk: RoaringBitmap = (0..300_000u32).collect();
            shrunk -= &((0..300_000u32).collect::<RoaringBitmap>() - set);
            shrunk |= set;
            let mut again = Vec::new();
            canonical_serialize(&shrunk, &mut again);
            assert_eq!(again, ours);
        }
    }

    /// The bytes follow from the content: the same pairs in any order, and
    /// spilled or not, make the same file.
    #[test]
    fn tables_are_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let mut pairs = pairs(20_000);
        let a = table_of(dir.path(), "a", &pairs);
        pairs.reverse();
        let b = table_of(dir.path(), "b", &pairs);
        assert_eq!(a.header_crc(), b.header_crc());
        assert_eq!(
            std::fs::read(dir.path().join("a")).unwrap(),
            std::fs::read(dir.path().join("b")).unwrap()
        );
    }

    #[test]
    fn keys_must_arrive_in_order_and_sets_stay_in_span() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t");
        let set: RoaringBitmap = [1u32].into_iter().collect();
        let mut keys = [b"a".to_vec(), b"b".to_vec()];
        keys.sort_by_key(|k| key_hash(k));
        let mut writer = TableWriter::create(&path, 0, 10).unwrap();
        writer.push(&keys[1], &set).unwrap();
        assert!(writer.push(&keys[0], &set).is_err());
        assert!(writer.push(&keys[1], &set).is_err());
        drop(writer);
        let mut writer = TableWriter::create(&path, 0, 10).unwrap();
        let wide: RoaringBitmap = [10u32].into_iter().collect();
        assert!(writer.push(b"a", &wide).is_err());
        drop(writer);
        // An abandoned writer leaves nothing behind.
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
        assert!(TableWriter::create(&path, 0, MAX_SPAN + 1).is_err());
    }

    #[test]
    fn a_flipped_bit_anywhere_is_refused_never_believed() {
        let dir = tempfile::tempdir().unwrap();
        let pairs = pairs(600);
        let path = dir.path().join("t");
        write_table(
            &path,
            0,
            1 << 20,
            pairs.iter().map(|(k, b)| (k.as_slice(), b)),
        )
        .unwrap();
        let intact = std::fs::read(&path).unwrap();
        let (mut refused, mut harmless) = (0, 0);
        for at in (0..intact.len())
            .step_by(intact.len() / 300)
            .chain([4, 9, 30, 113, 125])
        {
            let mut damaged = intact.clone();
            damaged[at] ^= 0x10;
            std::fs::write(&path, &damaged).unwrap();
            let answers = KeyTableFile::open(&path).and_then(|table| {
                let mut out = Vec::new();
                for (key, _) in &pairs {
                    let found = table.find(key, key_hash(key))?;
                    out.push(match found {
                        Some(entry) => Some(entry.set()?.to_bitmap()?),
                        None => None,
                    });
                }
                Ok(out)
            });
            match answers {
                Err(Error::Corrupted { .. }) => refused += 1,
                Err(other) => panic!("byte {at}: {other:?}"),
                Ok(sets) => {
                    for ((_, expected), got) in pairs.iter().zip(sets) {
                        assert_eq!(got.as_ref(), Some(expected), "byte {at} changed an answer");
                    }
                    harmless += 1;
                }
            }
        }
        assert!(refused > 250, "{refused} refused, {harmless} harmless");
    }
}
