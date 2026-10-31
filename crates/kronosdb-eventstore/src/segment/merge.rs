//! The log-wide index (ADR-0010): which merged key tables a log should have,
//! building one, and the manifest that names the live ones.
//!
//! Only final segments are merged: sealed, their `.keys` on disk, and ending
//! strictly below the watermark, so nothing a table covers can change again.
//! Those form a prefix of the log. Over it the tables follow a binary
//! counter with fan-in [`FAN_IN`]: a table at level L ≥ 1 covers the aligned
//! block of segments `[k·8^L, (k+1)·8^L)`, taken greedily from the largest
//! blocks down ([`targets`]). The set is a function of the final segments'
//! count and bases, the same on every node, and each table's bytes are a
//! function of the events in its range, however it was built.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::fs::{self, File};
use std::io::Write;
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use roaring::RoaringBitmap;

use crate::error::Error;
use crate::segment::key_table::{KeyTableFile, MAX_SPAN, TableInfo, TableWriter};
use crate::segment::writer::sync_dir;

/// Tables merged into one at each level.
pub const FAN_IN: usize = 8;

/// The directory of a context that holds its merged tables and manifest.
pub const INDEX_DIR: &str = "index";
const MANIFEST: &str = "MANIFEST";
const MANIFEST_MAGIC: [u8; 4] = *b"KMAN";
const MANIFEST_VERSION: u8 = 1;
const MANIFEST_ENTRY: usize = 32;

/// The merged tables the final segments should have, as ranges of segment
/// ordinals. `bounds[i]..bounds[i + 1]` are segment `i`'s positions, so
/// `bounds` holds one more element than there are final segments.
///
/// Greedy from the largest aligned block down; a block whose positions would
/// span more than [`MAX_SPAN`] is left to its eight children. Segments not
/// in any block keep their own `.keys`.
pub fn targets(bounds: &[u64]) -> Vec<Range<usize>> {
    let segments = bounds.len().saturating_sub(1);
    let mut out = Vec::new();
    let mut size = FAN_IN;
    while size * FAN_IN <= segments {
        size *= FAN_IN;
    }
    let mut lo = 0;
    while size >= FAN_IN {
        while lo + size <= segments {
            block(bounds, lo, size, &mut out);
            lo += size;
        }
        size /= FAN_IN;
    }
    out
}

fn block(bounds: &[u64], lo: usize, size: usize, out: &mut Vec<Range<usize>>) {
    if bounds[lo + size] - bounds[lo] <= MAX_SPAN {
        out.push(lo..lo + size);
    } else if size > FAN_IN {
        for child in 0..FAN_IN {
            block(bounds, lo + child * size / FAN_IN, size / FAN_IN, out);
        }
    }
}

/// The file name of the merged table over `[base, end)`.
pub fn table_file_name(base: u64, end: u64) -> String {
    format!("{base:020}-{end:020}.keys")
}

/// The range a merged table's file name names.
pub fn parse_table_file_name(name: &str) -> Option<(u64, u64)> {
    let stem = name.strip_suffix(".keys")?;
    let (base, end) = stem.split_once('-')?;
    if base.len() != 20 || end.len() != 20 {
        return None;
    }
    Some((base.parse().ok()?, end.parse().ok()?))
}

/// Where the merged table over `[base, end)` lives.
pub fn table_path(dir: &Path, base: u64, end: u64) -> PathBuf {
    dir.join(INDEX_DIR).join(table_file_name(base, end))
}

/// One table to build and what to build it from.
pub(crate) struct Job {
    pub base: u64,
    pub end: u64,
    /// Installed tables and segment `.keys` covering `[base, end)` exactly,
    /// in position order.
    pub inputs: Vec<Arc<KeyTableFile>>,
}

/// The first target table not yet built whose inputs are all at hand.
/// `tables` are the installed merged tables, by base; `segment_keys` gives
/// a final segment's own `.keys`.
pub(crate) fn next_job(
    bounds: &[u64],
    tables: &[Arc<KeyTableFile>],
    segment_keys: impl Fn(u64) -> Option<Arc<KeyTableFile>>,
) -> Option<Job> {
    let table_at = |base: u64| {
        tables
            .binary_search_by_key(&base, |table| table.base())
            .ok()
            .map(|i| &tables[i])
    };
    'targets: for target in targets(bounds) {
        let (base, end) = (bounds[target.start], bounds[target.end]);
        if table_at(base).is_some_and(|table| table.end() == end) {
            continue;
        }
        let mut inputs = Vec::new();
        let mut segment = target.start;
        while segment < target.end {
            let at = bounds[segment];
            match table_at(at) {
                Some(table) if table.end() <= end && table.span() > 0 => {
                    inputs.push(Arc::clone(table));
                    segment += bounds[segment..].partition_point(|&b| b < table.end());
                }
                _ => match segment_keys(at) {
                    Some(keys) => {
                        inputs.push(keys);
                        segment += 1;
                    }
                    None => continue 'targets,
                },
            }
        }
        return Some(Job { base, end, inputs });
    }
    None
}

/// What one merge cost.
#[derive(Debug, Clone, Copy, Default)]
pub struct MergeCost {
    /// Bytes of input tables read.
    pub read: u64,
    /// Bytes of the table written.
    pub written: u64,
    pub elapsed: Duration,
}

/// Keeps a writer at or under a byte rate (0: unlimited).
pub(crate) struct Throttle {
    rate: u64,
    started: Instant,
}

impl Throttle {
    pub(crate) fn new(bytes_per_sec: u64) -> Self {
        Self {
            rate: bytes_per_sec,
            started: Instant::now(),
        }
    }

    /// Sleeps until `written` bytes are within the rate.
    pub(crate) fn pace(&self, written: u64) {
        if self.rate == 0 {
            return;
        }
        let due = Duration::from_secs_f64(written as f64 / self.rate as f64);
        let elapsed = self.started.elapsed();
        if due > elapsed + Duration::from_millis(10) {
            std::thread::sleep(due - elapsed);
        }
    }
}

/// Builds the table over `[job.base, job.end)` at `path` by a k-way merge
/// of the job's inputs in (hash, key) order, re-basing each key's positions
/// and appending them in position order. `go_on` is asked after every key
/// with the bytes written so far (it may sleep to throttle); `false`
/// abandons the table and leaves no file. `Ok(None)` when abandoned.
pub(crate) fn build(
    path: &Path,
    job: &Job,
    mut go_on: impl FnMut(u64) -> bool,
) -> Result<Option<(TableInfo, MergeCost)>, Error> {
    let started = Instant::now();
    let mut at = job.base;
    for input in &job.inputs {
        if input.base() != at {
            return Err(Error::Internal {
                message: format!(
                    "merge inputs for {}..{} leave a gap or overlap at {at}",
                    job.base, job.end
                ),
            });
        }
        at = input.end();
    }
    if at != job.end {
        return Err(Error::Internal {
            message: format!("merge inputs for {}..{} end at {at}", job.base, job.end),
        });
    }

    let inputs = &job.inputs;
    let mut cursors = vec![0usize; inputs.len()];
    let mut heap = BinaryHeap::new();
    for (i, input) in inputs.iter().enumerate() {
        if !input.is_empty() {
            let entry = input.entry(0)?;
            heap.push(Reverse((entry.hash, entry.key, i)));
        }
    }
    let mut writer = TableWriter::create(path, job.base, job.end - job.base)?;
    let mut group: Vec<usize> = Vec::with_capacity(inputs.len());
    while let Some(Reverse((hash, key, first))) = heap.pop() {
        group.clear();
        group.push(first);
        while let Some(Reverse((h, k, i))) = heap.peek()
            && (*h, *k) == (hash, key)
        {
            group.push(*i);
            heap.pop();
        }
        // Inputs are in position order, so ascending input index appends
        // ascending positions.
        group.sort_unstable();
        let mut merged = RoaringBitmap::new();
        for &i in &group {
            let set = inputs[i].entry(cursors[i])?.set()?.to_bitmap()?;
            let delta = (inputs[i].base() - job.base) as u32;
            if merged.is_empty() && delta == 0 {
                merged = set;
            } else {
                merged
                    .append(set.iter().map(|p| p + delta))
                    .map_err(|_| Error::Internal {
                        message: "merged positions are out of order".into(),
                    })?;
            }
        }
        writer.push(key, &merged)?;
        for &i in &group {
            cursors[i] += 1;
            if cursors[i] < inputs[i].len() {
                let entry = inputs[i].entry(cursors[i])?;
                heap.push(Reverse((entry.hash, entry.key, i)));
            }
        }
        if !go_on(writer.written()) {
            return Ok(None);
        }
    }
    let info = writer.finish()?;
    Ok(Some((
        info,
        MergeCost {
            read: inputs.iter().map(|input| input.file_len()).sum(),
            written: info.len,
            elapsed: started.elapsed(),
        },
    )))
}

/// A live merged table, as the manifest names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestEntry {
    pub base: u64,
    pub end: u64,
    /// Bytes of the file.
    pub len: u64,
    /// The table's header checksum, which covers every byte through the
    /// check arrays.
    pub header_crc: u32,
}

impl ManifestEntry {
    pub fn of(table: &KeyTableFile) -> Self {
        Self {
            base: table.base(),
            end: table.end(),
            len: table.file_len(),
            header_crc: table.header_crc(),
        }
    }
}

/// The manifest's bytes:
///
/// ```text
/// magic "KMAN" | version 1 | 0 x3 | count u32 | 0 u32 |
/// per table: base u64 | end u64 | length u64 | header CRC32C u32 | 0 u32 |
/// CRC32C of everything before
/// ```
pub fn encode_manifest(entries: &[ManifestEntry]) -> Vec<u8> {
    let mut out = Vec::with_capacity(16 + entries.len() * MANIFEST_ENTRY + 4);
    out.extend_from_slice(&MANIFEST_MAGIC);
    out.push(MANIFEST_VERSION);
    out.extend_from_slice(&[0; 3]);
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    for entry in entries {
        out.extend_from_slice(&entry.base.to_le_bytes());
        out.extend_from_slice(&entry.end.to_le_bytes());
        out.extend_from_slice(&entry.len.to_le_bytes());
        out.extend_from_slice(&entry.header_crc.to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes());
    }
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

fn decode_manifest(bytes: &[u8]) -> Result<Vec<ManifestEntry>, Error> {
    let corrupted = |what: &str| Error::Corrupted {
        message: format!("index manifest {what}"),
    };
    if bytes.len() < 20 || bytes[0..4] != MANIFEST_MAGIC {
        return Err(corrupted("is not a manifest"));
    }
    if bytes[4] != MANIFEST_VERSION {
        return Err(corrupted("has an unknown version"));
    }
    let (body, crc) = bytes.split_at(bytes.len() - 4);
    if crc32c::crc32c(body) != u32::from_le_bytes(crc.try_into().unwrap()) {
        return Err(corrupted("checksum mismatch"));
    }
    let count = u32::from_le_bytes(body[8..12].try_into().unwrap()) as usize;
    if body.len() != 16 + count * MANIFEST_ENTRY {
        return Err(corrupted("has the wrong length"));
    }
    let entries: Vec<ManifestEntry> = body[16..]
        .chunks(MANIFEST_ENTRY)
        .map(|e| {
            let u64_at = |at: usize| u64::from_le_bytes(e[at..at + 8].try_into().unwrap());
            ManifestEntry {
                base: u64_at(0),
                end: u64_at(8),
                len: u64_at(16),
                header_crc: u32::from_le_bytes(e[24..28].try_into().unwrap()),
            }
        })
        .collect();
    if entries.windows(2).any(|pair| pair[0].end > pair[1].base)
        || entries.iter().any(|e| e.end < e.base)
    {
        return Err(corrupted("lists overlapping tables"));
    }
    Ok(entries)
}

/// The live tables of the log in `dir`; `None` when it has no manifest.
pub fn read_manifest(dir: &Path) -> Result<Option<Vec<ManifestEntry>>, Error> {
    match fs::read(dir.join(INDEX_DIR).join(MANIFEST)) {
        Ok(bytes) => decode_manifest(&bytes).map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Makes `entries` the live tables: written under a temporary name, synced,
/// renamed over the manifest and the directory synced. The rename is the
/// commit point of a merge.
pub fn write_manifest(dir: &Path, entries: &[ManifestEntry]) -> Result<(), Error> {
    let index = dir.join(INDEX_DIR);
    let tmp = index.join("MANIFEST.tmp");
    let mut file = File::create(&tmp)?;
    file.write_all(&encode_manifest(entries))?;
    file.sync_all()?;
    drop(file);
    fs::rename(&tmp, index.join(MANIFEST))?;
    sync_dir(&index)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn even(segments: usize, per: u64) -> Vec<u64> {
        (0..=segments as u64).map(|i| i * per).collect()
    }

    #[test]
    fn targets_follow_the_count_in_base_eight() {
        assert!(targets(&even(0, 10)).is_empty());
        assert!(targets(&even(7, 10)).is_empty());
        assert_eq!(targets(&even(8, 10)), vec![0..8]);
        assert_eq!(targets(&even(15, 10)), vec![0..8]);
        assert_eq!(targets(&even(16, 10)), vec![0..8, 8..16]);
        assert_eq!(targets(&even(64, 10)), vec![0..64]);
        assert_eq!(targets(&even(73, 10)), vec![0..64, 64..72]);
        // 2000 = 3·512 + 7·64 + 2·8 + 0: twelve tables, no segment left.
        let t = targets(&even(2000, 10));
        assert_eq!(t.len(), 12);
        assert_eq!(t.iter().map(|r| r.len()).sum::<usize>(), 2000);
        assert_eq!(t[0], 0..512);
        assert_eq!(t[10], 1984..1992);
        assert_eq!(t[11], 1992..2000);
        // Disjoint and in order.
        assert!(t.windows(2).all(|w| w[0].end == w[1].start));
    }

    #[test]
    fn a_block_too_wide_for_32_bits_is_left_to_its_children() {
        // 64 segments of 2^27 positions: 2^33 together, too wide; eight
        // blocks of 2^30 fit.
        let bounds = even(64, 1 << 27);
        let t = targets(&bounds);
        assert_eq!(t.len(), 8);
        assert!(t.iter().all(|r| r.len() == 8));
        // Eight segments of 2^30: even one level-1 block is too wide.
        assert!(targets(&even(8, 1 << 30)).is_empty());
        // Exactly MAX_SPAN fits.
        let mut bounds = even(8, 1 << 29);
        bounds[8] = MAX_SPAN;
        assert_eq!(targets(&bounds), vec![0..8]);
    }

    #[test]
    fn table_names_round_trip() {
        let name = table_file_name(12, 3_000_000_000_000);
        assert_eq!(parse_table_file_name(&name), Some((12, 3_000_000_000_000)));
        assert_eq!(parse_table_file_name("12-30.keys"), None);
        assert_eq!(parse_table_file_name(&format!("{name}.tmp")), None);
    }

    #[test]
    fn the_manifest_round_trips_and_refuses_damage() {
        let entries = vec![
            ManifestEntry {
                base: 0,
                end: 80,
                len: 1234,
                header_crc: 7,
            },
            ManifestEntry {
                base: 80,
                end: 90,
                len: 99,
                header_crc: 8,
            },
        ];
        let bytes = encode_manifest(&entries);
        assert_eq!(decode_manifest(&bytes).unwrap(), entries);
        for at in 0..bytes.len() {
            let mut damaged = bytes.clone();
            damaged[at] ^= 1;
            assert!(decode_manifest(&damaged).is_err(), "byte {at}");
        }
        assert!(decode_manifest(&encode_manifest(&[])).unwrap().is_empty());
    }
}
