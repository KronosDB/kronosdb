//! A verified block cache for sealed segments.
//!
//! A sealed segment is read in record-aligned blocks (about `block_size`
//! bytes each) with `pread`, and the cache serves them until it evicts them.
//! A record is CRC-checked after the read that brought it into memory and
//! before it is first served, never again until its block has been evicted
//! and read once more.
//!
//! That is what an mmap cannot do. A mapping hands out pages the kernel may
//! have dropped and re-read from disk since the last check, so a reader has
//! to re-verify on every read to keep the guarantee that no damaged byte is
//! served. With a block cache the process is the one that moves bytes from
//! the kernel into memory, so "verified once per load" is "verified every
//! time it came from disk". A byte flipped in RAM while a block is cached is
//! out of scope here, as it is for every other in-memory structure.
//!
//! The price is what a mapping does not pay: a syscall and a copy per block
//! that is not cached, and memory that the kernel's page cache also holds.
//! The cache pays off when its budget covers what is read repeatedly; below
//! that it is slower than the mapping (see the numbers in the read-path
//! commit message).
//!
//! # Verification
//!
//! A block that is loaded because a reader is going through the segment in
//! order (the previous request was for the block before it) is checked whole
//! when it arrives and read ahead of the reader. A
//! block loaded for a point read is not: its records are checked one by one,
//! as they are served, because a point read uses one record of sixteen
//! thousand bytes. Either way the first check of a record is the last until
//! the block is evicted.
//!
//! # Geometry
//!
//! Block `k` of a segment owns the event records whose header starts in
//! `[k * block_size, (k + 1) * block_size)`. Its extent runs from the first
//! such record to the first record of block `k + 1` (or the end of the last
//! event record), so a block always starts and ends on a record boundary and
//! holds whole records. The boundaries come from the segment's offset table,
//! which is loaded for every read anyway. Control records between two events
//! belong to the block before them and are verified with it.
//!
//! Positions are dense within a segment, so the events of one block have
//! consecutive positions and the block's offset array finds a record in O(1).
//! The array comes from the offset table, and a block checked whole also
//! walks its chain of records and insists that the two agree.
//!
//! # Concurrency
//!
//! The cache is sharded. A hit takes a shard's read lock and sets a flag; it
//! never reorders a list. A slot is filled once, under its own lock, so
//! concurrent readers that miss on the same block wait for one load instead
//! of each doing it. Eviction is LRU with a second chance for slots that were
//! hit.
//!
//! A segment file that is deleted or rewritten (a follower's truncation) gets
//! a new identity in the cache, so blocks of the old file are never served
//! for the new one.

use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use lru::LruCache;
use parking_lot::{Mutex, RwLock};

use crate::error::Error;
use crate::event::StoredEvent;
use crate::segment::crc::{self, Span};
use crate::segment::format;
use crate::segment::query::Locator;
use crate::segment::reader::StoredRecord;
use crate::segment::{RECORD_HEADER_SIZE, flags};

/// Default block size. Small enough that a point read does not drag in much
/// it will not use, large enough that a sequential read is a few syscalls
/// per megabyte.
pub const DEFAULT_BLOCK_SIZE: usize = 16 * 1024;
/// Default memory budget of the block cache.
pub const DEFAULT_BLOCK_CACHE_BYTES: usize = 256 * 1024 * 1024;
/// Default number of blocks read ahead of a sequential reader (256 KiB).
pub const DEFAULT_READAHEAD_BLOCKS: usize = 16;

/// How sealed segments are read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadMode {
    /// Through the verified block cache: bytes are CRC-checked when they come
    /// off the disk and trusted while cached.
    Blocks,
    /// Through a memory mapping, with a CRC check on every read. The kernel
    /// may re-read a mapped page from disk at any time without the process
    /// noticing, so only a check on each read keeps the guarantee.
    #[default]
    Mapped,
}

impl std::str::FromStr for ReadMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s {
            "blocks" => Ok(ReadMode::Blocks),
            "mapped" => Ok(ReadMode::Mapped),
            other => Err(format!(
                "unknown read path {other:?}: use \"blocks\" or \"mapped\""
            )),
        }
    }
}

const SHARDS: usize = 16;
const ORD: Ordering = Ordering::Relaxed;
/// Rough per-block bookkeeping cost counted against the budget on top of
/// the data and the offset array.
const BLOCK_OVERHEAD: usize = 128;

/// An open sealed segment file.
pub struct SealedFile {
    file: File,
    len: u64,
    /// Identity of this segment incarnation in the cache. Blocks are keyed
    /// by it, so a segment that is deleted and recreated under the same base
    /// (a follower's truncation) can never be answered from stale blocks.
    id: u64,
    base: u64,
    /// Last block this file was asked for; a request for the next one marks
    /// a sequential reader.
    last_block: AtomicU64,
    /// Where the last event record ends. The file may run on past it: a
    /// segment sealed by a crash between creating its successor and trimming
    /// it is still its preallocated length.
    data_end: OnceLock<u64>,
}

impl SealedFile {
    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// A run of whole records as they were read from the file.
///
/// A record is CRC-checked before it is served, once per load of its block:
/// a block loaded for a sequential read is checked whole when it comes in,
/// one loaded for a point read checks the records that are asked for, as
/// they are asked for. Either way no byte leaves the cache that was not
/// verified after the read that put it there.
pub struct Block {
    /// File offset of the first record.
    start: u64,
    /// File offset one past the last record.
    end: u64,
    /// Position of the first event record in the block.
    first_pos: u64,
    data: Vec<u8>,
    /// Offset of each event record's header, relative to `start`.
    offs: Vec<u32>,
    /// One bit per event record: already checked. Empty when the whole block
    /// was checked at load.
    verified: Vec<AtomicU64>,
}

impl Block {
    fn contains(&self, offset: u64) -> bool {
        offset >= self.start && offset < self.end
    }

    fn heap_bytes(&self) -> usize {
        self.data.capacity()
            + self.offs.capacity() * 4
            + self.verified.capacity() * 8
            + BLOCK_OVERHEAD
    }

    /// The event record for `position`, which the segment's offset table
    /// says starts at `offset`.
    fn record(
        &self,
        position: u64,
        offset: u64,
        counter: &AtomicU64,
    ) -> Result<StoredRecord<'_>, Error> {
        let index = position
            .checked_sub(self.first_pos)
            .map(|i| i as usize)
            .filter(|&i| i < self.offs.len())
            .ok_or_else(|| Error::Corrupted {
                message: format!("position {position} is not in the block at offset {offset}"),
            })?;
        let rel = self.offs[index] as usize;
        if self.start + rel as u64 != offset {
            return Err(Error::Corrupted {
                message: format!(
                    "segment index puts position {position} at offset {offset}, \
                     the segment has it at {}",
                    self.start + rel as u64
                ),
            });
        }
        let at = self.start + rel as u64;
        if !self.verified.is_empty() {
            let word = &self.verified[index / 64];
            let bit = 1u64 << (index % 64);
            if word.load(ORD) & bit == 0 {
                self.verify_record(rel, at, position)?;
                counter.fetch_add(1, ORD);
                word.fetch_or(bit, ORD);
            }
        }
        // Header and body are in bounds: this record was checked, here or
        // when the block was loaded.
        let flags = self.data[rel + 8];
        let len = u32::from_le_bytes(self.data[rel + 4..rel + 8].try_into().unwrap()) as usize;
        let body_end = rel + 8 + len;
        Ok(StoredRecord {
            flags,
            body: &self.data[rel + RECORD_HEADER_SIZE..body_end],
            framed: &self.data[rel + 4..body_end],
        })
    }

    /// Checks the one record at `rel`: it is in bounds, an event, holds the
    /// position the index promises, and its CRC holds.
    fn verify_record(&self, rel: usize, at: u64, position: u64) -> Result<(), Error> {
        let data = &self.data;
        if rel + RECORD_HEADER_SIZE > data.len() {
            return Err(Error::Corrupted {
                message: format!("record header at offset {at} extends beyond block"),
            });
        }
        let stored_crc = u32::from_le_bytes(data[rel..rel + 4].try_into().unwrap());
        let record_len = u32::from_le_bytes(data[rel + 4..rel + 8].try_into().unwrap()) as usize;
        let flags_byte = data[rel + 8];
        let end = rel + 8 + record_len;
        if record_len == 0 || end > data.len() {
            return Err(Error::Corrupted {
                message: format!("record at offset {at} extends beyond its block"),
            });
        }
        let body = &data[rel + RECORD_HEADER_SIZE..end];
        if crc::record_crc(flags_byte, body) != stored_crc {
            return Err(Error::Corrupted {
                message: format!("CRC mismatch at offset {at}"),
            });
        }
        if !flags::is_event(flags_byte)
            || body.len() < 8
            || u64::from_le_bytes(body[0..8].try_into().unwrap()) != position
        {
            return Err(Error::Corrupted {
                message: format!("record at offset {at} is not the event at position {position}"),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash)]
struct Key {
    id: u64,
    block: u32,
}

impl Key {
    fn shard(self) -> usize {
        let mixed = (self.id.wrapping_mul(0x9E37_79B9_7F4A_7C15)) ^ (self.block as u64);
        (mixed.wrapping_mul(0xC2B2_AE3D_27D4_EB4F) >> 59) as usize % SHARDS
    }
}

struct Slot {
    block: OnceLock<Arc<Block>>,
    /// Set by a hit. Eviction gives a referenced slot a second chance
    /// instead of every hit reordering the shard's list under a lock.
    referenced: AtomicBool,
    /// Serialises the one load of this slot.
    loading: Mutex<()>,
    /// Bytes counted against the shard for this slot; set under the shard
    /// lock when the block is accounted.
    accounted: AtomicUsize,
}

struct Shard {
    lru: LruCache<Key, Arc<Slot>>,
    bytes: usize,
}

/// Counters, for tests and benchmarks.
#[derive(Debug, Clone, Copy, Default)]
pub struct BlockCacheStats {
    pub loads: u64,
    pub bytes_loaded: u64,
    pub records_verified: u64,
    pub resident_bytes: u64,
}

pub struct BlockCache {
    shards: Vec<RwLock<Shard>>,
    shard_budget: usize,
    block_size: usize,
    readahead: usize,
    /// Cache identity per segment base: bumped away by `invalidate`.
    ids: Mutex<HashMap<u64, u64>>,
    next_id: AtomicU64,
    loads: AtomicU64,
    bytes_loaded: AtomicU64,
    records_verified: AtomicU64,
}

impl BlockCache {
    pub fn new(budget_bytes: usize, block_size: usize, readahead: usize) -> Self {
        let block_size = block_size.clamp(4096, 1 << 30);
        Self {
            shards: (0..SHARDS)
                .map(|_| {
                    RwLock::new(Shard {
                        lru: LruCache::unbounded(),
                        bytes: 0,
                    })
                })
                .collect(),
            shard_budget: (budget_bytes / SHARDS).max(1),
            block_size,
            readahead,
            ids: Mutex::new(HashMap::new()),
            next_id: AtomicU64::new(1),
            loads: AtomicU64::new(0),
            bytes_loaded: AtomicU64::new(0),
            records_verified: AtomicU64::new(0),
        }
    }

    pub fn stats(&self) -> BlockCacheStats {
        BlockCacheStats {
            loads: self.loads.load(ORD),
            bytes_loaded: self.bytes_loaded.load(ORD),
            records_verified: self.records_verified.load(ORD),
            resident_bytes: self.shards.iter().map(|s| s.read().bytes as u64).sum(),
        }
    }

    /// Opens a sealed segment file for block reads.
    pub fn open(&self, path: &Path, base: u64) -> Result<Arc<SealedFile>, Error> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        let id = *self
            .ids
            .lock()
            .entry(base)
            .or_insert_with(|| self.next_id.fetch_add(1, ORD));
        Ok(Arc::new(SealedFile {
            file,
            len,
            id,
            base,
            last_block: AtomicU64::new(u64::MAX - 1),
            data_end: OnceLock::new(),
        }))
    }

    /// Starts a new incarnation of the segment at `base`: blocks cached for
    /// the old one stay in memory until evicted but are never served again.
    pub fn retire(&self, base: u64) {
        self.ids.lock().remove(&base);
    }

    /// Forgets everything cached for the segment at `base`: its file was
    /// deleted or is about to be rewritten.
    pub fn invalidate(&self, base: u64) {
        let Some(id) = self.ids.lock().remove(&base) else {
            return;
        };
        for shard in &self.shards {
            let mut shard = shard.write();
            let doomed: Vec<Key> = shard
                .lru
                .iter()
                .filter(|(key, _)| key.id == id)
                .map(|(key, _)| *key)
                .collect();
            for key in doomed {
                if let Some(slot) = shard.lru.pop(&key) {
                    shard.bytes -= slot.accounted.load(ORD);
                }
            }
        }
    }

    /// A reader over one sealed segment. `index` is the segment's index; its
    /// offset table gives the block boundaries.
    pub fn reader(
        self: &Arc<Self>,
        file: Arc<SealedFile>,
        index: Arc<dyn Locator>,
    ) -> SealedReader {
        SealedReader {
            cache: Arc::clone(self),
            file,
            index,
            current: None,
        }
    }

    fn peek(&self, key: Key) -> bool {
        self.shards[key.shard()]
            .read()
            .lru
            .peek(&key)
            .is_some_and(|slot| slot.block.get().is_some())
    }

    fn get_or_load(
        &self,
        key: Key,
        load: impl FnOnce() -> Result<Block, Error>,
    ) -> Result<Arc<Block>, Error> {
        let shard_lock = &self.shards[key.shard()];
        // A hit takes a read lock and touches no shared list.
        {
            let shard = shard_lock.read();
            if let Some(slot) = shard.lru.peek(&key)
                && let Some(block) = slot.block.get()
            {
                if !slot.referenced.load(ORD) {
                    slot.referenced.store(true, ORD);
                }
                return Ok(Arc::clone(block));
            }
        }
        let slot = {
            let mut shard = shard_lock.write();
            match shard.lru.get(&key) {
                Some(slot) => Arc::clone(slot),
                None => {
                    let slot = Arc::new(Slot {
                        block: OnceLock::new(),
                        referenced: AtomicBool::new(false),
                        loading: Mutex::new(()),
                        accounted: AtomicUsize::new(0),
                    });
                    shard.lru.put(key, Arc::clone(&slot));
                    slot
                }
            }
        };
        let _loading = slot.loading.lock();
        if let Some(block) = slot.block.get() {
            return Ok(Arc::clone(block));
        }
        let block = match load() {
            Ok(block) => Arc::new(block),
            Err(error) => {
                let mut shard = shard_lock.write();
                if shard.lru.peek(&key).is_some_and(|s| Arc::ptr_eq(s, &slot)) {
                    shard.lru.pop(&key);
                }
                return Err(error);
            }
        };
        let size = block.heap_bytes();
        let _ = slot.block.set(Arc::clone(&block));
        let mut shard = shard_lock.write();
        if shard.lru.peek(&key).is_some_and(|s| Arc::ptr_eq(s, &slot)) {
            slot.accounted.store(size, ORD);
            shard.bytes += size;
            // Second chance for slots that were hit since they last moved.
            let mut chances = shard.lru.len() * 2 + 2;
            while shard.bytes > self.shard_budget {
                let Some((victim_key, victim)) = shard.lru.pop_lru() else {
                    break;
                };
                if chances > 0 && victim.referenced.swap(false, ORD) {
                    chances -= 1;
                    shard.lru.put(victim_key, victim);
                    continue;
                }
                shard.bytes -= victim.accounted.load(ORD);
            }
        }
        Ok(block)
    }
}

/// Reads records of one sealed segment through the block cache.
pub struct SealedReader {
    cache: Arc<BlockCache>,
    file: Arc<SealedFile>,
    index: Arc<dyn Locator>,
    current: Option<Arc<Block>>,
}

impl SealedReader {
    /// The record for `position`, which the segment's offset table places at
    /// `offset`. Its CRC was verified when its block was loaded.
    pub fn record_at(&mut self, position: u64, offset: u64) -> Result<StoredRecord<'_>, Error> {
        let covered = matches!(&self.current, Some(block) if block.contains(offset));
        if !covered {
            self.current = Some(self.fetch(offset)?);
        }
        self.current.as_ref().expect("set above").record(
            position,
            offset,
            &self.cache.records_verified,
        )
    }

    pub fn read_event_at(&mut self, position: u64, offset: u64) -> Result<StoredEvent, Error> {
        let record = self.record_at(position, offset)?;
        let (event, _) = format::deserialize_event(record.body)?;
        Ok(event)
    }

    fn key(&self, block: u64) -> Key {
        Key {
            id: self.file.id,
            block: block as u32,
        }
    }

    fn fetch(&self, offset: u64) -> Result<Arc<Block>, Error> {
        let k = offset / self.cache.block_size as u64;
        let previous = self.file.last_block.swap(k, ORD);
        let sequential = k == previous.wrapping_add(1);
        let block = self
            .cache
            .get_or_load(self.key(k), || self.load(k, sequential))?;
        if sequential && self.cache.readahead > 0 {
            self.read_ahead(k);
        }
        Ok(block)
    }

    /// A sequential reader is about to want the blocks after `k`: load the
    /// ones that are missing now, while this thread is here anyway.
    fn read_ahead(&self, k: u64) {
        if self.cache.peek(self.key(k + 1)) {
            return;
        }
        for next in k + 1..=k + self.cache.readahead as u64 {
            if self.cache.peek(self.key(next)) {
                continue;
            }
            // A failure here (a stride no record starts in, the end of the
            // segment, damage) surfaces, if it is real, when the reader
            // reaches the block.
            let _ = self
                .cache
                .get_or_load(self.key(next), || self.load(next, true));
        }
    }

    /// The offset of the `i`th event's record, from the segment's index.
    fn offset(&self, i: usize) -> Result<u64, Error> {
        self.index
            .offset_of(self.index.base_position() + i as u64)?
            .ok_or_else(|| Error::Corrupted {
                message: format!("segment {} index has no event {i}", self.file.base),
            })
    }

    /// The first event in `[lo, n)` whose record starts at or past `bound`:
    /// a binary search of the offset table.
    fn first_at_or_past(&self, bound: u64, mut lo: usize, mut hi: usize) -> Result<usize, Error> {
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.offset(mid)? < bound {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        Ok(lo)
    }

    /// Where the segment's last event record ends.
    fn data_end(&self, events: usize) -> Result<u64, Error> {
        if let Some(&end) = self.file.data_end.get() {
            return Ok(end);
        }
        let Some(last) = events.checked_sub(1) else {
            return Err(Error::Corrupted {
                message: format!("segment {} index lists no events", self.file.base),
            });
        };
        let last = self.offset(last)?;
        let header =
            read_exact_at(&self.file.file, last, RECORD_HEADER_SIZE).map_err(|e| {
                match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => Error::Corrupted {
                        message: format!(
                            "segment {} is shorter than its index says (last record at {last})",
                            self.file.base
                        ),
                    },
                    _ => Error::from(e),
                }
            })?;
        let record_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as u64;
        let end = last + 8 + record_len;
        if record_len == 0 || end > self.file.len {
            return Err(Error::Corrupted {
                message: format!(
                    "segment {} ends its last record at {end}, past its {} bytes",
                    self.file.base, self.file.len
                ),
            });
        }
        let _ = self.file.data_end.set(end);
        Ok(end)
    }

    /// Reads block `k` from the file. `whole` checks every record now (the
    /// block is about to be read through); otherwise records are checked as
    /// they are served.
    fn load(&self, k: u64, whole: bool) -> Result<Block, Error> {
        let block_size = self.cache.block_size as u64;
        let events = self.index.event_count() as usize;
        let lo = self.first_at_or_past(k * block_size, 0, events)?;
        if lo >= events {
            return Err(Error::Corrupted {
                message: format!(
                    "segment {} has no record in block {k}; its index and file disagree",
                    self.file.base
                ),
            });
        }
        let hi = self.first_at_or_past((k + 1) * block_size, lo, events)?;
        let offsets = (lo..hi)
            .map(|i| self.offset(i))
            .collect::<Result<Vec<u64>, Error>>()?;
        let last = hi >= events;
        let start = offsets[0];
        let end = if last {
            self.data_end(events)?
        } else {
            self.offset(hi)?
        };
        if end <= start || end > self.file.len || end - start > u64::from(u32::MAX) {
            return Err(Error::Corrupted {
                message: format!(
                    "segment {} index points past its file (block {k}: {start}..{end} of {})",
                    self.file.base, self.file.len
                ),
            });
        }

        let mut data =
            read_exact_at(&self.file.file, start, (end - start) as usize).map_err(|e| {
                match e.kind() {
                    std::io::ErrorKind::UnexpectedEof => Error::Corrupted {
                        message: format!(
                            "segment {} is shorter than its index says (block {k})",
                            self.file.base
                        ),
                    },
                    _ => Error::from(e),
                }
            })?;

        let first_pos = self.index.base_position() + lo as u64;
        let (offs, verified) = if whole {
            let offs = verify_block(&mut data, start, first_pos, last)?;
            // The offset table must agree with the chain that was just
            // verified, record for record: it is what later reads trust.
            if offs.len() != hi - lo
                || offs
                    .iter()
                    .zip(&offsets)
                    .any(|(&rel, &table)| start + u64::from(rel) != table)
            {
                return Err(Error::Corrupted {
                    message: format!(
                        "segment {} offset table disagrees with its records in block {k}",
                        self.file.base
                    ),
                });
            }
            self.cache
                .records_verified
                .fetch_add(offs.len() as u64, ORD);
            (offs, Vec::new())
        } else {
            let offs: Vec<u32> = offsets.iter().map(|&o| (o - start) as u32).collect();
            let words = offs.len().div_ceil(64);
            (offs, (0..words).map(|_| AtomicU64::new(0)).collect())
        };
        let end = start + data.len() as u64;
        self.cache.loads.fetch_add(1, ORD);
        self.cache.bytes_loaded.fetch_add(data.len() as u64, ORD);
        data.shrink_to_fit();
        Ok(Block {
            start,
            end,
            first_pos,
            data,
            offs,
            verified,
        })
    }
}

/// Walks the chain of records in `data`, checking every record's CRC and the
/// position of every event, and returns each event record's offset.
///
/// A zero record length ends the chain only in the last block of a segment,
/// where an old, untrimmed file may carry an unwritten tail; it is cut off
/// from `data`.
fn verify_block(
    data: &mut Vec<u8>,
    start: u64,
    first_pos: u64,
    last: bool,
) -> Result<Vec<u32>, Error> {
    // First the chain: where every record starts and ends.
    let mut spans: Vec<Span> = Vec::with_capacity(data.len() / 256 + 1);
    let mut heads: Vec<u32> = Vec::with_capacity(spans.capacity());
    let mut off = 0usize;
    while off < data.len() {
        if off + RECORD_HEADER_SIZE > data.len() {
            return Err(Error::Corrupted {
                message: format!(
                    "record header at offset {} extends beyond block",
                    start + off as u64
                ),
            });
        }
        let record_len = u32::from_le_bytes(data[off + 4..off + 8].try_into().unwrap()) as usize;
        if record_len == 0 {
            if last {
                data.truncate(off);
                break;
            }
            return Err(Error::Corrupted {
                message: format!("zero-length record at offset {}", start + off as u64),
            });
        }
        let end = off + 8 + record_len;
        if end > data.len() {
            return Err(Error::Corrupted {
                message: format!(
                    "record at offset {} extends beyond its block",
                    start + off as u64
                ),
            });
        }
        spans.push(Span {
            flags: data[off + 8],
            body_start: off + RECORD_HEADER_SIZE,
            body_end: end,
            stored: u32::from_le_bytes(data[off..off + 4].try_into().unwrap()),
        });
        heads.push(off as u32);
        off = end;
    }

    // Then every record's checksum, several at a time.
    if let Some(bad) = crc::first_mismatch(data, &spans) {
        return Err(Error::Corrupted {
            message: format!("CRC mismatch at offset {}", start + u64::from(heads[bad])),
        });
    }

    // And the events: consecutive positions, as the index assumes.
    let mut offs = Vec::with_capacity(spans.len());
    let mut next_pos = first_pos;
    for (span, &head) in spans.iter().zip(&heads) {
        if !flags::is_event(span.flags) {
            continue;
        }
        let at = start + u64::from(head);
        let body = &data[span.body_start..span.body_end];
        if body.len() < 8 {
            return Err(Error::Corrupted {
                message: format!("event record at offset {at} has no position"),
            });
        }
        let position = u64::from_le_bytes(body[0..8].try_into().unwrap());
        if position != next_pos {
            return Err(Error::Corrupted {
                message: format!(
                    "event at offset {at} holds position {position}, expected {next_pos}"
                ),
            });
        }
        next_pos += 1;
        offs.push(head);
    }
    Ok(offs)
}

/// `pread` into a fresh buffer without zeroing it first.
#[cfg(unix)]
fn read_exact_at(file: &File, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
    use std::os::unix::io::AsRawFd;

    let mut buf: Vec<u8> = Vec::with_capacity(len);
    let fd = file.as_raw_fd();
    let mut done = 0usize;
    while done < len {
        // SAFETY: the destination is the spare capacity of `buf`, `len -
        // done` bytes of it, and `pread` writes at most that many.
        let n = unsafe {
            libc::pread(
                fd,
                buf.as_mut_ptr().add(done).cast(),
                len - done,
                (offset + done as u64) as libc::off_t,
            )
        };
        if n < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() == std::io::ErrorKind::Interrupted {
                continue;
            }
            return Err(error);
        }
        if n == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        done += n as usize;
    }
    // SAFETY: `pread` initialised the first `len` bytes.
    unsafe { buf.set_len(len) };
    Ok(buf)
}

#[cfg(windows)]
fn read_exact_at(file: &File, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
    use std::os::windows::fs::FileExt;

    let mut buf = vec![0u8; len];
    let mut done = 0usize;
    while done < len {
        let n = file.seek_read(&mut buf[done..], offset + done as u64)?;
        if n == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
        done += n;
    }
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{AppendEvent, Position, Tag};
    use crate::segment::format::ControlRecord;
    use crate::segment::reader::SegmentReader;
    use crate::segment::segment_index::SegmentIndex;
    use crate::segment::writer::SegmentWriter;

    fn event(i: u64) -> AppendEvent {
        AppendEvent {
            identifier: format!("id-{i}"),
            name: format!("Type{}", i % 5),
            version: "1".into(),
            timestamp: 1_700_000_000_000 + i as i64,
            // Sizes vary so block boundaries land in different places.
            payload: vec![(i % 251) as u8; 20 + (i as usize * 37) % 300],
            metadata: vec![],
            tags: vec![Tag::from_str("n", &i.to_string())],
        }
    }

    struct Fixture {
        dir: tempfile::TempDir,
        path: std::path::PathBuf,
        index: Arc<SegmentIndex>,
        events: u64,
    }

    /// A sealed segment of `events` events with a control record after
    /// every 40th, and its index. `trim` cuts the preallocated tail off like
    /// a rotation does.
    fn fixture(events: u64, trim: bool) -> Fixture {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::new(dir.path(), Position(0), 4 << 20).unwrap();
        for i in 0..events {
            writer.append(&[event(i)]).unwrap();
            if i % 40 == 39 {
                writer
                    .write_control(&ControlRecord::WatermarkCheckpoint {
                        epoch: 1,
                        position: i,
                    })
                    .unwrap();
            }
        }
        let path = writer.active_segment_path();
        let len = writer.write_offset();
        drop(writer);
        if trim {
            std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_len(len)
                .unwrap();
        }
        let index = Arc::new(SegmentIndex::build_from_segment(&path).unwrap());
        Fixture {
            dir,
            path,
            index,
            events,
        }
    }

    fn reader(cache: &Arc<BlockCache>, f: &Fixture) -> SealedReader {
        let file = cache.open(&f.path, 0).unwrap();
        cache.reader(file, f.index.clone())
    }

    fn offset_of(f: &Fixture, pos: u64) -> u64 {
        f.index.get_offset(pos).unwrap()
    }

    #[test]
    fn every_event_reads_back_through_small_blocks() {
        for trim in [true, false] {
            let f = fixture(1500, trim);
            let cache = Arc::new(BlockCache::new(64 << 20, 4096, 2));
            let mut sealed = reader(&cache, &f);
            let mapped = SegmentReader::open(&f.path).unwrap();
            let expected: Vec<_> = mapped.iter(None).collect::<Result<_, _>>().unwrap();
            assert_eq!(expected.len() as u64, f.events);
            // Forward, then backward, so both the sequential and the random
            // access paths are used.
            let order: Vec<u64> = (0..f.events).chain((0..f.events).rev()).collect();
            for pos in order {
                let got = sealed.read_event_at(pos, offset_of(&f, pos)).unwrap();
                let want = &expected[pos as usize];
                assert_eq!(got.position, want.position, "position {pos}");
                assert_eq!(got.identifier, want.identifier, "position {pos}");
                assert_eq!(got.name, want.name, "position {pos}");
                assert_eq!(got.payload, want.payload, "position {pos}");
                assert_eq!(got.tags, want.tags, "position {pos}");
            }
            let stats = cache.stats();
            assert!(stats.loads > 10, "small blocks were expected: {stats:?}");
            // An untrimmed file's unwritten tail is never read.
            let data_bytes = std::fs::metadata(&f.path)
                .unwrap()
                .len()
                .min(offset_of(&f, f.events - 1) + 1024 * 1024);
            assert!(
                stats.bytes_loaded <= data_bytes,
                "{stats:?} of {data_bytes}"
            );
            assert_eq!(
                stats.records_verified, f.events,
                "each record verified once"
            );
        }
    }

    fn flip_event_byte(f: &Fixture, victim: u64) {
        let at = offset_of(f, victim) + 30;
        let mut bytes = std::fs::read(&f.path).unwrap();
        bytes[at as usize] ^= 0x01;
        std::fs::write(&f.path, &bytes).unwrap();
    }

    fn assert_crc_mismatch(error: Error) {
        assert!(
            matches!(&error, Error::Corrupted { message } if message.contains("CRC mismatch")),
            "got {error:?}"
        );
    }

    /// A block loaded for a point read checks the records it serves, as it
    /// serves them: the damaged record is refused, every time, and the
    /// records beside it are still good.
    #[test]
    fn a_flipped_byte_is_caught_when_its_record_is_served() {
        let f = fixture(1500, true);
        let victim = 700u64;
        flip_event_byte(&f, victim);

        let cache = Arc::new(BlockCache::new(64 << 20, 4096, 0));
        let mut sealed = reader(&cache, &f);
        sealed.read_event_at(10, offset_of(&f, 10)).unwrap();
        for _ in 0..2 {
            let error = sealed
                .record_at(victim, offset_of(&f, victim))
                .map(|_| ())
                .unwrap_err();
            assert_crc_mismatch(error);
        }
        let neighbour = victim + 1;
        assert_eq!(
            offset_of(&f, victim) / 4096,
            offset_of(&f, neighbour) / 4096,
            "the fixture should keep them in one block"
        );
        sealed
            .read_event_at(neighbour, offset_of(&f, neighbour))
            .unwrap();
    }

    /// A block loaded because a reader is going through the segment in order
    /// is checked whole when it comes in: the damaged record's neighbours are
    /// refused with it, and nothing of the block is cached.
    #[test]
    fn a_flipped_byte_is_caught_when_a_sequential_block_loads() {
        let f = fixture(1500, true);
        let victim = 700u64;
        flip_event_byte(&f, victim);

        let cache = Arc::new(BlockCache::new(64 << 20, 4096, 0));
        let mut sealed = reader(&cache, &f);
        let victim_block = offset_of(&f, victim) / 4096;
        // An event of the block just before this one makes the next fetch sequential.
        let before = (0..f.events)
            .find(|&p| offset_of(&f, p) / 4096 == victim_block - 1)
            .unwrap();
        sealed.read_event_at(before, offset_of(&f, before)).unwrap();
        // Ask for an intact record of the damaged block: it is refused,
        // because the block was checked whole and the damage is in it.
        let intact = (0..f.events)
            .find(|&p| p != victim && offset_of(&f, p) / 4096 == victim_block)
            .unwrap();
        let error = sealed
            .record_at(intact, offset_of(&f, intact))
            .map(|_| ())
            .unwrap_err();
        assert_crc_mismatch(error);
        // The damaged record itself never gets through either.
        let error = sealed
            .record_at(victim, offset_of(&f, victim))
            .map(|_| ())
            .unwrap_err();
        assert_crc_mismatch(error);
    }

    /// The guarantee is about bytes coming off the disk. A block that was
    /// verified and is still cached stays served; once it is gone, the next
    /// read verifies the disk's bytes again.
    #[test]
    fn damage_is_caught_when_a_cached_block_is_loaded_again() {
        let f = fixture(300, true);
        let cache = Arc::new(BlockCache::new(64 << 20, 4096, 0));
        let mut sealed = reader(&cache, &f);
        sealed.read_event_at(100, offset_of(&f, 100)).unwrap();

        let at = offset_of(&f, 100) + 30;
        let mut bytes = std::fs::read(&f.path).unwrap();
        bytes[at as usize] ^= 0x80;
        std::fs::write(&f.path, &bytes).unwrap();

        // Still cached: served from the verified copy.
        let mut again = reader(&cache, &f);
        again.read_event_at(100, offset_of(&f, 100)).unwrap();

        // Evicted (the segment was invalidated): read from disk, verified, refused.
        cache.invalidate(0);
        let mut fresh = reader(&cache, &f);
        let error = fresh
            .record_at(100, offset_of(&f, 100))
            .map(|_| ())
            .unwrap_err();
        assert!(matches!(error, Error::Corrupted { .. }), "got {error:?}");
    }

    #[test]
    fn invalidate_drops_blocks_of_a_segment_that_was_rewritten() {
        let f = fixture(300, true);
        let cache = Arc::new(BlockCache::new(64 << 20, 4096, 0));
        let mut sealed = reader(&cache, &f);
        let before = sealed.read_event_at(250, offset_of(&f, 250)).unwrap();

        // The same base comes back with different events past position 200,
        // as after a follower's truncation and re-replication.
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::new(dir.path(), Position(0), 4 << 20).unwrap();
        for i in 0..300u64 {
            let mut e = event(i);
            if i >= 200 {
                e.payload = vec![0xEE; 64];
            }
            writer.append(&[e]).unwrap();
        }
        let new_path = writer.active_segment_path();
        let len = writer.write_offset();
        drop(writer);
        std::fs::OpenOptions::new()
            .write(true)
            .open(&new_path)
            .unwrap()
            .set_len(len)
            .unwrap();
        std::fs::copy(&new_path, &f.path).unwrap();
        let new_index = Arc::new(SegmentIndex::build_from_segment(&f.path).unwrap());

        cache.invalidate(0);
        let file = cache.open(&f.path, 0).unwrap();
        let mut sealed = cache.reader(file, new_index.clone());
        let after = sealed
            .read_event_at(250, new_index.get_offset(250).unwrap())
            .unwrap();
        assert_ne!(before.payload, after.payload);
        assert_eq!(after.payload, vec![0xEE; 64]);
    }

    #[test]
    fn an_index_that_disagrees_with_the_file_is_corrupted_not_served() {
        let f = fixture(200, true);
        let mut skewed = SegmentIndex::new(0);
        for pos in 0..200u64 {
            // One byte off: not a record boundary.
            skewed.insert_event(pos, offset_of(&f, pos) + 1, "T", &[]);
        }
        let cache = Arc::new(BlockCache::new(64 << 20, 4096, 0));
        let file = cache.open(&f.path, 0).unwrap();
        let mut sealed = cache.reader(file, Arc::new(skewed));
        let error = sealed
            .record_at(5, offset_of(&f, 5) + 1)
            .map(|_| ())
            .unwrap_err();
        assert!(matches!(error, Error::Corrupted { .. }), "got {error:?}");
    }

    #[test]
    fn the_budget_bounds_what_stays_resident() {
        let f = fixture(1500, true);
        // Room for about four blocks per shard-share, far less than the file.
        let budget = 16 * 6 * 1024;
        let cache = Arc::new(BlockCache::new(budget, 4096, 0));
        let mut sealed = reader(&cache, &f);
        for round in 0..2 {
            for pos in 0..f.events {
                sealed.read_event_at(pos, offset_of(&f, pos)).unwrap();
            }
            let stats = cache.stats();
            assert!(
                stats.resident_bytes <= budget as u64 + 16 * 8192,
                "round {round}: {stats:?}"
            );
        }
        let stats = cache.stats();
        assert!(
            stats.records_verified > f.events,
            "evicted blocks are verified again on reload: {stats:?}"
        );
    }

    #[test]
    fn concurrent_readers_share_one_load_per_block() {
        let f = fixture(1500, true);
        let cache = Arc::new(BlockCache::new(64 << 20, 4096, 0));
        let file = cache.open(&f.path, 0).unwrap();
        std::thread::scope(|s| {
            for _ in 0..4 {
                s.spawn(|| {
                    let mut sealed = cache.reader(Arc::clone(&file), f.index.clone());
                    for pos in 0..f.events {
                        sealed.read_event_at(pos, offset_of(&f, pos)).unwrap();
                    }
                });
            }
        });
        let blocks: std::collections::BTreeSet<u64> =
            (0..f.events).map(|p| offset_of(&f, p) / 4096).collect();
        assert_eq!(
            cache.stats().loads,
            blocks.len() as u64,
            "one load per block"
        );
        drop(f.dir);
    }
}
