use std::collections::HashMap;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use lru::LruCache;
use memmap2::Mmap;
use parking_lot::{Mutex, RwLock};

use crate::error::Error;
use crate::segment::blocks::{BlockCache, SealedFile, SealedReader};
use crate::segment::query::SegmentKeys;
use crate::segment::sealed_index::SealedIndex;
use crate::segment::segment_index::SegmentIndex;

const ORD: Ordering = Ordering::Relaxed;

/// Default number of sealed segment mmaps to cache.
pub const DEFAULT_MMAP_CACHE_SIZE: usize = 100;

/// Most sealed segment indexes kept mapped at once. A mapping costs address
/// space and one entry in the process's map table, not heap; past this many
/// (a log of tens of thousands of segments) a pseudo-random one is unmapped,
/// and mapped again when next asked for, which costs an open and a few
/// hundred bytes of checks.
pub const MAX_MAPPED_INDEXES: usize = 16_384;

const SHARDS: usize = 16;

type Shard = RwLock<HashMap<u64, Arc<SealedIndex>>>;

/// Mapped `.idx` files of sealed segments, by base position. Every read of
/// every sealed segment asks here first (for its key filter), so the lookup
/// takes a sharded read lock and nothing else.
struct MappedIndexes {
    shards: Vec<Shard>,
    entries: AtomicUsize,
    max_entries: usize,
    victim: AtomicUsize,
}

impl MappedIndexes {
    fn new(max_entries: usize) -> Self {
        Self {
            shards: (0..SHARDS).map(|_| RwLock::new(HashMap::new())).collect(),
            entries: AtomicUsize::new(0),
            max_entries: max_entries.max(1),
            victim: AtomicUsize::new(0),
        }
    }

    fn shard(&self, base: u64) -> &Shard {
        let mixed = base.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 59;
        &self.shards[mixed as usize % SHARDS]
    }

    fn get(&self, base: u64) -> Option<Arc<SealedIndex>> {
        self.shard(base).read().get(&base).cloned()
    }

    fn put(&self, base: u64, index: Arc<SealedIndex>) {
        if self.shard(base).write().insert(base, index).is_none() {
            self.entries.fetch_add(1, ORD);
        }
        while self.entries.load(ORD) > self.max_entries {
            if !self.evict_one(base) {
                break;
            }
        }
    }

    fn evict_one(&self, keep: u64) -> bool {
        let start = self.victim.fetch_add(1, ORD);
        for step in 0..SHARDS {
            let mut shard = self.shards[(start + step) % SHARDS].write();
            if let Some(key) = shard.keys().copied().find(|&key| key != keep) {
                shard.remove(&key);
                self.entries.fetch_sub(1, ORD);
                return true;
            }
        }
        false
    }

    fn pop(&self, base: u64) {
        if self.shard(base).write().remove(&base).is_some() {
            self.entries.fetch_sub(1, ORD);
        }
    }

    fn len(&self) -> usize {
        self.entries.load(ORD)
    }
}

/// Indexes, mappings and open files of sealed segments.
///
/// A sealed segment's index is its mapped `.idx`, looked up in place: no
/// index is deserialized, so there is no heap cache of them to size or to
/// thrash. A segment sealed in memory whose `.idx` is not yet on disk is
/// served from its pinned in-memory index until it is.
pub struct IndexCache {
    mapped: MappedIndexes,
    /// LRU cache of mmap handles for sealed segments, keyed by base position.
    mmaps: Mutex<LruCache<u64, Arc<Mmap>>>,
    /// Open sealed segment files for block reads, keyed by base position.
    sealed_files: Mutex<LruCache<u64, Arc<SealedFile>>>,
    /// Verified blocks of sealed segments.
    blocks: Arc<BlockCache>,
    /// Indexes of segments sealed in memory but not yet on disk. Never
    /// evicted: until the `.idx` exists, this is the only copy, and every
    /// read of the segment resolves through it.
    pinned: Mutex<HashMap<u64, Arc<SegmentIndex>>>,

    // ── Hit/miss counters (lock-free) ──
    /// Index asked for and already mapped (or pinned).
    pub index_hits: AtomicU64,
    /// Index asked for and mapped from disk.
    pub index_misses: AtomicU64,
    pub mmap_hits: AtomicU64,
    pub mmap_misses: AtomicU64,
}

impl IndexCache {
    /// A cache keeping up to `mmap_capacity` segment mappings, with the
    /// default block cache.
    pub fn new(mmap_capacity: usize) -> Self {
        Self::with_blocks(
            mmap_capacity,
            BlockCache::new(
                crate::segment::blocks::DEFAULT_BLOCK_CACHE_BYTES,
                crate::segment::blocks::DEFAULT_BLOCK_SIZE,
                crate::segment::blocks::DEFAULT_READAHEAD_BLOCKS,
            ),
        )
    }

    /// Like `new`, with an explicitly configured block cache.
    pub fn with_blocks(mmap_capacity: usize, blocks: BlockCache) -> Self {
        Self {
            mapped: MappedIndexes::new(MAX_MAPPED_INDEXES),
            mmaps: Mutex::new(LruCache::new(
                NonZeroUsize::new(mmap_capacity.max(1)).unwrap(),
            )),
            sealed_files: Mutex::new(LruCache::new(
                NonZeroUsize::new(mmap_capacity.max(1)).unwrap(),
            )),
            blocks: Arc::new(blocks),
            pinned: Mutex::new(HashMap::new()),
            index_hits: AtomicU64::new(0),
            index_misses: AtomicU64::new(0),
            mmap_hits: AtomicU64::new(0),
            mmap_misses: AtomicU64::new(0),
        }
    }

    /// The index of sealed segment `base`, without touching the disk:
    /// mapped already, or pinned. `None` otherwise.
    pub fn index_cached(&self, base: u64) -> Option<Arc<dyn SegmentKeys>> {
        if let Some(index) = self.mapped.get(base) {
            self.index_hits.fetch_add(1, ORD);
            return Some(index);
        }
        let pinned = self.pinned.lock().get(&base).cloned()?;
        self.index_hits.fetch_add(1, ORD);
        Some(pinned)
    }

    /// The index of sealed segment `base`, mapping its `.idx` if it is not
    /// mapped yet.
    pub fn get_index(&self, segment_path: &Path, base: u64) -> Result<Arc<dyn SegmentKeys>, Error> {
        if let Some(index) = self.index_cached(base) {
            return Ok(index);
        }
        self.index_misses.fetch_add(1, ORD);
        let index = Arc::new(SealedIndex::open(
            &segment_path.with_extension("idx"),
            base,
        )?);
        self.mapped.put(base, Arc::clone(&index));
        Ok(index)
    }

    /// Keeps an index already mapped (at open, where recovery read it).
    pub fn adopt(&self, base: u64, index: Arc<SealedIndex>) {
        self.mapped.put(base, index);
    }

    /// How many sealed segment indexes are mapped.
    pub fn mapped_indexes(&self) -> usize {
        self.mapped.len()
    }

    /// Gets a cached mmap handle for a sealed segment, opening it if not cached.
    /// Sealed segments are immutable, so the mmap can be safely shared.
    pub fn get_mmap(&self, segment_path: &Path, base_position: u64) -> Result<Arc<Mmap>, Error> {
        if let Some(mmap) = self.mmaps.lock().get(&base_position) {
            self.mmap_hits.fetch_add(1, ORD);
            return Ok(Arc::clone(mmap));
        }
        self.mmap_misses.fetch_add(1, ORD);
        let file = std::fs::File::open(segment_path)?;
        let mmap = Arc::new(unsafe { Mmap::map(&file)? });
        self.mmaps.lock().put(base_position, Arc::clone(&mmap));
        Ok(mmap)
    }

    /// A reader over a sealed segment that serves records from the block
    /// cache. `index` must be the segment's own index.
    pub fn sealed_reader(
        &self,
        segment_path: &Path,
        base_position: u64,
        index: Arc<dyn SegmentKeys>,
    ) -> Result<SealedReader, Error> {
        let cached = self.sealed_files.lock().get(&base_position).map(Arc::clone);
        let file = match cached {
            Some(file) => file,
            None => {
                let file = self.blocks.open(segment_path, base_position)?;
                self.sealed_files
                    .lock()
                    .put(base_position, Arc::clone(&file));
                file
            }
        };
        Ok(self.blocks.reader(file, index))
    }

    /// The block cache, for statistics.
    pub fn block_cache(&self) -> &Arc<BlockCache> {
        &self.blocks
    }

    /// Invalidates all cache entries for a segment (e.g., after truncation
    /// deletes or reopens it).
    pub fn invalidate(&self, base_position: u64) {
        self.sealed_files.lock().pop(&base_position);
        self.blocks.invalidate(base_position);
        self.mapped.pop(base_position);
        self.mmaps.lock().pop(&base_position);
        self.pinned.lock().remove(&base_position);
    }

    /// Registers a freshly sealed segment's index before its `.idx` exists.
    /// Idempotent. Reads of the segment resolve through the pinned copy
    /// until `unpin_sealed`.
    pub fn pin_sealed(&self, base_position: u64, index: Arc<SegmentIndex>) {
        // A segment is sealed once per incarnation of its file. Blocks and
        // file handles cached under this base belong to an earlier
        // incarnation (a truncated and re-extended suffix, for one) that a
        // slow reader may have loaded after the truncation invalidated them.
        self.sealed_files.lock().pop(&base_position);
        self.blocks.retire(base_position);
        self.mapped.pop(base_position);
        self.pinned.lock().entry(base_position).or_insert(index);
    }

    /// The `.idx` is on disk: map it, and let go of the in-memory copy.
    pub fn unpin_sealed(&self, base_position: u64, segment_path: &Path) {
        match SealedIndex::open(&segment_path.with_extension("idx"), base_position) {
            Ok(index) => self.mapped.put(base_position, Arc::new(index)),
            // Reads map it themselves, and report what is wrong with it.
            Err(error) => tracing::warn!(
                base = base_position,
                %error,
                "could not map a sealed segment's index just written"
            ),
        }
        self.pinned.lock().remove(&base_position);
    }

    /// Whether the segment's index is still pinned (seal not yet on disk).
    pub fn is_pinned(&self, base_position: u64) -> bool {
        self.pinned.lock().contains_key(&base_position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::criteria::{Criterion, SourcingCondition};
    use crate::event::{AppendEvent, Position, Tag};
    use crate::segment::DEFAULT_SEGMENT_SIZE;
    use crate::segment::query::Probe;
    use crate::segment::writer::SegmentWriter;

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

    fn setup_sealed_segment(dir: &Path) -> (std::path::PathBuf, u64) {
        let mut writer = SegmentWriter::new(dir, Position(1), DEFAULT_SEGMENT_SIZE).unwrap();
        writer
            .append(&[
                make_event("OrderPlaced", vec![Tag::from_str("orderId", "A")]),
                make_event(
                    "PaymentReceived",
                    vec![
                        Tag::from_str("orderId", "A"),
                        Tag::from_str("paymentId", "P1"),
                    ],
                ),
            ])
            .unwrap();
        let seg_path = writer.active_segment_path();
        drop(writer);
        SegmentIndex::build_from_segment(&seg_path)
            .unwrap()
            .write_to_disk(&seg_path)
            .unwrap();
        (seg_path, 1)
    }

    fn probe_for(value: &str) -> Probe {
        Probe::new(&SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![Tag::from_str("orderId", value)],
            }],
        })
    }

    #[test]
    fn maps_once_and_answers_from_the_mapping() {
        let dir = tempfile::tempdir().unwrap();
        let (path, base) = setup_sealed_segment(dir.path());
        let cache = IndexCache::new(10);
        let index = cache.get_index(&path, base).unwrap();
        assert!(index.might_match(&probe_for("A")).unwrap());
        assert!(!index.might_match(&probe_for("NONEXISTENT")).unwrap());
        let positions: Vec<u64> = index
            .matching(&probe_for("A"))
            .unwrap()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(positions, vec![1, 2]);
        assert_eq!(cache.index_misses.load(ORD), 1);
        let again = cache.get_index(&path, base).unwrap();
        assert!(Arc::ptr_eq(&index, &again));
        assert_eq!(cache.index_misses.load(ORD), 1);
    }

    #[test]
    fn invalidate_unmaps() {
        let dir = tempfile::tempdir().unwrap();
        let (path, base) = setup_sealed_segment(dir.path());
        let cache = IndexCache::new(10);
        let first = cache.get_index(&path, base).unwrap();
        cache.invalidate(base);
        assert!(cache.index_cached(base).is_none());
        let second = cache.get_index(&path, base).unwrap();
        assert!(!Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn pinned_until_unpinned_then_mapped() {
        let dir = tempfile::tempdir().unwrap();
        let (path, base) = setup_sealed_segment(dir.path());
        let cache = IndexCache::new(10);
        let pinned = Arc::new(SegmentIndex::build_from_segment(&path).unwrap());
        cache.pin_sealed(base, Arc::clone(&pinned));
        assert!(cache.is_pinned(base));
        assert!(cache.index_cached(base).is_some());
        cache.unpin_sealed(base, &path);
        assert!(!cache.is_pinned(base));
        assert_eq!(cache.mapped_indexes(), 1);
        // Served from the mapping now, with no further open.
        let index = cache.get_index(&path, base).unwrap();
        assert_eq!(index.event_count(), 2);
        assert_eq!(cache.index_misses.load(ORD), 0);
    }

    #[test]
    fn the_mapping_count_is_capped_and_keeps_the_newest() {
        let mapped = MappedIndexes::new(3);
        let dir = tempfile::tempdir().unwrap();
        let (path, base) = setup_sealed_segment(dir.path());
        let index = Arc::new(SealedIndex::open(&path.with_extension("idx"), base).unwrap());
        for key in 0..50u64 {
            mapped.put(key, Arc::clone(&index));
            assert!(mapped.get(key).is_some(), "newest entry {key} kept");
            assert!(mapped.len() <= 3);
        }
        mapped.pop(49);
        assert_eq!(mapped.len(), 2);
    }
}
