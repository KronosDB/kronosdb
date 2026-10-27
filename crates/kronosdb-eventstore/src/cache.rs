use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use growable_bloom_filter::GrowableBloom;
use lru::LruCache;
use memmap2::Mmap;
use parking_lot::Mutex;

use crate::criteria::SourcingCondition;
use crate::error::Error;
use crate::segment::blocks::{BlockCache, SealedFile, SealedReader};
use crate::segment::segment_index::SegmentIndex;

const ORD: Ordering = Ordering::Relaxed;

/// A cache bounded by entry count and by the bytes its entries weigh, that
/// evicts a pseudo-random entry when over budget.
///
/// Weights are estimates the caller supplies; the budget is what makes a
/// cache of segment indexes mean "this much memory" regardless of whether
/// the segments are 32 MB or 256 MB.
///
/// Recency is the wrong signal here, for the same reason as in the bloom
/// cache: the usual way to need more segments than fit is a pass over many of
/// them in order (a replay, an aggregate whose events span the log), and
/// under LRU every segment of such a pass is evicted just before it is needed
/// again, so a log one segment larger than the budget misses on every read.
/// Evicting at random turns that cliff into a slope: when the working set is
/// 10% over the budget about 80% of a repeated pass still hits.
struct ByteCache<T> {
    entries: std::collections::HashMap<u64, (Arc<T>, usize)>,
    bytes: usize,
    max_bytes: usize,
    max_entries: usize,
    /// xorshift state.
    rng: u64,
}

impl<T> ByteCache<T> {
    fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: std::collections::HashMap::new(),
            bytes: 0,
            max_bytes,
            max_entries: if max_entries == 0 {
                usize::MAX
            } else {
                max_entries
            },
            rng: 0x9E37_79B9_7F4A_7C15,
        }
    }

    fn get(&self, key: &u64) -> Option<Arc<T>> {
        self.entries.get(key).map(|(value, _)| Arc::clone(value))
    }

    fn put(&mut self, key: u64, value: Arc<T>, weight: usize) {
        if let Some((_, old)) = self.entries.insert(key, (value, weight)) {
            self.bytes -= old;
        }
        self.bytes += weight;
        // The newest entry is kept even when it alone exceeds the budget: a
        // reader is about to use it.
        while self.entries.len() > 1
            && (self.entries.len() > self.max_entries || self.bytes > self.max_bytes)
        {
            self.evict_one(key);
        }
    }

    /// Drops a pseudo-random entry other than `keep`.
    fn evict_one(&mut self, keep: u64) {
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        let others = self.entries.len() - 1;
        let victim = self
            .entries
            .keys()
            .copied()
            .filter(|&key| key != keep)
            .nth(self.rng as usize % others)
            .expect("more than one entry");
        self.pop(&victim);
    }

    fn pop(&mut self, key: &u64) {
        if let Some((_, weight)) = self.entries.remove(key) {
            self.bytes -= weight;
        }
    }
}

/// Default heap budget for cached segment indexes.
pub const DEFAULT_INDEX_CACHE_BYTES: usize = 1 << 30;
/// Default heap budget for cached bloom filters.
pub const DEFAULT_BLOOM_CACHE_BYTES: usize = 128 << 20;

/// The offset-table cache gets this fraction of the index budget, and at
/// least `OFFSETS_MIN_BYTES`: a table is 8 bytes per event, roughly 1% of
/// the segment it describes.
const OFFSETS_BUDGET_DIVISOR: usize = 16;
const OFFSETS_MIN_BYTES: usize = 8 << 20;

/// Default number of sealed segment mmaps to cache.
pub const DEFAULT_MMAP_CACHE_SIZE: usize = 100;

const BLOOM_SHARDS: usize = 16;

type BloomShard = parking_lot::RwLock<std::collections::HashMap<u64, (Arc<GrowableBloom>, usize)>>;

/// Bloom filters of sealed segments, by base position.
///
/// Every read of every sealed segment asks its filter first, so the lookup
/// is the hottest one in the cache: sharded read locks, no recency list to
/// update, and the filter is evaluated under the read lock rather than
/// cloned out. Over budget, a pseudo-random entry goes. A query touches
/// every segment's filter in turn, which makes recency the wrong signal:
/// when the filters do not all fit, LRU would miss on every one of them.
struct BloomCache {
    shards: Vec<BloomShard>,
    bytes: std::sync::atomic::AtomicUsize,
    entries: std::sync::atomic::AtomicUsize,
    max_bytes: usize,
    max_entries: usize,
    victim: std::sync::atomic::AtomicUsize,
}

impl BloomCache {
    fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            shards: (0..BLOOM_SHARDS)
                .map(|_| parking_lot::RwLock::new(std::collections::HashMap::new()))
                .collect(),
            bytes: std::sync::atomic::AtomicUsize::new(0),
            entries: std::sync::atomic::AtomicUsize::new(0),
            max_bytes,
            max_entries: if max_entries == 0 {
                usize::MAX
            } else {
                max_entries
            },
            victim: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    fn shard(
        &self,
        base: u64,
    ) -> &parking_lot::RwLock<std::collections::HashMap<u64, (Arc<GrowableBloom>, usize)>> {
        let mixed = base.wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 59;
        &self.shards[mixed as usize % BLOOM_SHARDS]
    }

    /// Runs `f` on the filter for `base`, if cached.
    fn with<R>(&self, base: u64, f: impl FnOnce(&GrowableBloom) -> R) -> Option<R> {
        let shard = self.shard(base).read();
        shard.get(&base).map(|(bloom, _)| f(bloom))
    }

    fn put(&self, base: u64, bloom: Arc<GrowableBloom>, weight: usize) {
        let replaced = self.shard(base).write().insert(base, (bloom, weight));
        match replaced {
            Some((_, old)) => {
                self.bytes.fetch_sub(old, ORD);
            }
            None => {
                self.entries.fetch_add(1, ORD);
            }
        }
        self.bytes.fetch_add(weight, ORD);
        while self.entries.load(ORD) > 1
            && (self.entries.load(ORD) > self.max_entries || self.bytes.load(ORD) > self.max_bytes)
        {
            if !self.evict_one(base) {
                break;
            }
        }
    }

    fn evict_one(&self, keep: u64) -> bool {
        let start = self.victim.fetch_add(1, ORD);
        for step in 0..BLOOM_SHARDS {
            let mut shard = self.shards[(start + step) % BLOOM_SHARDS].write();
            if let Some(key) = shard.keys().copied().find(|&key| key != keep)
                && let Some((_, weight)) = shard.remove(&key)
            {
                self.bytes.fetch_sub(weight, ORD);
                self.entries.fetch_sub(1, ORD);
                return true;
            }
        }
        false
    }

    fn pop(&self, base: u64) {
        if let Some((_, weight)) = self.shard(base).write().remove(&base) {
            self.bytes.fetch_sub(weight, ORD);
            self.entries.fetch_sub(1, ORD);
        }
    }
}

/// A sourcing condition prepared for bloom filter checks: the filter keys of
/// its criteria, built once per query instead of once per segment.
pub struct BloomProbe {
    criteria: Vec<ProbeCriterion>,
}

struct ProbeCriterion {
    names: Vec<Vec<u8>>,
    tags: Vec<Vec<u8>>,
}

impl BloomProbe {
    pub fn new(condition: &SourcingCondition) -> Self {
        Self {
            criteria: condition
                .criteria
                .iter()
                .map(|criterion| ProbeCriterion {
                    names: criterion
                        .names
                        .iter()
                        .map(|name| make_bloom_key(b"__kronosdb_event_type__", name.as_bytes()))
                        .collect(),
                    tags: criterion
                        .tags
                        .iter()
                        .map(|tag| make_bloom_key(&tag.key, &tag.value))
                        .collect(),
                })
                .collect(),
        }
    }

    /// Whether any criterion might have a match in a segment with this filter.
    fn might_match(&self, bloom: &GrowableBloom) -> bool {
        self.criteria.iter().any(|criterion| {
            (criterion.names.is_empty() || criterion.names.iter().any(|key| bloom.contains(key)))
                && criterion.tags.iter().all(|key| bloom.contains(key))
        })
    }
}

/// Cache for sealed segment indices, bloom filters, and mmap handles.
///
/// Indices (`.idx` files) are the heavy objects — each can be several MB.
/// Bloom filters (`.bloom` files) are tiny (~1KB) so we cache more of them.
/// Mmap handles avoid repeated open()/mmap() syscalls for sealed segments.
///
/// Thread-safe via internal Mutex. The lock is only held during cache
/// lookups/inserts, not during file I/O — we load outside the lock
/// and insert after.
pub struct IndexCache {
    /// LRU cache of loaded segment indices, keyed by segment base position.
    indices: Mutex<ByteCache<SegmentIndex>>,
    /// Segment indexes loaded without their tag bitmaps: the position→offset
    /// table, which is all a read that takes every event needs, at a few
    /// percent of an index's memory and load time. Only `get_offsets` hands
    /// these out.
    offsets: Mutex<ByteCache<SegmentIndex>>,
    /// LRU cache of bloom filters, keyed by segment base position. Shared
    /// by `Arc` so a check evaluates the filter after releasing the lock:
    /// every read of every sealed segment passes through here.
    blooms: BloomCache,
    /// LRU cache of mmap handles for sealed segments, keyed by base position.
    mmaps: Mutex<LruCache<u64, Arc<Mmap>>>,
    /// Open sealed segment files for block reads, keyed by base position.
    sealed_files: Mutex<LruCache<u64, Arc<SealedFile>>>,
    /// Verified blocks of sealed segments.
    blocks: Arc<BlockCache>,
    /// Indexes of segments sealed in memory but not yet on disk. Never
    /// evicted: until the `.idx`/`.bloom` companions exist, this is the only
    /// copy, and every read of the segment resolves through it.
    pinned: Mutex<std::collections::HashMap<u64, Arc<SegmentIndex>>>,

    // ── Hit/miss counters (lock-free) ──
    pub index_hits: AtomicU64,
    pub index_misses: AtomicU64,
    pub offsets_hits: AtomicU64,
    pub offsets_misses: AtomicU64,
    pub mmap_hits: AtomicU64,
    pub mmap_misses: AtomicU64,
}

impl IndexCache {
    /// Creates a new cache with the given capacities.
    ///
    /// `index_capacity`: max number of `.idx` files to keep in memory.
    /// `bloom_capacity`: max number of `.bloom` files to keep in memory.
    pub fn new(index_capacity: usize, bloom_capacity: usize, mmap_capacity: usize) -> Self {
        Self::with_blocks(
            index_capacity,
            bloom_capacity,
            mmap_capacity,
            BlockCache::new(
                crate::segment::blocks::DEFAULT_BLOCK_CACHE_BYTES,
                crate::segment::blocks::DEFAULT_BLOCK_SIZE,
                crate::segment::blocks::DEFAULT_READAHEAD_BLOCKS,
            ),
        )
    }

    /// Bounds the cached indexes and bloom filters by estimated heap bytes,
    /// on top of their entry counts. Call before the cache is shared.
    pub fn with_byte_budgets(self, index_bytes: usize, bloom_bytes: usize) -> Self {
        self.indices.lock().max_bytes = index_bytes;
        self.offsets.lock().max_bytes =
            (index_bytes / OFFSETS_BUDGET_DIVISOR).max(OFFSETS_MIN_BYTES);
        let mut this = self;
        this.blooms.max_bytes = bloom_bytes;
        this
    }

    /// Estimated heap bytes held by cached (not pinned) indexes and bloom
    /// filters.
    pub fn resident_bytes(&self) -> (usize, usize) {
        (self.indices.lock().bytes, self.blooms.bytes.load(ORD))
    }

    /// Like `new`, with an explicitly configured block cache.
    pub fn with_blocks(
        index_capacity: usize,
        bloom_capacity: usize,
        mmap_capacity: usize,
        blocks: BlockCache,
    ) -> Self {
        Self {
            indices: Mutex::new(ByteCache::new(index_capacity, usize::MAX)),
            offsets: Mutex::new(ByteCache::new(0, OFFSETS_MIN_BYTES)),
            blooms: BloomCache::new(bloom_capacity, usize::MAX),
            mmaps: Mutex::new(LruCache::new(
                NonZeroUsize::new(mmap_capacity.max(1)).unwrap(),
            )),
            sealed_files: Mutex::new(LruCache::new(
                NonZeroUsize::new(mmap_capacity.max(1)).unwrap(),
            )),
            blocks: Arc::new(blocks),
            pinned: Mutex::new(std::collections::HashMap::new()),
            index_hits: AtomicU64::new(0),
            index_misses: AtomicU64::new(0),
            offsets_hits: AtomicU64::new(0),
            offsets_misses: AtomicU64::new(0),
            mmap_hits: AtomicU64::new(0),
            mmap_misses: AtomicU64::new(0),
        }
    }

    /// Checks the bloom filter for a segment to see if a sourcing condition
    /// might match any events in that segment.
    ///
    /// Returns:
    /// - `Some(true)` if the bloom filter says "maybe" (need to check the index)
    /// - `Some(false)` if the bloom filter says "definitely not" (skip this segment)
    /// - `None` if the bloom filter isn't cached and couldn't be loaded
    pub fn bloom_check(
        &self,
        segment_path: &Path,
        base_position: u64,
        condition: &SourcingCondition,
    ) -> Option<bool> {
        self.bloom_check_probe(
            || segment_path.to_path_buf(),
            base_position,
            &BloomProbe::new(condition),
        )
    }

    /// `bloom_check` for a prepared condition. `path` names the segment file
    /// and is only called when the filter has to be loaded from disk.
    pub fn bloom_check_probe(
        &self,
        path: impl FnOnce() -> std::path::PathBuf,
        base_position: u64,
        probe: &BloomProbe,
    ) -> Option<bool> {
        if let Some(result) = self
            .blooms
            .with(base_position, |bloom| probe.might_match(bloom))
        {
            return Some(result);
        }
        if let Some(index) = self.pinned_index(base_position) {
            return Some(probe.might_match(index.bloom()));
        }

        // Not cached — try to load from disk.
        let bloom_path = path().with_extension("bloom");
        let bloom = match SegmentIndex::read_bloom(&bloom_path) {
            Ok(bloom) => bloom,
            Err(e) => {
                // Degrades to "always check the index" for this segment —
                // correct but slower, so make the corruption visible.
                tracing::warn!(
                    path = %bloom_path.display(),
                    error = %e,
                    "failed to load .bloom file; falling back to index checks"
                );
                return None;
            }
        };
        let result = probe.might_match(&bloom);
        let weight = bloom_heap_bytes(&bloom);
        self.blooms.put(base_position, Arc::new(bloom), weight);
        Some(result)
    }

    /// `bloom_check_probe` without the disk: `None` when the filter is
    /// neither cached nor pinned, whether or not one exists on disk.
    pub fn bloom_check_cached(&self, base_position: u64, probe: &BloomProbe) -> Option<bool> {
        if let Some(result) = self
            .blooms
            .with(base_position, |bloom| probe.might_match(bloom))
        {
            return Some(result);
        }
        self.pinned_index(base_position)
            .map(|index| probe.might_match(index.bloom()))
    }

    /// `get_index` without the disk: `None` when the index is neither cached
    /// nor pinned.
    pub fn index_cached(&self, base_position: u64) -> Option<Arc<SegmentIndex>> {
        if let Some(index) = self.indices.lock().get(&base_position) {
            self.index_hits.fetch_add(1, ORD);
            return Some(index);
        }
        self.pinned_index(base_position).inspect(|_| {
            self.index_hits.fetch_add(1, ORD);
        })
    }

    /// Gets a segment index, loading from disk if not cached.
    /// Returns an Arc so the caller can query the index without holding the cache lock.
    pub fn get_index(
        &self,
        segment_path: &Path,
        base_position: u64,
    ) -> Result<Arc<SegmentIndex>, Error> {
        // Try cache first.
        {
            let indices = self.indices.lock();
            if let Some(index) = indices.get(&base_position) {
                self.index_hits.fetch_add(1, ORD);
                return Ok(index);
            }
        }

        if let Some(index) = self.pinned_index(base_position) {
            self.index_hits.fetch_add(1, ORD);
            return Ok(index);
        }

        self.index_misses.fetch_add(1, ORD);

        // Not cached — load from disk.
        let idx_path = segment_path.with_extension("idx");
        let index = Arc::new(SegmentIndex::read_idx(&idx_path)?);

        // Cache it (this might evict an older entry).
        {
            let weight = index.heap_bytes();
            let mut indices = self.indices.lock();
            indices.put(base_position, Arc::clone(&index), weight);
        }

        Ok(index)
    }

    /// The position→offset table of a sealed segment, as an index with no
    /// tag bitmaps (`SegmentIndex::is_positions_only`). Serves reads that
    /// take every event; anything that asks about tags or event types needs
    /// `get_index`. If the full index is already resident it is returned
    /// instead, since that costs nothing.
    pub fn get_offsets(
        &self,
        segment_path: &Path,
        base_position: u64,
    ) -> Result<Arc<SegmentIndex>, Error> {
        if let Some(index) = self.indices.lock().get(&base_position) {
            self.index_hits.fetch_add(1, ORD);
            return Ok(index);
        }
        if let Some(index) = self.pinned_index(base_position) {
            self.index_hits.fetch_add(1, ORD);
            return Ok(index);
        }
        if let Some(index) = self.offsets.lock().get(&base_position) {
            self.offsets_hits.fetch_add(1, ORD);
            return Ok(index);
        }
        self.offsets_misses.fetch_add(1, ORD);
        let index = Arc::new(SegmentIndex::read_idx_offsets(
            &segment_path.with_extension("idx"),
        )?);
        let weight = index.heap_bytes();
        self.offsets
            .lock()
            .put(base_position, Arc::clone(&index), weight);
        Ok(index)
    }

    /// Gets a cached mmap handle for a sealed segment, opening it if not cached.
    /// Sealed segments are immutable, so the mmap can be safely shared.
    pub fn get_mmap(&self, segment_path: &Path, base_position: u64) -> Result<Arc<Mmap>, Error> {
        // Try cache first.
        {
            let mut mmaps = self.mmaps.lock();
            if let Some(mmap) = mmaps.get(&base_position) {
                self.mmap_hits.fetch_add(1, ORD);
                return Ok(Arc::clone(mmap));
            }
        }

        self.mmap_misses.fetch_add(1, ORD);

        // Not cached — open and mmap the file.
        let file = std::fs::File::open(segment_path)?;
        let mmap = Arc::new(unsafe { Mmap::map(&file)? });

        // Cache it.
        {
            let mut mmaps = self.mmaps.lock();
            mmaps.put(base_position, Arc::clone(&mmap));
        }

        Ok(mmap)
    }

    /// A reader over a sealed segment that serves records from the block
    /// cache. `index` must be the segment's own index.
    pub fn sealed_reader(
        &self,
        segment_path: &Path,
        base_position: u64,
        index: Arc<SegmentIndex>,
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

    /// Invalidates all cache entries for a segment (e.g., after a transformation rewrites it).
    pub fn invalidate(&self, base_position: u64) {
        self.sealed_files.lock().pop(&base_position);
        self.blocks.invalidate(base_position);
        self.indices.lock().pop(&base_position);
        self.offsets.lock().pop(&base_position);
        self.blooms.pop(base_position);
        self.mmaps.lock().pop(&base_position);
        self.pinned.lock().remove(&base_position);
    }

    fn pinned_index(&self, base_position: u64) -> Option<Arc<SegmentIndex>> {
        self.pinned.lock().get(&base_position).map(Arc::clone)
    }

    /// Registers a freshly sealed segment's index before its companion
    /// files exist. Idempotent. Reads of the segment resolve through the
    /// pinned copy until `unpin_sealed`.
    pub fn pin_sealed(&self, base_position: u64, index: Arc<SegmentIndex>) {
        // A segment is sealed once per incarnation of its file. Blocks and
        // file handles cached under this base belong to an earlier
        // incarnation (a truncated and re-extended suffix, for one) that a
        // slow reader may have loaded after the truncation invalidated them.
        self.sealed_files.lock().pop(&base_position);
        self.blocks.retire(base_position);
        self.pinned.lock().entry(base_position).or_insert(index);
    }

    /// The companions are on disk: the index moves from pinned into the
    /// ordinary caches, warm, and is evictable like any other.
    pub fn unpin_sealed(&self, base_position: u64) {
        let Some(index) = self.pinned.lock().remove(&base_position) else {
            return;
        };
        let bloom = index.bloom().clone();
        let bloom_weight = bloom_heap_bytes(&bloom);
        self.blooms
            .put(base_position, Arc::new(bloom), bloom_weight);
        let weight = index.heap_bytes();
        self.indices.lock().put(base_position, index, weight);
    }

    /// Whether the segment's index is still pinned (seal not yet on disk).
    pub fn is_pinned(&self, base_position: u64) -> bool {
        self.pinned.lock().contains_key(&base_position)
    }
}

/// Estimated heap size of a bloom filter: its serialized size, which is
/// its bit vectors plus a few words per layer.
fn bloom_heap_bytes(bloom: &GrowableBloom) -> usize {
    bincode::serialized_size(bloom).map_or(0, |n| n as usize) + 64
}

/// Creates the bloom key for a tag (same format as the forward index key in segment_index.rs).
fn make_bloom_key(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + key.len() + value.len());
    k.extend_from_slice(&(key.len() as u32).to_le_bytes());
    k.extend_from_slice(key);
    k.extend_from_slice(value);
    k
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::criteria::Criterion;
    use crate::event::{AppendEvent, Position, Tag};
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

    fn setup_sealed_segment(dir: &Path) -> u64 {
        let mut writer = SegmentWriter::new(dir, Position(1), DEFAULT_SEGMENT_SIZE).unwrap();
        writer
            .append(&[
                make_event("OrderPlaced", vec![tag("orderId", "A")]),
                make_event(
                    "PaymentReceived",
                    vec![tag("orderId", "A"), tag("paymentId", "P1")],
                ),
            ])
            .unwrap();
        let seg_path = writer.active_segment_path();
        drop(writer);

        // Build index files.
        let index = SegmentIndex::build_from_segment(&seg_path).unwrap();
        index.write_to_disk(&seg_path).unwrap();

        1 // base position
    }

    #[test]
    fn bloom_check_positive() {
        let dir = tempfile::tempdir().unwrap();
        let base = setup_sealed_segment(dir.path());
        let seg_path = crate::segment::segment_path(dir.path(), base);
        let cache = IndexCache::new(10, 20, 100);

        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("orderId", "A")],
            }],
        };

        let result = cache.bloom_check(&seg_path, base, &cond);
        assert_eq!(result, Some(true));
    }

    #[test]
    fn bloom_check_negative() {
        let dir = tempfile::tempdir().unwrap();
        let base = setup_sealed_segment(dir.path());
        let seg_path = crate::segment::segment_path(dir.path(), base);
        let cache = IndexCache::new(10, 20, 100);

        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("orderId", "NONEXISTENT")],
            }],
        };

        let result = cache.bloom_check(&seg_path, base, &cond);
        assert_eq!(result, Some(false));
    }

    #[test]
    fn index_cache_loads_and_caches() {
        let dir = tempfile::tempdir().unwrap();
        let base = setup_sealed_segment(dir.path());
        let seg_path = crate::segment::segment_path(dir.path(), base);
        let cache = IndexCache::new(10, 20, 100);

        // First load — from disk.
        let index = cache.get_index(&seg_path, base).unwrap();
        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("orderId", "A")],
            }],
        };
        let result = index.matching(&cond).unwrap();
        let positions: Vec<u64> = result.iter().collect();
        assert_eq!(positions, vec![1, 2]);

        // Second load — should come from cache (same Arc).
        let index2 = cache.get_index(&seg_path, base).unwrap();
        assert!(std::sync::Arc::ptr_eq(&index, &index2));
    }

    #[test]
    fn invalidate_removes_from_cache() {
        let dir = tempfile::tempdir().unwrap();
        let base = setup_sealed_segment(dir.path());
        let seg_path = crate::segment::segment_path(dir.path(), base);
        let cache = IndexCache::new(10, 20, 100);

        // Populate cache.
        let _ = cache.bloom_check(
            &seg_path,
            base,
            &SourcingCondition {
                criteria: vec![Criterion {
                    names: vec![],
                    tags: vec![tag("orderId", "A")],
                }],
            },
        );
        let index1 = cache.get_index(&seg_path, base).unwrap();

        // Invalidate.
        cache.invalidate(base);

        // Next access should reload from disk (different Arc).
        let index2 = cache.get_index(&seg_path, base).unwrap();
        assert!(!std::sync::Arc::ptr_eq(&index1, &index2));
    }

    fn small_bloom(keys: &[&str]) -> GrowableBloom {
        let mut bloom = GrowableBloom::new(0.01, 100);
        for key in keys {
            bloom.insert(make_bloom_key(b"k", key.as_bytes()));
        }
        bloom
    }

    fn probe_for(value: &str) -> BloomProbe {
        BloomProbe::new(&SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![Tag::from_str("k", value)],
            }],
        })
    }

    #[test]
    fn a_probe_agrees_with_the_condition_it_was_built_from() {
        let bloom = small_bloom(&["a", "b"]);
        assert!(probe_for("a").might_match(&bloom));
        assert!(!probe_for("zzz-not-there").might_match(&bloom));
        // Several criteria: any one may match. Several tags: all must.
        let either = BloomProbe::new(&SourcingCondition {
            criteria: vec![
                Criterion {
                    names: vec![],
                    tags: vec![Tag::from_str("k", "nope")],
                },
                Criterion {
                    names: vec![],
                    tags: vec![Tag::from_str("k", "b")],
                },
            ],
        });
        assert!(either.might_match(&bloom));
        let both = BloomProbe::new(&SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![Tag::from_str("k", "a"), Tag::from_str("k", "nope")],
            }],
        });
        assert!(!both.might_match(&bloom));
        // No criteria match nothing; an empty criterion matches everything.
        assert!(!BloomProbe::new(&SourcingCondition { criteria: vec![] }).might_match(&bloom));
        assert!(
            BloomProbe::new(&SourcingCondition {
                criteria: vec![Criterion {
                    names: vec![],
                    tags: vec![]
                }]
            })
            .might_match(&bloom)
        );
    }

    #[test]
    fn the_bloom_cache_holds_to_its_byte_budget_and_keeps_the_newest() {
        let cache = BloomCache::new(0, 250);
        for base in 0..10u64 {
            cache.put(base, Arc::new(small_bloom(&["a"])), 100);
            assert!(cache.bytes.load(ORD) <= 250, "after {base}");
            assert!(
                cache.with(base, |_| ()).is_some(),
                "newest entry {base} kept"
            );
        }
        // Two fit; the rest were evicted.
        let resident = (0..10u64)
            .filter(|&b| cache.with(b, |_| ()).is_some())
            .count();
        assert_eq!(resident, 2);
        assert_eq!(cache.entries.load(ORD), 2);
        // Replacing an entry does not double count it.
        cache.put(9, Arc::new(small_bloom(&["b"])), 100);
        assert_eq!(cache.bytes.load(ORD), 200);
        cache.pop(9);
        assert_eq!(cache.bytes.load(ORD), 100);
    }

    #[test]
    fn index_cache_stays_within_its_byte_budget_and_keeps_the_newest() {
        let mut cache: ByteCache<u32> = ByteCache::new(0, 250);
        for key in 0..50u64 {
            cache.put(key, Arc::new(key as u32), 100);
            assert!(cache.bytes <= 250, "after {key}");
            assert!(cache.get(&key).is_some(), "newest entry {key} kept");
        }
        assert_eq!(cache.bytes, 200);
        assert_eq!(cache.entries.len(), 2);
        // An entry bigger than the budget is still kept: it is about to be used.
        cache.put(99, Arc::new(99), 1000);
        assert!(cache.get(&99).is_some());
        // Replacing an entry does not double count it.
        cache.put(99, Arc::new(100), 500);
        assert_eq!(cache.bytes, 500);
        cache.pop(&99);
        assert_eq!(cache.bytes, 0);
    }

    /// A pass over a few more segments than fit, repeated, is the access
    /// pattern of a replay. LRU misses on every read of it.
    #[test]
    fn repeated_passes_over_slightly_too_many_entries_mostly_hit() {
        let (segments, fit) = (49u64, 45usize);
        let mut cache: ByteCache<u64> = ByteCache::new(0, fit * 100);
        let (mut hits, mut reads) = (0u32, 0u32);
        for pass in 0..40 {
            for key in 0..segments {
                if cache.get(&key).is_some() {
                    if pass >= 5 {
                        hits += 1;
                    }
                } else {
                    cache.put(key, Arc::new(key), 100);
                }
                if pass >= 5 {
                    reads += 1;
                }
            }
        }
        let rate = hits as f64 / reads as f64;
        assert!(rate > 0.7, "hit rate {rate:.2}; LRU would be 0");
    }

    #[test]
    fn eviction_never_picks_the_entry_just_added() {
        let mut cache: ByteCache<u64> = ByteCache::new(3, usize::MAX);
        for key in 0..200u64 {
            cache.put(key, Arc::new(key), 1);
            assert!(cache.get(&key).is_some(), "newest entry {key} kept");
            assert!(cache.entries.len() <= 3);
        }
        assert_eq!(cache.entries.len(), 3);
    }
}
