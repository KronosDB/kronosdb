use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use growable_bloom_filter::GrowableBloom;
use lru::LruCache;
use memmap2::Mmap;
use parking_lot::Mutex;

use crate::criteria::{Criterion, SourcingCondition};
use crate::error::Error;
use crate::segment::segment_index::SegmentIndex;

const ORD: Ordering = Ordering::Relaxed;

/// Default number of sealed segment mmaps to cache.
pub const DEFAULT_MMAP_CACHE_SIZE: usize = 100;

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
    indices: Mutex<LruCache<u64, Arc<SegmentIndex>>>,
    /// LRU cache of bloom filters, keyed by segment base position.
    blooms: Mutex<LruCache<u64, GrowableBloom>>,
    /// LRU cache of mmap handles for sealed segments, keyed by base position.
    mmaps: Mutex<LruCache<u64, Arc<Mmap>>>,

    // ── Hit/miss counters (lock-free) ──
    pub index_hits: AtomicU64,
    pub index_misses: AtomicU64,
    pub mmap_hits: AtomicU64,
    pub mmap_misses: AtomicU64,
}

impl IndexCache {
    /// Creates a new cache with the given capacities.
    ///
    /// `index_capacity`: max number of `.idx` files to keep in memory.
    /// `bloom_capacity`: max number of `.bloom` files to keep in memory.
    pub fn new(index_capacity: usize, bloom_capacity: usize) -> Self {
        Self {
            indices: Mutex::new(LruCache::new(
                NonZeroUsize::new(index_capacity.max(1)).unwrap(),
            )),
            blooms: Mutex::new(LruCache::new(
                NonZeroUsize::new(bloom_capacity.max(1)).unwrap(),
            )),
            mmaps: Mutex::new(LruCache::new(
                NonZeroUsize::new(DEFAULT_MMAP_CACHE_SIZE).unwrap(),
            )),
            index_hits: AtomicU64::new(0),
            index_misses: AtomicU64::new(0),
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
        // Try cache first.
        {
            let mut blooms = self.blooms.lock();
            if let Some(bloom) = blooms.get(&base_position) {
                return Some(condition_might_match_bloom(bloom, condition));
            }
        }

        // Not cached — try to load from disk.
        let bloom_path = segment_path.with_extension("bloom");
        let bloom = SegmentIndex::read_bloom(&bloom_path).ok()?;
        let result = condition_might_match_bloom(&bloom, condition);

        // Cache it.
        {
            let mut blooms = self.blooms.lock();
            blooms.put(base_position, bloom);
        }

        Some(result)
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
            let mut indices = self.indices.lock();
            if let Some(index) = indices.get(&base_position) {
                self.index_hits.fetch_add(1, ORD);
                return Ok(Arc::clone(index));
            }
        }

        self.index_misses.fetch_add(1, ORD);

        // Not cached — load from disk.
        let idx_path = segment_path.with_extension("idx");
        let index = Arc::new(SegmentIndex::read_idx(&idx_path)?);

        // Cache it (this might evict an older entry).
        {
            let mut indices = self.indices.lock();
            indices.put(base_position, Arc::clone(&index));
        }

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

    /// Invalidates all cache entries for a segment (e.g., after a transformation rewrites it).
    pub fn invalidate(&self, base_position: u64) {
        self.indices.lock().pop(&base_position);
        self.blooms.lock().pop(&base_position);
        self.mmaps.lock().pop(&base_position);
    }
}

/// Checks if any criterion in the condition might match based on the bloom filter.
/// A condition matches if ANY criterion might match (OR logic).
/// A criterion might match if ALL its tags pass the bloom filter (AND logic).
fn condition_might_match_bloom(bloom: &GrowableBloom, condition: &SourcingCondition) -> bool {
    for criterion in &condition.criteria {
        if criterion_might_match_bloom(bloom, criterion) {
            return true;
        }
    }
    false
}

/// Checks if a single criterion might match based on the bloom filter.
fn criterion_might_match_bloom(bloom: &GrowableBloom, criterion: &Criterion) -> bool {
    // Check event type names — if any name is in the bloom, it might match.
    if !criterion.names.is_empty() {
        let any_name_present = criterion.names.iter().any(|name| {
            let key = make_bloom_key(b"__kronosdb_event_type__", name.as_bytes());
            bloom.contains(&key)
        });
        if !any_name_present {
            return false;
        }
    }

    // Check tags — ALL tags must be in the bloom for this criterion to possibly match.
    for tag in &criterion.tags {
        let key = make_bloom_key(&tag.key, &tag.value);
        if !bloom.contains(&key) {
            return false;
        }
    }

    true
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
        let cache = IndexCache::new(10, 20);

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
        let cache = IndexCache::new(10, 20);

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
        let cache = IndexCache::new(10, 20);

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
        let cache = IndexCache::new(10, 20);

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
}
