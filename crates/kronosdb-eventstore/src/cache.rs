use std::collections::HashMap;
use std::fs::File;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use lru::LruCache;
use memmap2::Mmap;
use parking_lot::{Mutex, RwLock};

use crate::error::Error;
use crate::segment::blocks::{BlockCache, SealedFile, SealedReader};
use crate::segment::query::Locator;
use crate::segment::sealed_index::SealedIndex;
use crate::segment::segment_index::SegmentIndex;

const ORD: Ordering = Ordering::Relaxed;

/// Open sealed segment files this process may keep at once, shared by every
/// context: the files reads past the mapping budget `pread` from, and the
/// block read path's (`read-path = "blocks"`). Sized from the descriptor
/// limit, so a log of thousands of segments is not reopened file by file on
/// every read; past it the least recently used file is closed.
#[derive(Debug)]
pub struct FileBudget {
    live: AtomicUsize,
    limit: usize,
    warned: AtomicBool,
}

impl FileBudget {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            live: AtomicUsize::new(0),
            limit: limit.max(1),
            warned: AtomicBool::new(false),
        })
    }

    /// The process's budget: half of its open-file limit (`RLIMIT_NOFILE`,
    /// the soft limit as it stands when first asked), the other half left
    /// to sockets, active segments and index files being opened.
    pub fn shared() -> Arc<Self> {
        static SHARED: OnceLock<Arc<FileBudget>> = OnceLock::new();
        Arc::clone(SHARED.get_or_init(|| Self::new(open_file_limit() / 2)))
    }

    /// Files held now.
    pub fn live(&self) -> usize {
        self.live.load(ORD)
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    fn took(&self) {
        self.live.fetch_add(1, ORD);
    }

    fn gave(&self, n: usize) {
        self.live.fetch_sub(n, ORD);
    }

    fn over(&self) -> bool {
        let over = self.live.load(ORD) > self.limit;
        if over && !self.warned.swap(true, ORD) {
            tracing::warn!(
                limit = self.limit,
                "the open-file budget is full: sealed segments past it are opened again on \
                 each read, which is slower; raise the open-file limit (ulimit -n)"
            );
        }
        over
    }
}

/// The soft open-file limit; 1024 where it cannot be read.
fn open_file_limit() -> usize {
    #[cfg(unix)]
    {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: getrlimit writes the struct it is given and nothing else.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } == 0 {
            return usize::try_from(limit.rlim_cur).unwrap_or(usize::MAX);
        }
    }
    1024
}

/// Raises this process's soft open-file limit as far as the hard limit
/// allows, as servers do at startup; a read past the mapping budget keeps a
/// file open per segment. Returns the soft limit in force afterwards. Call
/// it before the first engine opens: `FileBudget::shared` reads the limit
/// once.
pub fn raise_open_file_limit() -> usize {
    #[cfg(unix)]
    {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        // SAFETY: as in `open_file_limit`.
        if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) } != 0 {
            return open_file_limit();
        }
        // macOS reports an unlimited hard limit but refuses a soft limit
        // past the per-process maximum, so try that as a fallback.
        let wanted = limit.rlim_max.min(1 << 20);
        for candidate in [wanted, wanted.min(10_240)] {
            if candidate <= limit.rlim_cur {
                break;
            }
            let raised = libc::rlimit {
                rlim_cur: candidate,
                rlim_max: limit.rlim_max,
            };
            // SAFETY: setrlimit reads the struct it is given.
            if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) } == 0 {
                break;
            }
        }
    }
    open_file_limit()
}

/// Open files by segment base, closed least recently used first when the
/// file budget is over.
struct OpenFiles<V> {
    lru: Mutex<LruCache<u64, Arc<V>>>,
    budget: Arc<FileBudget>,
}

impl<V> OpenFiles<V> {
    fn new(budget: Arc<FileBudget>) -> Self {
        Self {
            lru: Mutex::new(LruCache::unbounded()),
            budget,
        }
    }

    fn get(&self, base: u64) -> Option<Arc<V>> {
        self.lru.lock().get(&base).map(Arc::clone)
    }

    fn put(&self, base: u64, file: Arc<V>) {
        let mut lru = self.lru.lock();
        if lru.put(base, file).is_none() {
            self.budget.took();
        }
        while lru.len() > 1 && self.budget.over() {
            lru.pop_lru();
            self.budget.gave(1);
        }
    }

    fn pop(&self, base: u64) {
        if self.lru.lock().pop(&base).is_some() {
            self.budget.gave(1);
        }
    }
}

impl<V> Drop for OpenFiles<V> {
    fn drop(&mut self) {
        self.budget.gave(self.lru.get_mut().len());
    }
}

/// Mappings of sealed segments and of their indexes this process may keep at
/// once, shared by every context. A mapping costs address space and one entry
/// in the kernel's per-process map table, not memory: the page cache decides
/// what is resident, mapped or not. So everything stays mapped until the
/// table runs short, and only then is one let go. Mapping a segment again
/// costs an open, a map, the unmap of another and fresh page faults on data
/// that is often already in memory, which is most of a read's time when it
/// happens on every read.
#[derive(Debug)]
pub struct MappingBudget {
    live: AtomicUsize,
    limit: usize,
    warned: AtomicBool,
}

impl MappingBudget {
    pub fn new(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            live: AtomicUsize::new(0),
            limit: limit.max(1),
            warned: AtomicBool::new(false),
        })
    }

    /// The process's budget: three quarters of the kernel's limit
    /// (`vm.max_map_count`, 65530 by default; the default where there is no
    /// such file), the rest left to the allocator, libraries and stacks.
    pub fn shared() -> Arc<Self> {
        static SHARED: OnceLock<Arc<MappingBudget>> = OnceLock::new();
        let limit = std::fs::read_to_string("/proc/sys/vm/max_map_count")
            .ok()
            .and_then(|text| text.trim().parse::<usize>().ok())
            .unwrap_or(65_530);
        Arc::clone(SHARED.get_or_init(|| Self::new(limit / 4 * 3)))
    }

    /// Mappings held now.
    pub fn live(&self) -> usize {
        self.live.load(ORD)
    }

    pub fn limit(&self) -> usize {
        self.limit
    }

    fn took(&self) {
        self.live.fetch_add(1, ORD);
    }

    fn gave(&self, n: usize) {
        self.live.fetch_sub(n, ORD);
    }

    /// Whether one more mapping fits.
    fn has_room(&self) -> bool {
        self.live.load(ORD) < self.limit
    }

    /// Whether more is mapped than the budget allows. Says so in the log the
    /// first time, because from then on reads pay for remapping.
    fn over(&self) -> bool {
        let over = self.live.load(ORD) > self.limit;
        if over && !self.warned.swap(true, ORD) {
            tracing::warn!(
                limit = self.limit,
                "the mapping budget is full: sealed segments past it are mapped again on \
                 each read, which is slower; raise vm.max_map_count to keep them all mapped"
            );
        }
        over
    }
}

const SHARDS: usize = 16;

type Shard = RwLock<HashMap<u64, Arc<SealedIndex>>>;

/// Mapped `.idx` files of sealed segments, by base position. Every read of
/// every sealed segment asks here first, so the lookup takes a sharded read
/// lock and nothing else. Over the budget, a pseudo-random one is unmapped.
struct MappedIndexes {
    shards: Vec<Shard>,
    entries: AtomicUsize,
    budget: Arc<MappingBudget>,
    victim: AtomicUsize,
}

impl MappedIndexes {
    fn new(budget: Arc<MappingBudget>) -> Self {
        Self {
            shards: (0..SHARDS).map(|_| RwLock::new(HashMap::new())).collect(),
            entries: AtomicUsize::new(0),
            budget,
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
            self.budget.took();
        }
        while self.budget.over() {
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
                self.budget.gave(1);
                return true;
            }
        }
        false
    }

    fn pop(&self, base: u64) {
        if self.shard(base).write().remove(&base).is_some() {
            self.entries.fetch_sub(1, ORD);
            self.budget.gave(1);
        }
    }

    fn len(&self) -> usize {
        self.entries.load(ORD)
    }
}

impl Drop for MappedIndexes {
    fn drop(&mut self) {
        self.budget.gave(self.entries.load(ORD));
    }
}

/// Mapped sealed segment files, by base position. Over the budget, the least
/// recently used is unmapped.
struct MappedSegments {
    lru: Mutex<LruCache<u64, Arc<Mmap>>>,
    budget: Arc<MappingBudget>,
}

impl MappedSegments {
    fn new(budget: Arc<MappingBudget>) -> Self {
        Self {
            lru: Mutex::new(LruCache::unbounded()),
            budget,
        }
    }

    fn get(&self, base: u64) -> Option<Arc<Mmap>> {
        self.lru.lock().get(&base).map(Arc::clone)
    }

    fn put(&self, base: u64, mmap: Arc<Mmap>) {
        let mut lru = self.lru.lock();
        if lru.put(base, mmap).is_none() {
            self.budget.took();
        }
        while lru.len() > 1 && self.budget.over() {
            lru.pop_lru();
            self.budget.gave(1);
        }
    }

    fn pop(&self, base: u64) {
        if self.lru.lock().pop(&base).is_some() {
            self.budget.gave(1);
        }
    }

    fn len(&self) -> usize {
        self.lru.lock().len()
    }
}

impl Drop for MappedSegments {
    fn drop(&mut self) {
        self.budget.gave(self.lru.get_mut().len());
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
    /// Mapped sealed segment files, kept while the mapping budget allows.
    mmaps: MappedSegments,
    /// Open sealed segment files for block reads, keyed by base position.
    sealed_files: OpenFiles<SealedFile>,
    /// Open sealed segment files that reads past the mapping budget
    /// `pread` from, keyed by base position.
    files: OpenFiles<File>,
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
    /// Read past the mapping budget from a file already open.
    pub file_hits: AtomicU64,
    /// Read past the mapping budget from a file opened for it.
    pub file_misses: AtomicU64,
}

impl IndexCache {
    /// A cache with the default block cache, mapping within `budget` and
    /// keeping files open within the process's file budget.
    pub fn new(budget: Arc<MappingBudget>) -> Self {
        Self::with_blocks(
            BlockCache::new(
                crate::segment::blocks::DEFAULT_BLOCK_CACHE_BYTES,
                crate::segment::blocks::DEFAULT_BLOCK_SIZE,
                crate::segment::blocks::DEFAULT_READAHEAD_BLOCKS,
            ),
            budget,
            FileBudget::shared(),
        )
    }

    /// Like `new`, with an explicitly configured block cache and file budget.
    pub fn with_blocks(
        blocks: BlockCache,
        budget: Arc<MappingBudget>,
        files: Arc<FileBudget>,
    ) -> Self {
        Self {
            mapped: MappedIndexes::new(Arc::clone(&budget)),
            mmaps: MappedSegments::new(budget),
            sealed_files: OpenFiles::new(Arc::clone(&files)),
            files: OpenFiles::new(files),
            blocks: Arc::new(blocks),
            pinned: Mutex::new(HashMap::new()),
            index_hits: AtomicU64::new(0),
            index_misses: AtomicU64::new(0),
            mmap_hits: AtomicU64::new(0),
            mmap_misses: AtomicU64::new(0),
            file_hits: AtomicU64::new(0),
            file_misses: AtomicU64::new(0),
        }
    }

    /// The index of sealed segment `base`, without touching the disk:
    /// mapped already, or pinned. `None` otherwise.
    pub fn index_cached(&self, base: u64) -> Option<Arc<dyn Locator>> {
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
    pub fn get_index(&self, segment_path: &Path, base: u64) -> Result<Arc<dyn Locator>, Error> {
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

    /// How many sealed segment files are mapped.
    pub fn mapped_segments(&self) -> usize {
        self.mmaps.len()
    }

    /// Gets a cached mmap handle for a sealed segment, opening it if not cached.
    /// Sealed segments are immutable, so the mmap can be safely shared.
    pub fn get_mmap(&self, segment_path: &Path, base_position: u64) -> Result<Arc<Mmap>, Error> {
        if let Some(mmap) = self.mmaps.get(base_position) {
            self.mmap_hits.fetch_add(1, ORD);
            return Ok(mmap);
        }
        self.mmap_misses.fetch_add(1, ORD);
        let file = std::fs::File::open(segment_path)?;
        let mmap = Arc::new(unsafe { Mmap::map(&file)? });
        self.mmaps.put(base_position, Arc::clone(&mmap));
        Ok(mmap)
    }

    /// The mapping of a sealed segment if it is mapped already or one more
    /// mapping fits the budget; `None` past the budget, where mapping it
    /// would unmap another (and that one's next read map it again), so the
    /// caller reads the records it needs with `pread` instead.
    pub fn mapped_within_budget(
        &self,
        segment_path: &Path,
        base_position: u64,
    ) -> Result<Option<Arc<Mmap>>, Error> {
        if let Some(mmap) = self.mmaps.get(base_position) {
            self.mmap_hits.fetch_add(1, ORD);
            return Ok(Some(mmap));
        }
        if !self.mmaps.budget.has_room() {
            return Ok(None);
        }
        self.get_mmap(segment_path, base_position).map(Some)
    }

    /// An open sealed segment file to `pread` records from, its header
    /// checked when it was opened.
    pub fn segment_file(
        &self,
        segment_path: &Path,
        base_position: u64,
    ) -> Result<Arc<File>, Error> {
        if let Some(file) = self.files.get(base_position) {
            self.file_hits.fetch_add(1, ORD);
            return Ok(file);
        }
        self.file_misses.fetch_add(1, ORD);
        let file = File::open(segment_path)?;
        crate::segment::reader::check_segment_header(&file, base_position)?;
        let file = Arc::new(file);
        self.files.put(base_position, Arc::clone(&file));
        Ok(file)
    }

    /// A reader over a sealed segment that serves records from the block
    /// cache. `index` must be the segment's own index.
    pub fn sealed_reader(
        &self,
        segment_path: &Path,
        base_position: u64,
        index: Arc<dyn Locator>,
    ) -> Result<SealedReader, Error> {
        let file = match self.sealed_files.get(base_position) {
            Some(file) => file,
            None => {
                let file = self.blocks.open(segment_path, base_position)?;
                self.sealed_files.put(base_position, Arc::clone(&file));
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
        self.sealed_files.pop(base_position);
        self.files.pop(base_position);
        self.blocks.invalidate(base_position);
        self.mapped.pop(base_position);
        self.mmaps.pop(base_position);
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
        self.sealed_files.pop(base_position);
        self.files.pop(base_position);
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

    /// The in-memory index of a segment whose seal is in flight.
    pub fn pinned(&self, base_position: u64) -> Option<Arc<SegmentIndex>> {
        self.pinned.lock().get(&base_position).cloned()
    }

    /// Whether the segment's index is still pinned (seal not yet on disk).
    pub fn is_pinned(&self, base_position: u64) -> bool {
        self.pinned.lock().contains_key(&base_position)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{AppendEvent, Position, Tag};
    use crate::segment::DEFAULT_SEGMENT_SIZE;
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

    #[test]
    fn maps_once_and_answers_from_the_mapping() {
        let dir = tempfile::tempdir().unwrap();
        let (path, base) = setup_sealed_segment(dir.path());
        let cache = IndexCache::new(MappingBudget::new(10));
        let index = cache.get_index(&path, base).unwrap();
        assert_eq!(index.position_range(), 1..3);
        assert!(index.offset_of(1).unwrap().is_some());
        assert_eq!(cache.index_misses.load(ORD), 1);
        let again = cache.get_index(&path, base).unwrap();
        assert!(Arc::ptr_eq(&index, &again));
        assert_eq!(cache.index_misses.load(ORD), 1);
    }

    #[test]
    fn invalidate_unmaps() {
        let dir = tempfile::tempdir().unwrap();
        let (path, base) = setup_sealed_segment(dir.path());
        let cache = IndexCache::new(MappingBudget::new(10));
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
        let cache = IndexCache::new(MappingBudget::new(10));
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
        let budget = MappingBudget::new(3);
        let mapped = MappedIndexes::new(Arc::clone(&budget));
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
        assert_eq!(budget.live(), 2);
        drop(mapped);
        assert_eq!(budget.live(), 0);
    }

    /// Segments and indexes share one budget, and every context's cache
    /// counts against it: what one cache maps, another has less room for.
    #[test]
    fn segment_and_index_mappings_share_one_budget() {
        let dir = tempfile::tempdir().unwrap();
        let (path, base) = setup_sealed_segment(dir.path());
        let budget = MappingBudget::new(4);
        let first = IndexCache::new(Arc::clone(&budget));
        let second = IndexCache::new(Arc::clone(&budget));
        first.get_index(&path, base).unwrap();
        first.get_mmap(&path, base).unwrap();
        assert_eq!(budget.live(), 2);
        // Under the budget nothing is let go; past it, the least recently
        // used segment of the cache that maps one more goes.
        for key in 100..103u64 {
            second.get_mmap(&path, key).unwrap();
        }
        assert_eq!(budget.live(), 4);
        assert_eq!(second.mapped_segments(), 2);
        assert!(second.mmaps.get(102).is_some() && second.mmaps.get(101).is_some());
        assert_eq!(first.mapped_segments(), 1);
        drop(second);
        assert_eq!(budget.live(), 2);
        first.invalidate(base);
        assert_eq!(budget.live(), 0);
    }
}
