use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::broadcast;

use crate::api::EventStore;
use crate::error::Error;
use crate::append::{AppendRequest, AppendResponse};
use crate::cache::IndexCache;
use crate::criteria::SourcingCondition;
use crate::event::{Position, SequencedEvent, StoredEvent, Tag};
use crate::stream::{CommitNotification, EventStream};

use crate::index::tag_index::TagIndex;
use crate::segment::reader::SegmentReader;
use crate::segment::segment_index::SegmentIndex;
use crate::segment::writer::SegmentWriter;
use crate::segment::{self, DEFAULT_SEGMENT_SIZE};

/// Default capacity for the commit notification channel.
const COMMIT_CHANNEL_CAPACITY: usize = 256;

/// Default number of segment indices to cache.
pub const DEFAULT_INDEX_CACHE_SIZE: usize = 50;

/// Default number of bloom filters to cache.
pub const DEFAULT_BLOOM_CACHE_SIZE: usize = 200;

/// Cached list of segment bases, avoiding readdir and stat syscalls on every query.
///
/// All segments before `sealed_count` are known to have companion `.idx`/`.bloom`
/// files. The last segment is the active (writable) segment.
#[derive(Clone)]
struct SegmentList {
    /// Segment base positions in ascending order.
    bases: Vec<u64>,
    /// Number of segments known to be sealed (have .idx/.bloom companions).
    /// Invariant: sealed_count < bases.len() (the active segment is never sealed).
    sealed_count: usize,
}

impl SegmentList {
    fn is_sealed(&self, index: usize) -> bool {
        index < self.sealed_count
    }
}

/// Default group commit interval (0 = disabled, sync per write).
const DEFAULT_GROUP_COMMIT_INTERVAL_MS: u64 = 0;

/// Configuration options for an event store engine.
#[derive(Debug, Clone)]
pub struct StoreOptions {
    pub max_segment_size: u64,
    pub index_cache_size: usize,
    pub bloom_cache_size: usize,
    /// Group commit interval in milliseconds. 0 = disabled (sync per write).
    pub group_commit_interval_ms: u64,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            max_segment_size: DEFAULT_SEGMENT_SIZE,
            index_cache_size: DEFAULT_INDEX_CACHE_SIZE,
            bloom_cache_size: DEFAULT_BLOOM_CACHE_SIZE,
            group_commit_interval_ms: DEFAULT_GROUP_COMMIT_INTERVAL_MS,
        }
    }
}

/// The main event store. Combines the segment writer, tag index, and
/// concurrency control into a single API.
///
/// All operations take `&self`. The writer is behind a Mutex for interior
/// mutability, allowing reads to proceed concurrently with each other
/// (they don't need the writer lock). Only appends lock the writer.
///
/// Tags are stored on events in segments (source of truth).
/// The tag index is a derived in-memory structure rebuilt from segments on recovery.
/// Tag mutations (retagging, redaction) are handled by the transformation system,
/// not by direct mutation on the store.
pub struct EventStoreEngine {
    /// Directory where segment files are stored.
    dir: PathBuf,

    /// The segment writer. Behind Mutex for interior mutability —
    /// only the append path locks this.
    writer: parking_lot::Mutex<SegmentWriter>,

    /// The tag index for the active segment. Protected by RwLock for concurrent read access.
    tag_index: Arc<RwLock<TagIndex>>,

    /// The committed position — the position of the last event visible to readers.
    committed_position: Arc<AtomicU64>,

    /// Broadcast channel for notifying stream subscribers of new commits.
    commit_tx: broadcast::Sender<CommitNotification>,

    /// LRU cache for sealed segment indices, bloom filters, and mmap handles.
    cache: Arc<IndexCache>,

    /// Cached segment list — avoids readdir + stat syscalls on every query.
    /// Updated on rotation within the append path (under writer lock).
    segments: RwLock<SegmentList>,
}

impl EventStoreEngine {
    /// Creates a new event store in the given directory with default options.
    pub fn create(dir: &Path) -> Result<Self, Error> {
        Self::create_with_options(dir, DEFAULT_SEGMENT_SIZE)
    }

    /// Creates a new event store with custom segment size.
    pub fn create_with_options(dir: &Path, max_segment_size: u64) -> Result<Self, Error> {
        Self::create_with_store_options(dir, &StoreOptions {
            max_segment_size,
            ..Default::default()
        })
    }

    /// Creates a new event store with full options.
    pub fn create_with_store_options(dir: &Path, opts: &StoreOptions) -> Result<Self, Error> {
        std::fs::create_dir_all(dir)?;
        let writer = SegmentWriter::new(dir, Position(1), opts.max_segment_size)?;
        let active_base = writer.active_base_position();
        let (commit_tx, _) = broadcast::channel(COMMIT_CHANNEL_CAPACITY);

        Ok(Self {
            dir: dir.to_path_buf(),
            writer: parking_lot::Mutex::new(writer),
            tag_index: Arc::new(RwLock::new(TagIndex::new())),
            committed_position: Arc::new(AtomicU64::new(0)),
            commit_tx,
            cache: Arc::new(IndexCache::new(opts.index_cache_size, opts.bloom_cache_size)),
            segments: RwLock::new(SegmentList {
                bases: vec![active_base],
                sealed_count: 0,
            }),
        })
    }

    /// Opens an existing event store, recovering from the last valid state.
    pub fn open(dir: &Path) -> Result<Self, Error> {
        Self::open_with_options(dir, DEFAULT_SEGMENT_SIZE)
    }

    /// Opens an existing event store with custom segment size.
    pub fn open_with_options(dir: &Path, max_segment_size: u64) -> Result<Self, Error> {
        Self::open_with_store_options(dir, &StoreOptions {
            max_segment_size,
            ..Default::default()
        })
    }

    /// Opens an existing event store with full options.
    pub fn open_with_store_options(dir: &Path, opts: &StoreOptions) -> Result<Self, Error> {
        let writer = SegmentWriter::open(dir, opts.max_segment_size)?;
        let head = writer.head();
        let active_base = writer.active_base_position();

        // Rebuild the active segment's tag index from its events.
        // Sealed segments have their own `.idx` files on disk.
        let mut tag_index = TagIndex::new();
        rebuild_active_segment_index(dir, &mut tag_index)?;

        // Build the cached segment list from disk (one-time cost on startup).
        let all_bases = segment::list_segment_files(dir)?;
        let sealed_count = count_sealed_segments(dir, &all_bases, active_base);

        let committed = if head.0 > 0 { head.0 - 1 } else { 0 };
        let (commit_tx, _) = broadcast::channel(COMMIT_CHANNEL_CAPACITY);

        Ok(Self {
            dir: dir.to_path_buf(),
            writer: parking_lot::Mutex::new(writer),
            tag_index: Arc::new(RwLock::new(tag_index)),
            committed_position: Arc::new(AtomicU64::new(committed)),
            commit_tx,
            cache: Arc::new(IndexCache::new(opts.index_cache_size, opts.bloom_cache_size)),
            segments: RwLock::new(SegmentList {
                bases: all_bases,
                sealed_count,
            }),
        })
    }

    /// Appends events to the store, optionally with a DCB consistency condition.
    ///
    /// 1. Checks DCB condition against the tag index
    /// 2. Writes events to the active segment (tags included on disk)
    /// 3. Updates the in-memory tag index
    /// 4. Advances the committed position
    pub fn append(&self, request: AppendRequest) -> Result<AppendResponse, Error> {
        // Lock the writer for the entire append operation.
        // DCB condition check + write + index update must be atomic.
        let mut writer = self.writer.lock();

        // Step 1: Check DCB condition.
        {
            let index = self.tag_index.read();
            if let Some(condition) = &request.condition {
                if let Some(conflicting_pos) = index.check_condition(condition) {
                    return Err(Error::ConsistencyConditionViolated {
                        conflicting_position: conflicting_pos,
                    });
                }
            }
        }

        if request.events.is_empty() {
            let head = writer.head();
            return Ok(AppendResponse {
                first_position: head,
                count: 0,
                consistency_marker: Position(self.committed_position.load(Ordering::Acquire)),
            });
        }

        // Remember the active segment base before writing — rotation detection.
        let old_active_base = writer.active_base_position();

        // Step 2: Write events to segment (tags are persisted with the event).
        let (first_position, count) = writer.append(&request.events)?;

        // Step 2b: Detect rotation and update cached segment list.
        let new_active_base = writer.active_base_position();
        if new_active_base != old_active_base {
            let mut seg_list = self.segments.write();
            seg_list.sealed_count += 1;
            seg_list.bases.push(new_active_base);
        }

        // Step 3: Update in-memory tag index.
        {
            let mut index = self.tag_index.write();
            let mut pos = first_position;
            for event in &request.events {
                index.index_event(pos, &event.name, &event.tags);
                pos = pos.next();
            }
        }

        // Step 4: Advance committed position.
        let last_position = Position(first_position.0 + count as u64 - 1);
        self.committed_position
            .store(last_position.0, Ordering::Release);

        // Step 5: Notify stream subscribers.
        // Ignore send errors — they just mean no active subscribers.
        let _ = self.commit_tx.send(CommitNotification {
            committed_position: last_position.0,
        });

        Ok(AppendResponse {
            first_position,
            count,
            consistency_marker: last_position,
        })
    }

    /// Gets tags for an event at the given position by reading from the segment.
    pub fn get_tags(&self, position: Position) -> Result<Vec<Tag>, Error> {
        let committed = self.committed_position.load(Ordering::Acquire);
        if position.0 > committed || position.0 == 0 {
            return Err(Error::Corrupted {
                message: format!("position {} does not exist", position.0),
            });
        }

        // Use cached segment list instead of readdir.
        let seg_list = self.segments.read().clone();
        let seg_idx = match seg_list.bases.binary_search(&position.0) {
            Ok(i) => i,
            Err(0) => {
                return Err(Error::Corrupted {
                    message: format!("no segment contains position {}", position.0),
                })
            }
            Err(i) => i - 1,
        };

        let base = seg_list.bases[seg_idx];
        let seg_path = segment::segment_path(&self.dir, base);

        // Use cached mmap for sealed segments.
        let reader = if seg_list.is_sealed(seg_idx) {
            let mmap = self.cache.get_mmap(&seg_path, base)?;
            SegmentReader::from_shared_mmap(mmap)?
        } else {
            SegmentReader::open(&seg_path)?
        };

        for result in reader.iter(None) {
            let event = result?;
            if event.position == position {
                return Ok(event.tags);
            }
            if event.position > position {
                break;
            }
        }

        Err(Error::Corrupted {
            message: format!("event at position {} not found in segment", position.0),
        })
    }

    /// Creates a live event stream subscription.
    pub fn subscribe(&self, from_position: Position, condition: SourcingCondition) -> EventStream {
        EventStream::new(
            condition,
            from_position,
            self.commit_tx.subscribe(),
            Arc::clone(&self.committed_position),
        )
    }

    /// Returns the current head position (next position to be assigned).
    pub fn head(&self) -> Position {
        self.writer.lock().head()
    }

    /// Returns the tail position (first event in the store).
    pub fn tail(&self) -> Position {
        let committed = self.committed_position.load(Ordering::Acquire);
        if committed == 0 {
            Position(0)
        } else {
            Position(1) // TODO: Track actual tail for truncated stores.
        }
    }

    /// Reads events matching a query from `from_position` up to the current head.
    /// This is the "Source" operation — a finite read.
    ///
    /// For sealed segments: uses cached bloom filters, indices, and mmap handles.
    /// For the active segment: uses the in-memory tag index.
    ///
    /// No readdir or stat syscalls — segment list and sealed status are cached.
    pub fn source(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
    ) -> Result<Vec<SequencedEvent>, Error> {
        let committed = self.committed_position.load(Ordering::Acquire);

        // Read cached segment list — no readdir syscall.
        let seg_list = self.segments.read().clone();
        if seg_list.bases.is_empty() {
            return Ok(vec![]);
        }

        let mut events = Vec::new();

        for (i, &base) in seg_list.bases.iter().enumerate() {
            let seg_path = segment::segment_path(&self.dir, base);
            let is_last = i + 1 == seg_list.bases.len();
            let seg_end = if !is_last {
                seg_list.bases[i + 1] - 1
            } else {
                committed
            };

            if base > committed {
                break;
            }
            if seg_end < from_position.0 {
                continue;
            }

            // Determine matching positions — no stat syscalls for sealed check.
            let matching_positions = if seg_list.is_sealed(i) {
                // Sealed segment — check bloom filter first, then load index via cache.
                if let Some(false) = self.cache.bloom_check(&seg_path, base, condition) {
                    continue; // Bloom filter says definitely no match — skip segment.
                }
                let seg_index = self.cache.get_index(&seg_path, base)?;
                seg_index.matching(condition)
            } else {
                // Active segment — use in-memory index.
                let index = self.tag_index.read();
                index.matching_bitmap(condition, from_position)
            };

            let matching_positions = match matching_positions {
                Some(bm) => bm,
                None => continue, // No matches in this segment.
            };

            // Use cached mmap for sealed segments, fresh open for active.
            let reader = if seg_list.is_sealed(i) {
                let mmap = self.cache.get_mmap(&seg_path, base)?;
                SegmentReader::from_shared_mmap(mmap)?
            } else {
                SegmentReader::open(&seg_path)?
            };

            for result in reader.iter(Some(Position(committed + 1))) {
                let stored = result?;

                if stored.position.0 < from_position.0 {
                    continue;
                }

                if matching_positions.contains(stored.position.0) {
                    events.push(stored.into_sequenced());
                }
            }
        }

        Ok(events)
    }

    /// Like `source`, but returns `StoredEvent` (includes tags on each event).
    /// Used by the admin console event browser where tags are displayed inline.
    pub fn source_stored(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
        limit: usize,
    ) -> Result<Vec<StoredEvent>, Error> {
        let committed = self.committed_position.load(Ordering::Acquire);

        let seg_list = self.segments.read().clone();
        if seg_list.bases.is_empty() {
            return Ok(vec![]);
        }

        let mut events = Vec::new();

        for (i, &base) in seg_list.bases.iter().enumerate() {
            let seg_path = segment::segment_path(&self.dir, base);
            let is_last = i + 1 == seg_list.bases.len();
            let seg_end = if !is_last {
                seg_list.bases[i + 1] - 1
            } else {
                committed
            };

            if base > committed {
                break;
            }
            if seg_end < from_position.0 {
                continue;
            }

            let matching_positions = if seg_list.is_sealed(i) {
                if let Some(false) = self.cache.bloom_check(&seg_path, base, condition) {
                    continue;
                }
                let seg_index = self.cache.get_index(&seg_path, base)?;
                seg_index.matching(condition)
            } else {
                let index = self.tag_index.read();
                index.matching_bitmap(condition, from_position)
            };

            let matching_positions = match matching_positions {
                Some(bm) => bm,
                None => continue,
            };

            let reader = if seg_list.is_sealed(i) {
                let mmap = self.cache.get_mmap(&seg_path, base)?;
                SegmentReader::from_shared_mmap(mmap)?
            } else {
                SegmentReader::open(&seg_path)?
            };

            for result in reader.iter(Some(Position(committed + 1))) {
                let stored = result?;

                if stored.position.0 < from_position.0 {
                    continue;
                }

                if matching_positions.contains(stored.position.0) {
                    events.push(stored);
                    if events.len() >= limit {
                        return Ok(events);
                    }
                }
            }
        }

        Ok(events)
    }
}

impl EventStore for EventStoreEngine {
    fn append(&self, request: AppendRequest) -> Result<AppendResponse, Error> {
        self.append(request)
    }

    fn source(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
    ) -> Result<Vec<SequencedEvent>, Error> {
        self.source(from_position, condition)
    }

    fn subscribe(&self, from_position: Position, condition: SourcingCondition) -> EventStream {
        self.subscribe(from_position, condition)
    }

    fn head(&self) -> Position {
        self.head()
    }

    fn tail(&self) -> Position {
        self.tail()
    }

    fn get_tags(&self, position: Position) -> Result<Vec<Tag>, Error> {
        self.get_tags(position)
    }
}

/// Counts how many segments are sealed (have companion .idx/.bloom files).
/// Called once during open to populate the cached segment list.
fn count_sealed_segments(dir: &Path, bases: &[u64], active_base: u64) -> usize {
    let mut count = 0;
    for &base in bases {
        if base == active_base {
            break; // Active segment — not sealed.
        }
        let seg_path = segment::segment_path(dir, base);
        if SegmentIndex::has_companion_files(&seg_path) {
            count += 1;
        } else {
            break; // Gap in sealed segments — stop counting.
        }
    }
    count
}

/// Rebuilds the tag index for the active (unsealed) segment.
///
/// Sealed segments have `.idx` companion files on disk and don't need replay.
/// Only the active segment (the last one without `.idx`) is replayed.
/// If a sealed segment is missing its `.idx`, it's rebuilt from the segment data.
fn rebuild_active_segment_index(dir: &Path, index: &mut TagIndex) -> Result<(), Error> {
    let segments = segment::list_segment_files(dir)?;

    for base_pos in segments {
        let seg_path = segment::segment_path(dir, base_pos);

        if SegmentIndex::has_companion_files(&seg_path) {
            // Sealed segment with valid index files — skip replay.
            continue;
        }

        // No companion files — either active segment or sealed segment
        // with missing index. Rebuild the index from segment data.
        let reader = SegmentReader::open(&seg_path)?;

        for result in reader.iter(None) {
            let event = result?;
            index.index_event(event.position, &event.name, &event.tags);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::AppendEvent;
    use crate::append::AppendCondition;
    use crate::criteria::{Criterion, SourcingCondition};
    use crate::event::Tag;

    fn tag(key: &str, value: &str) -> Tag {
        Tag::from_str(key, value)
    }

    fn make_event(name: &str, tags: Vec<Tag>) -> AppendEvent {
        AppendEvent {
            identifier: format!("id-{name}"),
            name: name.into(),
            version: "1.0".into(),
            timestamp: 1712345678000,
            payload: b"test-data".to_vec(),
            metadata: vec![],
            tags,
        }
    }

    #[test]
    fn create_and_append() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        let request = AppendRequest {
            condition: None,
            events: vec![
                make_event("OrderPlaced", vec![tag("orderId", "A")]),
                make_event("PaymentReceived", vec![tag("orderId", "A")]),
            ],
        };

        let response = store.append(request).unwrap();
        assert_eq!(response.first_position, Position(1));
        assert_eq!(response.count, 2);
        assert_eq!(response.consistency_marker, Position(2));
        assert_eq!(store.head(), Position(3));
    }

    #[test]
    fn dcb_condition_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        store
            .append(AppendRequest {
                condition: None,
                events: vec![make_event("OrderPlaced", vec![tag("orderId", "A")])],
            })
            .unwrap();

        let result = store.append(AppendRequest {
            condition: Some(AppendCondition {
                consistency_marker: Position(1),
                criteria: SourcingCondition {
                    criteria: vec![Criterion {
                        names: vec!["OrderPlaced".into()],
                        tags: vec![tag("orderId", "A")],
                    }],
                },
            }),
            events: vec![make_event("OrderConfirmed", vec![tag("orderId", "A")])],
        });

        assert!(result.is_ok());
    }

    #[test]
    fn dcb_condition_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        store
            .append(AppendRequest {
                condition: None,
                events: vec![
                    make_event("OrderPlaced", vec![tag("orderId", "A")]),
                    make_event("OrderCancelled", vec![tag("orderId", "A")]),
                ],
            })
            .unwrap();

        let result = store.append(AppendRequest {
            condition: Some(AppendCondition {
                consistency_marker: Position(0),
                criteria: SourcingCondition {
                    criteria: vec![Criterion {
                        names: vec![],
                        tags: vec![tag("orderId", "A")],
                    }],
                },
            }),
            events: vec![make_event("OrderPlaced", vec![tag("orderId", "A")])],
        });

        assert!(matches!(
            result,
            Err(Error::ConsistencyConditionViolated { .. })
        ));
    }

    #[test]
    fn source_query() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        store
            .append(AppendRequest {
                condition: None,
                events: vec![
                    make_event("OrderPlaced", vec![tag("orderId", "A")]),
                    make_event("OrderPlaced", vec![tag("orderId", "B")]),
                    make_event("PaymentReceived", vec![tag("orderId", "A")]),
                ],
            })
            .unwrap();

        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("orderId", "A")],
            }],
        };

        let events = store.source(Position(1), &cond).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].name, "OrderPlaced");
        assert_eq!(events[1].name, "PaymentReceived");
    }

    #[test]
    fn get_tags_from_segment() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        store
            .append(AppendRequest {
                condition: None,
                events: vec![make_event(
                    "OrderPlaced",
                    vec![tag("orderId", "A"), tag("region", "EU")],
                )],
            })
            .unwrap();

        let tags = store.get_tags(Position(1)).unwrap();
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&tag("orderId", "A")));
        assert!(tags.contains(&tag("region", "EU")));
    }

    #[test]
    fn tags_survive_recovery() {
        let dir = tempfile::tempdir().unwrap();

        {
            let store = EventStoreEngine::create(dir.path()).unwrap();
            store
                .append(AppendRequest {
                    condition: None,
                    events: vec![
                        make_event("OrderPlaced", vec![tag("orderId", "A")]),
                        make_event(
                            "PaymentReceived",
                            vec![tag("orderId", "A"), tag("paymentId", "P1")],
                        ),
                    ],
                })
                .unwrap();
        }

        {
            let store = EventStoreEngine::open(dir.path()).unwrap();
            assert_eq!(store.head(), Position(3));

            let cond = SourcingCondition {
                criteria: vec![Criterion {
                    names: vec![],
                    tags: vec![tag("orderId", "A")],
                }],
            };
            let events = store.source(Position(1), &cond).unwrap();
            assert_eq!(events.len(), 2);

            let cond = SourcingCondition {
                criteria: vec![Criterion {
                    names: vec![],
                    tags: vec![tag("paymentId", "P1")],
                }],
            };
            let events = store.source(Position(1), &cond).unwrap();
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].name, "PaymentReceived");

            // Tags readable from segment.
            let tags = store.get_tags(Position(1)).unwrap();
            assert!(tags.contains(&tag("orderId", "A")));
        }
    }

    #[test]
    fn head_and_tail() {
        let dir = tempfile::tempdir().unwrap();
        let store = EventStoreEngine::create(dir.path()).unwrap();

        assert_eq!(store.head(), Position(1));
        assert_eq!(store.tail(), Position(0));
    }
}
