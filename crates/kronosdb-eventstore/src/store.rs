use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::sync::broadcast;

use crate::error::Error;
use crate::append::{AppendRequest, AppendResponse};
use crate::criteria::SourcingCondition;
use crate::event::{Position, SequencedEvent, Tag};
use crate::stream::{CommitNotification, EventStream};

use crate::index::tag_index::TagIndex;
use crate::segment::reader::SegmentReader;
use crate::segment::segment_index::SegmentIndex;
use crate::segment::writer::SegmentWriter;
use crate::segment::{self, DEFAULT_SEGMENT_SIZE};

/// Default capacity for the commit notification channel.
const COMMIT_CHANNEL_CAPACITY: usize = 256;

/// The main event store. Combines the segment writer, tag index, and
/// concurrency control into a single API.
///
/// Write operations go through the single writer (&mut self).
/// Read operations can proceed concurrently.
///
/// Tags are stored on events in segments (source of truth).
/// The tag index is a derived in-memory structure rebuilt from segments on recovery.
/// Tag mutations (retagging, redaction) are handled by the transformation system,
/// not by direct mutation on the store.
pub struct EventStore {
    /// Directory where segment files are stored.
    dir: PathBuf,

    /// The segment writer. Only accessed by the write path.
    writer: SegmentWriter,

    /// The tag index for the active segment. Protected by RwLock for concurrent read access.
    tag_index: Arc<RwLock<TagIndex>>,

    /// The committed position — the position of the last event visible to readers.
    committed_position: Arc<AtomicU64>,

    /// Broadcast channel for notifying stream subscribers of new commits.
    commit_tx: broadcast::Sender<CommitNotification>,

    /// Maximum segment size.
    max_segment_size: u64,
}

impl EventStore {
    /// Creates a new event store in the given directory.
    pub fn create(dir: &Path) -> Result<Self, Error> {
        Self::create_with_options(dir, DEFAULT_SEGMENT_SIZE)
    }

    /// Creates a new event store with custom segment size.
    pub fn create_with_options(dir: &Path, max_segment_size: u64) -> Result<Self, Error> {
        std::fs::create_dir_all(dir)?;
        let writer = SegmentWriter::new(dir, Position(1), max_segment_size)?;
        let (commit_tx, _) = broadcast::channel(COMMIT_CHANNEL_CAPACITY);

        Ok(Self {
            dir: dir.to_path_buf(),
            writer,
            tag_index: Arc::new(RwLock::new(TagIndex::new())),
            committed_position: Arc::new(AtomicU64::new(0)),
            commit_tx,
            max_segment_size,
        })
    }

    /// Opens an existing event store, recovering from the last valid state.
    ///
    /// Sealed segments with valid `.idx` files don't need replay.
    /// The active segment (no `.idx`) is replayed to rebuild its in-memory tag index.
    pub fn open(dir: &Path) -> Result<Self, Error> {
        Self::open_with_options(dir, DEFAULT_SEGMENT_SIZE)
    }

    /// Opens an existing event store with custom segment size.
    pub fn open_with_options(dir: &Path, max_segment_size: u64) -> Result<Self, Error> {
        let writer = SegmentWriter::open(dir, max_segment_size)?;
        let head = writer.head();

        // Rebuild the active segment's tag index from its events.
        // Sealed segments have their own `.idx` files on disk.
        let mut tag_index = TagIndex::new();
        rebuild_active_segment_index(dir, &mut tag_index)?;

        let committed = if head.0 > 0 { head.0 - 1 } else { 0 };
        let (commit_tx, _) = broadcast::channel(COMMIT_CHANNEL_CAPACITY);

        Ok(Self {
            dir: dir.to_path_buf(),
            writer,
            tag_index: Arc::new(RwLock::new(tag_index)),
            committed_position: Arc::new(AtomicU64::new(committed)),
            commit_tx,
            max_segment_size,
        })
    }

    /// Appends events to the store, optionally with a DCB consistency condition.
    ///
    /// 1. Checks DCB condition against the tag index
    /// 2. Writes events to the active segment (tags included on disk)
    /// 3. Updates the in-memory tag index
    /// 4. Advances the committed position
    pub fn append(&mut self, request: AppendRequest) -> Result<AppendResponse, Error> {
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
            let head = self.writer.head();
            return Ok(AppendResponse {
                first_position: head,
                count: 0,
                consistency_marker: Position(self.committed_position.load(Ordering::Acquire)),
            });
        }

        // Step 2: Write events to segment (tags are persisted with the event).
        let (first_position, count) = self.writer.append(&request.events)?;

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

        // Find the segment containing this position and read the event.
        let segment_bases = segment::list_segment_files(&self.dir)?;
        let seg_idx = match segment_bases.binary_search(&position.0) {
            Ok(i) => i,
            Err(0) => {
                return Err(Error::Corrupted {
                    message: format!("no segment contains position {}", position.0),
                })
            }
            Err(i) => i - 1,
        };

        let seg_path = segment::segment_path(&self.dir, segment_bases[seg_idx]);
        let reader = SegmentReader::open(&seg_path)?;

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
    ///
    /// The stream will first replay all matching historical events from
    /// `from_position` (like Source), then switch to live mode and push
    /// new matching events as they are appended.
    ///
    /// The caller is responsible for driving the stream by:
    /// 1. Calling `source()` to get historical events up to the current head
    /// 2. Waiting on `stream.receiver` for commit notifications
    /// 3. Calling `source()` again from the cursor to get new events
    /// 4. Repeating until cancelled
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
        self.writer.head()
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
    /// For sealed segments: loads per-segment `.idx` files (with bloom filter skip).
    /// For the active segment: uses the in-memory tag index.
    pub fn source(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
    ) -> Result<Vec<SequencedEvent>, Error> {
        let committed = self.committed_position.load(Ordering::Acquire);

        let segment_bases = segment::list_segment_files(&self.dir)?;
        if segment_bases.is_empty() {
            return Ok(vec![]);
        }

        let mut events = Vec::new();

        for (i, &base) in segment_bases.iter().enumerate() {
            let seg_path = segment::segment_path(&self.dir, base);
            let is_last = i + 1 == segment_bases.len();
            let seg_end = if !is_last {
                segment_bases[i + 1] - 1
            } else {
                committed
            };

            if base > committed {
                break;
            }
            if seg_end < from_position.0 {
                continue;
            }

            // Determine matching positions for this segment.
            let matching_positions = if SegmentIndex::has_companion_files(&seg_path) {
                // Sealed segment — load per-segment index from disk.
                // TODO: LRU cache for loaded indices.
                let seg_index = SegmentIndex::read_idx(&seg_path.with_extension("idx"))?;
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

            // Forward-scan the segment, collecting matching events.
            let reader = SegmentReader::open(&seg_path)?;

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
        let mut store = EventStore::create(dir.path()).unwrap();

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
        let mut store = EventStore::create(dir.path()).unwrap();

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
        let mut store = EventStore::create(dir.path()).unwrap();

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
        let mut store = EventStore::create(dir.path()).unwrap();

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
        let mut store = EventStore::create(dir.path()).unwrap();

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
            let mut store = EventStore::create(dir.path()).unwrap();
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
            let store = EventStore::open(dir.path()).unwrap();
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
        let store = EventStore::create(dir.path()).unwrap();

        assert_eq!(store.head(), Position(1));
        assert_eq!(store.tail(), Position(0));
    }
}
