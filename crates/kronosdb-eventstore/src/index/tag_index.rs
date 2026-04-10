use std::collections::HashMap;

use roaring::RoaringTreemap;

use crate::append::AppendCondition;
use crate::criteria::{Criterion, SourcingCondition};
use crate::event::{Position, Tag};

/// Reserved tag key used to index event type names.
/// When an event with name "OrderPlaced" is appended, we insert a synthetic
/// tag with key=EVENT_TYPE_TAG_KEY and value=b"OrderPlaced" into the index.
/// This lets us handle name-based filtering with the same bitmap operations
/// as tag-based filtering.
const EVENT_TYPE_TAG_KEY: &[u8] = b"__kronosdb_event_type__";

/// In-memory forward-only tag index using roaring bitmaps.
///
/// Maps tag(key,value) → bitmap of event positions.
/// Used for queries and DCB condition checks.
///
/// Tags are stored on events in the segment files (source of truth).
/// This index is rebuilt from segments on recovery. Tag mutations
/// (AddTags/RemoveTags) are replayed from the tag mutations WAL.
///
/// There is no reverse index (position → tags). GetTags reads the
/// event from the segment and applies mutations from the WAL.
///
/// This struct is NOT thread-safe on its own. The caller is responsible
/// for synchronization via RwLock.
pub struct TagIndex {
    /// Forward index: tag(key+value) → roaring bitmap of event positions.
    forward: HashMap<Vec<u8>, RoaringTreemap>,

    /// Bitmap of all known event positions. Used when a criterion has
    /// no tags and no names (matches everything).
    all_positions: RoaringTreemap,
}

impl TagIndex {
    pub fn new() -> Self {
        Self {
            forward: HashMap::new(),
            all_positions: RoaringTreemap::new(),
        }
    }

    /// Indexes an event's tags and name. Called on append.
    pub fn index_event(&mut self, position: Position, event_name: &str, tags: &[Tag]) {
        let pos = position.0;

        self.all_positions.insert(pos);

        // Index the event type name as a synthetic tag.
        let type_tag_key = make_forward_key(EVENT_TYPE_TAG_KEY, event_name.as_bytes());
        self.forward
            .entry(type_tag_key)
            .or_insert_with(RoaringTreemap::new)
            .insert(pos);

        // Index each user-provided tag.
        for tag in tags {
            let forward_key = make_forward_key(&tag.key, &tag.value);
            self.forward
                .entry(forward_key)
                .or_insert_with(RoaringTreemap::new)
                .insert(pos);
        }
    }

    /// Adds tags to an existing event's index. Used by the AddTags RPC.
    pub fn add_tags(&mut self, position: Position, tags: &[Tag]) {
        let pos = position.0;

        for tag in tags {
            let forward_key = make_forward_key(&tag.key, &tag.value);
            self.forward
                .entry(forward_key)
                .or_insert_with(RoaringTreemap::new)
                .insert(pos);
        }
    }

    /// Removes tags from an existing event's index. Used by the RemoveTags RPC.
    pub fn remove_tags(&mut self, position: Position, tags: &[Tag]) {
        let pos = position.0;

        for tag in tags {
            let forward_key = make_forward_key(&tag.key, &tag.value);
            if let Some(bitmap) = self.forward.get_mut(&forward_key) {
                bitmap.remove(pos);
            }
        }
    }

    /// Checks a DCB consistency condition.
    ///
    /// Returns `Some(position)` of the first conflicting event if the condition
    /// is violated, or `None` if it passes (safe to append).
    pub fn check_condition(&self, condition: &AppendCondition) -> Option<Position> {
        let after = condition.consistency_marker.0;
        self.find_matching_after(&condition.criteria, after)
    }

    /// Finds event positions matching a query, starting from `from_position` (inclusive).
    ///
    /// Returns matching positions in ascending order.
    pub fn matching_positions(
        &self,
        condition: &SourcingCondition,
        from_position: Position,
    ) -> Vec<u64> {
        let combined = self.resolve(condition);
        match combined {
            Some(bitmap) => bitmap
                .iter()
                .filter(|&pos| pos >= from_position.0)
                .collect(),
            None => vec![0u64; 0],
        }
    }

    /// Returns the raw roaring bitmap of matching positions.
    /// Used by the source operation for efficient segment skipping.
    pub fn matching_bitmap(
        &self,
        condition: &SourcingCondition,
        from_position: Position,
    ) -> Option<RoaringTreemap> {
        let mut bitmap = self.resolve(condition)?;
        if from_position.0 > 0 {
            bitmap.remove_range(0..from_position.0);
        }
        if bitmap.is_empty() {
            None
        } else {
            Some(bitmap)
        }
    }

    /// Finds the first event matching the condition after the given position.
    fn find_matching_after(&self, condition: &SourcingCondition, after: u64) -> Option<Position> {
        let combined = self.resolve(condition)?;
        combined.iter().find(|&pos| pos > after).map(Position)
    }

    /// Resolves a sourcing condition into a single bitmap of matching positions.
    /// OR across criteria, where each criterion is AND across tags.
    fn resolve(&self, condition: &SourcingCondition) -> Option<RoaringTreemap> {
        let mut result: Option<RoaringTreemap> = None;

        for criterion in &condition.criteria {
            if let Some(criterion_bitmap) = self.resolve_criterion(criterion) {
                match &mut result {
                    Some(existing) => *existing |= &criterion_bitmap,
                    None => result = Some(criterion_bitmap),
                }
            }
        }

        result
    }

    /// Resolves a single criterion into a bitmap.
    fn resolve_criterion(&self, criterion: &Criterion) -> Option<RoaringTreemap> {
        let mut parts: Vec<&RoaringTreemap> = Vec::new();

        for tag in &criterion.tags {
            let key = make_forward_key(&tag.key, &tag.value);
            match self.forward.get(&key) {
                Some(bitmap) => parts.push(bitmap),
                None => return None,
            }
        }

        let name_bitmap;
        if !criterion.names.is_empty() {
            let mut names_combined: Option<RoaringTreemap> = None;
            for name in &criterion.names {
                let key = make_forward_key(EVENT_TYPE_TAG_KEY, name.as_bytes());
                if let Some(bitmap) = self.forward.get(&key) {
                    match &mut names_combined {
                        Some(existing) => *existing |= bitmap,
                        None => names_combined = Some(bitmap.clone()),
                    }
                }
            }
            match names_combined {
                Some(bitmap) => {
                    name_bitmap = bitmap;
                    parts.push(&name_bitmap);
                }
                None => return None,
            }
        }

        if parts.is_empty() {
            return Some(self.all_positions.clone());
        }

        let mut result = parts[0].clone();
        for part in &parts[1..] {
            result &= *part;
        }

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }
}

/// Creates the forward index map key from a tag key and value.
fn make_forward_key(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(4 + key.len() + value.len());
    k.extend_from_slice(&(key.len() as u32).to_le_bytes());
    k.extend_from_slice(key);
    k.extend_from_slice(value);
    k
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tag(key: &str, value: &str) -> Tag {
        Tag::from_str(key, value)
    }

    #[test]
    fn index_and_query_by_single_tag() {
        let mut index = TagIndex::new();

        index.index_event(Position(1), "OrderPlaced", &[tag("orderId", "A")]);
        index.index_event(Position(2), "OrderPlaced", &[tag("orderId", "B")]);
        index.index_event(Position(3), "PaymentReceived", &[tag("orderId", "A")]);

        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("orderId", "A")],
            }],
        };

        let positions = index.matching_positions(&cond, Position(1));
        assert_eq!(positions, vec![1, 3]);
    }

    #[test]
    fn query_with_and_tags() {
        let mut index = TagIndex::new();

        index.index_event(
            Position(1),
            "OrderPlaced",
            &[tag("orderId", "A"), tag("region", "EU")],
        );
        index.index_event(
            Position(2),
            "OrderPlaced",
            &[tag("orderId", "A"), tag("region", "US")],
        );
        index.index_event(
            Position(3),
            "OrderPlaced",
            &[tag("orderId", "B"), tag("region", "EU")],
        );

        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("orderId", "A"), tag("region", "EU")],
            }],
        };

        let positions = index.matching_positions(&cond, Position(1));
        assert_eq!(positions, vec![1]);
    }

    #[test]
    fn query_with_or_criteria() {
        let mut index = TagIndex::new();

        index.index_event(Position(1), "OrderPlaced", &[tag("orderId", "A")]);
        index.index_event(Position(2), "OrderPlaced", &[tag("orderId", "B")]);
        index.index_event(Position(3), "OrderPlaced", &[tag("orderId", "C")]);

        let cond = SourcingCondition {
            criteria: vec![
                Criterion {
                    names: vec![],
                    tags: vec![tag("orderId", "A")],
                },
                Criterion {
                    names: vec![],
                    tags: vec![tag("orderId", "C")],
                },
            ],
        };

        let positions = index.matching_positions(&cond, Position(1));
        assert_eq!(positions, vec![1, 3]);
    }

    #[test]
    fn query_by_event_name() {
        let mut index = TagIndex::new();

        index.index_event(Position(1), "OrderPlaced", &[tag("orderId", "A")]);
        index.index_event(Position(2), "PaymentReceived", &[tag("orderId", "A")]);
        index.index_event(Position(3), "OrderShipped", &[tag("orderId", "A")]);

        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec!["OrderPlaced".into(), "OrderShipped".into()],
                tags: vec![tag("orderId", "A")],
            }],
        };

        let positions = index.matching_positions(&cond, Position(1));
        assert_eq!(positions, vec![1, 3]);
    }

    #[test]
    fn dcb_condition_passes_when_no_conflict() {
        let mut index = TagIndex::new();

        index.index_event(Position(1), "OrderPlaced", &[tag("orderId", "A")]);
        index.index_event(Position(2), "PaymentReceived", &[tag("orderId", "A")]);

        let condition = AppendCondition {
            consistency_marker: Position(2),
            criteria: SourcingCondition {
                criteria: vec![Criterion {
                    names: vec![],
                    tags: vec![tag("orderId", "A")],
                }],
            },
        };

        assert!(index.check_condition(&condition).is_none());
    }

    #[test]
    fn dcb_condition_fails_when_conflict_exists() {
        let mut index = TagIndex::new();

        index.index_event(Position(1), "OrderPlaced", &[tag("orderId", "A")]);
        index.index_event(Position(2), "PaymentReceived", &[tag("orderId", "A")]);
        index.index_event(Position(3), "OrderCancelled", &[tag("orderId", "A")]);

        let condition = AppendCondition {
            consistency_marker: Position(1),
            criteria: SourcingCondition {
                criteria: vec![Criterion {
                    names: vec![],
                    tags: vec![tag("orderId", "A")],
                }],
            },
        };

        let conflict = index.check_condition(&condition);
        assert_eq!(conflict, Some(Position(2)));
    }

    #[test]
    fn add_tags_updates_forward_index() {
        let mut index = TagIndex::new();

        index.index_event(Position(1), "OrderPlaced", &[tag("orderId", "A")]);

        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("region", "EU")],
            }],
        };
        assert_eq!(index.matching_positions(&cond, Position(1)), vec![0u64; 0]);

        index.add_tags(Position(1), &[tag("region", "EU")]);

        assert_eq!(index.matching_positions(&cond, Position(1)), vec![1]);
    }

    #[test]
    fn remove_tags_updates_forward_index() {
        let mut index = TagIndex::new();

        index.index_event(
            Position(1),
            "OrderPlaced",
            &[tag("orderId", "A"), tag("region", "EU")],
        );

        index.remove_tags(Position(1), &[tag("region", "EU")]);

        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("region", "EU")],
            }],
        };
        assert_eq!(index.matching_positions(&cond, Position(1)), vec![0u64; 0]);

        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("orderId", "A")],
            }],
        };
        assert_eq!(index.matching_positions(&cond, Position(1)), vec![1]);
    }

    #[test]
    fn query_from_position() {
        let mut index = TagIndex::new();

        for i in 1..=5 {
            index.index_event(Position(i), "Event", &[tag("stream", "main")]);
        }

        let cond = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![tag("stream", "main")],
            }],
        };

        let positions = index.matching_positions(&cond, Position(3));
        assert_eq!(positions, vec![3, 4, 5]);
    }
}
