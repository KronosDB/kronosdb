//! Snapshots on the log (ADR-0005).
//!
//! A snapshot is not a file in a side store — it is a record in the log, so
//! it inherits quorum durability, replication, backup, and crash recovery
//! rather than reimplementing them. Each snapshot is one `$snapshot.written`
//! system event (`docs/system-events.md`), invisible to the ordinary read
//! path and served back only through the dedicated snapshot RPCs:
//!
//! ```text
//! $snapshot.written    opaque client state, tagged `$snapshot:{key}`
//! ```
//!
//! The server never interprets a snapshot. The key is one client-composed
//! byte string (the client folds any entity id into it), the state is opaque
//! bytes, and `position` is the client's fold-time consistency marker
//! (next-exclusive — the sequence replay resumes AT), NOT the record's own
//! log position (the record lands later). All semantics — fitness, invalidation,
//! versioning — are the client's business; invalidation is the client
//! renaming its key. A newer snapshot supersedes older ones purely by being
//! later in the log; superseded records remain as inert history.

use serde::{Deserialize, Serialize};

use crate::append::AppendRequest;
use crate::criteria::{Criterion, SourcingCondition};
use crate::error::Error;
use crate::event::{AppendEvent, Position, Tag};
use crate::store::EventStoreEngine;

/// Appended when a client stores a snapshot. Payload is a bincoded
/// [`SnapshotRecord`].
pub const WRITTEN: &str = "$snapshot.written";

/// Correlation tag: its value is the client's opaque key. `$`-prefixed, so
/// a client DCB condition can structurally never match a snapshot record.
const KEY_TAG: &str = "$snapshot";

/// A snapshot as served back to a client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// Opaque client state, returned byte-exact.
    pub state: Vec<u8>,
    /// The fold-time consistency marker. Next-exclusive: events with
    /// position >= this are not summarized by `state` and must be replayed
    /// on top of it.
    pub position: Position,
}

/// The on-log payload of a `$snapshot.written` event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SnapshotRecord {
    fold_position: u64,
    state: Vec<u8>,
}

/// The tag correlating snapshot records for one key.
fn key_tag(key: &[u8]) -> Tag {
    Tag::new(KEY_TAG.as_bytes().to_vec(), key.to_vec())
}

/// Matches every snapshot record for one key.
fn by_key(key: &[u8]) -> SourcingCondition {
    SourcingCondition {
        criteria: vec![Criterion {
            names: vec![],
            tags: vec![key_tag(key)],
        }],
    }
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn encode(record: &SnapshotRecord) -> Result<Vec<u8>, Error> {
    bincode::serialize(record).map_err(|error| Error::Corrupted {
        message: format!("could not encode snapshot: {error}"),
    })
}

fn decode(bytes: &[u8]) -> Result<SnapshotRecord, Error> {
    bincode::deserialize(bytes).map_err(|error| Error::Corrupted {
        message: format!("could not decode snapshot: {error}"),
    })
}

/// Builds the append that stores a snapshot. Unconditional: the latest
/// record for a key wins purely by log order, so concurrent writers need no
/// coordination — both records land, the later one supersedes.
///
/// Public so the cluster routing layer can build the request on a follower
/// and forward it to the leader as a system append.
pub fn append_request(key: &[u8], state: Vec<u8>, fold_position: Position) -> AppendRequest {
    AppendRequest {
        condition: None,
        events: vec![AppendEvent {
            identifier: format!("snapshot-{}", fold_position.0),
            name: WRITTEN.into(),
            version: "1".into(),
            timestamp: now_ms(),
            payload: encode(&SnapshotRecord {
                fold_position: fold_position.0,
                state,
            })
            .expect("bincode of bytes+u64 cannot fail"),
            metadata: vec![],
            tags: vec![key_tag(key)],
        }],
    }
}

/// Stores a snapshot, returning the log position of its record. Blocking —
/// the record is quorum-durable on return, like any append.
pub fn append(
    engine: &EventStoreEngine,
    key: &[u8],
    state: Vec<u8>,
    fold_position: Position,
) -> Result<Position, Error> {
    let response = engine.append_system(append_request(key, state, fold_position))?;
    Ok(response.first_position)
}

/// The latest snapshot for a key whose record landed strictly below `below`,
/// or `None` — a miss is always legal and always safe (the caller replays
/// from the beginning).
///
/// `below` is how the fused read stays one consistent view: bounded by the
/// marker frozen at the start of the read, a record that lands mid-read can
/// never be returned with state summarizing positions past that marker.
pub fn latest(
    engine: &EventStoreEngine,
    key: &[u8],
    below: Option<Position>,
) -> Result<Option<Snapshot>, Error> {
    let below = below.unwrap_or(Position(u64::MAX));
    let Some(record_pos) = engine.latest_matching(&by_key(key), below)? else {
        return Ok(None);
    };
    let stored = engine.read_stored_at(record_pos)?;
    let record = decode(&stored.payload)?;
    Ok(Some(Snapshot {
        state: record.state,
        position: Position(record.fold_position),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Tag;
    use crate::store::EventStoreEngine;

    fn engine() -> (tempfile::TempDir, EventStoreEngine) {
        let dir = tempfile::tempdir().unwrap();
        let engine = EventStoreEngine::create(dir.path()).unwrap();
        (dir, engine)
    }

    fn user_event(name: &str, tags: Vec<Tag>) -> AppendEvent {
        AppendEvent {
            identifier: format!("id-{name}"),
            name: name.into(),
            version: "1".into(),
            timestamp: 0,
            payload: b"payload".to_vec(),
            metadata: vec![],
            tags,
        }
    }

    fn append_user(engine: &EventStoreEngine, name: &str) -> Position {
        engine
            .append(AppendRequest {
                condition: None,
                events: vec![user_event(name, vec![Tag::from_str("orderId", "1")])],
            })
            .unwrap()
            .first_position
    }

    #[test]
    fn a_miss_is_none_not_an_error() {
        let (_dir, engine) = engine();
        assert_eq!(latest(&engine, b"course:cs-101", None).unwrap(), None);
    }

    #[test]
    fn roundtrip_state_and_fold_position() {
        let (_dir, engine) = engine();
        append_user(&engine, "CourseCreated");
        let marker = engine.head();

        append(&engine, b"course:cs-101", b"folded state".to_vec(), marker).unwrap();

        let snap = latest(&engine, b"course:cs-101", None).unwrap().unwrap();
        assert_eq!(snap.state, b"folded state");
        assert_eq!(snap.position, marker);
    }

    #[test]
    fn latest_wins_by_log_order() {
        let (_dir, engine) = engine();
        append(&engine, b"k", b"v1".to_vec(), Position(1)).unwrap();
        append(&engine, b"k", b"v2".to_vec(), Position(2)).unwrap();
        append(&engine, b"k", b"v3".to_vec(), Position(3)).unwrap();

        let snap = latest(&engine, b"k", None).unwrap().unwrap();
        assert_eq!(snap.state, b"v3");
    }

    #[test]
    fn keys_are_isolated() {
        let (_dir, engine) = engine();
        append(&engine, b"a", b"state-a".to_vec(), Position(0)).unwrap();
        append(&engine, b"b", b"state-b".to_vec(), Position(0)).unwrap();

        assert_eq!(
            latest(&engine, b"a", None).unwrap().unwrap().state,
            b"state-a"
        );
        assert_eq!(
            latest(&engine, b"b", None).unwrap().unwrap().state,
            b"state-b"
        );
        assert_eq!(latest(&engine, b"c", None).unwrap(), None);
    }

    #[test]
    fn below_bound_excludes_later_records() {
        let (_dir, engine) = engine();
        let first = append(&engine, b"k", b"old".to_vec(), Position(0)).unwrap();
        append(&engine, b"k", b"new".to_vec(), Position(1)).unwrap();

        // Bounded at the second record's position: only the first is visible.
        let snap = latest(&engine, b"k", Some(Position(first.0 + 1)))
            .unwrap()
            .unwrap();
        assert_eq!(snap.state, b"old");

        // Bounded at the first record: nothing is visible.
        assert_eq!(latest(&engine, b"k", Some(first)).unwrap(), None);
    }

    #[test]
    fn snapshot_records_are_invisible_to_the_client_read_path() {
        let (_dir, engine) = engine();
        append_user(&engine, "CourseCreated");
        append(&engine, b"k", b"state".to_vec(), Position(1)).unwrap();

        // The client read path never returns the record...
        let all = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![],
            }],
        };
        let events = engine.source(Position(0), &all).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, "CourseCreated");

        // ...and the visible head hides its position.
        assert_eq!(engine.visible_head(), Position(1));
        assert_eq!(engine.head(), Position(2));
    }

    #[test]
    fn snapshot_records_never_trip_client_dcb_conditions() {
        let (_dir, engine) = engine();
        append(&engine, b"orderId", b"state".to_vec(), Position(0)).unwrap();

        // A client condition on a user tag whose value happens to equal a
        // snapshot key must not conflict: the tag KEY differs.
        let response = engine.append(AppendRequest {
            condition: Some(crate::append::AppendCondition {
                consistency_marker: Position(0),
                criteria: SourcingCondition {
                    criteria: vec![Criterion {
                        names: vec![],
                        tags: vec![Tag::from_str("orderId", "orderId")],
                    }],
                },
            }),
            events: vec![user_event(
                "OrderPlaced",
                vec![Tag::from_str("orderId", "1")],
            )],
        });
        assert!(response.is_ok());
    }

    #[test]
    fn clients_cannot_forge_or_query_snapshot_records() {
        let (_dir, engine) = engine();

        // Forging the event type is rejected.
        let forged = engine.append(AppendRequest {
            condition: None,
            events: vec![user_event(WRITTEN, vec![])],
        });
        assert!(matches!(forged, Err(Error::ReservedNamespace { .. })));

        // Querying by the snapshot tag is rejected.
        let condition = by_key(b"k");
        assert!(engine.source(Position(0), &condition).is_err());
    }

    #[test]
    fn latest_survives_segment_rotation() {
        let dir = tempfile::tempdir().unwrap();
        // Tiny segments force rotation so the lookup exercises the sealed
        // bloom → index → max path, not just the active tag index.
        let engine = EventStoreEngine::create_with_options(dir.path(), 4096).unwrap();

        append(&engine, b"k", b"early".to_vec(), Position(0)).unwrap();
        for i in 0..64 {
            append_user(&engine, &format!("Event{i}"));
        }
        let marker = engine.head();
        append(&engine, b"k", b"late".to_vec(), marker).unwrap();
        for i in 0..64 {
            append_user(&engine, &format!("More{i}"));
        }

        let snap = latest(&engine, b"k", None).unwrap().unwrap();
        assert_eq!(snap.state, b"late");
        assert_eq!(snap.position, marker);
    }
}
