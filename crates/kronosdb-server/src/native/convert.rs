//! Frames to engine types and back. Plain functions, no I/O.

use bytes::Bytes;
use kronosdb_eventstore::append::RecordBatch;
use kronosdb_eventstore::criteria::{Criterion, SourcingCondition};
use kronosdb_eventstore::error::Error;
use kronosdb_eventstore::wire;
use kronosdb_protocol as p;
use kronosdb_protocol::ErrorCode;
use kronosdb_protocol::net::Failure;

pub use kronosdb_eventstore::wire::{append_condition, criterion};

/// Read criteria: an empty list matches every event. The engine resolves an
/// empty list to no matches, so it becomes one criterion that matches all.
pub fn read_condition(criteria: &[p::Criterion<'_>]) -> SourcingCondition {
    if criteria.is_empty() {
        return SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![],
            }],
        };
    }
    SourcingCondition {
        criteria: criteria.iter().map(criterion).collect(),
    }
}

/// The events of an append, kept as the records they arrived in once they
/// have been checked. `backing` is the frame they came from.
pub fn record_batch(records: &p::Records<'_>, backing: &Bytes) -> Result<RecordBatch, Failure> {
    wire::record_batch(records, backing).map_err(|bad| invalid(bad.0))
}

pub fn invalid(message: impl Into<String>) -> Failure {
    Failure::new(ErrorCode::INVALID_ARGUMENT, message)
}

/// The one place engine errors become protocol failures. A consistency
/// conflict is not one: callers answer it with AppendConflict.
pub fn failure(e: Error) -> Failure {
    let (code, message) = match e {
        Error::ConsistencyConditionViolated {
            conflicting_position,
        } => (
            ErrorCode::REJECTED,
            format!(
                "consistency condition violated: conflicting event at position {}",
                conflicting_position.0
            ),
        ),
        Error::Io(err) => (ErrorCode::INTERNAL, format!("I/O error: {err}")),
        Error::Corrupted { message } => {
            (ErrorCode::CORRUPTED, format!("data corrupted: {message}"))
        }
        Error::Unavailable { message } => (
            ErrorCode::UNAVAILABLE,
            format!("temporarily unavailable: {message}"),
        ),
        Error::Rejected { message } => (ErrorCode::REJECTED, message),
        Error::Internal { message } => (ErrorCode::INTERNAL, format!("internal error: {message}")),
        Error::PositionNotFound { position } => (
            ErrorCode::NOT_FOUND,
            format!("no event at position {position}"),
        ),
        Error::ContextNotFound { name } => {
            (ErrorCode::NOT_FOUND, format!("context not found: {name}"))
        }
        Error::ContextAlreadyExists { name } => (
            ErrorCode::ALREADY_EXISTS,
            format!("context already exists: {name}"),
        ),
        Error::InvalidContextName { name, reason } => (
            ErrorCode::INVALID_ARGUMENT,
            format!("invalid context name '{name}': {reason}"),
        ),
        Error::SnapshotNotFound { key } => {
            (ErrorCode::NOT_FOUND, format!("snapshot not found: {key}"))
        }
        Error::ReservedNamespace { detail } => (
            ErrorCode::INVALID_ARGUMENT,
            format!("reserved namespace: {detail}"),
        ),
    };
    Failure::new(code, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kronosdb_eventstore::event::Tag;

    #[test]
    fn empty_read_criteria_match_everything() {
        let c = read_condition(&[]);
        assert_eq!(c.criteria.len(), 1);
        assert!(c.criteria[0].names.is_empty() && c.criteria[0].tags.is_empty());
    }

    #[test]
    fn append_records_are_kept_as_they_arrived() {
        let mut buf = p::RecordsBuf::new();
        buf.push(&p::EventRecord {
            position: 0,
            identifier: "e-1",
            name: "OrderPlaced",
            version: "1",
            timestamp: 5,
            metadata: vec![p::MetadataEntry {
                key: "k",
                value: "v",
            }],
            tags: vec![p::Tag {
                key: b"orderId",
                value: b"o-1",
            }],
            payload: b"{}",
        })
        .unwrap();
        let frame = Bytes::copy_from_slice(buf.as_records().bytes());
        let records = p::Records::from_raw(buf.len(), &frame).unwrap();
        let batch = record_batch(&records, &frame).unwrap();
        assert_eq!(batch.len(), 1);
        assert_eq!(batch.bytes(), records.bytes());
        // The same record, decoded the long way.
        let events = wire::append_events(&records).unwrap();
        assert_eq!(events[0].name, "OrderPlaced");
        assert_eq!(events[0].tags, vec![Tag::from_str("orderId", "o-1")]);
        assert_eq!(events[0].metadata, vec![("k".into(), "v".into())]);
    }

    #[test]
    fn a_malformed_record_is_an_invalid_argument_with_the_decoders_reason() {
        // flags 1, then a body cut short.
        for bytes in [vec![1u8, 0, 0, 0, 1], vec![3, 0, 0, 0, 0, 1, 2]] {
            let backing = Bytes::copy_from_slice(&bytes);
            let records = p::Records::from_raw(1, &backing).unwrap();
            let failure = record_batch(&records, &backing).err().unwrap();
            let expected = wire::append_events(&records).err().unwrap();
            assert_eq!(failure.code, ErrorCode::INVALID_ARGUMENT);
            assert_eq!(failure.message, expected.0);
        }
    }
}
