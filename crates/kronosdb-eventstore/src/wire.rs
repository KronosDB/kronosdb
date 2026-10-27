//! Engine types to and from native protocol frames. Plain functions, no I/O,
//! shared by the server's client-facing handlers and the internode services.

use kronosdb_protocol as p;
use kronosdb_protocol::ErrorCode;
use kronosdb_protocol::net::{Failure, NetError};

use bytes::Bytes;

use crate::append::{AppendCondition, RecordBatch};
use crate::criteria::{Criterion, SourcingCondition};
use crate::error::Error;
use crate::event::{AppendEvent, Position, Tag};
use crate::segment::format;

pub fn tag(t: &p::Tag<'_>) -> Tag {
    Tag::new(t.key.to_vec(), t.value.to_vec())
}

pub fn criterion(c: &p::Criterion<'_>) -> Criterion {
    Criterion {
        names: c.names.iter().map(|n| n.to_string()).collect(),
        tags: c.tags.iter().map(tag).collect(),
    }
}

/// Append conditions keep an empty list empty: it conflicts with nothing.
pub fn append_condition(c: &p::Condition<'_>) -> AppendCondition {
    AppendCondition {
        consistency_marker: Position(c.marker),
        criteria: SourcingCondition {
            criteria: c.criteria.iter().map(criterion).collect(),
        },
    }
}

/// A record in an append that is not an event, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BadRecord(pub String);

/// The events of an append, from records in their stored layout.
pub fn append_events(records: &p::Records<'_>) -> Result<Vec<AppendEvent>, BadRecord> {
    records
        .iter()
        .enumerate()
        .map(|(i, record)| {
            if record.flags != 0 {
                return Err(BadRecord(format!(
                    "record {i} has flags {:#04x}; an event has 0",
                    record.flags
                )));
            }
            let event = record
                .event()
                .map_err(|e| BadRecord(format!("record {i} is not an event: {e}")))?;
            Ok(AppendEvent {
                identifier: event.identifier.to_string(),
                name: event.name.to_string(),
                version: event.version.to_string(),
                timestamp: event.timestamp,
                payload: event.payload.to_vec(),
                metadata: event
                    .metadata
                    .iter()
                    .map(|m| (m.key.to_string(), m.value.to_string()))
                    .collect(),
                tags: event.tags.iter().map(tag).collect(),
            })
        })
        .collect()
}

/// The records of an append, checked in place and kept as they are: the
/// client's bytes become the batch, with nothing decoded or copied. Accepts
/// and rejects exactly what [`append_events`] does, with the same messages;
/// the reserved `$` namespace is the engine's to refuse
/// ([`RecordBatch::check_client`]).
///
/// `backing` is the buffer `records` was decoded from, so the batch can share
/// it; records from anywhere else are copied.
pub fn record_batch(records: &p::Records<'_>, backing: &Bytes) -> Result<RecordBatch, BadRecord> {
    let mut reserved = false;
    for (i, record) in records.iter().enumerate() {
        if record.flags != 0 {
            return Err(BadRecord(format!(
                "record {i} has flags {:#04x}; an event has 0",
                record.flags
            )));
        }
        let Some(scan) = format::check_event(record.body) else {
            // Only the protocol's decoder says why, and only a bad record
            // pays for asking.
            let why = match record.event() {
                Err(e) => e.to_string(),
                Ok(_) => "the event is malformed".to_string(),
            };
            return Err(BadRecord(format!("record {i} is not an event: {why}")));
        };
        reserved |= crate::system::is_system_name_bytes(scan.name)
            || scan
                .tags()
                .any(|(key, _)| key.first() == Some(&crate::system::SYSTEM_PREFIX));
    }
    let bytes = records.bytes();
    let (from, len) = (backing.as_ptr() as usize, backing.len());
    let at = bytes.as_ptr() as usize;
    let shared = if bytes.is_empty() || (at >= from && at + bytes.len() <= from + len) {
        backing.slice_ref(bytes)
    } else {
        Bytes::copy_from_slice(bytes)
    };
    Ok(RecordBatch::from_checked(records.len(), shared, reserved))
}

/// A batch as the wire's `records`, to forward without re-encoding.
pub fn batch_records(batch: &RecordBatch) -> Result<p::Records<'_>, Error> {
    p::Records::from_raw(batch.len(), batch.bytes()).map_err(|e| Error::Internal {
        message: format!("a checked batch does not frame as records: {e}"),
    })
}

/// The records of an append, in the layout the wire carries (position 0, no CRC).
pub fn append_records(events: &[AppendEvent]) -> Result<p::RecordsBuf, p::EncodeError> {
    let mut records = p::RecordsBuf::new();
    for event in events {
        records.push(&p::EventRecord {
            position: 0,
            identifier: &event.identifier,
            name: &event.name,
            version: &event.version,
            timestamp: event.timestamp,
            metadata: event
                .metadata
                .iter()
                .map(|(key, value)| p::MetadataEntry { key, value })
                .collect(),
            tags: event
                .tags
                .iter()
                .map(|t| p::Tag {
                    key: &t.key,
                    value: &t.value,
                })
                .collect(),
            payload: &event.payload,
        })?;
    }
    Ok(records)
}

pub fn wire_condition(condition: &AppendCondition) -> p::Condition<'_> {
    p::Condition {
        marker: condition.consistency_marker.0,
        criteria: condition
            .criteria
            .criteria
            .iter()
            .map(|c| p::Criterion {
                names: c.names.iter().map(String::as_str).collect(),
                tags: c
                    .tags
                    .iter()
                    .map(|t| p::Tag {
                        key: &t.key,
                        value: &t.value,
                    })
                    .collect(),
            })
            .collect(),
    }
}

/// What a failed peer call means to the caller, by what the peer said or by
/// how the connection ended. Anything short of the peer answering is
/// `Unavailable`: the peer may be down, restarting or partitioned, and the
/// same call is expected to work again.
pub fn net_error(error: NetError) -> Error {
    match error {
        NetError::Failed(failure) => failure_error(failure),
        other => Error::Unavailable {
            message: other.to_string(),
        },
    }
}

fn failure_error(failure: Failure) -> Error {
    let message = failure.message;
    match failure.code {
        ErrorCode::UNAVAILABLE | ErrorCode::CANCELLED => Error::Unavailable { message },
        ErrorCode::REJECTED
        | ErrorCode::INVALID_ARGUMENT
        | ErrorCode::NOT_FOUND
        | ErrorCode::ALREADY_EXISTS
        | ErrorCode::UNAUTHENTICATED
        | ErrorCode::PERMISSION_DENIED
        | ErrorCode::RESOURCE_EXHAUSTED => Error::Rejected { message },
        ErrorCode::CORRUPTED => Error::Corrupted { message },
        _ => Error::Internal { message },
    }
}

/// `error` with what was being attempted put in front of its message, class
/// kept: a forwarded call that fails stays `Unavailable` (retry) or
/// `Rejected` (don't), and says which call it was.
pub fn with_context(error: Error, what: impl std::fmt::Display) -> Error {
    match error {
        Error::Unavailable { message } => Error::Unavailable {
            message: format!("{what}: {message}"),
        },
        Error::Rejected { message } => Error::Rejected {
            message: format!("{what}: {message}"),
        },
        Error::Corrupted { message } => Error::Corrupted {
            message: format!("{what}: {message}"),
        },
        Error::Internal { message } => Error::Internal {
            message: format!("{what}: {message}"),
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event() -> AppendEvent {
        AppendEvent {
            identifier: "e-1".into(),
            name: "OrderPlaced".into(),
            version: "1".into(),
            timestamp: 5,
            payload: b"{}".to_vec(),
            metadata: vec![("k".into(), "v".into())],
            tags: vec![Tag::from_str("orderId", "o-1")],
        }
    }

    #[test]
    fn empty_append_criteria_conflict_with_nothing() {
        let c = append_condition(&p::Condition {
            marker: 7,
            criteria: vec![],
        });
        assert!(c.criteria.criteria.is_empty());
        assert_eq!(c.consistency_marker, Position(7));
    }

    #[test]
    fn append_events_round_trip_through_records() {
        let records = append_records(&[event()]).unwrap();
        let events = append_events(&records.as_records()).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].name, "OrderPlaced");
        assert_eq!(events[0].payload, b"{}");
        assert_eq!(events[0].tags, vec![Tag::from_str("orderId", "o-1")]);
        assert_eq!(events[0].metadata, vec![("k".into(), "v".into())]);
    }

    #[test]
    fn conditions_round_trip_through_the_wire_form() {
        let condition = AppendCondition {
            consistency_marker: Position(9),
            criteria: SourcingCondition {
                criteria: vec![Criterion {
                    names: vec!["OrderPlaced".into()],
                    tags: vec![Tag::from_str("orderId", "o-1")],
                }],
            },
        };
        let back = append_condition(&wire_condition(&condition));
        assert_eq!(back.consistency_marker, Position(9));
        assert_eq!(back.criteria.criteria.len(), 1);
        assert_eq!(back.criteria.criteria[0].names, vec!["OrderPlaced"]);
        assert_eq!(
            back.criteria.criteria[0].tags,
            vec![Tag::from_str("orderId", "o-1")]
        );
    }

    #[test]
    fn peer_failures_keep_their_class() {
        let failed = |code| NetError::Failed(Failure::new(code, "why"));
        assert!(matches!(
            net_error(failed(ErrorCode::UNAVAILABLE)),
            Error::Unavailable { .. }
        ));
        assert!(matches!(
            net_error(failed(ErrorCode::PERMISSION_DENIED)),
            Error::Rejected { .. }
        ));
        assert!(matches!(
            net_error(failed(ErrorCode::CORRUPTED)),
            Error::Corrupted { .. }
        ));
        assert!(matches!(
            net_error(NetError::Closed),
            Error::Unavailable { .. }
        ));
    }

    #[test]
    fn context_keeps_the_class() {
        let error = with_context(
            Error::Rejected {
                message: "no".into(),
            },
            "forward append",
        );
        assert!(matches!(error, Error::Rejected { message } if message == "forward append: no"));
    }
}
