use bytes::Bytes;

use crate::criteria::SourcingCondition;
use crate::error::Error;
use crate::event::{AppendEvent, Position};
use crate::segment::format;

/// The DCB consistency condition for an append.
///
/// "Reject this append if any event matching the criteria exists
///  at a position greater than `consistency_marker`."
///
/// The `consistency_marker` is typically the position returned by a
/// previous Source call — it represents the point up to which the
/// client has already observed events.
#[derive(Debug, Clone)]
pub struct AppendCondition {
    /// Position after which to check for conflicting events.
    /// Events at positions > consistency_marker are checked.
    pub consistency_marker: Position,
    /// The criteria defining which events would conflict.
    pub criteria: SourcingCondition,
}

/// Request to append events, optionally with a DCB condition.
pub struct AppendRequest {
    /// If present, the append is rejected if the condition is violated.
    pub condition: Option<AppendCondition>,
    /// Events to append. All-or-nothing: either all are appended or none.
    pub events: Vec<AppendEvent>,
}

/// An append whose events are already event records, as clients send them.
/// The cheap way in: nothing is decoded into [`AppendEvent`]s on the way to
/// the segment.
pub struct RecordAppend {
    /// If present, the append is rejected if the condition is violated.
    pub condition: Option<AppendCondition>,
    /// Events to append. All-or-nothing: either all are appended or none.
    pub events: RecordBatch,
}

/// Event records in the layout the protocol carries and segments store,
/// minus the CRC: `len u32 | flags u8 | body` back to back, `flags` 0 and the
/// body an event whose first eight bytes are its position.
///
/// Every record in a batch has been checked: framed, a well-formed event,
/// valid UTF-8 where the format says text. The writer sets each position and
/// computes each CRC as it copies the records into the segment, and indexes
/// each event's type and tags by reading them in place.
#[derive(Clone)]
pub struct RecordBatch {
    count: u32,
    bytes: Bytes,
    /// Some event has a `$` event type or a tag key starting with `$`: a
    /// client may not append it. Decided once, when the batch is built.
    reserved: bool,
}

impl RecordBatch {
    /// A batch of `count` records already checked by the caller: framing,
    /// `flags` 0, [`format::check_event`] on every body, and `reserved`
    /// from what that returned.
    pub(crate) fn from_checked(count: u32, bytes: Bytes, reserved: bool) -> Self {
        Self {
            count,
            bytes,
            reserved,
        }
    }

    /// The batch for `events`, serialized once, here, before anyone holds the
    /// writer lock. For the callers that have events rather than records:
    /// the server's own subsystems and tests.
    pub fn from_events(events: &[AppendEvent]) -> Self {
        let size: usize = events
            .iter()
            .map(|event| 5 + format::serialized_append_event_len(event))
            .sum();
        let mut bytes = Vec::with_capacity(size);
        let mut reserved = false;
        for event in events {
            let start = bytes.len();
            bytes.extend_from_slice(&[0; 4]);
            bytes.push(crate::segment::flags::EVENT);
            format::serialize_append_event(Position(0), event, &mut bytes);
            let len = (bytes.len() - start - 4) as u32;
            bytes[start..start + 4].copy_from_slice(&len.to_le_bytes());
            reserved |= crate::system::is_system_name(&event.name)
                || event.tags.iter().any(crate::system::is_system_tag);
        }
        Self {
            count: events.len() as u32,
            bytes: Bytes::from(bytes),
            reserved,
        }
    }

    /// Number of events.
    pub fn len(&self) -> u32 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The records, back to back.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Bytes the batch takes in a segment: each record gains its CRC.
    pub(crate) fn stored_len(&self) -> u64 {
        self.bytes.len() as u64 + 4 * u64::from(self.count)
    }

    /// Rejects a batch a client may not append: one that forges into the `$`
    /// namespace. A client must not be able to forge an event the server
    /// claims to have written about itself.
    pub fn check_client(&self) -> Result<(), Error> {
        if !self.reserved {
            return Ok(());
        }
        // The rare case: find what to name in the error, in event order, the
        // event type before the tags.
        let mut cursor = 0;
        while cursor < self.bytes.len() {
            let len = u32::from_le_bytes(self.bytes[cursor..cursor + 4].try_into().unwrap());
            let body = &self.bytes[cursor + 5..cursor + 4 + len as usize];
            cursor += 4 + len as usize;
            let Some(scan) = format::check_event(body) else {
                continue;
            };
            if crate::system::is_system_name_bytes(scan.name) {
                return Err(Error::ReservedNamespace {
                    detail: format!(
                        "event type '{}' uses the reserved '$' prefix",
                        String::from_utf8_lossy(scan.name)
                    ),
                });
            }
            for (key, _) in scan.tags() {
                if key.first() == Some(&crate::system::SYSTEM_PREFIX) {
                    return Err(Error::ReservedNamespace {
                        detail: format!(
                            "tag key '{}' uses the reserved '$' prefix",
                            String::from_utf8_lossy(key)
                        ),
                    });
                }
            }
        }
        Ok(())
    }
}

/// Result of a successful append.
pub struct AppendResponse {
    /// Position of the first event appended.
    pub first_position: Position,
    /// Number of events appended.
    pub count: u32,
    /// The new consistency marker (position of the last event appended).
    /// Can be used as `consistency_marker` in a subsequent append.
    pub consistency_marker: Position,
}
