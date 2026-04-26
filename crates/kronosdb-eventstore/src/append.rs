use crate::criteria::SourcingCondition;
use crate::event::{AppendEvent, Position};

/// An applied Raft log-entry identifier threaded from the state machine down
/// into the event-segment writer. Written inline as a `RaftMarker::normal`
/// record alongside the events it produced, so that on restart the segment
/// scan recovers `last_applied` without any extra fsync or sidecar file.
///
/// Mirrors the fields of `openraft::LogId<NodeId>` that matter at the
/// storage layer (`leader_id.term` and `index`), but stays crate-local so
/// `store.rs` does not depend on `raft::types`.
#[derive(Debug, Clone, Copy)]
pub struct AppliedLogId {
    pub term: u64,
    pub index: u64,
}

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
