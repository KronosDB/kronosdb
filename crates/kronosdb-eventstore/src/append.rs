use crate::criteria::SourcingCondition;
use crate::event::{AppendEvent, Position};

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
