use crate::event::Position;
use crate::tag::Tag;

/// A criterion matches an event if:
/// - ALL tags in `tags` are present on the event (AND logic)
/// - AND the event name is in `names` (if `names` is non-empty)
///
/// If `names` is empty, only the tags are checked.
#[derive(Debug, Clone)]
pub struct Criterion {
    /// Event type names. If non-empty, the event must match one of these.
    pub names: Vec<String>,
    /// Tags that must ALL be present on the event.
    pub tags: Vec<Tag>,
}

/// A query is a list of criteria. An event matches if ANY criterion matches (OR logic).
///
/// This gives you: (tags1 AND name1) OR (tags2 AND name2) OR ...
#[derive(Debug, Clone)]
pub struct Query {
    pub criteria: Vec<Criterion>,
}

/// The DCB consistency condition for an append.
///
/// "Reject this append if any event matching the query exists
///  at a position greater than `consistency_marker`."
///
/// The `consistency_marker` is typically the position returned by a
/// previous Source call — it represents the point up to which the
/// client has already observed events.
#[derive(Debug, Clone)]
pub struct ConsistencyCondition {
    /// Position after which to check for conflicting events.
    /// Events at positions > consistency_marker are checked.
    pub consistency_marker: Position,
    /// The query defining which events would conflict.
    pub query: Query,
}

/// Request to append events, optionally with a DCB condition.
pub struct AppendRequest {
    /// If present, the append is rejected if the condition is violated.
    pub condition: Option<ConsistencyCondition>,
    /// Events to append. All-or-nothing: either all are appended or none.
    pub events: Vec<crate::event::AppendEvent>,
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
