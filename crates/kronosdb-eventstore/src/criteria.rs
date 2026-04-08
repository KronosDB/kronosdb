use crate::event::Tag;

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

/// Filtering criteria for sourcing events.
/// A list of criteria where an event matches if ANY criterion matches (OR logic).
///
/// This gives you: (tags1 AND name1) OR (tags2 AND name2) OR ...
#[derive(Debug, Clone)]
pub struct SourcingCondition {
    pub criteria: Vec<Criterion>,
}
