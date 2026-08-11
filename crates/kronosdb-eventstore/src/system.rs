//! The `$` system-event namespace.
//!
//! KronosDB stores some of its own internal state as events in the log it
//! serves to applications — see `docs/system-events.md` for the rules that
//! govern them. Two of those rules are enforced here:
//!
//! - **Server-authored only**: client appends carrying a `$`-prefixed event
//!   type or tag key are rejected.
//! - **Invisible to clients**: system events are never returned by the client
//!   read path, system tags are stripped from events that are, and client
//!   queries may not reach into the namespace.
//!
//! Invisibility is what keeps system events private implementation detail: no
//! application can build a projection on something it cannot observe, so the
//! types stay free to evolve. The API is the contract, not the log.

use crate::criteria::SourcingCondition;
use crate::error::Error;
use crate::event::{AppendEvent, StoredEvent, Tag};

/// Prefix reserved for event type names and tag keys owned by the server.
pub const SYSTEM_PREFIX: u8 = b'$';

/// Tag carried by every system event and by nothing else.
///
/// Its value is fixed so that "every system event" is one exact index
/// lookup — which is what lets the store resolve the client-visible head
/// without decoding events. Subsystems correlate their own events with
/// their own tags (`$schedule:{token}`); this one only answers *is this
/// event invisible*.
const MARKER_KEY: &str = "$sys";
const MARKER_VALUE: &str = "1";

/// The tag marking an event invisible to clients.
pub fn marker() -> Tag {
    Tag::from_str(MARKER_KEY, MARKER_VALUE)
}

/// Whether a tag is the system marker.
pub fn is_marker(tag: &Tag) -> bool {
    tag.key == MARKER_KEY.as_bytes() && tag.value == MARKER_VALUE.as_bytes()
}

/// Matches every system event.
pub fn marker_condition() -> SourcingCondition {
    SourcingCondition {
        criteria: vec![crate::criteria::Criterion {
            names: vec![],
            tags: vec![marker()],
        }],
    }
}

/// Which events a read is allowed to observe.
///
/// The two variants are the two read paths described in
/// `docs/system-events.md`: the client path sees only application events, the
/// internal path sees everything. Subsystems that own system events (the
/// scheduler projection, transformation jobs) read with `Internal`; every
/// path that can reach a client reads with `Client`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Visibility {
    /// System events are filtered out and system tags stripped.
    Client,
    /// Everything in the log is visible.
    Internal,
}

impl Visibility {
    /// Whether this visibility hides the `$` namespace.
    pub fn hides_system(self) -> bool {
        matches!(self, Visibility::Client)
    }
}

/// Whether an event type name is in the reserved namespace.
pub fn is_system_name(name: &str) -> bool {
    name.as_bytes().first() == Some(&SYSTEM_PREFIX)
}

/// Whether a tag key is in the reserved namespace.
pub fn is_system_tag(tag: &Tag) -> bool {
    tag.key.first() == Some(&SYSTEM_PREFIX)
}

/// Whether an event may be returned to a client.
///
/// Judged by event type alone: a user event carrying system tags (the event
/// a schedule fires, for instance) stays visible — only its system tags are
/// stripped, by [`strip_system_tags`].
pub fn is_client_visible(event: &StoredEvent) -> bool {
    !is_system_name(&event.name)
}

/// Removes reserved tags from a tag list bound for a client.
pub fn strip_system_tags(tags: &mut Vec<Tag>) {
    tags.retain(|tag| !is_system_tag(tag));
}

/// Rejects a client append that would write into the reserved namespace.
///
/// Applies to event types and tag keys alike: provenance is the definition of
/// a system event, so a client must not be able to forge one.
pub fn validate_client_append(events: &[AppendEvent]) -> Result<(), Error> {
    for event in events {
        if is_system_name(&event.name) {
            return Err(Error::ReservedNamespace {
                detail: format!("event type '{}' uses the reserved '$' prefix", event.name),
            });
        }
        for tag in &event.tags {
            if is_system_tag(tag) {
                return Err(Error::ReservedNamespace {
                    detail: format!(
                        "tag key '{}' uses the reserved '$' prefix",
                        String::from_utf8_lossy(&tag.key)
                    ),
                });
            }
        }
    }
    Ok(())
}

/// Rejects a client query that reaches into the reserved namespace.
///
/// Filtering results is not sufficient on its own: a user event can carry
/// system tags, so a query matching on one would observe system state through
/// an event that is itself legitimately visible.
pub fn validate_client_condition(condition: &SourcingCondition) -> Result<(), Error> {
    for criterion in &condition.criteria {
        for name in &criterion.names {
            if is_system_name(name) {
                return Err(Error::ReservedNamespace {
                    detail: format!("query references reserved event type '{name}'"),
                });
            }
        }
        for tag in &criterion.tags {
            if is_system_tag(tag) {
                return Err(Error::ReservedNamespace {
                    detail: format!(
                        "query references reserved tag key '{}'",
                        String::from_utf8_lossy(&tag.key)
                    ),
                });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::criteria::Criterion;

    fn event(name: &str, tags: Vec<Tag>) -> AppendEvent {
        AppendEvent {
            identifier: "id".into(),
            name: name.into(),
            version: "1".into(),
            timestamp: 0,
            payload: vec![],
            metadata: vec![],
            tags,
        }
    }

    #[test]
    fn client_appends_may_not_forge_system_events() {
        let events = vec![event("$schedule.created", vec![])];
        assert!(validate_client_append(&events).is_err());
    }

    #[test]
    fn client_appends_may_not_forge_system_tags() {
        let events = vec![event(
            "OrderPlaced",
            vec![Tag::from_str("$schedule", "tok")],
        )];
        assert!(validate_client_append(&events).is_err());
    }

    #[test]
    fn ordinary_appends_are_accepted() {
        let events = vec![event("OrderPlaced", vec![Tag::from_str("orderId", "1")])];
        assert!(validate_client_append(&events).is_ok());
    }

    #[test]
    fn client_queries_may_not_reach_into_the_namespace() {
        let by_name = SourcingCondition {
            criteria: vec![Criterion {
                names: vec!["$schedule.created".into()],
                tags: vec![],
            }],
        };
        assert!(validate_client_condition(&by_name).is_err());

        // A user event can carry system tags, so this one would leak system
        // state through an otherwise-visible event.
        let by_tag = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![Tag::from_str("$schedule", "tok")],
            }],
        };
        assert!(validate_client_condition(&by_tag).is_err());
    }

    #[test]
    fn system_tags_are_stripped_but_the_event_survives() {
        let mut tags = vec![
            Tag::from_str("orderId", "1"),
            Tag::from_str("$schedule", "tok"),
        ];
        strip_system_tags(&mut tags);
        assert_eq!(tags, vec![Tag::from_str("orderId", "1")]);
    }

    #[test]
    fn visibility_is_judged_by_event_type() {
        let user = StoredEvent {
            position: crate::event::Position(0),
            identifier: "id".into(),
            name: "OrderPlaced".into(),
            version: "1".into(),
            timestamp: 0,
            payload: vec![],
            metadata: vec![],
            tags: vec![Tag::from_str("$schedule", "tok")],
        };
        // Carries a system tag, but it is the application's own event.
        assert!(is_client_visible(&user));

        let system = StoredEvent {
            name: "$schedule.created".into(),
            ..user
        };
        assert!(!is_client_visible(&system));
    }
}
