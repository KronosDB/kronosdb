//! Scheduled events (ADR-0003).
//!
//! A schedule is not a row in a side table — it is events in the log, so it
//! inherits quorum durability, replication, and crash recovery rather than
//! reimplementing them. Its whole lifecycle lives under the `$` namespace
//! (`docs/system-events.md`) and is invisible to applications:
//!
//! ```text
//! $schedule.created    the target event and when it comes due
//! $schedule.cancelled  resolved without firing
//! <the target event>   appended by the leader when due, tagged back to its schedule
//! ```
//!
//! Every event in a schedule's lifecycle carries `$schedule:{token}`, which
//! turns exactly-once firing into a DCB condition rather than a coordination
//! protocol: fire and cancel are both conditional appends that exclude each
//! other, so whichever commits first makes the other's condition fail. A
//! leader that dies mid-fire cannot double-fire, because the new leader's
//! append is refused by the same guard.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::append::{AppendCondition, AppendRequest};
use crate::criteria::{Criterion, SourcingCondition};
use crate::error::Error;
use crate::event::{AppendEvent, Position, StoredEvent, Tag};
use crate::store::EventStoreEngine;

/// Appended when a schedule is created. Payload is a bincoded [`ScheduleSpec`].
pub const CREATED: &str = "$schedule.created";
/// Appended when a live schedule is cancelled before it fires.
pub const CANCELLED: &str = "$schedule.cancelled";
/// Appended alongside the target event, in the same atomic batch, recording
/// that the schedule resolved by firing.
pub const FIRED: &str = "$schedule.fired";

/// Correlation tag: ties one schedule's events together and scopes its guard.
const TOKEN_KEY: &str = "$schedule";

/// The tag correlating one schedule's events.
pub fn token_tag(token: &str) -> Tag {
    Tag::from_str(TOKEN_KEY, token)
}

fn token_of(event: &StoredEvent) -> Option<String> {
    event
        .tags
        .iter()
        .find(|tag| tag.key == TOKEN_KEY.as_bytes())
        .map(|tag| String::from_utf8_lossy(&tag.value).into_owned())
}

/// The event to append when the schedule comes due.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScheduledEvent {
    pub identifier: String,
    pub name: String,
    pub version: String,
    pub payload: Vec<u8>,
    pub metadata: Vec<(String, String)>,
    pub tags: Vec<(Vec<u8>, Vec<u8>)>,
}

impl ScheduledEvent {
    /// Builds the append form, adding the tag that correlates the fired event
    /// back to its schedule.
    ///
    /// Deliberately *not* marked as a system event: this is the application's
    /// own event and must stay visible, and must keep counting toward the
    /// client-visible head. Only the correlation tag is stripped on the way
    /// out.
    fn to_append_event(&self, token: &str) -> AppendEvent {
        let mut tags: Vec<Tag> = self
            .tags
            .iter()
            .map(|(key, value)| Tag::new(key.clone(), value.clone()))
            .collect();
        tags.push(token_tag(token));

        AppendEvent {
            identifier: self.identifier.clone(),
            name: self.name.clone(),
            version: self.version.clone(),
            // Stamped when it actually lands, not when it was scheduled.
            timestamp: now_ms(),
            payload: self.payload.clone(),
            metadata: self.metadata.clone(),
            tags,
        }
    }
}

/// What a client asked for: an event, and when it should land.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScheduleSpec {
    /// When the event comes due, in millis since epoch.
    pub due_ms: i64,
    pub target: ScheduledEvent,
}

/// A schedule that has been created and neither fired nor cancelled.
#[derive(Debug, Clone, PartialEq)]
pub struct LiveSchedule {
    pub token: String,
    pub due_ms: i64,
    /// Position of the `$schedule.created` event — scopes the liveness guard.
    pub created_at: Position,
    pub target: ScheduledEvent,
}

fn now_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn encode(spec: &ScheduleSpec) -> Result<Vec<u8>, Error> {
    bincode::serialize(spec).map_err(|error| Error::Corrupted {
        message: format!("could not encode schedule: {error}"),
    })
}

fn decode(bytes: &[u8]) -> Result<ScheduleSpec, Error> {
    bincode::deserialize(bytes).map_err(|error| Error::Corrupted {
        message: format!("could not decode schedule: {error}"),
    })
}

/// Matches everything in one schedule's lifecycle.
fn by_token(token: &str) -> SourcingCondition {
    SourcingCondition {
        criteria: vec![Criterion {
            names: vec![],
            tags: vec![token_tag(token)],
        }],
    }
}

/// The guard shared by firing and cancelling: *nothing has resolved this
/// schedule yet*. Scoped to positions at or after the event following
/// creation, so the `$schedule.created` event itself never trips it.
fn still_live(token: &str, created_at: Position) -> AppendCondition {
    AppendCondition {
        consistency_marker: Position(created_at.0 + 1),
        criteria: by_token(token),
    }
}

/// Creates a schedule, returning the position of its `$schedule.created`.
///
/// Idempotent on `token`: a retried call after a timed-out one is refused
/// rather than scheduling the event twice.
pub fn schedule(
    engine: &EventStoreEngine,
    token: &str,
    spec: &ScheduleSpec,
) -> Result<Position, Error> {
    let event = AppendEvent {
        identifier: format!("schedule-{token}"),
        name: CREATED.into(),
        version: "1".into(),
        timestamp: now_ms(),
        payload: encode(spec)?,
        metadata: vec![],
        tags: vec![token_tag(token)],
    };

    let response = engine.append_system(AppendRequest {
        // Whole log: this token must not already exist.
        condition: Some(AppendCondition {
            consistency_marker: Position(0),
            criteria: by_token(token),
        }),
        events: vec![event],
    })?;
    Ok(response.first_position)
}

/// Cancels a live schedule.
///
/// Returns `ConsistencyConditionViolated` if the schedule already resolved —
/// the caller learns the event fired rather than being told nothing happened.
pub fn cancel(engine: &EventStoreEngine, token: &str) -> Result<(), Error> {
    // Deliberately anchored on the creation event rather than on whether the
    // schedule is still live: a schedule that already resolved must report the
    // conflict, not be mistaken for one that never existed.
    let Some(created_at) = created_at(engine, token)? else {
        return Err(Error::SnapshotNotFound {
            key: format!("schedule {token}"),
        });
    };

    engine.append_system(AppendRequest {
        condition: Some(still_live(token, created_at)),
        events: vec![AppendEvent {
            identifier: format!("cancel-{token}"),
            name: CANCELLED.into(),
            version: "1".into(),
            timestamp: now_ms(),
            payload: vec![],
            metadata: vec![],
            tags: vec![token_tag(token)],
        }],
    })?;
    Ok(())
}

/// Appends the target event, unless this schedule already resolved.
///
/// The guard is what makes firing exactly-once across failover: a fire that
/// races a cancel, or a new leader repeating a dead leader's work, is refused.
/// The target event and the record of its firing go in one all-or-nothing
/// batch, so the projection can never observe a fired schedule that still
/// looks live.
pub fn fire(engine: &EventStoreEngine, live: &LiveSchedule) -> Result<(), Error> {
    engine.append_system(AppendRequest {
        condition: Some(still_live(&live.token, live.created_at)),
        events: vec![
            live.target.to_append_event(&live.token),
            AppendEvent {
                identifier: format!("fired-{}", live.token),
                name: FIRED.into(),
                version: "1".into(),
                timestamp: now_ms(),
                payload: vec![],
                metadata: vec![],
                tags: vec![token_tag(&live.token)],
            },
        ],
    })?;
    Ok(())
}

/// Position of a token's `$schedule.created`, whether or not it still lives.
fn created_at(engine: &EventStoreEngine, token: &str) -> Result<Option<Position>, Error> {
    let events = engine.source_internal(Position(0), &by_token(token), usize::MAX)?;
    Ok(events
        .iter()
        .find(|event| event.name == CREATED)
        .map(|event| event.position))
}

/// Reads one schedule's current state from the log, or `None` once it has
/// fired or been cancelled.
pub fn find(engine: &EventStoreEngine, token: &str) -> Result<Option<LiveSchedule>, Error> {
    let events = engine.source_internal(Position(0), &by_token(token), usize::MAX)?;
    let mut projection = Projection::default();
    projection.apply(events);
    Ok(projection.live.remove(token))
}

/// In-memory view of every live schedule, folded from the log.
///
/// Rebuilt by replaying from position 0 on startup and advanced incrementally
/// after that, so a restart or a failover needs no separate recovery path —
/// the log is the state. Every node may run one; only the leader fires.
#[derive(Debug)]
pub struct Projection {
    cursor: Position,
    live: HashMap<String, LiveSchedule>,
}

impl Default for Projection {
    fn default() -> Self {
        Self {
            cursor: Position::ZERO,
            live: HashMap::new(),
        }
    }
}

impl Projection {
    /// Folds in every schedule event committed since the last call.
    pub fn advance(&mut self, engine: &EventStoreEngine) -> Result<(), Error> {
        let head = engine.head();
        if head <= self.cursor {
            return Ok(());
        }
        // Every system event, not just this subsystem's: other subsystems'
        // events carry no `$schedule` token and are skipped in `apply`.
        let condition = crate::system::marker_condition();
        let events = engine.source_internal(self.cursor, &condition, usize::MAX)?;
        self.apply(events);
        self.cursor = head;
        Ok(())
    }

    fn apply(&mut self, events: Vec<StoredEvent>) {
        for event in events {
            let Some(token) = token_of(&event) else {
                continue;
            };
            match event.name.as_str() {
                CREATED => match decode(&event.payload) {
                    Ok(spec) => {
                        self.live.insert(
                            token.clone(),
                            LiveSchedule {
                                token,
                                due_ms: spec.due_ms,
                                created_at: event.position,
                                target: spec.target,
                            },
                        );
                    }
                    Err(error) => {
                        tracing::warn!(%error, token, "undecodable schedule ignored");
                    }
                },
                // Cancelled, or the target event landing: either way the
                // schedule has resolved and stops being live.
                _ => {
                    self.live.remove(&token);
                }
            }
        }
    }

    /// Live schedules due at or before `now_ms`, oldest first so a backlog
    /// after downtime fires in the order it was meant to.
    pub fn due(&self, now_ms: i64) -> Vec<LiveSchedule> {
        let mut due: Vec<LiveSchedule> = self
            .live
            .values()
            .filter(|schedule| schedule.due_ms <= now_ms)
            .cloned()
            .collect();
        due.sort_by_key(|schedule| (schedule.due_ms, schedule.created_at));
        due
    }

    /// Every live schedule, soonest first. Backs `ListSchedules`.
    pub fn list(&self) -> Vec<LiveSchedule> {
        let mut all: Vec<LiveSchedule> = self.live.values().cloned().collect();
        all.sort_by_key(|schedule| (schedule.due_ms, schedule.created_at));
        all
    }

    pub fn len(&self) -> usize {
        self.live.len()
    }

    pub fn is_empty(&self) -> bool {
        self.live.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(name: &str, due_ms: i64) -> ScheduleSpec {
        ScheduleSpec {
            due_ms,
            target: ScheduledEvent {
                identifier: format!("evt-{name}"),
                name: name.into(),
                version: "1".into(),
                payload: b"body".to_vec(),
                metadata: vec![],
                tags: vec![(b"orderId".to_vec(), b"A".to_vec())],
            },
        }
    }

    fn engine() -> (tempfile::TempDir, EventStoreEngine) {
        let dir = tempfile::tempdir().unwrap();
        let engine = EventStoreEngine::create(dir.path()).unwrap();
        (dir, engine)
    }

    #[test]
    fn a_due_schedule_fires_the_target_event() {
        let (_dir, engine) = engine();
        schedule(&engine, "tok", &spec("PaymentTimedOut", 1_000)).unwrap();

        let mut projection = Projection::default();
        projection.advance(&engine).unwrap();
        let due = projection.due(2_000);
        assert_eq!(due.len(), 1);

        fire(&engine, &due[0]).unwrap();

        // The fired event is the application's own type and reaches clients.
        let visible = engine
            .source(
                Position(0),
                &SourcingCondition {
                    criteria: vec![Criterion {
                        names: vec![],
                        tags: vec![Tag::from_str("orderId", "A")],
                    }],
                },
            )
            .unwrap();
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].name, "PaymentTimedOut");

        // And the schedule is no longer live.
        projection.advance(&engine).unwrap();
        assert!(projection.is_empty());
    }

    /// A client that has read everything must land exactly on the head it is
    /// given, or it reports lag it can never clear.
    #[test]
    fn a_drained_client_reaches_the_visible_head() {
        let (_dir, engine) = engine();

        // Trailing system events: created, then cancelled.
        schedule(&engine, "cancelled", &spec("NeverFires", 1_000)).unwrap();
        cancel(&engine, "cancelled").unwrap();
        assert_eq!(engine.head(), Position(2));
        assert_eq!(engine.visible_head(), Position(0), "nothing readable yet");

        // Fire one, so a visible event lands with a system event behind it.
        schedule(&engine, "fires", &spec("ReminderDue", 0)).unwrap();
        let mut projection = Projection::default();
        projection.advance(&engine).unwrap();
        fire(&engine, &projection.due(now_ms())[0]).unwrap();

        // created(2) + target(3) + fired(4) => head 5, last readable at 3.
        assert_eq!(engine.head(), Position(5));
        assert_eq!(engine.visible_head(), Position(4));

        let all = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![Tag::from_str("orderId", "A")],
            }],
        };
        let events = engine.source(Position(0), &all).unwrap();
        let cursor = events.last().unwrap().position.0 + 1;
        assert_eq!(
            Position(cursor),
            engine.visible_head(),
            "a drained cursor must equal the head clients are shown"
        );
    }

    #[test]
    fn a_schedule_not_yet_due_does_not_fire() {
        let (_dir, engine) = engine();
        schedule(&engine, "tok", &spec("PaymentTimedOut", 10_000)).unwrap();

        let mut projection = Projection::default();
        projection.advance(&engine).unwrap();
        assert!(projection.due(9_999).is_empty());
        assert_eq!(projection.list().len(), 1);
    }

    #[test]
    fn firing_twice_is_refused() {
        let (_dir, engine) = engine();
        schedule(&engine, "tok", &spec("PaymentTimedOut", 0)).unwrap();

        let mut projection = Projection::default();
        projection.advance(&engine).unwrap();
        let due = projection.due(now_ms());
        assert_eq!(due.len(), 1);

        fire(&engine, &due[0]).unwrap();
        // A new leader repeating a dead leader's work sees the same guard.
        let repeat = fire(&engine, &due[0]);
        assert!(matches!(
            repeat,
            Err(Error::ConsistencyConditionViolated { .. })
        ));
    }

    #[test]
    fn cancelling_beats_a_later_fire() {
        let (_dir, engine) = engine();
        schedule(&engine, "tok", &spec("PaymentTimedOut", 0)).unwrap();

        let mut projection = Projection::default();
        projection.advance(&engine).unwrap();
        let live = projection.due(now_ms()).remove(0);

        cancel(&engine, "tok").unwrap();
        let late = fire(&engine, &live);
        assert!(matches!(
            late,
            Err(Error::ConsistencyConditionViolated { .. })
        ));

        projection.advance(&engine).unwrap();
        assert!(projection.is_empty());
    }

    #[test]
    fn cancelling_after_firing_reports_the_conflict() {
        let (_dir, engine) = engine();
        schedule(&engine, "tok", &spec("PaymentTimedOut", 0)).unwrap();

        let mut projection = Projection::default();
        projection.advance(&engine).unwrap();
        let live = projection.due(now_ms()).remove(0);
        fire(&engine, &live).unwrap();

        // Not a silent no-op: the caller learns the event is in the log.
        assert!(matches!(
            cancel(&engine, "tok"),
            Err(Error::ConsistencyConditionViolated { .. })
        ));
    }

    #[test]
    fn scheduling_the_same_token_twice_is_refused() {
        let (_dir, engine) = engine();
        schedule(&engine, "tok", &spec("PaymentTimedOut", 1_000)).unwrap();
        assert!(matches!(
            schedule(&engine, "tok", &spec("PaymentTimedOut", 1_000)),
            Err(Error::ConsistencyConditionViolated { .. })
        ));
    }

    #[test]
    fn overdue_schedules_survive_a_restart_and_fire_in_due_order() {
        let dir = tempfile::tempdir().unwrap();
        {
            let engine = EventStoreEngine::create(dir.path()).unwrap();
            schedule(&engine, "later", &spec("Second", 5_000)).unwrap();
            schedule(&engine, "sooner", &spec("First", 1_000)).unwrap();
            schedule(&engine, "gone", &spec("Cancelled", 2_000)).unwrap();
            cancel(&engine, "gone").unwrap();
        }

        // A fresh process rebuilds purely by replaying the log.
        let engine = EventStoreEngine::open(dir.path()).unwrap();
        let mut projection = Projection::default();
        projection.advance(&engine).unwrap();

        let due = projection.due(10_000);
        let order: Vec<&str> = due.iter().map(|s| s.token.as_str()).collect();
        assert_eq!(order, vec!["sooner", "later"]);

        for live in &due {
            fire(&engine, live).unwrap();
        }
        projection.advance(&engine).unwrap();
        assert!(projection.is_empty());
    }

    #[test]
    fn schedules_are_invisible_to_clients_but_still_fire() {
        let (_dir, engine) = engine();
        schedule(&engine, "tok", &spec("ReminderDue", 0)).unwrap();

        let by_order = SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![Tag::from_str("orderId", "A")],
            }],
        };
        // Nothing to see before it fires, even though $schedule.created is
        // in the log at position 0.
        assert!(engine.source(Position(0), &by_order).unwrap().is_empty());
        assert_eq!(engine.head(), Position(1));

        let mut projection = Projection::default();
        projection.advance(&engine).unwrap();
        fire(&engine, &projection.due(now_ms())[0]).unwrap();

        let fired = engine
            .source_stored(Position(0), &by_order, 10)
            .unwrap()
            .remove(0);
        assert_eq!(fired.name, "ReminderDue");
        // Correlation tags are plumbing; the client sees only its own.
        assert_eq!(fired.tags, vec![Tag::from_str("orderId", "A")]);
    }
}
