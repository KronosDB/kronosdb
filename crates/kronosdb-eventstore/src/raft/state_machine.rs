use std::io::Cursor;
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftLogId, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StoredMembership,
};
use serde::{Deserialize, Serialize};

use crate::append::AppendRequest;
use crate::context::ContextManager;
use crate::error::Error;

use super::types::{NodeId, RaftRejectReason, RaftRequest, RaftResponse, TypeConfig};

#[cfg(feature = "bench-instrumentation")]
use super::bench_instrumentation::{Region, Timer};

/// Snapshot body version. Increment on breaking layout changes so an
/// older install path can detect and refuse incompatible payloads.
/// Phase 4 ships at version 1; Phase 4-03 may evolve it.
const SNAPSHOT_VERSION: u8 = 1;

/// Raft snapshot payload — the bytes that live in `Snapshot.snapshot`
/// (the `Cursor<Vec<u8>>` per D-02). Carries every context's events,
/// head position (implied by the last event's position), and tags;
/// the per-context tag index is NOT serialized because 04-02's install
/// rebuilds it deterministically by re-appending events.
///
/// Field order is the canonical bincode layout for v1: changing it
/// requires bumping `SNAPSHOT_VERSION` and adding a migration branch
/// in the install path.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StateMachineSnapshot {
    version: u8,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    contexts: Vec<ContextSnapshot>,
}

/// Per-context slice of the snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ContextSnapshot {
    name: String,
    /// Events ordered by position ascending, covering positions 1..head.
    events: Vec<SnapshotEvent>,
}

/// One event frozen for transport in a snapshot. Mirrors `StoredEvent`
/// field-for-field but uses `(Vec<u8>, Vec<u8>)` tag tuples to stay in
/// lock-step with `RaftAppendEvent`'s wire shape (types.rs:84-93) so
/// 04-02's install can re-append these via `AppendEvent` directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotEvent {
    position: u64,
    identifier: String,
    name: String,
    version: String,
    timestamp: i64,
    payload: Vec<u8>,
    metadata: Vec<(String, String)>,
    tags: Vec<(Vec<u8>, Vec<u8>)>,
}

/// The Raft state machine.
///
/// Applies committed Raft log entries to the local EventStoreEngine.
/// This is the bridge between Raft consensus and the event store.
pub struct EventStoreStateMachine {
    contexts: Arc<ContextManager>,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
}

impl EventStoreStateMachine {
    pub fn new(contexts: Arc<ContextManager>) -> Self {
        Self {
            contexts,
            last_applied: None,
            last_membership: StoredMembership::default(),
        }
    }

    /// Apply a single Raft request to the event store.
    ///
    /// Returns `Result<RaftResponse, StorageError<NodeId>>` so every apply-time
    /// error is explicitly classified per D-04..D-08:
    /// - DCB consistency violations → `Ok(RaftResponse::AppendRejected { .. })`
    ///   (deterministic, valid apply response).
    /// - Deterministic-apply violations (`ContextNotFound`, `Io`, `Corrupted`)
    ///   → `Err(StorageError<NodeId>)` (openraft halts the node cleanly).
    /// - `ContextAlreadyExists` on `CreateContext` replay → idempotent success.
    fn apply_request(
        &self,
        req: &RaftRequest,
    ) -> Result<RaftResponse, StorageError<NodeId>> {
        match req {
            RaftRequest::Append {
                context,
                events,
                condition,
            } => {
                #[cfg(feature = "bench-instrumentation")]
                let _t = Timer::new(Region::ApplyEventPath);
                let append_events: Vec<_> = events.iter().map(|e| e.to_event()).collect();
                let append_req = AppendRequest {
                    condition: condition.as_ref().map(|c| c.to_condition()),
                    events: append_events,
                };

                match self
                    .contexts
                    .with_context(context, |store| store.append(append_req))
                {
                    Ok(resp) => Ok(RaftResponse::Append {
                        first_position: resp.first_position.0,
                        count: resp.count,
                        consistency_marker: resp.consistency_marker.0,
                    }),
                    Err(Error::ConsistencyConditionViolated {
                        conflicting_position,
                    }) => {
                        // D-04: DCB violation is deterministic across nodes given the
                        // same prior state. Return as a successful apply response with
                        // the typed rejection variant; the apply Result is Ok here —
                        // AppendRejected is not a StorageError.
                        Ok(RaftResponse::AppendRejected {
                            reason: RaftRejectReason::ConsistencyConditionViolated {
                                conflicting_position: conflicting_position.0,
                            },
                        })
                    }
                    Err(Error::ContextNotFound { name }) => {
                        // D-05: Follower determinism violation. CreateContext for this
                        // name must have committed before this append — if we get here,
                        // log replay is out of order. Halt the node via StorageError.
                        Err(StorageError::from_io_error(
                            openraft::ErrorSubject::StateMachine,
                            openraft::ErrorVerb::Write,
                            std::io::Error::new(
                                std::io::ErrorKind::NotFound,
                                format!("apply: context not found: {name}"),
                            ),
                        ))
                    }
                    Err(Error::Io(io_err)) => {
                        // D-06: Unexpected storage I/O failure during deterministic apply.
                        // Node can no longer be trusted to stay in lockstep.
                        Err(StorageError::from_io_error(
                            openraft::ErrorSubject::StateMachine,
                            openraft::ErrorVerb::Write,
                            io_err,
                        ))
                    }
                    Err(Error::Corrupted { message }) => {
                        // D-07: Corruption detected at apply time; halt the node.
                        Err(StorageError::from_io_error(
                            openraft::ErrorSubject::StateMachine,
                            openraft::ErrorVerb::Write,
                            std::io::Error::new(std::io::ErrorKind::InvalidData, message),
                        ))
                    }
                    Err(other) => {
                        // Any other Error variant (InvalidContextName, ContextAlreadyExists,
                        // SnapshotNotFound) is not expected on the append path; treat as
                        // fatal determinism violation rather than silently Ok.
                        Err(StorageError::from_io_error(
                            openraft::ErrorSubject::StateMachine,
                            openraft::ErrorVerb::Write,
                            std::io::Error::new(
                                std::io::ErrorKind::Other,
                                format!("unexpected apply error: {other}"),
                            ),
                        ))
                    }
                }
            }
            RaftRequest::CreateContext { name } => {
                match self.contexts.create_context(name) {
                    Ok(()) => Ok(RaftResponse::ContextCreated),
                    Err(Error::ContextAlreadyExists { .. }) => {
                        // D-08: idempotent replay of a committed CreateContext is
                        // expected during log replay; not an error.
                        tracing::debug!(name = %name, "create_context: already exists (replay)");
                        Ok(RaftResponse::ContextCreated)
                    }
                    Err(Error::InvalidContextName { name, reason }) => {
                        Err(StorageError::from_io_error(
                            openraft::ErrorSubject::StateMachine,
                            openraft::ErrorVerb::Write,
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidInput,
                                format!("invalid context name '{name}': {reason}"),
                            ),
                        ))
                    }
                    Err(other) => Err(StorageError::from_io_error(
                        openraft::ErrorSubject::StateMachine,
                        openraft::ErrorVerb::Write,
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("create_context unexpected error: {other}"),
                        ),
                    )),
                }
            }
        }
    }
}

impl RaftStateMachine<TypeConfig> for EventStoreStateMachine {
    type SnapshotBuilder = EventStoreSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<NodeId>>,
            StoredMembership<NodeId, openraft::BasicNode>,
        ),
        StorageError<NodeId>,
    > {
        Ok((self.last_applied, self.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut responses = Vec::new();

        for entry in entries {
            self.last_applied = Some(*entry.get_log_id());

            match entry.payload {
                EntryPayload::Normal(req) => {
                    let resp = self.apply_request(&req)?;
                    responses.push(resp);
                }
                EntryPayload::Membership(ref membership) => {
                    self.last_membership =
                        StoredMembership::new(Some(*entry.get_log_id()), membership.clone());
                    responses.push(RaftResponse::Ok);
                }
                EntryPayload::Blank => {
                    responses.push(RaftResponse::Ok);
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        EventStoreSnapshotBuilder {
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            contexts: Arc::clone(&self.contexts),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let sm_snapshot: StateMachineSnapshot = bincode::deserialize(&data).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Read,
                std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            )
        })?;

        self.last_applied = sm_snapshot.last_applied;
        self.last_membership = sm_snapshot.last_membership;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        // openraft contract: return None iff there is no snapshot to offer.
        // Before any apply there is nothing to snapshot; after the first
        // apply we can always materialize a fresh snapshot on demand from
        // the authoritative segment state.
        if self.last_applied.is_none() {
            return Ok(None);
        }
        let mut builder = EventStoreSnapshotBuilder {
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            contexts: Arc::clone(&self.contexts),
        };
        Ok(Some(builder.build_snapshot().await?))
    }
}

/// Builds a Raft snapshot from the current state machine state.
///
/// Holds a cloned `Arc<ContextManager>` so `build_snapshot` can walk every
/// context via `list_contexts` + `get_context`, calling the new
/// `EventStoreEngine::source_all` helper for each to materialize the
/// per-context event stream (Phase 4, SNAP-01).
pub struct EventStoreSnapshotBuilder {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    contexts: Arc<ContextManager>,
}

impl RaftSnapshotBuilder<TypeConfig> for EventStoreSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let names = self.contexts.list_contexts();
        let mut context_snaps = Vec::with_capacity(names.len());
        for name in names {
            let engine = self.contexts.get_context(&name).map_err(|e| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::StateMachine,
                    openraft::ErrorVerb::Read,
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("snapshot: get_context({name}) failed: {e}"),
                    ),
                )
            })?;
            let stored = engine.source_all(crate::event::Position(1)).map_err(|e| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::StateMachine,
                    openraft::ErrorVerb::Read,
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("snapshot: source_all({name}) failed: {e}"),
                    ),
                )
            })?;
            let events = stored
                .into_iter()
                .map(|s| SnapshotEvent {
                    position: s.position.0,
                    identifier: s.identifier,
                    name: s.name,
                    version: s.version,
                    timestamp: s.timestamp,
                    payload: s.payload,
                    metadata: s.metadata,
                    tags: s.tags.into_iter().map(|t| (t.key, t.value)).collect(),
                })
                .collect();
            context_snaps.push(ContextSnapshot { name, events });
        }

        let sm_snapshot = StateMachineSnapshot {
            version: SNAPSHOT_VERSION,
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            contexts: context_snaps,
        };

        let data = bincode::serialize(&sm_snapshot).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Write,
                std::io::Error::new(std::io::ErrorKind::Other, e),
            )
        })?;

        let snapshot_id = self
            .last_applied
            .map(|id| format!("{}-{}", id.leader_id, id.index))
            .unwrap_or_else(|| "empty".to_string());

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::Position;
    use crate::segment::DEFAULT_SEGMENT_SIZE;
    use openraft::{CommittedLeaderId, Entry, EntryPayload, Membership, RaftLogId};
    use std::collections::{BTreeMap, BTreeSet};

    fn log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId {
            leader_id: CommittedLeaderId::new(term, 0),
            index,
        }
    }

    fn make_append_entry(
        term: u64,
        index: u64,
        context: &str,
        event_name: &str,
    ) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(term, index),
            payload: EntryPayload::Normal(RaftRequest::Append {
                context: context.to_string(),
                events: vec![super::super::types::RaftAppendEvent {
                    identifier: format!("evt-{index}"),
                    name: event_name.to_string(),
                    version: "1.0".to_string(),
                    timestamp: 1712345678000,
                    payload: b"data".to_vec(),
                    metadata: vec![],
                    tags: vec![(b"id".to_vec(), format!("{index}").into_bytes())],
                }],
                condition: None,
            }),
        }
    }

    fn blank_entry(term: u64, index: u64) -> Entry<TypeConfig> {
        let mut e = Entry::<TypeConfig>::default();
        e.set_log_id(&log_id(term, index));
        e
    }

    fn create_sm() -> (EventStoreStateMachine, Arc<ContextManager>) {
        let dir = tempfile::tempdir().unwrap();
        // Leak the tempdir so it lives long enough.
        let dir = Box::leak(Box::new(dir));
        let contexts = Arc::new(ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap());
        contexts.create_context("default").unwrap();
        let sm = EventStoreStateMachine::new(Arc::clone(&contexts));
        (sm, contexts)
    }

    #[tokio::test]
    async fn initial_applied_state_is_empty() {
        let (mut sm, _ctx) = create_sm();
        let (applied, membership) = sm.applied_state().await.unwrap();
        assert!(applied.is_none());
        assert_eq!(membership.membership().voter_ids().count(), 0);
    }

    #[tokio::test]
    async fn apply_append_entries() {
        let (mut sm, contexts) = create_sm();

        let entries = vec![
            make_append_entry(1, 1, "default", "OrderPlaced"),
            make_append_entry(1, 2, "default", "PaymentReceived"),
        ];

        let responses = sm.apply(entries).await.unwrap();
        assert_eq!(responses.len(), 2);

        match &responses[0] {
            RaftResponse::Append {
                first_position,
                count,
                ..
            } => {
                assert_eq!(*first_position, 1);
                assert_eq!(*count, 1);
            }
            other => panic!("expected Append, got {:?}", other),
        }

        match &responses[1] {
            RaftResponse::Append {
                first_position,
                count,
                ..
            } => {
                assert_eq!(*first_position, 2);
                assert_eq!(*count, 1);
            }
            other => panic!("expected Append, got {:?}", other),
        }

        // Verify events in the store.
        let store = contexts.get_context("default").unwrap();
        assert_eq!(store.head(), Position(3));
    }

    #[tokio::test]
    async fn apply_tracks_last_applied() {
        let (mut sm, _ctx) = create_sm();

        sm.apply(vec![blank_entry(1, 1), blank_entry(1, 2)])
            .await
            .unwrap();

        let (applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(applied.unwrap().index, 2);
    }

    #[tokio::test]
    async fn apply_membership_entry() {
        let (mut sm, _ctx) = create_sm();

        let mut voter_set = BTreeSet::new();
        voter_set.insert(1u64);
        voter_set.insert(2u64);

        let mut nodes = BTreeMap::new();
        nodes.insert(
            1u64,
            openraft::BasicNode {
                addr: "addr1".to_string(),
            },
        );
        nodes.insert(
            2u64,
            openraft::BasicNode {
                addr: "addr2".to_string(),
            },
        );
        let membership = Membership::new(vec![voter_set], nodes);

        let entry = Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Membership(membership),
        };

        let responses = sm.apply(vec![entry]).await.unwrap();
        assert_eq!(responses.len(), 1);
        assert!(matches!(responses[0], RaftResponse::Ok));

        let (_, stored_membership) = sm.applied_state().await.unwrap();
        let voter_ids: Vec<u64> = stored_membership.membership().voter_ids().collect();
        assert!(voter_ids.contains(&1));
        assert!(voter_ids.contains(&2));
    }

    #[tokio::test]
    async fn apply_create_context() {
        let (mut sm, contexts) = create_sm();

        let entry = Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Normal(RaftRequest::CreateContext {
                name: "orders".to_string(),
            }),
        };

        let responses = sm.apply(vec![entry]).await.unwrap();
        assert!(matches!(responses[0], RaftResponse::ContextCreated));
        assert!(contexts.context_exists("orders"));
    }

    #[tokio::test]
    async fn apply_to_nonexistent_context_returns_fatal_storage_error() {
        let (mut sm, _ctx) = create_sm();
        let entry = make_append_entry(1, 1, "nonexistent", "OrderPlaced");
        let result = sm.apply(vec![entry]).await;
        assert!(
            result.is_err(),
            "expected fatal StorageError on missing context, got {result:?}"
        );
    }

    fn make_conditional_append_entry(
        term: u64,
        index: u64,
        context: &str,
        event_name: &str,
        consistency_marker: u64,
    ) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(term, index),
            payload: EntryPayload::Normal(RaftRequest::Append {
                context: context.to_string(),
                events: vec![super::super::types::RaftAppendEvent {
                    identifier: format!("evt-{index}"),
                    name: event_name.to_string(),
                    version: "1.0".to_string(),
                    timestamp: 1712345678000,
                    payload: b"data".to_vec(),
                    metadata: vec![],
                    tags: vec![(b"id".to_vec(), b"1".to_vec())],
                }],
                condition: Some(super::super::types::RaftAppendCondition {
                    consistency_marker,
                    criteria: vec![super::super::types::RaftCriterion {
                        names: vec![],
                        tags: vec![(b"id".to_vec(), b"1".to_vec())],
                    }],
                }),
            }),
        }
    }

    #[tokio::test]
    async fn apply_returns_append_rejected_on_dcb_violation() {
        let (mut sm, contexts) = create_sm();

        // First entry: unconditional append, tags id=1, lands at position 1.
        let e1 = make_append_entry(1, 1, "default", "OrderPlaced");
        sm.apply(vec![e1]).await.unwrap();
        let store = contexts.get_context("default").unwrap();
        let head_after_first = store.head();

        // Second entry: conditional append with consistency_marker=0 and
        // criterion matching id=1 — should be rejected because the first
        // event already matches.
        let e2 = make_conditional_append_entry(1, 2, "default", "OrderPlaced", 0);
        let responses = sm.apply(vec![e2]).await.unwrap();
        assert_eq!(responses.len(), 1);
        match &responses[0] {
            RaftResponse::AppendRejected {
                reason:
                    RaftRejectReason::ConsistencyConditionViolated {
                        conflicting_position,
                    },
            } => {
                assert_eq!(*conflicting_position, 1);
            }
            other => panic!("expected AppendRejected, got {other:?}"),
        }

        // DCB rejection must NOT advance head.
        assert_eq!(store.head(), head_after_first);
    }

    #[tokio::test]
    async fn apply_create_context_idempotent_on_replay() {
        let (mut sm, contexts) = create_sm();

        let e1 = Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Normal(RaftRequest::CreateContext {
                name: "orders".to_string(),
            }),
        };
        let e2 = Entry {
            log_id: log_id(1, 2),
            payload: EntryPayload::Normal(RaftRequest::CreateContext {
                name: "orders".to_string(),
            }),
        };

        let responses = sm.apply(vec![e1, e2]).await.unwrap();
        assert_eq!(responses.len(), 2);
        assert!(matches!(responses[0], RaftResponse::ContextCreated));
        assert!(matches!(responses[1], RaftResponse::ContextCreated));
        assert!(contexts.context_exists("orders"));
    }

    #[tokio::test]
    async fn snapshot_roundtrip() {
        let (mut sm, _ctx) = create_sm();

        sm.apply(vec![blank_entry(1, 1), blank_entry(1, 2)])
            .await
            .unwrap();

        // Build snapshot.
        let mut builder = sm.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();

        assert_eq!(snapshot.meta.last_log_id.unwrap().index, 2);

        // Inspect the payload bytes directly — the test state machine has
        // one context ("default") created in create_sm(); the two applied
        // entries are blank (Membership/Blank payloads, no Append), so
        // "default" must be present with zero events and version=1.
        let bytes = snapshot.snapshot.get_ref().clone();
        let decoded: StateMachineSnapshot = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.version, SNAPSHOT_VERSION);
        assert_eq!(decoded.contexts.len(), 1);
        assert_eq!(decoded.contexts[0].name, "default");
        assert!(decoded.contexts[0].events.is_empty());

        // Install snapshot into a fresh state machine.
        let (mut sm2, _ctx2) = create_sm();
        let data = snapshot.snapshot;
        sm2.install_snapshot(&snapshot.meta, data).await.unwrap();

        let (applied, _) = sm2.applied_state().await.unwrap();
        assert_eq!(applied.unwrap().index, 2);
    }

    // ------------------------------------------------------------------
    // Phase 4 SNAP-01 (Task 2): real snapshot payload — contexts + events.
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn get_current_snapshot_none_before_apply() {
        let (mut sm, _ctx) = create_sm();
        let snap = sm.get_current_snapshot().await.unwrap();
        assert!(snap.is_none(), "expected None before any apply, got Some");
    }

    #[tokio::test]
    async fn get_current_snapshot_some_after_apply() {
        let (mut sm, _ctx) = create_sm();
        sm.apply(vec![make_append_entry(1, 1, "default", "A")])
            .await
            .unwrap();
        let snap = sm
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("expected Some snapshot after apply");
        assert_eq!(snap.meta.last_log_id.unwrap().index, 1);
    }

    #[tokio::test]
    async fn build_snapshot_carries_all_events_from_all_contexts() {
        let (mut sm, contexts) = create_sm();
        contexts.create_context("orders").unwrap();
        contexts.create_context("payments").unwrap();
        let entries = vec![
            make_append_entry(1, 1, "orders", "OrderPlaced"),
            make_append_entry(1, 2, "orders", "OrderShipped"),
            make_append_entry(1, 3, "orders", "OrderDelivered"),
            make_append_entry(1, 4, "payments", "PaymentReceived"),
            make_append_entry(1, 5, "payments", "PaymentReconciled"),
        ];
        sm.apply(entries).await.unwrap();
        let mut builder = sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();
        let bytes = snap.snapshot.get_ref().clone();
        let decoded: StateMachineSnapshot = bincode::deserialize(&bytes).unwrap();
        assert_eq!(decoded.version, SNAPSHOT_VERSION);
        assert_eq!(decoded.last_applied.unwrap().index, 5);
        // list_contexts() returns names sorted; look up by name rather
        // than positional index.
        let orders = decoded
            .contexts
            .iter()
            .find(|c| c.name == "orders")
            .unwrap();
        let payments = decoded
            .contexts
            .iter()
            .find(|c| c.name == "payments")
            .unwrap();
        assert_eq!(orders.events.len(), 3);
        assert_eq!(payments.events.len(), 2);
        assert_eq!(orders.events[0].position, 1);
        assert_eq!(orders.events[1].position, 2);
        assert_eq!(orders.events[2].position, 3);
        assert_eq!(orders.events[0].name, "OrderPlaced");
        assert_eq!(payments.events[0].position, 1);
        assert_eq!(payments.events[1].position, 2);
        // "default" context from create_sm() is present but empty:
        let default = decoded
            .contexts
            .iter()
            .find(|c| c.name == "default")
            .unwrap();
        assert!(default.events.is_empty());
    }

    #[tokio::test]
    async fn snapshot_roundtrips_event_tags() {
        let (mut sm, _ctx) = create_sm();
        sm.apply(vec![make_append_entry(1, 1, "default", "Tagged")])
            .await
            .unwrap();
        let mut builder = sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();
        let bytes = snap.snapshot.get_ref().clone();
        let decoded: StateMachineSnapshot = bincode::deserialize(&bytes).unwrap();
        let default = decoded
            .contexts
            .iter()
            .find(|c| c.name == "default")
            .unwrap();
        assert_eq!(default.events.len(), 1);
        // make_append_entry seeds one tag: ("id", index_as_bytes).
        assert_eq!(default.events[0].tags.len(), 1);
        assert_eq!(default.events[0].tags[0].0, b"id");
        assert_eq!(default.events[0].tags[0].1, b"1");
    }
}
