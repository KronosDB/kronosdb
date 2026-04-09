use std::io::Cursor;
use std::sync::Arc;

use tracing::{warn};

use openraft::storage::RaftStateMachine;
use openraft::{
    Entry, EntryPayload, LogId, OptionalSend, RaftLogId, RaftSnapshotBuilder, Snapshot,
    SnapshotMeta, StorageError, StoredMembership,
};
use serde::{Deserialize, Serialize};

use kronosdb_eventstore::append::AppendRequest;
use kronosdb_eventstore::context::ContextManager;

use crate::types::{NodeId, RaftRequest, RaftResponse, TypeConfig};

/// Raft snapshot: serialized state machine metadata.
/// The actual event data lives in the EventStoreEngine segments —
/// we just need to track what's been applied.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct StateMachineSnapshot {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
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
    fn apply_request(&self, req: &RaftRequest) -> RaftResponse {
        match req {
            RaftRequest::Append {
                context,
                events,
                condition,
            } => {
                let append_events: Vec<_> = events.iter().map(|e| e.to_event()).collect();
                let append_req = AppendRequest {
                    condition: condition.as_ref().map(|c| c.to_condition()),
                    events: append_events,
                };

                match self.contexts.with_context(context, |store| store.append(append_req)) {
                    Ok(resp) => RaftResponse::Append {
                        first_position: resp.first_position.0,
                        count: resp.count,
                        consistency_marker: resp.consistency_marker.0,
                    },
                    Err(e) => {
                        // Log but don't fail the state machine — Raft entries are committed
                        // and must be applied. DCB violations on followers are expected
                        // (the leader already validated).
                        warn!(context = %context, error = %e, "state machine apply error");
                        RaftResponse::Ok
                    }
                }
            }
            RaftRequest::CreateContext { name } => {
                if let Err(e) = self.contexts.create_context(name) {
                    warn!(name = %name, error = %e, "state machine create context error");
                }
                RaftResponse::ContextCreated
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
                    let resp = self.apply_request(&req);
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
        // For now, we don't persist Raft snapshots — the event store segments
        // ARE the durable state. On restart, Raft replays from the log.
        // This can be optimized later with periodic Raft snapshots.
        Ok(None)
    }
}

/// Builds a Raft snapshot from the current state machine state.
pub struct EventStoreSnapshotBuilder {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
}

impl RaftSnapshotBuilder<TypeConfig> for EventStoreSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let sm_snapshot = StateMachineSnapshot {
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
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
    use openraft::{CommittedLeaderId, Entry, EntryPayload, Membership, RaftLogId};
    use kronosdb_eventstore::event::Position;
    use kronosdb_eventstore::segment::DEFAULT_SEGMENT_SIZE;
    use std::collections::{BTreeMap, BTreeSet};

    fn log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId {
            leader_id: CommittedLeaderId::new(term, 0),
            index,
        }
    }

    fn make_append_entry(term: u64, index: u64, context: &str, event_name: &str) -> Entry<TypeConfig> {
        Entry {
            log_id: log_id(term, index),
            payload: EntryPayload::Normal(RaftRequest::Append {
                context: context.to_string(),
                events: vec![crate::types::RaftAppendEvent {
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
        let contexts = Arc::new(
            ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap(),
        );
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
            RaftResponse::Append { first_position, count, .. } => {
                assert_eq!(*first_position, 1);
                assert_eq!(*count, 1);
            }
            other => panic!("expected Append, got {:?}", other),
        }

        match &responses[1] {
            RaftResponse::Append { first_position, count, .. } => {
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

        sm.apply(vec![
            blank_entry(1, 1),
            blank_entry(1, 2),
        ]).await.unwrap();

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
        nodes.insert(1u64, openraft::BasicNode { addr: "addr1".to_string() });
        nodes.insert(2u64, openraft::BasicNode { addr: "addr2".to_string() });
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
    async fn apply_to_nonexistent_context_doesnt_crash() {
        let (mut sm, _ctx) = create_sm();

        let entry = make_append_entry(1, 1, "nonexistent", "OrderPlaced");
        let responses = sm.apply(vec![entry]).await.unwrap();

        // Should return Ok (with warning), not crash.
        assert_eq!(responses.len(), 1);
        assert!(matches!(responses[0], RaftResponse::Ok));
    }

    #[tokio::test]
    async fn snapshot_roundtrip() {
        let (mut sm, _ctx) = create_sm();

        sm.apply(vec![blank_entry(1, 1), blank_entry(1, 2)]).await.unwrap();

        // Build snapshot.
        let mut builder = sm.get_snapshot_builder().await;
        let snapshot = builder.build_snapshot().await.unwrap();

        assert_eq!(snapshot.meta.last_log_id.unwrap().index, 2);

        // Install snapshot into a fresh state machine.
        let (mut sm2, _ctx2) = create_sm();
        let data = snapshot.snapshot;
        sm2.install_snapshot(&snapshot.meta, data).await.unwrap();

        let (applied, _) = sm2.applied_state().await.unwrap();
        assert_eq!(applied.unwrap().index, 2);
    }
}
