use std::io::Cursor;
use std::sync::Arc;

use openraft::storage::RaftStateMachine;
use openraft::{
    CommittedLeaderId, Entry, EntryPayload, LogId, OptionalSend, RaftLogId, RaftSnapshotBuilder,
    Snapshot, SnapshotMeta, StorageError, StoredMembership,
};
use serde::{Deserialize, Serialize};

use crate::append::{AppendRequest, AppliedLogId};
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
    /// Construct a state machine, recovering `last_applied` from on-disk
    /// `RaftMarker` records in each context's event segments (Option D).
    ///
    /// On fresh data dirs (no contexts discovered) or legacy data predating
    /// marker writes, recovery yields `None` and the state machine starts
    /// with `last_applied = None` — identical to pre-Option D semantics.
    ///
    /// NodeId is reconstructed as `0` because marker records store only
    /// `(term, index)`: openraft's `LogId` equality comparison treats the
    /// node-id field as part of `leader_id`, so using a fixed sentinel is
    /// sound for the recovery path where we only need openraft to accept
    /// `applied_state()` as "something at least this high is applied".
    /// Subsequent `apply()` calls overwrite `last_applied` with the full
    /// real `LogId` carried on each entry.
    pub fn new(contexts: Arc<ContextManager>) -> Result<Self, Error> {
        let last_applied = contexts.max_applied_log_id()?.map(|applied| LogId {
            leader_id: CommittedLeaderId::new(applied.term, 0),
            index: applied.index,
        });
        Ok(Self {
            contexts,
            last_applied,
            last_membership: StoredMembership::default(),
        })
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
        log_id: &LogId<NodeId>,
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

                // Piggyback `last_applied` onto the event-segment fsync by
                // routing through `append_with_raft`. The segment writer
                // emits a `RaftMarker::normal(term, index, count)` inline
                // with the events; on restart, scanning markers recovers
                // `last_applied` with no extra fsync and no sidecar file.
                let applied = AppliedLogId {
                    term: log_id.leader_id.get_term(),
                    index: log_id.index,
                };
                match self
                    .contexts
                    .with_context(context, |store| store.append_with_raft(append_req, applied))
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
            let log_id = *entry.get_log_id();
            self.last_applied = Some(log_id);

            match entry.payload {
                EntryPayload::Normal(req) => {
                    // Only Normal entries produce durable effects via the event
                    // segment — thread the entry's LogId down so the segment
                    // writer can emit a `RaftMarker::normal(term, index, count)`
                    // record inside the same fsync. Membership/Blank below do
                    // NOT emit markers; they're recovered as idempotent replay
                    // past `last_applied` on restart.
                    let resp = self.apply_request(&req, &log_id)?;
                    responses.push(resp);
                }
                EntryPayload::Membership(ref membership) => {
                    self.last_membership =
                        StoredMembership::new(Some(log_id), membership.clone());
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

        // Version check BEFORE any destructive action (atomicity guard — a
        // payload we cannot interpret must NOT wipe follower state). The
        // unknown-version error literal below is the sole site of that
        // string in the module; 04-01 deliberately left its stub untouched
        // so the literal appears exactly once.
        if sm_snapshot.version != SNAPSHOT_VERSION {
            return Err(StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Read,
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unsupported snapshot version {}", sm_snapshot.version),
                ),
            ));
        }

        // D-03: wipe-and-replace. Close existing engines, remove context dirs,
        // fsync data_dir — all inside ContextManager::reset_all.
        self.contexts.reset_all().map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Write,
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("install_snapshot: reset_all failed: {e}"),
                ),
            )
        })?;

        // Rebuild each context by creating it and re-appending its events
        // with no condition. Positions align because disk was wiped: a fresh
        // engine assigns Position(1) to its first append, matching what the
        // leader's snapshot records.
        for ctx in &sm_snapshot.contexts {
            self.contexts.create_context(&ctx.name).map_err(|e| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::StateMachine,
                    openraft::ErrorVerb::Write,
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!(
                            "install_snapshot: create_context({}) failed: {e}",
                            ctx.name
                        ),
                    ),
                )
            })?;

            if ctx.events.is_empty() {
                continue;
            }

            // Rebuild event list for this context. Each SnapshotEvent becomes
            // an AppendEvent in the same order; append all of them in a
            // single AppendRequest to preserve the leader's intra-batch
            // position ordering.
            let append_events: Vec<crate::event::AppendEvent> = ctx
                .events
                .iter()
                .map(|se| crate::event::AppendEvent {
                    identifier: se.identifier.clone(),
                    name: se.name.clone(),
                    version: se.version.clone(),
                    timestamp: se.timestamp,
                    payload: se.payload.clone(),
                    metadata: se.metadata.clone(),
                    tags: se
                        .tags
                        .iter()
                        .map(|(k, v)| crate::event::Tag {
                            key: k.clone(),
                            value: v.clone(),
                        })
                        .collect(),
                })
                .collect();

            // Per apply-time authority (Phase 3): install is NOT an apply.
            // Condition is None unconditionally — snapshot bytes are
            // authoritative; DCB evaluation must NOT run here. This append
            // call also rebuilds the per-context tag index (bloom filter +
            // per-tag roaring bitmap) as a side effect — see must_haves truth
            // about tag index rebuild.
            let expected_first_pos = ctx.events.first().map(|e| e.position);
            let resp = self
                .contexts
                .with_context(&ctx.name, |engine| {
                    engine.append(crate::append::AppendRequest {
                        condition: None,
                        events: append_events,
                    })
                })
                .map_err(|e| {
                    StorageError::from_io_error(
                        openraft::ErrorSubject::StateMachine,
                        openraft::ErrorVerb::Write,
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!("install_snapshot: append({}) failed: {e}", ctx.name),
                        ),
                    )
                })?;

            // Sanity: the first position assigned must match what the leader
            // recorded for this event. If disk wipe + fresh create_context
            // did not produce Position(1), something is very wrong and we
            // must halt rather than apply on a divergent base.
            if let Some(expected) = expected_first_pos {
                if resp.first_position.0 != expected {
                    return Err(StorageError::from_io_error(
                        openraft::ErrorSubject::StateMachine,
                        openraft::ErrorVerb::Write,
                        std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!(
                                "install_snapshot: position drift in {}: leader={} follower={}",
                                ctx.name, expected, resp.first_position.0
                            ),
                        ),
                    ));
                }
            }
        }

        // D-06: restore last_applied and last_membership LAST, after all
        // context state is rebuilt successfully. Doing it last means a
        // partial-install crash leaves last_applied unchanged so a retry
        // rewinds all the way.
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
        let sm = EventStoreStateMachine::new(Arc::clone(&contexts)).unwrap();
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

    // ------------------------------------------------------------------
    // Phase 4 SNAP-02 (Task 2): install_snapshot wipe-and-replace.
    // ------------------------------------------------------------------

    #[tokio::test]
    async fn install_snapshot_restores_single_context_events() {
        // Leader: apply 3 events to "default".
        let (mut leader_sm, leader_ctx) = create_sm();
        leader_sm
            .apply(vec![
                make_append_entry(1, 1, "default", "A"),
                make_append_entry(1, 2, "default", "B"),
                make_append_entry(1, 3, "default", "C"),
            ])
            .await
            .unwrap();
        let mut builder = leader_sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();

        // Fresh follower — different tempdir, different ContextManager.
        let (mut follower_sm, follower_ctx) = create_sm();
        follower_sm
            .install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();

        let engine = follower_ctx.get_context("default").unwrap();
        let all = engine.source_all(crate::event::Position(1)).unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].position.0, 1);
        assert_eq!(all[1].position.0, 2);
        assert_eq!(all[2].position.0, 3);
        assert_eq!(all[0].name, "A");
        let (applied, _) = follower_sm.applied_state().await.unwrap();
        assert_eq!(applied.unwrap().index, 3);

        drop(leader_ctx);
    }

    #[tokio::test]
    async fn install_snapshot_removes_context_absent_from_snapshot() {
        // Leader: only "default" with 1 event.
        let (mut leader_sm, _leader_ctx) = create_sm();
        leader_sm
            .apply(vec![make_append_entry(1, 1, "default", "A")])
            .await
            .unwrap();
        let mut builder = leader_sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();

        // Follower: has "stale" with events, plus "default" with its own events
        // — divergent state representing a lagging rejoiner.
        let (mut follower_sm, follower_ctx) = create_sm();
        follower_ctx.create_context("stale").unwrap();
        follower_sm
            .apply(vec![
                make_append_entry(1, 1, "stale", "old1"),
                make_append_entry(1, 2, "stale", "old2"),
                make_append_entry(1, 3, "default", "follower-diverged"),
            ])
            .await
            .unwrap();
        assert!(follower_ctx.context_exists("stale"));

        // Install — "stale" must be removed, "default" must be rebuilt.
        follower_sm
            .install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();

        assert!(
            !follower_ctx.context_exists("stale"),
            "stale context should be wiped by install"
        );
        assert!(follower_ctx.context_exists("default"));
        let engine = follower_ctx.get_context("default").unwrap();
        let all = engine.source_all(crate::event::Position(1)).unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].name, "A", "follower's divergent event was wiped");
    }

    #[tokio::test]
    async fn install_snapshot_restores_last_applied_and_membership() {
        let (mut leader_sm, _) = create_sm();
        // Apply two entries to drive last_applied forward.
        leader_sm
            .apply(vec![
                make_append_entry(2, 7, "default", "X"),
                make_append_entry(2, 8, "default", "Y"),
            ])
            .await
            .unwrap();
        let mut builder = leader_sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();
        let meta = snap.meta.clone();

        let (mut follower_sm, _) = create_sm();
        follower_sm
            .install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();

        let (applied, _) = follower_sm.applied_state().await.unwrap();
        assert_eq!(applied, meta.last_log_id);
    }

    #[tokio::test]
    async fn install_snapshot_rejects_unknown_version_without_wiping() {
        let (mut follower_sm, follower_ctx) = create_sm();
        follower_ctx.create_context("orders").unwrap();
        follower_sm
            .apply(vec![make_append_entry(1, 1, "orders", "Existing")])
            .await
            .unwrap();

        // Hand-roll a bogus payload with version=99. Serde is structural: we
        // build a struct that shares the layout but carries a different u8.
        #[derive(serde::Serialize)]
        struct BogusSnap {
            version: u8,
            last_applied: Option<LogId<NodeId>>,
            last_membership: StoredMembership<NodeId, openraft::BasicNode>,
            contexts: Vec<ContextSnapshot>,
        }
        let bogus = BogusSnap {
            version: 99,
            last_applied: None,
            last_membership: StoredMembership::default(),
            contexts: vec![],
        };
        let bytes = bincode::serialize(&bogus).unwrap();
        let cursor = Box::new(Cursor::new(bytes));
        let meta: SnapshotMeta<NodeId, openraft::BasicNode> = SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id: "bogus".into(),
        };

        let result = follower_sm.install_snapshot(&meta, cursor).await;
        assert!(result.is_err(), "expected version-mismatch StorageError");
        // Crucially, the follower's state was NOT wiped:
        assert!(follower_ctx.context_exists("orders"));
        let engine = follower_ctx.get_context("orders").unwrap();
        assert_eq!(
            engine.source_all(crate::event::Position(1)).unwrap().len(),
            1
        );
    }

    #[tokio::test]
    async fn install_snapshot_multi_context_roundtrip() {
        let (mut leader_sm, leader_ctx) = create_sm();
        leader_ctx.create_context("orders").unwrap();
        leader_ctx.create_context("payments").unwrap();
        leader_sm
            .apply(vec![
                make_append_entry(1, 1, "orders", "O1"),
                make_append_entry(1, 2, "orders", "O2"),
                make_append_entry(1, 3, "orders", "O3"),
                make_append_entry(1, 4, "payments", "P1"),
                make_append_entry(1, 5, "payments", "P2"),
            ])
            .await
            .unwrap();
        let mut builder = leader_sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.unwrap();

        let (mut follower_sm, follower_ctx) = create_sm();
        follower_sm
            .install_snapshot(&snap.meta, snap.snapshot)
            .await
            .unwrap();

        let orders = follower_ctx.get_context("orders").unwrap();
        let payments = follower_ctx.get_context("payments").unwrap();
        assert_eq!(
            orders.source_all(crate::event::Position(1)).unwrap().len(),
            3
        );
        assert_eq!(
            payments
                .source_all(crate::event::Position(1))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            orders.source_all(crate::event::Position(1)).unwrap()[0].name,
            "O1"
        );
        assert_eq!(
            payments.source_all(crate::event::Position(1)).unwrap()[0].name,
            "P1"
        );
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
