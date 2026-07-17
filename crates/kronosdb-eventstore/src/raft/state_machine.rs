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

use super::snapshot_store::{PersistedSnapshot, SnapshotStore};
use super::types::{
    BatchAppendResult, NodeId, RaftRejectReason, RaftRequest, RaftResponse, TypeConfig,
};

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

/// Outcome of `EventStoreStateMachine::reconcile_with_log`.
///
/// Surfaced to `cluster::init_context` so it can take follow-up action
/// that cannot be done from inside the state machine (the SM has no
/// handle to `LogStore` and cannot promote `committed` itself).
#[derive(Debug, Clone, Default)]
pub struct Reconciliation {
    /// True when `last_applied` was rewritten with the real log entry's
    /// `LogId` (resolves the `node_id=0` sentinel mismatch).
    pub last_applied_rewritten: bool,
    /// Present when the caller must promote `log.committed` to the given
    /// `LogId` to satisfy invariant I1 (`last_applied <= committed`).
    pub committed_promoted_to: Option<LogId<NodeId>>,
}

/// The Raft state machine.
///
/// Applies committed Raft log entries to the local EventStoreEngine.
/// This is the bridge between Raft consensus and the event store.
pub struct EventStoreStateMachine {
    contexts: Arc<ContextManager>,
    snapshot_store: Arc<SnapshotStore>,
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
    ///
    /// `last_membership` is hydrated from the latest on-disk snapshot. This
    /// is the load-bearing path for surviving restart-after-purge: once the
    /// cluster-init `Membership` log entry is purged, the snapshot's meta
    /// is the only durable carrier of voter set. Without it, `applied_state`
    /// would return `StoredMembership::default()` and openraft's startup
    /// would default the node to Learner — see `raft/snapshot_store.rs`
    /// for the full failure-mode writeup.
    ///
    /// When a snapshot exists, its `last_log_id` provides a real (non-
    /// sentinel) `LogId` for `last_applied`. The marker recovery is only
    /// authoritative when it is strictly ahead of the snapshot (a Normal
    /// entry was applied after the last snapshot was built); in that case
    /// `reconcile_with_log` rewrites the sentinel `node_id` once it can
    /// see the real entry in the log.
    pub fn new(
        contexts: Arc<ContextManager>,
        snapshot_store: Arc<SnapshotStore>,
    ) -> Result<Self, Error> {
        let snap_meta = snapshot_store.load_latest_meta().map_err(Error::Io)?;

        let marker_applied = contexts.max_applied_log_id()?.map(|applied| LogId {
            leader_id: CommittedLeaderId::new(applied.term, 0),
            index: applied.index,
        });

        // Highest applied across the two recovery sources. The snapshot
        // gives a real `node_id`; the marker provides a sentinel. We
        // prefer the snapshot's LogId when their indices tie so subsequent
        // openraft invariants (which compare full `LogId` lexicographically)
        // see real `node_id`s instead of the marker sentinel.
        let snap_applied = snap_meta.as_ref().and_then(|m| m.last_log_id);
        let last_applied = match (snap_applied, marker_applied) {
            (Some(s), Some(m)) if m.index > s.index => Some(m),
            (Some(s), _) => Some(s),
            (None, m) => m,
        };

        // Membership hydration priority: membership.bin (written on every
        // membership apply — survives restarts that happen before the first
        // snapshot) > latest snapshot meta > empty. When only the snapshot
        // carries it, heal forward by writing membership.bin now.
        let last_membership = match snapshot_store.load_membership().map_err(Error::Io)? {
            Some(m) => m,
            None => {
                let from_snap = snap_meta
                    .as_ref()
                    .map(|m| m.last_membership.clone())
                    .unwrap_or_default();
                if from_snap.membership().voter_ids().count() > 0 {
                    snapshot_store
                        .save_membership(&from_snap)
                        .map_err(Error::Io)?;
                }
                from_snap
            }
        };

        tracing::info!(
            target: "raft.recovery",
            snap_applied = ?snap_applied,
            marker_applied = ?marker_applied,
            chosen_last_applied = ?last_applied,
            voter_count = last_membership.membership().voter_ids().count(),
            "state machine recovered (snapshot meta + on-disk markers)"
        );

        Ok(Self {
            contexts,
            snapshot_store,
            last_applied,
            last_membership,
        })
    }

    /// Reconciles the state-machine's recovered `last_applied` against the
    /// log store's view of the world.
    ///
    /// Marker-based recovery (`new` above) reconstructs `last_applied` with
    /// a sentinel `node_id=0` because a `RaftMarker` only stores `(term,
    /// index)`. openraft's `LogId` equality is `(term, node_id, index)` —
    /// with a sentinel, the SM's recovered `last_applied` does not compare
    /// equal to the log entry at the same index (which has the real
    /// `node_id`). Empirically this produces two CRASH-02 shapes:
    ///
    /// - **Shape 1 (node ahead):** marker-durable apply of index I whose
    ///   log-flushed callback didn't fire pre-crash. On restart:
    ///   `applied=I (sentinel)`, `committed=I-1`. openraft starts replay
    ///   from `committed+1=I` and, due to sentinel mismatch, re-delivers
    ///   entry I (and later entries) to `apply()`, which calls
    ///   `append_with_raft` for each — double-writing events into the
    ///   segment. Heads diverge: restarted node's head overshoots.
    ///
    /// - **Shape 2 (node behind):** marker-durable apply of index I; log
    ///   has `committed=I`, `last_log>I`. Sentinel mismatch confuses
    ///   openraft's "apply after catch-up" path: entries beyond I are
    ///   never delivered to the SM. Heads diverge: restarted node stays
    ///   at head=I while survivors advance.
    ///
    /// # Fix
    ///
    /// 1. If the log contains an entry at `last_applied.index`, replace
    ///    the SM's `last_applied` with that entry's full `LogId` (real
    ///    `node_id`). This resolves Shape 2 and also prevents Shape 1's
    ///    double-apply path because openraft's equality comparison now
    ///    succeeds for entry I — replay starts from `applied+1 = I+1`.
    ///
    /// 2. If the SM's `last_applied.index > log.committed.index`, the
    ///    markers reflect a durable local apply whose log-flushed callback
    ///    didn't complete pre-crash. The local log entry at that index
    ///    MUST be present (markers are only written after the entry is
    ///    in the log batch) and, by Raft log-matching, any future leader
    ///    winning election must preserve it. It is therefore safe to
    ///    promote `log.committed` up to `last_applied.index`. This
    ///    resolves Shape 1's `applied > committed` asymmetry.
    ///
    /// `reconcile_with_log` is idempotent: on a cluster that crashed
    /// without in-flight applies (clean shutdown), the log entry at
    /// `last_applied.index` already matches the SM's `last_applied` term
    /// and index, and `committed >= last_applied`, so no mutation happens.
    pub fn reconcile_with_log<F>(
        &mut self,
        log_last: Option<LogId<NodeId>>,
        log_committed: Option<LogId<NodeId>>,
        read_entry: F,
    ) -> Result<Reconciliation, Error>
    where
        F: FnOnce(u64) -> Result<Option<LogId<NodeId>>, Error>,
    {
        let Some(sm_applied) = self.last_applied else {
            // Fresh data dir / no markers yet — nothing to reconcile. The
            // SM's `last_applied` is already `None` and openraft will
            // start from committed/log state alone.
            tracing::info!(
                target: "raft.recovery",
                "reconcile: state machine has no last_applied — nothing to reconcile"
            );
            return Ok(Reconciliation::default());
        };

        // Invariant I0 (structural): marker-durable apply cannot exceed
        // the log's last entry. A marker is only written inside the same
        // append that persists the log entry; if the marker is on disk
        // but the log entry is missing, the log store was corrupted or
        // rebuilt from a stale source.
        if let Some(last) = log_last {
            if sm_applied.index > last.index {
                return Err(Error::Corrupted {
                    message: format!(
                        "reconcile: state machine last_applied (index {}) exceeds log last_log_id (index {}); log and event segments are inconsistent",
                        sm_applied.index, last.index
                    ),
                });
            }
        } else {
            // Log has no entries but SM has markers. Same contradiction.
            return Err(Error::Corrupted {
                message: format!(
                    "reconcile: state machine last_applied (index {}) set but raft log is empty; log and event segments are inconsistent",
                    sm_applied.index
                ),
            });
        }

        let mut report = Reconciliation::default();

        // I3: rewrite last_applied's node_id to match the real log entry.
        let real_entry_log_id = read_entry(sm_applied.index)?;
        if let Some(real) = real_entry_log_id {
            if real != sm_applied {
                tracing::info!(
                    target: "raft.recovery",
                    old = ?sm_applied,
                    new = ?real,
                    "reconcile: rewriting state machine last_applied with real log entry LogId"
                );
                self.last_applied = Some(real);
                report.last_applied_rewritten = true;
            }
        } else {
            // Entry at sm_applied.index is missing from the log. This
            // means the log was truncated below the state machine's
            // apply point — a situation that should only arise if a
            // snapshot install advanced the SM but left old log entries
            // purged. Leave last_applied as-is; openraft will treat the
            // sentinel node_id as "at least this index applied" and rely
            // on snapshot-install membership to move forward.
            tracing::warn!(
                target: "raft.recovery",
                ?sm_applied,
                "reconcile: log has no entry at last_applied.index — keeping sentinel (likely post-snapshot install)"
            );
        }

        // I1: promote committed up to last_applied if marker-evidence is
        // ahead of the log-flushed callback's recorded commit.
        let sm_applied_final = self.last_applied.expect("set above or unchanged");
        let should_promote = match log_committed {
            Some(c) => c.index < sm_applied_final.index,
            None => true,
        };
        if should_promote {
            tracing::info!(
                target: "raft.recovery",
                old = ?log_committed,
                new = ?sm_applied_final,
                "reconcile: promoting log committed to match state machine last_applied"
            );
            report.committed_promoted_to = Some(sm_applied_final);
        }

        Ok(report)
    }

    /// Flushes coalesced Append entries: one `append_with_raft_batch` call
    /// per context (order preserved within each context), one fsync per
    /// batch. Per-item DCB rejections land in their entry's response slot;
    /// everything else is a fatal `StorageError` per D-04..D-08.
    fn flush_append_batch(
        &self,
        pending: &mut Vec<(usize, String, AppendRequest, AppliedLogId)>,
        responses: &mut [Option<RaftResponse>],
    ) -> Result<(), StorageError<NodeId>> {
        if pending.is_empty() {
            return Ok(());
        }
        #[cfg(feature = "bench-instrumentation")]
        let _t = Timer::new(Region::ApplyEventPath);

        // Group by context, preserving arrival order within each context —
        // an item's DCB check must see every earlier item's writes.
        let mut by_context: Vec<(String, Vec<(usize, AppendRequest, AppliedLogId)>)> = Vec::new();
        for (slot, ctx, req, applied) in pending.drain(..) {
            match by_context.iter_mut().find(|(c, _)| *c == ctx) {
                Some((_, items)) => items.push((slot, req, applied)),
                None => by_context.push((ctx, vec![(slot, req, applied)])),
            }
        }

        for (ctx, items) in by_context {
            let slots: Vec<usize> = items.iter().map(|(slot, _, _)| *slot).collect();
            let batch: Vec<(AppendRequest, AppliedLogId)> =
                items.into_iter().map(|(_, req, a)| (req, a)).collect();
            let results = self
                .contexts
                .with_context(&ctx, |store| store.append_with_raft_batch(batch))
                .map_err(|e| Self::fatal_append_error(&ctx, e))?;

            for (slot, item_result) in slots.into_iter().zip(results) {
                responses[slot] = Some(match item_result {
                    Ok(resp) => RaftResponse::Append {
                        first_position: resp.first_position.0,
                        count: resp.count,
                        consistency_marker: resp.consistency_marker.0,
                    },
                    // D-04: deterministic rejection is a valid apply response.
                    Err(Error::ConsistencyConditionViolated {
                        conflicting_position,
                    }) => RaftResponse::AppendRejected {
                        reason: RaftRejectReason::ConsistencyConditionViolated {
                            conflicting_position: conflicting_position.0,
                        },
                    },
                    Err(other) => return Err(Self::fatal_append_error(&ctx, other)),
                });
            }
        }
        Ok(())
    }

    /// Maps a fatal append-path error to a `StorageError` per D-05..D-08
    /// (same classification as `apply_request`'s Append arm).
    fn fatal_append_error(context: &str, err: Error) -> StorageError<NodeId> {
        match err {
            Error::ContextNotFound { name } => StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Write,
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("apply: context not found: {name} (batch context {context})"),
                ),
            ),
            Error::Io(io_err) => StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Write,
                io_err,
            ),
            Error::Corrupted { message } => StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Write,
                std::io::Error::new(std::io::ErrorKind::InvalidData, message),
            ),
            other => StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Write,
                std::io::Error::other(format!("unexpected apply error: {other}")),
            ),
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
            RaftRequest::AppendBatch { .. } => {
                // Handled directly in `apply` (needs response-slot plumbing);
                // reaching here would be a routing bug.
                Err(StorageError::from_io_error(
                    openraft::ErrorSubject::StateMachine,
                    openraft::ErrorVerb::Write,
                    std::io::Error::other(
                        "AppendBatch must be applied via apply(), not apply_request()",
                    ),
                ))
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
        let entries: Vec<Entry<TypeConfig>> = entries.into_iter().collect();
        let mut responses: Vec<Option<RaftResponse>> = Vec::with_capacity(entries.len());
        responses.resize_with(entries.len(), || None);

        // Append entries are coalesced and flushed as ONE engine batch per
        // context — one fsync per apply() call instead of one per entry.
        // Without this, concurrent consensus appends serialize on their
        // individual group-commit windows and throughput collapses to
        // ~1/interval regardless of writer concurrency. Non-append Normal
        // entries (CreateContext) act as ordering barriers.
        let mut pending: Vec<(usize, String, AppendRequest, AppliedLogId)> = Vec::new();

        for (i, entry) in entries.into_iter().enumerate() {
            let log_id = *entry.get_log_id();
            self.last_applied = Some(log_id);

            match entry.payload {
                EntryPayload::Normal(RaftRequest::Append {
                    context,
                    events,
                    condition,
                }) => {
                    // Only Append entries produce durable effects via the
                    // event segment — the entry's LogId is threaded down so
                    // the segment writer emits a `RaftMarker::normal(term,
                    // index, count)` record inside the same fsync.
                    // Membership/Blank below do NOT emit markers; they're
                    // recovered as idempotent replay past `last_applied`.
                    let append_req = AppendRequest {
                        condition: condition.as_ref().map(|c| c.to_condition()),
                        events: events.iter().map(|e| e.to_event()).collect(),
                    };
                    let applied = AppliedLogId {
                        term: log_id.leader_id.get_term(),
                        index: log_id.index,
                    };
                    pending.push((i, context, append_req, applied));
                }
                EntryPayload::Normal(RaftRequest::AppendBatch { context, items }) => {
                    // A proposer-coalesced entry: N independent appends to
                    // one context under ONE raft marker. Ordering barrier
                    // like any append — flush what's pending first.
                    self.flush_append_batch(&mut pending, &mut responses)?;
                    let applied = AppliedLogId {
                        term: log_id.leader_id.get_term(),
                        index: log_id.index,
                    };
                    let requests: Vec<AppendRequest> = items
                        .iter()
                        .map(|item| AppendRequest {
                            condition: item.condition.as_ref().map(|c| c.to_condition()),
                            events: item.events.iter().map(|e| e.to_event()).collect(),
                        })
                        .collect();
                    let results = self
                        .contexts
                        .with_context(&context, |store| {
                            store.append_with_raft_entry_batch(requests, applied)
                        })
                        .map_err(|e| Self::fatal_append_error(&context, e))?;
                    let mut batch_results = Vec::with_capacity(results.len());
                    for item_result in results {
                        batch_results.push(match item_result {
                            Ok(resp) => BatchAppendResult::Append {
                                first_position: resp.first_position.0,
                                count: resp.count,
                                consistency_marker: resp.consistency_marker.0,
                            },
                            Err(Error::ConsistencyConditionViolated {
                                conflicting_position,
                            }) => BatchAppendResult::Rejected {
                                reason: RaftRejectReason::ConsistencyConditionViolated {
                                    conflicting_position: conflicting_position.0,
                                },
                            },
                            Err(other) => {
                                return Err(Self::fatal_append_error(&context, other));
                            }
                        });
                    }
                    responses[i] = Some(RaftResponse::AppendBatch {
                        results: batch_results,
                    });
                }
                EntryPayload::Normal(req) => {
                    // CreateContext (and any future non-append request):
                    // appends logged before it must land first.
                    self.flush_append_batch(&mut pending, &mut responses)?;
                    let resp = self.apply_request(&req, &log_id)?;
                    responses[i] = Some(resp);
                }
                EntryPayload::Membership(ref membership) => {
                    self.last_membership = StoredMembership::new(Some(log_id), membership.clone());
                    // Durable immediately — the log entry alone is not enough:
                    // once applied it leaves openraft's startup rescan window,
                    // and the next snapshot may be thousands of entries away.
                    self.snapshot_store
                        .save_membership(&self.last_membership)
                        .map_err(|e| {
                            StorageError::from_io_error(
                                openraft::ErrorSubject::StateMachine,
                                openraft::ErrorVerb::Write,
                                e,
                            )
                        })?;
                    responses[i] = Some(RaftResponse::Ok);
                }
                EntryPayload::Blank => {
                    responses[i] = Some(RaftResponse::Ok);
                }
            }
        }
        self.flush_append_batch(&mut pending, &mut responses)?;

        Ok(responses
            .into_iter()
            .map(|r| r.expect("every applied entry produces a response"))
            .collect())
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        EventStoreSnapshotBuilder {
            last_applied: self.last_applied,
            last_membership: self.last_membership.clone(),
            contexts: Arc::clone(&self.contexts),
            snapshot_store: Arc::clone(&self.snapshot_store),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
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
        // engine assigns Position(0) to its first append, matching what the
        // leader's snapshot records.
        for ctx in &sm_snapshot.contexts {
            self.contexts.create_context(&ctx.name).map_err(|e| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::StateMachine,
                    openraft::ErrorVerb::Write,
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("install_snapshot: create_context({}) failed: {e}", ctx.name),
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
            // did not produce Position(0), something is very wrong and we
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

        // Keep membership.bin in sync with the installed snapshot so the
        // startup hydration priority (membership.bin first) never resurrects
        // a pre-install voter set.
        self.snapshot_store
            .save_membership(&self.last_membership)
            .map_err(|e| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::StateMachine,
                    openraft::ErrorVerb::Write,
                    e,
                )
            })?;

        // Persist the installed snapshot to disk. Without this, a restart
        // after install would lose `last_membership` (since markers don't
        // carry membership) and the node would default to Learner — exactly
        // the bug we're fixing for the locally-built snapshot path. The
        // disk write happens AFTER the in-memory restore so a crash here
        // leaves us in a recoverable state: SM has new state in memory,
        // disk still has the old snapshot, next restart re-applies the
        // committed log range to catch up.
        self.snapshot_store
            .write(&PersistedSnapshot {
                meta: meta.clone(),
                data,
            })
            .map_err(|e| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::Snapshot(Some(meta.signature())),
                    openraft::ErrorVerb::Write,
                    e,
                )
            })?;

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        // openraft contract: return the latest persisted snapshot if one
        // exists, else None. openraft's startup helper rebuilds via
        // `get_snapshot_builder` when this returns None and logs have been
        // purged (helper.rs:132-143), so we don't need to materialize on
        // demand here — that would just duplicate work and produce a
        // snapshot whose meta doesn't match anything on disk.
        match self.snapshot_store.load_latest().map_err(|e| {
            StorageError::from_io_error(openraft::ErrorSubject::Store, openraft::ErrorVerb::Read, e)
        })? {
            Some(persisted) => Ok(Some(Snapshot {
                meta: persisted.meta,
                snapshot: Box::new(Cursor::new(persisted.data)),
            })),
            None => Ok(None),
        }
    }
}

/// Builds a Raft snapshot from the current state machine state.
///
/// Holds a cloned `Arc<ContextManager>` so `build_snapshot` can walk every
/// context via `list_contexts` + `get_context`, calling the new
/// `EventStoreEngine::source_all` helper for each to materialize the
/// per-context event stream (Phase 4, SNAP-01). Also carries an
/// `Arc<SnapshotStore>` so the built snapshot is persisted to disk before
/// it is handed back to openraft — without that, restart loses
/// `last_membership` and the node refuses to elect.
pub struct EventStoreSnapshotBuilder {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
    contexts: Arc<ContextManager>,
    snapshot_store: Arc<SnapshotStore>,
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
            let stored = engine.source_all(crate::event::Position(0)).map_err(|e| {
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

        // Persist before handing back to openraft. If this fails, openraft
        // sees the build as failed and won't purge logs against this
        // snapshot — preserving the invariant that purge is only safe once
        // a snapshot covering those entries is durable.
        self.snapshot_store
            .write(&PersistedSnapshot {
                meta: meta.clone(),
                data: data.clone(),
            })
            .map_err(|e| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::Snapshot(Some(meta.signature())),
                    openraft::ErrorVerb::Write,
                    e,
                )
            })?;

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

/// Synthesize and persist a rescue snapshot for a data dir that was created
/// by a pre-fix server: openraft built and purged in-memory snapshots, but
/// nothing was ever written to disk. The cluster-init `Membership` log entry
/// at index 1 is gone, no other Membership entries survive, and the only
/// durable carrier of the voter set was the snapshot's `meta.last_membership`
/// — which doesn't exist. On the next restart `applied_state()` returns
/// `StoredMembership::default()`, openraft defaults the node to Learner, and
/// writes hang forever (see `restart_after_snapshot_single_node.rs`).
///
/// This helper rebuilds the snapshot bytes from the live event segments
/// (the only authoritative state we still have) and pairs them with a
/// membership reconstructed from `cluster_config`. Caller MUST verify that
/// the rescue is safe before invoking — specifically, that the running node
/// is one of the synthesized voters, otherwise the membership we write would
/// silently exclude this node from its own cluster.
pub fn synthesize_rescue_snapshot(
    contexts: &Arc<ContextManager>,
    snapshot_store: &SnapshotStore,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, openraft::BasicNode>,
) -> Result<(), Error> {
    let names = contexts.list_contexts();
    let mut context_snaps = Vec::with_capacity(names.len());
    for name in names {
        let engine = contexts.get_context(&name)?;
        let stored = engine.source_all(crate::event::Position(0))?;
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
        last_applied,
        last_membership: last_membership.clone(),
        contexts: context_snaps,
    };

    let data = bincode::serialize(&sm_snapshot).map_err(|e| Error::Corrupted {
        message: format!("rescue snapshot serialize: {e}"),
    })?;

    let snapshot_id = last_applied
        .map(|id| format!("rescue-{}-{}", id.leader_id, id.index))
        .unwrap_or_else(|| "rescue-empty".to_string());

    let meta = SnapshotMeta {
        last_log_id: last_applied,
        last_membership,
        snapshot_id,
    };

    snapshot_store
        .write(&PersistedSnapshot { meta, data })
        .map_err(Error::Io)?;

    Ok(())
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
        let snap_store = Arc::new(SnapshotStore::new(dir.path().join("snapshots")).unwrap());
        let sm = EventStoreStateMachine::new(Arc::clone(&contexts), snap_store).unwrap();
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
                assert_eq!(*first_position, 0);
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
                assert_eq!(*first_position, 1);
                assert_eq!(*count, 1);
            }
            other => panic!("expected Append, got {:?}", other),
        }

        // Verify events in the store: 2 events committed → head = 2 (next slot).
        let store = contexts.get_context("default").unwrap();
        assert_eq!(store.head(), Position(2));
    }

    /// The coalesced apply path: a mixed batch in ONE apply() call — appends
    /// to two contexts, a CreateContext barrier, an append to the created
    /// context, and a mid-batch DCB rejection — must produce ordered,
    /// per-entry responses identical to entry-at-a-time semantics.
    #[tokio::test]
    async fn apply_mixed_batch_coalesces_appends() {
        let (mut sm, contexts) = create_sm();
        contexts.create_context("other").unwrap();

        // Entry 5 conflicts with entry 1: same `id` tag value, condition
        // demanding no prior event with it exists past marker 0.
        let conflicting = Entry {
            log_id: log_id(1, 5),
            payload: EntryPayload::Normal(RaftRequest::Append {
                context: "default".to_string(),
                events: vec![super::super::types::RaftAppendEvent {
                    identifier: "evt-dup".to_string(),
                    name: "OrderPlaced".to_string(),
                    version: "1.0".to_string(),
                    timestamp: 1712345678000,
                    payload: b"data".to_vec(),
                    metadata: vec![],
                    tags: vec![(b"id".to_vec(), b"1".to_vec())],
                }],
                condition: Some(super::super::types::RaftAppendCondition {
                    consistency_marker: 0,
                    criteria: vec![super::super::types::RaftCriterion {
                        names: vec![],
                        tags: vec![(b"id".to_vec(), b"1".to_vec())],
                    }],
                }),
            }),
        };

        let entries = vec![
            make_append_entry(1, 1, "default", "OrderPlaced"),
            make_append_entry(1, 2, "other", "OrderPlaced"),
            Entry {
                log_id: log_id(1, 3),
                payload: EntryPayload::Normal(RaftRequest::CreateContext {
                    name: "created-mid-batch".to_string(),
                }),
            },
            make_append_entry(1, 4, "created-mid-batch", "OrderPlaced"),
            conflicting,
            make_append_entry(1, 6, "default", "PaymentReceived"),
        ];

        let responses = sm.apply(entries).await.unwrap();
        assert_eq!(responses.len(), 6);
        assert!(matches!(
            responses[0],
            RaftResponse::Append { count: 1, .. }
        ));
        assert!(matches!(
            responses[1],
            RaftResponse::Append { count: 1, .. }
        ));
        assert!(matches!(responses[2], RaftResponse::ContextCreated));
        assert!(matches!(
            responses[3],
            RaftResponse::Append { count: 1, .. }
        ));
        assert!(matches!(responses[4], RaftResponse::AppendRejected { .. }));
        assert!(matches!(
            responses[5],
            RaftResponse::Append { count: 1, .. }
        ));

        // The rejected entry wrote nothing: default has exactly 2 events.
        assert_eq!(contexts.get_context("default").unwrap().head(), Position(2));
        assert_eq!(contexts.get_context("other").unwrap().head(), Position(1));
        assert_eq!(
            contexts.get_context("created-mid-batch").unwrap().head(),
            Position(1)
        );

        let (applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(applied.unwrap().index, 6);
    }

    /// An AppendBatch entry: per-item results in order, in-batch DCB
    /// visibility (item 2's condition sees item 0's events), one shared
    /// consistency marker, rejected items write nothing.
    #[tokio::test]
    async fn apply_append_batch_entry() {
        let (mut sm, contexts) = create_sm();

        fn item(id_tag: &str, condition_tag: Option<&str>) -> super::super::types::BatchAppendItem {
            super::super::types::BatchAppendItem {
                events: vec![super::super::types::RaftAppendEvent {
                    identifier: format!("evt-{id_tag}"),
                    name: "OrderPlaced".to_string(),
                    version: "1.0".to_string(),
                    timestamp: 1712345678000,
                    payload: b"data".to_vec(),
                    metadata: vec![],
                    tags: vec![(b"id".to_vec(), id_tag.as_bytes().to_vec())],
                }],
                condition: condition_tag.map(|tag| super::super::types::RaftAppendCondition {
                    consistency_marker: 0,
                    criteria: vec![super::super::types::RaftCriterion {
                        names: vec![],
                        tags: vec![(b"id".to_vec(), tag.as_bytes().to_vec())],
                    }],
                }),
            }
        }

        let entry = Entry {
            log_id: log_id(1, 1),
            payload: EntryPayload::Normal(RaftRequest::AppendBatch {
                context: "default".to_string(),
                items: vec![
                    item("a", None),
                    item("b", None),
                    // Conflicts with item 0 INSIDE the same batch.
                    item("a-dup", Some("a")),
                    item("c", None),
                ],
            }),
        };

        let responses = sm.apply(vec![entry]).await.unwrap();
        assert_eq!(responses.len(), 1);
        let results = match &responses[0] {
            RaftResponse::AppendBatch { results } => results,
            other => panic!("expected AppendBatch, got {other:?}"),
        };
        assert_eq!(results.len(), 4);
        assert!(matches!(
            results[0],
            BatchAppendResult::Append {
                first_position: 0,
                count: 1,
                ..
            }
        ));
        assert!(matches!(
            results[1],
            BatchAppendResult::Append {
                first_position: 1,
                count: 1,
                ..
            }
        ));
        assert!(matches!(
            results[2],
            BatchAppendResult::Rejected {
                reason: RaftRejectReason::ConsistencyConditionViolated {
                    conflicting_position: 0
                }
            }
        ));
        assert!(matches!(
            results[3],
            BatchAppendResult::Append {
                first_position: 2,
                count: 1,
                ..
            }
        ));

        // Rejected item wrote nothing: exactly 3 events landed.
        let store = contexts.get_context("default").unwrap();
        assert_eq!(store.head(), Position(3));

        let (applied, _) = sm.applied_state().await.unwrap();
        assert_eq!(applied.unwrap().index, 1);
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

        // First entry: unconditional append, tags id=1, lands at position 0.
        let e1 = make_append_entry(1, 1, "default", "OrderPlaced");
        sm.apply(vec![e1]).await.unwrap();
        let store = contexts.get_context("default").unwrap();
        let head_after_first = store.head();

        // Second entry: conditional append with consistency_marker=0 and
        // criterion matching id=1 — should be rejected because the first
        // event already matches at position 0 (>= marker).
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
                assert_eq!(*conflicting_position, 0);
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
    async fn get_current_snapshot_some_after_build() {
        // openraft's contract: `get_current_snapshot` returns the latest
        // *persisted* snapshot, not one synthesized on demand. A snapshot
        // becomes available only after the snapshot builder runs (either
        // openraft's policy fires or a caller drives the builder
        // explicitly). This test exercises the post-build path.
        let (mut sm, _ctx) = create_sm();
        sm.apply(vec![make_append_entry(1, 1, "default", "A")])
            .await
            .unwrap();
        let mut builder = sm.get_snapshot_builder().await;
        let _ = builder.build_snapshot().await.unwrap();
        let snap = sm
            .get_current_snapshot()
            .await
            .unwrap()
            .expect("expected Some snapshot after build_snapshot");
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
        assert_eq!(orders.events[0].position, 0);
        assert_eq!(orders.events[1].position, 1);
        assert_eq!(orders.events[2].position, 2);
        assert_eq!(orders.events[0].name, "OrderPlaced");
        assert_eq!(payments.events[0].position, 0);
        assert_eq!(payments.events[1].position, 1);
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
        let all = engine.source_all(crate::event::Position(0)).unwrap();
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].position.0, 0);
        assert_eq!(all[1].position.0, 1);
        assert_eq!(all[2].position.0, 2);
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
        let all = engine.source_all(crate::event::Position(0)).unwrap();
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
            engine.source_all(crate::event::Position(0)).unwrap().len(),
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
            orders.source_all(crate::event::Position(0)).unwrap().len(),
            3
        );
        assert_eq!(
            payments
                .source_all(crate::event::Position(0))
                .unwrap()
                .len(),
            2
        );
        assert_eq!(
            orders.source_all(crate::event::Position(0)).unwrap()[0].name,
            "O1"
        );
        assert_eq!(
            payments.source_all(crate::event::Position(0)).unwrap()[0].name,
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

    /// Sentinel-to-real rewrite of `last_applied` when the log has an entry
    /// at the same index. This is the Shape-2 fix: without it, openraft
    /// compares `last_applied` (node_id=0) to log entry (node_id=1) and
    /// refuses to deliver subsequent entries to the state machine.
    #[test]
    fn reconcile_rewrites_sentinel_last_applied_with_real_log_id() {
        // SM recovered from markers: sentinel node_id=0, index=12.
        let dir = tempfile::tempdir().unwrap();
        let contexts = Arc::new(ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap());
        contexts.create_context("default").unwrap();
        let snap_store = Arc::new(SnapshotStore::new(dir.path().join("snapshots")).unwrap());
        let mut sm = EventStoreStateMachine::new(Arc::clone(&contexts), snap_store).unwrap();
        sm.last_applied = Some(LogId {
            leader_id: CommittedLeaderId::new(1, 0),
            index: 12,
        });

        // Log reports last=14, committed=14. Entry at index 12 carries the
        // real leader node_id=1 (the pre-crash leader).
        let log_last = Some(LogId {
            leader_id: CommittedLeaderId::new(1, 1),
            index: 14,
        });
        let log_committed = log_last;
        let real_at_12 = LogId {
            leader_id: CommittedLeaderId::new(1, 1),
            index: 12,
        };

        let report = sm
            .reconcile_with_log(log_last, log_committed, |idx| {
                assert_eq!(idx, 12, "should query log entry at sm.last_applied.index");
                Ok(Some(real_at_12))
            })
            .expect("reconcile should succeed");

        assert!(report.last_applied_rewritten, "sentinel must be rewritten");
        assert!(
            report.committed_promoted_to.is_none(),
            "committed already >= last_applied"
        );
        assert_eq!(sm.last_applied, Some(real_at_12));
    }

    /// Committed promotion when marker evidence is ahead of the recorded
    /// log committed. This is the Shape-1 fix: markers at index 12 are
    /// durable but `committed.bin` still reads 11 because the log-flushed
    /// callback didn't fire before crash. Promoting committed prevents
    /// openraft from re-delivering entry 12 to the SM via apply.
    #[test]
    fn reconcile_promotes_committed_when_apply_ahead_of_log_commit() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = Arc::new(ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap());
        contexts.create_context("default").unwrap();
        let snap_store = Arc::new(SnapshotStore::new(dir.path().join("snapshots")).unwrap());
        let mut sm = EventStoreStateMachine::new(Arc::clone(&contexts), snap_store).unwrap();
        sm.last_applied = Some(LogId {
            leader_id: CommittedLeaderId::new(1, 0),
            index: 12,
        });

        let log_last = Some(LogId {
            leader_id: CommittedLeaderId::new(1, 1),
            index: 14,
        });
        let log_committed = Some(LogId {
            leader_id: CommittedLeaderId::new(1, 1),
            index: 11,
        });
        let real_at_12 = LogId {
            leader_id: CommittedLeaderId::new(1, 1),
            index: 12,
        };

        let report = sm
            .reconcile_with_log(log_last, log_committed, |_| Ok(Some(real_at_12)))
            .expect("reconcile should succeed");

        assert!(report.last_applied_rewritten);
        assert_eq!(
            report.committed_promoted_to,
            Some(real_at_12),
            "committed must be promoted to the real log_id at the apply index"
        );
    }

    /// Clean shutdown: SM's last_applied already matches the log entry's
    /// LogId, and committed >= applied. Reconciliation must be a no-op.
    #[test]
    fn reconcile_is_noop_on_clean_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = Arc::new(ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap());
        contexts.create_context("default").unwrap();
        let snap_store = Arc::new(SnapshotStore::new(dir.path().join("snapshots")).unwrap());
        let mut sm = EventStoreStateMachine::new(Arc::clone(&contexts), snap_store).unwrap();
        let real_at_10 = LogId {
            leader_id: CommittedLeaderId::new(1, 1),
            index: 10,
        };
        sm.last_applied = Some(real_at_10);

        let log_last = Some(LogId {
            leader_id: CommittedLeaderId::new(1, 1),
            index: 10,
        });
        let log_committed = log_last;

        let report = sm
            .reconcile_with_log(log_last, log_committed, |_| Ok(Some(real_at_10)))
            .expect("reconcile should succeed");

        assert!(!report.last_applied_rewritten, "already real — no rewrite");
        assert!(
            report.committed_promoted_to.is_none(),
            "committed already matches — no promote"
        );
        assert_eq!(sm.last_applied, Some(real_at_10));
    }

    /// Fresh data dir: SM has no last_applied, log is empty. Reconcile
    /// must short-circuit without querying the log.
    #[test]
    fn reconcile_short_circuits_when_state_machine_empty() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = Arc::new(ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap());
        contexts.create_context("default").unwrap();
        let snap_store = Arc::new(SnapshotStore::new(dir.path().join("snapshots")).unwrap());
        let mut sm = EventStoreStateMachine::new(Arc::clone(&contexts), snap_store).unwrap();
        assert!(sm.last_applied.is_none());

        let report = sm
            .reconcile_with_log(None, None, |_| {
                panic!("read_entry must not be called when SM is empty")
            })
            .expect("reconcile should succeed");

        assert!(!report.last_applied_rewritten);
        assert!(report.committed_promoted_to.is_none());
    }

    /// SM has last_applied but the log is missing the entry at that index
    /// (would only occur after snapshot install that purged old log
    /// entries). Expected: warn-and-proceed, leave sentinel in place,
    /// don't promote committed past last_log. Caller remains free to
    /// continue; openraft snapshot-install path moves forward on its own.
    #[test]
    fn reconcile_keeps_sentinel_when_log_entry_missing() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = Arc::new(ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap());
        contexts.create_context("default").unwrap();
        let snap_store = Arc::new(SnapshotStore::new(dir.path().join("snapshots")).unwrap());
        let mut sm = EventStoreStateMachine::new(Arc::clone(&contexts), snap_store).unwrap();
        let sentinel = LogId {
            leader_id: CommittedLeaderId::new(5, 0),
            index: 20,
        };
        sm.last_applied = Some(sentinel);

        let log_last = Some(LogId {
            leader_id: CommittedLeaderId::new(5, 1),
            index: 20,
        });
        let log_committed = log_last;

        let report = sm
            .reconcile_with_log(log_last, log_committed, |_| Ok(None))
            .expect("reconcile should succeed");

        assert!(!report.last_applied_rewritten);
        assert!(report.committed_promoted_to.is_none());
        assert_eq!(sm.last_applied, Some(sentinel), "sentinel kept on miss");
    }

    /// SM says last_applied > log's last_log_id. This is structurally
    /// impossible (markers are written alongside log entries) and indicates
    /// corruption. Reconciliation must return Err, not silently proceed.
    #[test]
    fn reconcile_errors_when_state_machine_ahead_of_log() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = Arc::new(ContextManager::new(dir.path(), DEFAULT_SEGMENT_SIZE).unwrap());
        contexts.create_context("default").unwrap();
        let snap_store = Arc::new(SnapshotStore::new(dir.path().join("snapshots")).unwrap());
        let mut sm = EventStoreStateMachine::new(Arc::clone(&contexts), snap_store).unwrap();
        sm.last_applied = Some(LogId {
            leader_id: CommittedLeaderId::new(1, 0),
            index: 50,
        });

        let log_last = Some(LogId {
            leader_id: CommittedLeaderId::new(1, 1),
            index: 10,
        });

        let err = sm
            .reconcile_with_log(log_last, log_last, |_| {
                panic!("should not be called — bail out before read")
            })
            .expect_err("should error on SM-ahead-of-log");
        matches!(err, Error::Corrupted { .. });
    }
}
