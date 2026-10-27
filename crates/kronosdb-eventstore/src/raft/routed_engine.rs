//! Cluster-aware routing facade for the native event data plane.

use std::sync::Arc;

use kronosdb_protocol as p;
use parking_lot::RwLock;

use crate::api::EventStore;
use crate::append::{AppendCondition, AppendRequest, AppendResponse, RecordAppend, RecordBatch};
use crate::criteria::SourcingCondition;
use crate::error::Error;
use crate::event::{Position, SequencedEvent, Tag};
use crate::replication::control::ReplicationControl;
use crate::replication::peer::PeerPool;
use crate::store::EventStoreEngine;
use crate::stream::EventStream;
use crate::wire;

use super::cluster::PeerConfig;

/// Cluster-aware event-store facade for the native segment data plane.
/// Reads are bounded by the local committed watermark. Appends execute only
/// on the currently claimed leader and are forwarded there from other nodes.
pub struct NativeEngine {
    local_engine: Arc<EventStoreEngine>,
    context_name: String,
    control: Arc<ReplicationControl>,
    peers: Arc<RwLock<Vec<PeerConfig>>>,
    pool: Arc<PeerPool>,
}

impl NativeEngine {
    pub(super) fn new(
        local_engine: Arc<EventStoreEngine>,
        context_name: String,
        control: Arc<ReplicationControl>,
        peers: Arc<RwLock<Vec<PeerConfig>>>,
        pool: Arc<PeerPool>,
    ) -> Self {
        Self {
            local_engine,
            context_name,
            control,
            peers,
            pool,
        }
    }

    async fn forward_append(
        &self,
        condition: Option<&AppendCondition>,
        batch: &RecordBatch,
        epoch: u64,
        leader_id: u64,
        system: bool,
    ) -> Result<AppendResponse, Error> {
        let leader = self
            .peers
            .read()
            .iter()
            .find(|peer| peer.id == leader_id)
            .cloned()
            .ok_or_else(|| Error::Unavailable {
                message: format!("claimed leader {leader_id} is absent from voter config"),
            })?;
        // The records go to the leader as they are.
        let records = wire::batch_records(batch)?;
        let forward = p::Frame::ForwardAppend(p::ForwardAppend {
            context: &self.context_name,
            epoch,
            leader_id,
            system,
            condition: condition.map(wire::wire_condition),
            events: records,
        });
        let reply = self
            .pool
            .request(&leader.addr, &forward)
            .await
            .map_err(|error| {
                wire::with_context(error, format!("forward native append to node {leader_id}"))
            })?;

        match reply.decode() {
            Ok(p::Frame::ForwardAppended(appended)) => {
                if appended.epoch != epoch {
                    return Err(Error::Unavailable {
                        message: format!(
                            "claimed leader changed epoch during append: sent {epoch}, received {}",
                            appended.epoch
                        ),
                    });
                }
                Ok(AppendResponse {
                    first_position: Position(appended.first_position),
                    count: appended.count,
                    consistency_marker: Position(appended.marker),
                })
            }
            Ok(p::Frame::ForwardAppendConflict(conflict)) => {
                Err(Error::ConsistencyConditionViolated {
                    conflicting_position: Position(conflict.conflicting_position),
                })
            }
            Ok(p::Frame::ForwardAppendRetry(retry)) => Err(Error::Unavailable {
                message: format!("native append must be retried: {}", retry.reason),
            }),
            _ => Err(Error::Internal {
                message: "claimed leader answered a forwarded append with something else".into(),
            }),
        }
    }
}

/// An append on its way to the leader.
enum Routed {
    /// A client's events as records.
    Records(RecordAppend),
    /// Events, and whether the server itself wrote them.
    Events(AppendRequest, bool),
}

impl NativeEngine {
    /// The shared append route: leader-local staged write or forward to the
    /// claimed leader. Events that are `Events` with `system` set take the
    /// server-authored staging path (snapshot records); everything else is a
    /// client append, checked.
    async fn route_append(&self, append: Routed) -> Result<AppendResponse, Error> {
        let claim = self.control.claim().ok_or_else(|| Error::Unavailable {
            message: "no leader has claimed the write path yet — the node is starting or \
                      an election is in progress; retry shortly"
                .into(),
        })?;
        if claim.leader_id == self.control.node_id() {
            if !self.control.is_local_writable(claim.epoch, claim.leader_id)
                || self.local_engine.replication_epoch() != claim.epoch
            {
                return Err(Error::Unavailable {
                    message: "this node leads but is still installing its write epoch — it \
                              just started, just won an election, or a context was just \
                              created; retry shortly"
                        .into(),
                });
            }
            // The writer-lock section is staged where `StageMode` says; the
            // durability wait is awaited, so no thread is pinned for the
            // fsync duration.
            let result = match append {
                Routed::Records(request) => self.local_engine.append_records_async(request).await,
                Routed::Events(request, system) => {
                    self.local_engine.append_async(request, system).await
                }
            };
            if !self.control.is_local_writable(claim.epoch, claim.leader_id) {
                return Err(Error::Unavailable {
                    message: "leadership was lost before the append was acknowledged, so its \
                              outcome is unknown; retry with a consistency condition to \
                              stay idempotent"
                        .into(),
                });
            }
            result
        } else {
            let (condition, batch, system) = match append {
                Routed::Records(request) => (request.condition, request.events, false),
                Routed::Events(request, system) => (
                    request.condition,
                    RecordBatch::from_events(&request.events),
                    system,
                ),
            };
            self.forward_append(
                condition.as_ref(),
                &batch,
                claim.epoch,
                claim.leader_id,
                system,
            )
            .await
        }
    }
}

#[async_trait::async_trait]
impl EventStore for NativeEngine {
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, Error> {
        self.append_records(RecordAppend {
            condition: request.condition,
            events: RecordBatch::from_events(&request.events),
        })
        .await
    }

    async fn append_records(&self, request: RecordAppend) -> Result<AppendResponse, Error> {
        // Checked here as well as in the leader's `append_stage` so a
        // follower rejects a reserved-namespace append before forwarding it.
        // Leader-side, the same error maps to the forward protocol's Retry
        // arm — the client would spin on a request that can never succeed.
        request.events.check_client()?;
        if let Some(condition) = &request.condition {
            crate::system::validate_client_condition(&condition.criteria)?;
        }

        self.route_append(Routed::Records(request)).await
    }

    fn source(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
    ) -> Result<Vec<SequencedEvent>, Error> {
        self.local_engine.source(from_position, condition)
    }

    fn source_page(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
        up_to: Position,
        limit: usize,
    ) -> Result<Vec<SequencedEvent>, Error> {
        self.local_engine
            .source_page(from_position, condition, up_to, limit)
    }

    fn read_records(
        &self,
        condition: &SourcingCondition,
        page: &crate::store::PageBounds<'_>,
        out: &mut Vec<u8>,
    ) -> Result<crate::store::RecordPage, Error> {
        self.local_engine.read_records(condition, page, out)
    }

    fn subscribe(
        &self,
        from_position: Position,
        condition: SourcingCondition,
        exclude_names: &[String],
    ) -> EventStream {
        self.local_engine
            .subscribe(from_position, condition, exclude_names)
    }

    fn head(&self) -> Position {
        self.local_engine.head()
    }

    fn visible_head(&self) -> Position {
        self.local_engine.visible_head()
    }

    fn tail(&self) -> Position {
        self.local_engine.tail()
    }

    fn get_tags(&self, position: Position) -> Result<Vec<Tag>, Error> {
        self.local_engine.get_tags(position)
    }

    fn has_matches_between(
        &self,
        from: Position,
        to: Position,
        condition: &SourcingCondition,
    ) -> bool {
        self.local_engine.has_matches_between(from, to, condition)
    }

    fn get_sequence_at(&self, timestamp_millis: i64) -> Result<Option<Position>, Error> {
        self.local_engine.get_sequence_at(timestamp_millis)
    }

    async fn append_snapshot(
        &self,
        key: Vec<u8>,
        state: Vec<u8>,
        fold_position: Position,
    ) -> Result<Position, Error> {
        // The server frames the record itself — the client's key and state
        // are data inside it, never a client-authored `$` event. Forwarded
        // as a system append so the leader stages it through the same seam.
        let request = crate::snapshot::append_request(&key, state, fold_position);
        let response = self.route_append(Routed::Events(request, true)).await?;
        Ok(response.first_position)
    }

    fn get_snapshot(
        &self,
        key: &[u8],
        below: Option<Position>,
    ) -> Result<Option<crate::snapshot::Snapshot>, Error> {
        crate::snapshot::latest(&self.local_engine, key, below)
    }
}
