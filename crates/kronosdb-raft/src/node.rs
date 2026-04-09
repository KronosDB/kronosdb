//! `RaftEventStore` — the transparent decorator that makes the event store
//! cluster-aware without changing anything downstream.
//!
//! Implements `EventStore` trait:
//! - **Appends** go through Raft consensus (proposed to leader, replicated, then applied)
//! - **Reads** go directly to the local engine (eventually consistent on followers)
//! - **Subscribes** attach to the local engine (events appear after Raft applies them)
//!
//! If this node is not the leader, appends are forwarded to the leader
//! via the ForwardWrite RPC. Clients never need to know who the leader is.

use std::sync::Arc;

use openraft::Raft;
use tonic::transport::Channel;

use kronosdb_eventstore::api::EventStore;
use kronosdb_eventstore::append::{AppendRequest, AppendResponse};
use kronosdb_eventstore::criteria::SourcingCondition;
use kronosdb_eventstore::error::Error;
use kronosdb_eventstore::event::{Position, SequencedEvent, Tag};
use kronosdb_eventstore::store::EventStoreEngine;
use kronosdb_eventstore::stream::EventStream;

use crate::proto;
use crate::proto::raft_transport_client::RaftTransportClient;
use crate::types::{
    RaftAppendCondition, RaftAppendEvent, RaftCriterion, RaftRequest, RaftResponse,
    TypeConfig,
};

/// A cluster-aware event store that routes writes through Raft
/// and reads directly from the local engine.
pub struct RaftEventStore {
    raft: Arc<Raft<TypeConfig>>,
    local_engine: Arc<EventStoreEngine>,
    context_name: String,
}

impl RaftEventStore {
    pub fn new(
        raft: Arc<Raft<TypeConfig>>,
        local_engine: Arc<EventStoreEngine>,
        context_name: String,
    ) -> Self {
        Self {
            raft,
            local_engine,
            context_name,
        }
    }

    pub fn raft(&self) -> &Raft<TypeConfig> {
        &self.raft
    }

    fn build_raft_request(&self, request: &AppendRequest) -> RaftRequest {
        RaftRequest::Append {
            context: self.context_name.clone(),
            events: request.events.iter().map(RaftAppendEvent::from_event).collect(),
            condition: request.condition.as_ref().map(|c| RaftAppendCondition {
                consistency_marker: c.consistency_marker.0,
                criteria: c.criteria.criteria.iter().map(|cr| RaftCriterion {
                    names: cr.names.clone(),
                    tags: cr.tags.iter().map(|t| (t.key.clone(), t.value.clone())).collect(),
                }).collect(),
            }),
        }
    }

    /// Forwards a write to the current leader via the ForwardWrite RPC.
    async fn forward_to_leader(&self, raft_req: &RaftRequest) -> Result<RaftResponse, Error> {
        let metrics = self.raft.metrics().borrow().clone();
        let leader_id = metrics.current_leader.ok_or_else(|| Error::Corrupted {
            message: "no leader available, try again later".into(),
        })?;

        let leader_node = metrics
            .membership_config
            .membership()
            .get_node(&leader_id)
            .ok_or_else(|| Error::Corrupted {
                message: format!("leader {leader_id} address not found in membership"),
            })?;

        let endpoint = format!("http://{}", leader_node.addr);
        let channel = Channel::from_shared(endpoint.clone())
            .map_err(|e| Error::Corrupted {
                message: format!("invalid leader endpoint: {e}"),
            })?
            .connect()
            .await
            .map_err(|e| Error::Corrupted {
                message: format!("connect to leader at {endpoint}: {e}"),
            })?;

        let mut client = RaftTransportClient::new(channel);

        let data = bincode::serialize(raft_req).map_err(|e| Error::Corrupted {
            message: format!("serialize forward request: {e}"),
        })?;

        let resp = client
            .forward_write(proto::ForwardWriteRequest { data })
            .await
            .map_err(|e| Error::Corrupted {
                message: format!("forward write to leader: {e}"),
            })?;

        let raft_resp: RaftResponse = bincode::deserialize(&resp.into_inner().data)
            .map_err(|e| Error::Corrupted {
                message: format!("deserialize leader response: {e}"),
            })?;

        Ok(raft_resp)
    }
}

impl EventStore for RaftEventStore {
    fn append(&self, request: AppendRequest) -> Result<AppendResponse, Error> {
        let raft_req = self.build_raft_request(&request);

        let response = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                // Try local client_write first.
                match self.raft.client_write(raft_req.clone()).await {
                    Ok(resp) => Ok(resp.data),
                    Err(e) => {
                        // If not leader, forward to leader.
                        let err_str = format!("{e}");
                        if err_str.contains("forward request to") || err_str.contains("ForwardToLeader") {
                            self.forward_to_leader(&raft_req).await
                        } else {
                            Err(Error::Corrupted {
                                message: format!("raft write failed: {e}"),
                            })
                        }
                    }
                }
            })
        })?;

        match response {
            RaftResponse::Append {
                first_position,
                count,
                consistency_marker,
            } => Ok(AppendResponse {
                first_position: Position(first_position),
                count,
                consistency_marker: Position(consistency_marker),
            }),
            _ => Err(Error::Corrupted {
                message: "unexpected raft response type for append".into(),
            }),
        }
    }

    fn source(
        &self,
        from_position: Position,
        condition: &SourcingCondition,
    ) -> Result<Vec<SequencedEvent>, Error> {
        self.local_engine.source(from_position, condition)
    }

    fn subscribe(&self, from_position: Position, condition: SourcingCondition) -> EventStream {
        self.local_engine.subscribe(from_position, condition)
    }

    fn head(&self) -> Position {
        self.local_engine.head()
    }

    fn tail(&self) -> Position {
        self.local_engine.tail()
    }

    fn get_tags(&self, position: Position) -> Result<Vec<Tag>, Error> {
        self.local_engine.get_tags(position)
    }
}
