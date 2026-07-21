//! Cluster-aware routing facade for the native event data plane.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;
use tonic::transport::Channel;

use crate::api::EventStore;
use crate::append::{AppendRequest, AppendResponse};
use crate::criteria::SourcingCondition;
use crate::error::Error;
use crate::event::{Position, SequencedEvent, Tag};
use crate::replication::control::ReplicationControl;
use crate::replication::peer::PeerTransportConfig;
use crate::replication::proto as replication_proto;
use crate::replication::proto::segment_replication_client::SegmentReplicationClient;
use crate::store::EventStoreEngine;
use crate::stream::EventStream;

use super::cluster::PeerConfig;

/// Cluster-aware event-store facade for the native segment data plane.
/// Reads are bounded by the local committed watermark. Appends execute only
/// on the currently claimed leader and are forwarded there from other nodes.
pub struct NativeEngine {
    local_engine: Arc<EventStoreEngine>,
    context_name: String,
    control: Arc<ReplicationControl>,
    peers: Arc<RwLock<Vec<PeerConfig>>>,
    transport: PeerTransportConfig,
    channels: Arc<tokio::sync::RwLock<HashMap<String, Channel>>>,
}

impl NativeEngine {
    pub(super) fn new(
        local_engine: Arc<EventStoreEngine>,
        context_name: String,
        control: Arc<ReplicationControl>,
        peers: Arc<RwLock<Vec<PeerConfig>>>,
        transport: PeerTransportConfig,
        channels: Arc<tokio::sync::RwLock<HashMap<String, Channel>>>,
    ) -> Self {
        Self {
            local_engine,
            context_name,
            control,
            peers,
            transport,
            channels,
        }
    }

    async fn channel(&self, address: &str) -> Result<Channel, Error> {
        if let Some(channel) = self.channels.read().await.get(address).cloned() {
            return Ok(channel);
        }
        let channel = self.transport.connect(address).await?;
        self.channels
            .write()
            .await
            .insert(address.to_string(), channel.clone());
        Ok(channel)
    }

    async fn forward_append(
        &self,
        request: AppendRequest,
        epoch: u64,
        leader_id: u64,
    ) -> Result<AppendResponse, Error> {
        let leader = self
            .peers
            .read()
            .iter()
            .find(|peer| peer.id == leader_id)
            .cloned()
            .ok_or_else(|| Error::Corrupted {
                message: format!("claimed leader {leader_id} is absent from voter config"),
            })?;
        let channel = self.channel(&leader.addr).await?;
        let mut client = SegmentReplicationClient::new(channel)
            .max_decoding_message_size(crate::replication::PEER_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(crate::replication::PEER_MAX_MESSAGE_BYTES);
        let request = self.transport.request(encode_forward_append(
            &self.context_name,
            epoch,
            leader_id,
            request,
        ))?;
        let response = client
            .forward_append(request)
            .await
            .map_err(|error| Error::Corrupted {
                message: format!("forward native append to node {leader_id}: {error}"),
            })?
            .into_inner();

        match response.result {
            Some(replication_proto::forward_append_response::Result::Success(success)) => {
                if success.epoch != epoch {
                    return Err(Error::Corrupted {
                        message: format!(
                            "claimed leader changed epoch during append: sent {epoch}, received {}",
                            success.epoch
                        ),
                    });
                }
                Ok(AppendResponse {
                    first_position: Position(success.first_position),
                    count: success.count,
                    consistency_marker: Position(success.consistency_marker),
                })
            }
            Some(replication_proto::forward_append_response::Result::Rejected(rejected)) => {
                Err(Error::ConsistencyConditionViolated {
                    conflicting_position: Position(rejected.conflicting_position),
                })
            }
            Some(replication_proto::forward_append_response::Result::Retry(retry)) => {
                Err(Error::Corrupted {
                    message: format!("native append must be retried: {}", retry.reason),
                })
            }
            None => Err(Error::Corrupted {
                message: "claimed leader returned an empty append response".into(),
            }),
        }
    }
}

fn encode_forward_append(
    context: &str,
    epoch: u64,
    leader_id: u64,
    request: AppendRequest,
) -> replication_proto::ForwardAppendRequest {
    let events = request
        .events
        .into_iter()
        .map(|event| replication_proto::ForwardEvent {
            identifier: event.identifier,
            name: event.name,
            version: event.version,
            timestamp: event.timestamp,
            payload: event.payload,
            metadata: event
                .metadata
                .into_iter()
                .map(|(key, value)| replication_proto::ForwardMetadata { key, value })
                .collect(),
            tags: event
                .tags
                .into_iter()
                .map(|tag| replication_proto::ForwardTag {
                    key: tag.key,
                    value: tag.value,
                })
                .collect(),
        })
        .collect();
    let condition = request
        .condition
        .map(|condition| replication_proto::ForwardCondition {
            consistency_marker: condition.consistency_marker.0,
            criteria: condition
                .criteria
                .criteria
                .into_iter()
                .map(|criterion| replication_proto::ForwardCriterion {
                    names: criterion.names,
                    tags: criterion
                        .tags
                        .into_iter()
                        .map(|tag| replication_proto::ForwardTag {
                            key: tag.key,
                            value: tag.value,
                        })
                        .collect(),
                })
                .collect(),
        });
    replication_proto::ForwardAppendRequest {
        context: context.to_string(),
        epoch,
        leader_id,
        events,
        condition,
    }
}

#[async_trait::async_trait]
impl EventStore for NativeEngine {
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, Error> {
        let claim = self.control.claim().ok_or_else(|| Error::Corrupted {
            message: "native append unavailable: no committed leader claim".into(),
        })?;
        if claim.leader_id == self.control.node_id() {
            if !self.control.is_local_writable(claim.epoch, claim.leader_id)
                || self.local_engine.replication_epoch() != claim.epoch
            {
                return Err(Error::Corrupted {
                    message: "native append unavailable: context is not installed in the active leader epoch".into(),
                });
            }
            // The writer-lock section runs on the blocking pool (it holds a
            // lock for microseconds); the durability wait is awaited here so
            // no thread is pinned for the fsync duration.
            let engine = Arc::clone(&self.local_engine);
            let staged = tokio::task::spawn_blocking(move || engine.append_stage(request))
                .await
                .map_err(|error| Error::Corrupted {
                    message: format!("native append worker panicked: {error}"),
                })?;
            let result = match staged {
                Ok(staged) => self.local_engine.append_finish_async(staged).await,
                Err(error) => Err(error),
            };
            if !self.control.is_local_writable(claim.epoch, claim.leader_id) {
                return Err(Error::Corrupted {
                    message: "native append lost its leader fence before acknowledgement".into(),
                });
            }
            result
        } else {
            self.forward_append(request, claim.epoch, claim.leader_id)
                .await
        }
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

    fn get_sequence_at(&self, timestamp_millis: i64) -> Result<Option<Position>, Error> {
        self.local_engine.get_sequence_at(timestamp_millis)
    }
}
