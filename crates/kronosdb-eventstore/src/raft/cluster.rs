//! `ClusterManager` — every node is always a Raft node.
//!
//! A single-node deployment is simply a one-voter Raft cluster that instantly
//! self-elects. When peers join, the cluster scales up without any mode switch.
//!
//! The gRPC layer always calls `cluster_manager.get_store(context)` and gets
//! back `Arc<dyn EventStore>` backed by `RaftEngine`.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use openraft::{BasicNode, Config, Raft};
use parking_lot::RwLock;
use tonic::transport::Channel;

use crate::api::EventStore;
use crate::append::{AppendRequest, AppendResponse};
use crate::context::ContextManager;
use crate::criteria::SourcingCondition;
use crate::error::Error;
use crate::event::{Position, SequencedEvent, Tag};
use crate::store::EventStoreEngine;
use crate::stream::EventStream;

use super::log_store::LogStore;
use super::network::NetworkFactory;
use super::proto;
use super::proto::raft_transport_client::RaftTransportClient;
use super::state_machine::EventStoreStateMachine;
use super::types::{
    NodeId, RaftAppendCondition, RaftAppendEvent, RaftCriterion, RaftRequest, RaftResponse,
    TypeConfig,
};

/// Node type determines how a node participates in the cluster.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeType {
    /// Full Raft voter + candidate. Participates in consensus, stores events.
    Standard,
    /// Raft learner. Receives log entries but doesn't vote. Read-only event store.
    PassiveBackup,
}

/// Configuration for a cluster peer.
#[derive(Debug, Clone)]
pub struct PeerConfig {
    pub id: NodeId,
    pub addr: String,
}

/// Cluster configuration.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This node's ID.
    pub node_id: NodeId,
    /// This node's type.
    pub node_type: NodeType,
    /// This node's advertised address (for peers to connect to).
    pub advertise_addr: String,
    /// All voter peers (including self).
    pub voters: Vec<PeerConfig>,
    /// All learner peers (including self if passive backup).
    pub learners: Vec<PeerConfig>,
    /// Raft configuration.
    pub raft_config: Config,
}

/// Manages event store access — always backed by Raft consensus.
///
/// Every node is a Raft node. A single-node deployment starts as a
/// one-voter cluster that instantly self-elects as leader.
pub struct ClusterManager {
    context_manager: Arc<ContextManager>,
    /// Per-context Raft-backed stores.
    raft_stores: RwLock<HashMap<String, Arc<RaftEngine>>>,
    /// Per-context Raft nodes (for the transport service).
    raft_nodes: RwLock<HashMap<String, Arc<Raft<TypeConfig>>>>,
    cluster_config: ClusterConfig,
}

impl ClusterManager {
    /// Creates a new cluster manager. Every node is always a Raft node.
    pub fn new(context_manager: Arc<ContextManager>, cluster_config: ClusterConfig) -> Self {
        Self {
            context_manager,
            raft_stores: RwLock::new(HashMap::new()),
            raft_nodes: RwLock::new(HashMap::new()),
            cluster_config,
        }
    }

    /// Returns true if this node has multiple voters (multi-node cluster).
    pub fn is_multi_node(&self) -> bool {
        self.cluster_config.voters.len() > 1
    }

    /// Initializes Raft for a context.
    pub async fn init_context(&self, context_name: &str) -> Result<(), Error> {
        let local_engine = self.context_manager.get_context(context_name)?;

        // Create Raft log store in a subdirectory of the context.
        let raft_dir = self
            .context_manager
            .data_dir()
            .join(context_name)
            .join("raft");
        let log_store = LogStore::new(&raft_dir).map_err(Error::Io)?;

        // Create state machine wrapping the context manager.
        let state_machine = EventStoreStateMachine::new(Arc::clone(&self.context_manager));

        // Create Raft node.
        let raft = Raft::new(
            self.cluster_config.node_id,
            Arc::new(self.cluster_config.raft_config.clone()),
            NetworkFactory,
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| Error::Corrupted {
            message: format!("failed to create raft node: {e}"),
        })?;

        let raft = Arc::new(raft);

        // Wrap in RaftEngine.
        let raft_store = Arc::new(RaftEngine::new(
            Arc::clone(&raft),
            local_engine,
            context_name.to_string(),
        ));

        self.raft_stores
            .write()
            .insert(context_name.to_string(), raft_store);
        self.raft_nodes
            .write()
            .insert(context_name.to_string(), raft);

        Ok(())
    }

    /// Gets an event store for a context (always Raft-backed).
    pub fn get_store(&self, context_name: &str) -> Result<Arc<dyn EventStore>, Error> {
        let stores = self.raft_stores.read();
        stores
            .get(context_name)
            .cloned()
            .map(|s| s as Arc<dyn EventStore>)
            .ok_or_else(|| Error::ContextNotFound {
                name: context_name.to_string(),
            })
    }

    /// Gets the underlying ContextManager (for admin, snapshot store access, etc.).
    pub fn context_manager(&self) -> &Arc<ContextManager> {
        &self.context_manager
    }

    /// Gets a Raft node for a context (for building the transport service).
    pub fn get_raft_node(&self, context_name: &str) -> Option<Arc<Raft<TypeConfig>>> {
        self.raft_nodes.read().get(context_name).cloned()
    }

    /// Gets all Raft nodes (for building the transport service).
    pub fn get_all_raft_nodes(&self) -> HashMap<String, Arc<Raft<TypeConfig>>> {
        self.raft_nodes.read().clone()
    }

    /// Returns the cluster config.
    pub fn cluster_config(&self) -> &ClusterConfig {
        &self.cluster_config
    }

    /// Bootstraps the Raft cluster.
    ///
    /// For single-node: initializes with just this node → instant leader.
    /// For multi-node: the lowest-numbered voter node bootstraps.
    ///
    /// Safe to call multiple times — if already initialized, this is a no-op.
    pub async fn bootstrap(&self) -> Result<(), Error> {
        let config = &self.cluster_config;

        // Only the lowest-ID voter bootstraps.
        let min_voter_id = config
            .voters
            .iter()
            .map(|p| p.id)
            .min()
            .unwrap_or(config.node_id);
        if config.node_id != min_voter_id {
            return Ok(()); // Not our job to bootstrap.
        }

        // Build initial membership from voter configs.
        let mut members = BTreeMap::new();
        for peer in &config.voters {
            members.insert(
                peer.id,
                BasicNode {
                    addr: peer.addr.clone(),
                },
            );
        }

        // Try to initialize on the default context's Raft node.
        if let Some(raft) = self.get_raft_node("default") {
            match raft.initialize(members).await {
                Ok(_) => {}
                Err(e) => {
                    // Already initialized is not an error.
                    let msg = format!("{e}");
                    if !msg.contains("already initialized") && !msg.contains("NotAllowed") {
                        return Err(Error::Corrupted {
                            message: format!("failed to bootstrap cluster: {e}"),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    /// Adds a learner to the cluster (for passive backup nodes).
    pub async fn add_learner(&self, context: &str, id: NodeId, addr: String) -> Result<(), Error> {
        let raft = self
            .get_raft_node(context)
            .ok_or_else(|| Error::ContextNotFound {
                name: context.to_string(),
            })?;

        raft.add_learner(id, BasicNode { addr }, true)
            .await
            .map_err(|e| Error::Corrupted {
                message: format!("failed to add learner: {e}"),
            })?;

        Ok(())
    }

    /// Changes the voter membership (for dynamic membership changes).
    pub async fn change_membership(
        &self,
        context: &str,
        voter_ids: Vec<NodeId>,
    ) -> Result<(), Error> {
        let raft = self
            .get_raft_node(context)
            .ok_or_else(|| Error::ContextNotFound {
                name: context.to_string(),
            })?;

        let members: BTreeMap<NodeId, BasicNode> = voter_ids
            .into_iter()
            .map(|id| {
                let addr = self
                    .cluster_config
                    .voters
                    .iter()
                    .find(|p| p.id == id)
                    .map(|p| p.addr.clone())
                    .unwrap_or_default();
                (id, BasicNode { addr })
            })
            .collect();

        raft.change_membership(
            members
                .keys()
                .copied()
                .collect::<std::collections::BTreeSet<_>>(),
            false,
        )
        .await
        .map_err(|e| Error::Corrupted {
            message: format!("failed to change membership: {e}"),
        })?;

        Ok(())
    }
}

// ─── RaftEngine ─────────────────────────────────────────────────────

/// A cluster-aware event store backed by Raft consensus.
///
/// - **Appends** go through Raft consensus (proposed to leader, replicated, then applied).
/// - **Reads** go directly to the local engine (eventually consistent on followers).
/// - **Subscribes** attach to the local engine (events appear after Raft applies them).
///
/// If this node is not the leader, appends are forwarded to the leader
/// via the ForwardWrite RPC. Clients never need to know who the leader is.
pub struct RaftEngine {
    raft: Arc<Raft<TypeConfig>>,
    local_engine: Arc<EventStoreEngine>,
    context_name: String,
}

impl RaftEngine {
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
            events: request
                .events
                .iter()
                .map(RaftAppendEvent::from_event)
                .collect(),
            condition: request.condition.as_ref().map(|c| RaftAppendCondition {
                consistency_marker: c.consistency_marker.0,
                criteria: c
                    .criteria
                    .criteria
                    .iter()
                    .map(|cr| RaftCriterion {
                        names: cr.names.clone(),
                        tags: cr
                            .tags
                            .iter()
                            .map(|t| (t.key.clone(), t.value.clone()))
                            .collect(),
                    })
                    .collect(),
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

        let raft_resp: RaftResponse =
            bincode::deserialize(&resp.into_inner().data).map_err(|e| Error::Corrupted {
                message: format!("deserialize leader response: {e}"),
            })?;

        Ok(raft_resp)
    }
}

#[async_trait::async_trait]
impl EventStore for RaftEngine {
    async fn append(&self, request: AppendRequest) -> Result<AppendResponse, Error> {
        let raft_req = self.build_raft_request(&request);

        let response = match self.raft.client_write(raft_req.clone()).await {
            Ok(resp) => Ok(resp.data),
            Err(e) => {
                let err_str = format!("{e}");
                if err_str.contains("forward request to") || err_str.contains("ForwardToLeader") {
                    self.forward_to_leader(&raft_req).await
                } else {
                    Err(Error::Corrupted {
                        message: format!("raft write failed: {e}"),
                    })
                }
            }
        }?;

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

    fn get_sequence_at(&self, timestamp_millis: i64) -> Result<Option<Position>, Error> {
        self.local_engine.get_sequence_at(timestamp_millis)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::DEFAULT_SEGMENT_SIZE;

    fn make_contexts(dir: &std::path::Path) -> Arc<ContextManager> {
        let ctx = Arc::new(ContextManager::new(dir, DEFAULT_SEGMENT_SIZE).unwrap());
        ctx.create_context("default").unwrap();
        ctx
    }

    fn single_node_config(addr: &str) -> ClusterConfig {
        ClusterConfig {
            node_id: 1,
            node_type: NodeType::Standard,
            advertise_addr: addr.into(),
            voters: vec![PeerConfig {
                id: 1,
                addr: addr.into(),
            }],
            learners: vec![],
            raft_config: super::super::types::default_raft_config(),
        }
    }

    #[tokio::test]
    async fn single_node_init_and_get_store() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());

        let cluster =
            ClusterManager::new(Arc::clone(&contexts), single_node_config("127.0.0.1:50051"));
        assert!(!cluster.is_multi_node());

        cluster.init_context("default").await.unwrap();
        cluster.bootstrap().await.unwrap();

        let store = cluster.get_store("default").unwrap();
        assert_eq!(store.head(), Position(1));
    }

    #[tokio::test]
    async fn context_not_found_before_init() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());

        let cluster =
            ClusterManager::new(Arc::clone(&contexts), single_node_config("127.0.0.1:50051"));

        let result = cluster.get_store("default");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn get_raft_node_returns_node_after_init() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());

        let cluster =
            ClusterManager::new(Arc::clone(&contexts), single_node_config("127.0.0.1:50051"));

        assert!(cluster.get_raft_node("default").is_none());

        cluster.init_context("default").await.unwrap();

        assert!(cluster.get_raft_node("default").is_some());
    }

    #[tokio::test]
    async fn context_manager_accessible() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());
        let cluster =
            ClusterManager::new(Arc::clone(&contexts), single_node_config("127.0.0.1:50051"));

        assert!(cluster.context_manager().context_exists("default"));
    }
}
