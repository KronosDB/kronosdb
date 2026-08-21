//! `ClusterManager` — every node is always a Raft node.
//!
//! A single-node deployment is simply a one-voter Raft cluster that instantly
//! self-elects. When peers join, the cluster scales up without any mode switch.
//!
//! The gRPC layer always calls `cluster_manager.get_store(context)` and gets
//! an `Arc<dyn EventStore>` backed by the native segment data plane.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use openraft::{BasicNode, Config, Raft};
use parking_lot::RwLock;
use tonic::transport::Channel;

use crate::api::EventStore;
use crate::context::ContextManager;
use crate::error::Error;

use super::handler_registry::{HandlerKind, HandlerRegistration, HandlerRoutingTable};
use super::log_store::{LogStore, LogStoreConfig};
use super::network::NetworkFactory;
use super::proto;
use super::proto::raft_transport_client::RaftTransportClient;
use super::routed_engine::NativeEngine;
use super::snapshot_store::SnapshotStore;
use super::state_machine::{AppliedControlState, EventStoreStateMachine};
use super::types::{NodeId, RaftRequest, RaftResponse, TypeConfig};
use crate::replication::control::ReplicationControl;
use crate::replication::peer::PeerTransportConfig;

/// Node type determines how a node participates in the cluster.
#[derive(Debug, Clone, PartialEq)]
pub enum NodeType {
    /// Full Raft voter + candidate. Participates in consensus, stores events.
    Standard,
    /// Raft learner. Receives log entries but doesn't vote. Read-only event store.
    PassiveBackup,
}

/// Configuration for a cluster peer.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// Raft configuration for the metadata control plane.
    pub raft_config: Config,
    /// TLS identity/trust used for all outbound peer connections.
    pub peer_transport: PeerTransportConfig,
}

/// One context's replication catch-up as seen from the claimed leader.
#[derive(Debug, Clone)]
pub struct LearnerCatchup {
    pub context: String,
    /// The leader's local durable tail (next-exclusive position).
    pub leader_tail: u64,
    /// The follower's last durably-acked cursor, if any ack was observed.
    pub follower_position: Option<u64>,
}

/// Manages event store access — always backed by Raft consensus.
///
/// Every node is a Raft node. A single-node deployment starts as a
/// one-voter cluster that instantly self-elects as leader.
pub struct ClusterManager {
    pub(super) context_manager: Arc<ContextManager>,
    /// Per-context native routing facades. Reads stay local; appends execute
    /// only behind the claimed-leader gate or forward to that leader.
    stores: RwLock<HashMap<String, Arc<dyn EventStore>>>,
    /// Metadata-only Raft control plane.
    raft: RwLock<Option<Arc<Raft<TypeConfig>>>>,
    pub(super) control_updates: tokio::sync::watch::Sender<AppliedControlState>,
    pub(super) control: Arc<ReplicationControl>,
    /// Exact voter topology used by the native data plane in the current
    /// membership epoch.
    pub(super) active_voters: Arc<RwLock<Vec<PeerConfig>>>,
    /// Address directory for configured voters and learners. Membership
    /// changes select from this directory and then publish a new active set.
    pub(super) known_peers: RwLock<HashMap<NodeId, PeerConfig>>,
    /// Serializes catch-up/claim establishment with membership changes.
    pub(super) topology_lock: tokio::sync::Mutex<()>,
    /// Cached peer channels keyed by advertised address.
    forward_channels: Arc<tokio::sync::RwLock<HashMap<String, Channel>>>,
    /// Replicated messaging-handler routing table (ADR-0007). Written by
    /// the state machine, read by the server's dispatch paths.
    handler_routing: Arc<HandlerRoutingTable>,
    pub(super) coordinator_started: AtomicBool,
    pub(super) cluster_config: ClusterConfig,
}

impl ClusterManager {
    /// Creates a new cluster manager. Every node is always a Raft node.
    pub fn new(context_manager: Arc<ContextManager>, cluster_config: ClusterConfig) -> Self {
        let control = ReplicationControl::new(
            cluster_config.node_id,
            cluster_config.voters.iter().map(|peer| peer.id).collect(),
        );
        let known_peers = cluster_config
            .voters
            .iter()
            .chain(&cluster_config.learners)
            .cloned()
            .map(|peer| (peer.id, peer))
            .collect();
        let (control_updates, _) = tokio::sync::watch::channel(AppliedControlState::default());
        Self {
            context_manager,
            stores: RwLock::new(HashMap::new()),
            raft: RwLock::new(None),
            control_updates,
            control,
            active_voters: Arc::new(RwLock::new(cluster_config.voters.clone())),
            known_peers: RwLock::new(known_peers),
            topology_lock: tokio::sync::Mutex::new(()),
            forward_channels: Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            handler_routing: Arc::new(HandlerRoutingTable::new()),
            coordinator_started: AtomicBool::new(false),
            cluster_config,
        }
    }

    /// Returns true if this node has multiple voters (multi-node cluster).
    pub fn is_multi_node(&self) -> bool {
        self.active_voters.read().len() > 1
    }

    /// Initializes the node's metadata-only Raft group. Idempotent — subsequent
    /// calls are no-ops.
    ///
    /// The metadata journal is node-wide and stored under `<data_dir>/raft`.
    /// Event segments remain isolated under their context directories.
    pub async fn init_raft(&self) -> Result<(), Error> {
        if self.raft.read().is_some() {
            return Ok(());
        }

        let raft_dir = self.context_manager.data_dir().join("raft");
        let log_store = LogStore::new(&raft_dir, LogStoreConfig::default()).map_err(Error::Io)?;

        // On-disk snapshot store. Lives in `<raft_dir>/snapshots`. Used to
        // persist `last_membership` across restart — without this, the
        // cluster-init Membership log entry being purged leaves no durable
        // carrier of voter set and the node defaults to Learner on restart.
        let snapshot_store =
            Arc::new(SnapshotStore::new(raft_dir.join("snapshots")).map_err(Error::Io)?);

        // Metadata state is recovered only from the control snapshot/store.
        // Event segments are never inspected or rewritten by openraft.
        let state_machine = EventStoreStateMachine::new(
            Arc::clone(&self.context_manager),
            Arc::clone(&snapshot_store),
            Arc::clone(&self.handler_routing),
            self.control_updates.clone(),
        )?;

        // Create the metadata Raft node.
        let raft = Raft::new(
            self.cluster_config.node_id,
            Arc::new(self.cluster_config.raft_config.clone()),
            NetworkFactory::new(self.cluster_config.peer_transport.clone()),
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| Error::Corrupted {
            message: format!("failed to create raft node: {e}"),
        })?;

        let raft = Arc::new(raft);

        *self.raft.write() = Some(raft);

        Ok(())
    }

    /// Registers a native routing facade for one context. Idempotent.
    pub fn register_context(&self, context_name: &str) -> Result<(), Error> {
        if self.stores.read().contains_key(context_name) {
            return Ok(());
        }
        let local_engine = self.context_manager.get_context(context_name)?;
        let store: Arc<dyn EventStore> = Arc::new(NativeEngine::new(
            local_engine,
            context_name.to_string(),
            Arc::clone(&self.control),
            Arc::clone(&self.active_voters),
            self.cluster_config.peer_transport.clone(),
            Arc::clone(&self.forward_channels),
        ));
        self.stores.write().insert(context_name.to_string(), store);
        Ok(())
    }

    /// Initializes the shared Raft group (if needed) and registers a context.
    pub async fn init_context(&self, context_name: &str) -> Result<(), Error> {
        self.init_raft().await?;
        self.register_context(context_name)
    }

    /// Creates a context through Raft consensus so every node in the cluster
    /// applies it. Idempotent (replayed/duplicate creates succeed). Forwards
    /// to the leader when called on a follower.
    ///
    /// The name is validated BEFORE proposing: `apply` treats an invalid name
    /// as a fatal state-machine error, so it must never reach the log.
    pub async fn create_context_replicated(&self, name: &str) -> Result<(), Error> {
        crate::context::validate_context_name(name)?;
        let existed = self.context_manager.context_exists(name);

        if self.raft_node().is_none() {
            return Err(Error::Corrupted {
                message: "create_context_replicated called before init_raft".into(),
            });
        }

        let raft_req = RaftRequest::CreateContext {
            name: name.to_string(),
        };
        let response = self.submit_control_request(raft_req).await?;

        match response {
            RaftResponse::ContextCreated => {
                if !existed {
                    self.control.close_gate();
                }
                Ok(())
            }
            other => Err(Error::Corrupted {
                message: format!("unexpected raft response for create_context: {other:?}"),
            }),
        }
    }

    // ── Messaging-fabric registry (ADR-0007) ───────────────────────────

    /// The replicated handler routing table, shared with the state machine.
    pub fn handler_routing(&self) -> Arc<HandlerRoutingTable> {
        Arc::clone(&self.handler_routing)
    }

    /// This node's id, for stamping handler registrations.
    pub fn local_node_id(&self) -> NodeId {
        self.cluster_config.node_id
    }

    /// Registers a messaging handler in the replicated routing table.
    /// Forwards to the Raft leader when called on a follower.
    pub async fn register_handler(&self, registration: HandlerRegistration) -> Result<(), Error> {
        if registration.bus.is_empty()
            || registration.message_type.is_empty()
            || registration.client_id.is_empty()
        {
            return Err(Error::Corrupted {
                message: "handler registration requires bus, message_type, and client_id".into(),
            });
        }
        self.submit_control_request(RaftRequest::RegisterHandler { registration })
            .await
            .map(|_| ())
    }

    /// Removes one handler registration (explicit unsubscribe).
    pub async fn deregister_handler(
        &self,
        bus: &str,
        kind: HandlerKind,
        message_type: &str,
        client_id: &str,
    ) -> Result<(), Error> {
        self.submit_control_request(RaftRequest::DeregisterHandler {
            bus: bus.to_string(),
            kind,
            message_type: message_type.to_string(),
            client_id: client_id.to_string(),
            node_id: self.cluster_config.node_id,
        })
        .await
        .map(|_| ())
    }

    /// Removes all of a client's registrations made through this node
    /// (disconnect / heartbeat reap).
    pub async fn deregister_client_handlers(&self, client_id: &str) -> Result<(), Error> {
        self.submit_control_request(RaftRequest::DeregisterClient {
            client_id: client_id.to_string(),
            node_id: self.cluster_config.node_id,
        })
        .await
        .map(|_| ())
    }

    /// Drops every registration owned by this node. Written at startup so
    /// rows stranded by a crash never outlive the restart.
    pub async fn clear_node_handlers(&self) -> Result<(), Error> {
        self.submit_control_request(RaftRequest::ClearNodeHandlers {
            node_id: self.cluster_config.node_id,
        })
        .await
        .map(|_| ())
    }

    /// A peer node's advertised address, from live membership (falling
    /// back to the configured peer directory).
    pub fn peer_address(&self, node_id: NodeId) -> Option<String> {
        if let Some(raft) = self.raft_node() {
            let metrics = raft.metrics().borrow().clone();
            if let Some(node) = metrics.membership_config.membership().get_node(&node_id) {
                return Some(node.addr.clone());
            }
        }
        self.known_peers
            .read()
            .get(&node_id)
            .map(|p| p.addr.clone())
    }

    /// A cached, authenticated gRPC channel to a peer address. Used by the
    /// messaging fabric to forward commands/queries between nodes.
    pub async fn peer_channel(&self, address: &str) -> Result<Channel, Error> {
        self.cached_channel(address).await
    }

    /// Wraps a peer request with this cluster's auth token / TLS identity.
    pub fn peer_request<T>(&self, message: T) -> Result<tonic::Request<T>, Error> {
        self.cluster_config.peer_transport.request(message)
    }

    pub(super) async fn submit_control_request(
        &self,
        request: RaftRequest,
    ) -> Result<RaftResponse, Error> {
        let raft = self.raft_node().ok_or_else(|| Error::Corrupted {
            message: "metadata control plane is not initialized".into(),
        })?;
        match raft.client_write(request.clone()).await {
            Ok(response) => Ok(response.data),
            Err(error) => {
                let should_forward = matches!(
                    error.api_error(),
                    Some(openraft::error::ClientWriteError::ForwardToLeader(_))
                );
                if !should_forward {
                    return Err(Error::Corrupted {
                        message: format!("control-plane write failed: {error}"),
                    });
                }
                self.forward_control_request(&raft, &request).await
            }
        }
    }

    pub(super) async fn cached_channel(&self, address: &str) -> Result<Channel, Error> {
        if let Some(channel) = self.forward_channels.read().await.get(address).cloned() {
            return Ok(channel);
        }
        let channel = self.cluster_config.peer_transport.connect(address).await?;
        self.forward_channels
            .write()
            .await
            .insert(address.to_string(), channel.clone());
        Ok(channel)
    }

    async fn forward_control_request(
        &self,
        raft: &Raft<TypeConfig>,
        request: &RaftRequest,
    ) -> Result<RaftResponse, Error> {
        let metrics = raft.metrics().borrow().clone();
        let leader_id = metrics.current_leader.ok_or_else(|| Error::Corrupted {
            message: "no control-plane leader available".into(),
        })?;
        let leader = metrics
            .membership_config
            .membership()
            .get_node(&leader_id)
            .ok_or_else(|| Error::Corrupted {
                message: format!("leader {leader_id} address missing from membership"),
            })?;
        let channel = self.cached_channel(&leader.addr).await?;
        let mut client = RaftTransportClient::new(channel);
        let data = bincode::serialize(request).map_err(|error| Error::Corrupted {
            message: format!("serialize control request: {error}"),
        })?;
        let request = self
            .cluster_config
            .peer_transport
            .request(proto::ForwardWriteRequest { data })?;
        let response = client
            .forward_write(request)
            .await
            .map_err(|error| Error::Corrupted {
                message: format!("forward control request: {error}"),
            })?;
        bincode::deserialize(&response.into_inner().data).map_err(|error| Error::Corrupted {
            message: format!("deserialize control response: {error}"),
        })
    }

    /// Gets an event store for a context (native segment replication).
    ///
    /// Registers the facade lazily when the context exists in the
    /// `ContextManager` but no native facade has been built yet — this is how
    /// contexts created at runtime (via a replicated `CreateContext` applied
    /// on this node) become servable without a restart.
    pub fn get_store(&self, context_name: &str) -> Result<Arc<dyn EventStore>, Error> {
        if let Some(store) = self.stores.read().get(context_name) {
            return Ok(Arc::clone(store));
        }
        if self.context_manager.context_exists(context_name) && self.raft_node().is_some() {
            self.register_context(context_name)?;
            if let Some(store) = self.stores.read().get(context_name) {
                return Ok(Arc::clone(store));
            }
        }
        Err(Error::ContextNotFound {
            name: context_name.to_string(),
        })
    }

    /// Shared claimed-leader gate used by the native data plane and its gRPC service.
    pub fn replication_control(&self) -> Arc<ReplicationControl> {
        Arc::clone(&self.control)
    }

    /// True when the committed native claim still matches OpenRaft's current
    /// leader/term and this node can either execute or forward appends.
    pub fn native_ready(&self) -> bool {
        let Some(claim) = self.control.claim() else {
            return false;
        };
        let Some(raft) = self.raft_node() else {
            return false;
        };
        let metrics = raft.metrics().borrow().clone();
        if metrics.current_leader != Some(claim.leader_id) || metrics.current_term != claim.term {
            return false;
        }
        claim.leader_id != self.control.node_id() || claim.writable
    }

    /// True when this node holds the writable native claim — the only node
    /// that may append directly to its local engine.
    ///
    /// Writes that bypass the routed path (the server's own system events)
    /// must check this first: `EventStoreEngine::append_system` writes to the
    /// local segment unconditionally, so a follower calling it would break
    /// the byte-identity the replication protocol depends on.
    pub fn is_writable_leader(&self) -> bool {
        match self.control.claim() {
            Some(claim) => claim.writable && claim.leader_id == self.cluster_config.node_id,
            None => false,
        }
    }

    /// Gets the underlying ContextManager (for admin, snapshot store access, etc.).
    pub fn context_manager(&self) -> &Arc<ContextManager> {
        &self.context_manager
    }

    /// Gets the node's shared Raft node (for the transport service and
    /// membership operations). `None` before `init_raft()`.
    pub fn raft_node(&self) -> Option<Arc<Raft<TypeConfig>>> {
        self.raft.read().clone()
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

        // Initialize the node's shared Raft group.
        if let Some(raft) = self.raft_node() {
            match raft.initialize(members).await {
                Ok(_) => {}
                Err(e) => {
                    // Already initialized is not an error: openraft raises a
                    // typed `InitializeError::NotAllowed` whenever the log or
                    // vote is non-empty (fresh-leader re-init AND post-crash
                    // restart both land here). Matching the typed variant —
                    // instead of the error's display string — survives
                    // openraft upgrades that rephrase the message.
                    let already_initialized = matches!(
                        e.api_error(),
                        Some(openraft::error::InitializeError::NotAllowed(_))
                    );
                    if !already_initialized {
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
    pub async fn add_learner(&self, id: NodeId, addr: String) -> Result<(), Error> {
        let raft = self.raft_node().ok_or_else(|| Error::Corrupted {
            message: "add_learner called before init_raft".into(),
        })?;

        // Non-blocking: waits only for the membership entry to commit on the
        // existing quorum, never for the learner's own replication progress —
        // a down or stalled learner must not hang the caller.
        raft.add_learner(id, BasicNode { addr: addr.clone() }, false)
            .await
            .map_err(|e| Error::Corrupted {
                message: format!("failed to add learner: {e}"),
            })?;
        self.known_peers.write().insert(id, PeerConfig { id, addr });

        Ok(())
    }

    /// Per-context replication lag of one follower, measured from the claimed
    /// leader's session progress. `follower_position` is `None` when no ack
    /// from that follower has been observed for the context — an idle or
    /// never-connected session, indistinguishable here.
    pub fn replication_catchup_status(
        &self,
        node_id: NodeId,
    ) -> Result<Vec<LearnerCatchup>, Error> {
        let progress = self.control.progress_of(node_id);
        let mut out = Vec::new();
        for context in self.context_manager.list_contexts() {
            let engine = self.context_manager.get_context(&context)?;
            out.push(LearnerCatchup {
                leader_tail: engine.local_tail().0,
                follower_position: progress.get(&context).copied(),
                context,
            });
        }
        Ok(out)
    }

    /// True when this node holds the writable data-plane claim — the only
    /// vantage point from which session progress is authoritative.
    pub fn is_claimed_leader(&self) -> bool {
        self.control
            .claim()
            .map(|claim| claim.writable && claim.leader_id == self.cluster_config.node_id)
            .unwrap_or(false)
    }

    /// Changes both metadata consensus and native data-plane voter membership.
    /// The write gate stays closed from before the Raft transition until the
    /// coordinator has established a fresh claim with the new exact voter set.
    pub async fn change_membership(&self, mut voter_ids: Vec<NodeId>) -> Result<(), Error> {
        let _topology_guard = self.topology_lock.lock().await;
        self.control.close_gate();

        voter_ids.sort_unstable();
        voter_ids.dedup();
        if voter_ids.is_empty() {
            return Err(Error::Corrupted {
                message: "native voter membership must not be empty".into(),
            });
        }

        let voters = {
            let known = self.known_peers.read();
            voter_ids
                .iter()
                .map(|id| {
                    known.get(id).cloned().ok_or_else(|| Error::Corrupted {
                        message: format!("node {id} is absent from the peer directory"),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        let raft = self.raft_node().ok_or_else(|| Error::Corrupted {
            message: "change_membership called before init_raft".into(),
        })?;

        raft.change_membership(voter_ids.to_vec(), false)
            .await
            .map_err(|e| Error::Corrupted {
                message: format!("failed to change membership: {e}"),
            })?;

        // Publish peers and voter IDs while the active-topology write lock
        // excludes claim/follower snapshots. `set_voters` fences any old claim.
        let mut active = self.active_voters.write();
        *active = voters;
        self.control.set_voters(voter_ids);

        Ok(())
    }
}
