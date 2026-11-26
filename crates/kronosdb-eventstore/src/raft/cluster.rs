//! `ClusterManager` — every node is always a Raft node.
//!
//! A single-node deployment is simply a one-voter Raft cluster that instantly
//! self-elects. When peers join, the cluster scales up without any mode switch.
//!
//! The protocol layers always call `cluster_manager.get_store(context)` and
//! get an `Arc<dyn EventStore>` backed by the native segment data plane.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use openraft::raft::VoteRequest;
use openraft::{BasicNode, Config, Raft};
use parking_lot::RwLock;

use crate::api::EventStore;
use crate::context::ContextManager;
use crate::error::Error;
use crate::wire;

use super::amnesia::{self, Amnesia, FirstStart, VoteRuling};
use super::handler_registry::{HandlerKind, HandlerRegistration, HandlerRoutingTable};
use super::log_store::{LogStore, LogStoreConfig};
use super::network::NetworkFactory;
use super::routed_engine::NativeEngine;
use super::snapshot_store::SnapshotStore;
use super::state_machine::{AppliedControlState, EventStoreStateMachine};
use super::types::{NodeId, RaftRequest, RaftResponse, TypeConfig};
use crate::replication::control::ReplicationControl;
use crate::replication::peer::{PeerPool, PeerTransportConfig};
use kronosdb_protocol as p;

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
    /// Native connections to peers: forwarded appends, forwarded control
    /// writes and cursor queries.
    pub(super) peers: Arc<PeerPool>,
    /// Replicated messaging-handler routing table (ADR-0007). Written by
    /// the state machine, read by the server's dispatch paths.
    handler_routing: Arc<HandlerRoutingTable>,
    /// One-shot startup cleanup of rows this node stranded in a previous
    /// life. Ordered BEFORE this node's first registration write — a late
    /// background clear would wipe fresh registrations.
    handler_clear_once: tokio::sync::OnceCell<()>,
    pub(super) coordinator_started: AtomicBool,
    amnesia_task_started: AtomicBool,
    pub(super) cluster_config: ClusterConfig,
    /// What the archive loops last did (ADR-0011), for metrics.
    pub(super) archive_status: Arc<crate::tier::ArchiveStatus>,
    /// Whether this node started empty and still abstains ([`amnesia`]).
    pub(super) amnesia: Arc<Amnesia>,
}

/// How often an amnesiac node looks whether it has become whole.
const AMNESIA_CHECK: Duration = Duration::from_millis(100);

/// How often an amnesiac bootstrapper asks the other voters whether the
/// cluster is new.
const FIRST_START_PROBE: Duration = Duration::from_millis(250);

/// A bootstrapper that is still waiting for voters says so this often.
const FIRST_START_REMINDER: Duration = Duration::from_secs(30);

/// Makes a restarted leader come back as a voter that voted for itself.
///
/// openraft restores a node as leader at startup, with no election, when its
/// stored vote says it was one. That is sound only while every other voter
/// remembers the terms it has seen. A voter that lost its disk does not: it
/// would follow this leader in a term the rest of the cluster has left
/// behind, and its acknowledgements would let entries commit there. So the
/// committed vote is turned into a plain vote of the same term, which is the
/// state a restarted leader has in Raft as published. To lead again it has to
/// win a new term, and a vote is what a voter that started empty withholds.
/// A lone voter keeps its leadership: nobody else could have moved on.
async fn forget_leadership(log_store: &mut LogStore, node_id: NodeId) -> Result<(), Error> {
    use openraft::storage::RaftLogStorage;
    let storage = |action: &str, error: &dyn std::fmt::Display| Error::Internal {
        message: format!("failed to {action} the stored Raft vote: {error}"),
    };
    let Some(vote) = RaftLogStorage::<TypeConfig>::read_vote(log_store)
        .await
        .map_err(|error| storage("read", &error))?
    else {
        return Ok(());
    };
    if vote.is_committed() && vote.leader_id().voted_for() == Some(node_id) {
        let plain = openraft::Vote::new(vote.leader_id().get_term(), node_id);
        RaftLogStorage::<TypeConfig>::save_vote(log_store, &plain)
            .await
            .map_err(|error| storage("rewrite", &error))?;
    }
    Ok(())
}

/// Why a node does not report ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotReady {
    /// No committed leader claim this node could execute or forward writes
    /// to: an election, a leader still taking over, or a node cut off.
    NoLeaderClaim,
    /// A follower that has a leader but has not held the whole committed log
    /// since it started.
    CatchingUp,
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
        let voter_ids: Vec<NodeId> = cluster_config.voters.iter().map(|peer| peer.id).collect();
        let amnesia = Arc::new(Amnesia::new(
            context_manager.data_dir(),
            cluster_config.node_id,
            &voter_ids,
        ));
        Self {
            context_manager,
            stores: RwLock::new(HashMap::new()),
            raft: RwLock::new(None),
            control_updates,
            control,
            active_voters: Arc::new(RwLock::new(cluster_config.voters.clone())),
            known_peers: RwLock::new(known_peers),
            topology_lock: tokio::sync::Mutex::new(()),
            peers: Arc::new(PeerPool::new(cluster_config.peer_transport.clone())),
            handler_routing: Arc::new(HandlerRoutingTable::new()),
            handler_clear_once: tokio::sync::OnceCell::new(),
            coordinator_started: AtomicBool::new(false),
            amnesia_task_started: AtomicBool::new(false),
            cluster_config,
            archive_status: Arc::default(),
            amnesia,
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
        // Before the stores below create anything: a voter that finds no Raft
        // state here has forgotten its vote and whatever it acknowledged, and
        // must say so durably before it talks to anyone.
        let has_raft_state = amnesia::raft_state_exists(&raft_dir).map_err(Error::Io)?;
        let amnesiac = self.amnesia.begin(has_raft_state).map_err(Error::Io)?;
        // Deferred group commit: the log's own thread fsyncs every interval
        // and openraft keeps appending meanwhile, so a burst of handler
        // registrations shares an fsync instead of paying one each on the
        // async runtime. One millisecond is well under any control-plane
        // latency anyone notices.
        let mut log_store = LogStore::new(
            &raft_dir,
            LogStoreConfig {
                group_commit_interval: Some(std::time::Duration::from_millis(1)),
                ..LogStoreConfig::default()
            },
        )
        .map_err(Error::Io)?;
        let other_voters = self
            .cluster_config
            .voters
            .iter()
            .any(|peer| peer.id != self.cluster_config.node_id);
        if other_voters {
            forget_leadership(&mut log_store, self.cluster_config.node_id).await?;
        }

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
        .map_err(|e| Error::Internal {
            message: format!("failed to create raft node: {e}"),
        })?;

        let raft = Arc::new(raft);
        if amnesiac {
            // Standing for election is a vote for itself, and a node that
            // lost its log must not lead.
            raft.runtime_config().elect(false);
        }

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
            Arc::clone(&self.peers),
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
            return Err(Error::Unavailable {
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
            other => Err(Error::Internal {
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
    ///
    /// The node's startup crash-cleanup (`ClearNodeHandlers`) is ordered
    /// before the first registration so it can never wipe fresh rows.
    pub async fn register_handler(&self, registration: HandlerRegistration) -> Result<(), Error> {
        if registration.bus.is_empty()
            || registration.message_type.is_empty()
            || registration.client_id.is_empty()
        {
            return Err(Error::Internal {
                message: "handler registration requires bus, message_type, and client_id".into(),
            });
        }
        self.clear_node_handlers().await?;
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

    /// Drops every registration this node stranded in a previous life.
    /// Runs at most once per process (a crash cannot deregister its
    /// clients); every registration write awaits it first, so the clear
    /// is always log-ordered before this node's first fresh row.
    pub async fn clear_node_handlers(&self) -> Result<(), Error> {
        self.handler_clear_once
            .get_or_try_init(|| async {
                self.submit_control_request(RaftRequest::ClearNodeHandlers {
                    node_id: self.cluster_config.node_id,
                })
                .await
                .map(|_| ())
            })
            .await
            .map(|_| ())
    }

    /// A peer node's advertised address, from live membership (falling
    /// back to the configured peer directory).
    pub fn peer_address(&self, node_id: NodeId) -> Option<String> {
        if let Some(raft) = self.raft_node() {
            let metrics = raft.metrics();
            let current = metrics.borrow();
            if let Some(node) = current.membership_config.membership().get_node(&node_id) {
                return Some(node.addr.clone());
            }
        }
        self.known_peers
            .read()
            .get(&node_id)
            .map(|p| p.addr.clone())
    }

    /// The native connections to peers, authenticated with this cluster's
    /// peer credentials.
    pub fn peers(&self) -> Arc<PeerPool> {
        Arc::clone(&self.peers)
    }

    pub(super) async fn submit_control_request(
        &self,
        request: RaftRequest,
    ) -> Result<RaftResponse, Error> {
        let raft = self.raft_node().ok_or_else(|| Error::Unavailable {
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
                    return Err(Error::Unavailable {
                        message: format!("control-plane write failed: {error}"),
                    });
                }
                self.forward_control_request(&raft, &request).await
            }
        }
    }

    async fn forward_control_request(
        &self,
        raft: &Raft<TypeConfig>,
        request: &RaftRequest,
    ) -> Result<RaftResponse, Error> {
        let metrics = raft.metrics().borrow().clone();
        let leader_id = metrics.current_leader.ok_or_else(|| Error::Unavailable {
            message: "no control-plane leader available".into(),
        })?;
        let leader = metrics
            .membership_config
            .membership()
            .get_node(&leader_id)
            .ok_or_else(|| Error::Unavailable {
                message: format!("leader {leader_id} address missing from membership"),
            })?;
        let data = bincode::serialize(request).map_err(|error| Error::Internal {
            message: format!("serialize control request: {error}"),
        })?;
        let reply = self
            .peers
            .request(
                &leader.addr,
                &p::Frame::ForwardWrite(p::ForwardWrite { data: &data }),
            )
            .await
            .map_err(|error| wire::with_context(error, "forward control request"))?;
        let Ok(p::Frame::ForwardWriteReply(reply)) = reply.decode() else {
            return Err(Error::Internal {
                message: "the leader answered a forwarded control request with something else"
                    .into(),
            });
        };
        bincode::deserialize(reply.data).map_err(|error| Error::Internal {
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

    /// Shared claimed-leader gate used by the native data plane and its services.
    pub fn replication_control(&self) -> Arc<ReplicationControl> {
        Arc::clone(&self.control)
    }

    /// What the archive loops last did (ADR-0011).
    pub fn archive_status(&self) -> &crate::tier::ArchiveStatus {
        &self.archive_status
    }

    /// True when the committed native claim still matches OpenRaft's current
    /// leader/term and this node can either execute or forward appends, and,
    /// on a follower, every context has caught up with the leader once since
    /// the process started.
    ///
    /// The second half is what lets an orchestrator treat "ready" as "a
    /// healthy replica": a voter that comes back with an empty disk has a
    /// leader to forward to within a second, but it holds none of the log
    /// until its Tail sessions have delivered it. Counting it as healthy
    /// would let the next voter be taken down while this one is still empty.
    pub fn native_ready(&self) -> bool {
        self.native_not_ready().is_none()
    }

    /// Why [`ClusterManager::native_ready`] is false, or `None` when the node
    /// is ready.
    pub fn native_not_ready(&self) -> Option<NotReady> {
        let Some(claim) = self.control.claim() else {
            return Some(NotReady::NoLeaderClaim);
        };
        let Some(raft) = self.raft_node() else {
            return Some(NotReady::NoLeaderClaim);
        };
        let metrics = raft.metrics().borrow().clone();
        if metrics.current_leader != Some(claim.leader_id) || metrics.current_term != claim.term {
            return Some(NotReady::NoLeaderClaim);
        }
        if claim.leader_id == self.control.node_id() {
            return (!claim.writable).then_some(NotReady::NoLeaderClaim);
        }
        // A node that still abstains from votes and leader claims is not a
        // replica the cluster can count on, whatever its logs hold by now.
        if self.amnesia.is_amnesiac() {
            return Some(NotReady::CatchingUp);
        }
        let caught_up = self.context_manager.list_contexts().iter().all(|context| {
            self.context_manager
                .get_context(context)
                .is_ok_and(|engine| engine.has_caught_up())
        });
        (!caught_up).then_some(NotReady::CatchingUp)
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

    /// Resolves when the Raft core has stopped, with the reason. Never
    /// resolves before `init_raft()` has run.
    ///
    /// The core is a single task that nothing restarts. Without it this node
    /// neither votes nor follows a leader nor changes membership, while its
    /// listeners stay up and its last leader claim stays in place: it looks
    /// alive and answers nothing. The caller is expected to end the process.
    /// A clean [`Raft::shutdown`] resolves this too ("raft stopped").
    pub async fn raft_core_stopped(&self) -> String {
        match self.raft_node() {
            Some(raft) => core_stopped(raft.metrics()).await,
            None => std::future::pending().await,
        }
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
    /// A node that started without Raft state beside other voters is
    /// amnesiac ([`amnesia`]) and initializes nothing here. A task takes
    /// over: it ends the amnesia once the node holds the whole log, and on
    /// the lowest-numbered voter it initializes the cluster once every other
    /// voter has answered and is empty too, which is what a cluster's first
    /// start looks like. Startup does not wait for that.
    ///
    /// Safe to call multiple times — if already initialized, this is a no-op.
    pub async fn bootstrap(self: &Arc<Self>) -> Result<(), Error> {
        if self.amnesia.is_amnesiac() {
            if !self.amnesia_task_started.swap(true, Ordering::AcqRel) {
                tokio::spawn(Arc::clone(self).run_amnesia());
            }
            return Ok(());
        }
        // Only the lowest-ID voter bootstraps.
        if !self.amnesia.is_bootstrapper() {
            return Ok(()); // Not our job to bootstrap.
        }
        self.initialize_group().await
    }

    /// Initializes the node's shared Raft group with the configured voters,
    /// unless its log or vote says it already is (every restart): openraft
    /// logs an error for an initialize it refuses.
    async fn initialize_group(&self) -> Result<(), Error> {
        // Build initial membership from voter configs.
        let mut members = BTreeMap::new();
        for peer in &self.cluster_config.voters {
            members.insert(
                peer.id,
                BasicNode {
                    addr: peer.addr.clone(),
                },
            );
        }

        if let Some(raft) = self.raft_node() {
            let initialized = raft.is_initialized().await.map_err(|e| Error::Internal {
                message: format!("failed to read the Raft group's state: {e}"),
            })?;
            if initialized {
                return Ok(());
            }
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
                        return Err(Error::Internal {
                            message: format!("failed to bootstrap cluster: {e}"),
                        });
                    }
                }
            }
        }

        Ok(())
    }

    // ── A voter that started empty ([`amnesia`]) ───────────────────────

    /// Whether this node started empty and still abstains from votes and
    /// leader claims.
    pub fn is_amnesiac(&self) -> bool {
        self.amnesia.is_amnesiac()
    }

    /// This node's amnesia, for the services that answer peers.
    pub fn amnesia(&self) -> Arc<Amnesia> {
        Arc::clone(&self.amnesia)
    }

    /// Whether Raft may answer a peer's vote request. `false` means the
    /// request is answered "not granted" without Raft seeing it: this node is
    /// amnesiac. The one request that ends the amnesia is the first election
    /// of a new cluster ([`amnesia::rule_on_vote`]).
    pub fn admit_vote(&self, request: &VoteRequest<NodeId>) -> bool {
        match self.amnesia.rule_on_vote(request) {
            VoteRuling::Normal => true,
            VoteRuling::Refuse => {
                tracing::debug!(
                    request = %request.vote,
                    "vote request refused: this node started empty and has not caught up"
                );
                false
            }
            VoteRuling::FirstStart => {
                let because = "the lowest-id voter is electing itself with an empty log, so the \
                               cluster is new and there was nothing to forget";
                match self.leave_amnesia(because) {
                    Ok(()) => true,
                    Err(error) => {
                        tracing::warn!(%error, "vote request refused");
                        false
                    }
                }
            }
        }
    }

    /// Ends this node's amnesia: the marker goes first, durably, and then
    /// the node may vote, stand for election and count in a claim again.
    fn leave_amnesia(&self, because: &str) -> Result<(), Error> {
        self.amnesia.leave(because).map_err(|error| {
            Error::Io(std::io::Error::new(
                error.kind(),
                format!(
                    "could not remove the amnesia marker: {error}. This node keeps abstaining \
                     from votes and leader claims until it can; check that the data directory \
                     is writable"
                ),
            ))
        })?;
        if let Some(raft) = self.raft_node() {
            raft.runtime_config()
                .elect(self.cluster_config.raft_config.enable_elect);
        }
        Ok(())
    }

    /// True once this node holds everything the cluster had committed at
    /// some moment since it started: it has applied a leader's claim, holds
    /// every context that claim names, and each of its contexts has caught up
    /// with a leader. The claim is what says the context list is complete: a
    /// node that starts empty knows only the contexts its own configuration
    /// creates until the metadata group has told it the rest.
    fn holds_whole_log(&self) -> bool {
        let Some(claim) = self.control_updates.borrow().leader_claim.clone() else {
            return false;
        };
        let contexts = self.context_manager.list_contexts();
        claim
            .per_context_tails
            .keys()
            .all(|context| contexts.contains(context))
            && contexts.iter().all(|context| {
                self.context_manager
                    .get_context(context)
                    .is_ok_and(|engine| engine.has_caught_up())
            })
    }

    /// Runs while this node is amnesiac. Every node waits here to hold the
    /// whole log. The lowest-id voter also asks the others whether the
    /// cluster is new, and initializes it if every one of them is empty.
    async fn run_amnesia(self: Arc<Self>) {
        let mut probing = self.amnesia.is_bootstrapper();
        let mut next_probe = Instant::now();
        let mut next_reminder = Instant::now() + FIRST_START_REMINDER;
        while self.amnesia.is_amnesiac() {
            if self.holds_whole_log() {
                match self.leave_amnesia("every context has caught up with the leader") {
                    Ok(()) => return,
                    Err(error) => tracing::warn!(%error, "still amnesiac"),
                }
            } else if probing && Instant::now() >= next_probe {
                match self.probe_first_start().await {
                    FirstStart::Initialize => match self.start_new_cluster().await {
                        Ok(()) => return,
                        Err(error) => {
                            tracing::warn!(%error, "could not start the new cluster; trying again")
                        }
                    },
                    FirstStart::NotNew { holder } => {
                        probing = false;
                        tracing::info!(
                            holder,
                            "voter {holder} holds data, so this cluster is not new: this node \
                             lost its data directory. It does not initialize a cluster and \
                             waits for a leader to catch up with"
                        );
                    }
                    FirstStart::Wait { unanswered } => {
                        if Instant::now() >= next_reminder {
                            next_reminder = Instant::now() + FIRST_START_REMINDER;
                            tracing::warn!(
                                ?unanswered,
                                "this node has no Raft state and is the lowest-id voter. It \
                                 starts a new cluster only once every other voter has answered \
                                 and is empty too, and these have not answered. Start them, or \
                                 check that this node can reach them"
                            );
                        }
                    }
                }
                next_probe = Instant::now() + FIRST_START_PROBE;
            }
            tokio::time::sleep(AMNESIA_CHECK).await;
        }
    }

    /// Asks every other configured voter what it holds.
    async fn probe_first_start(&self) -> FirstStart {
        let voters = &self.cluster_config.voters;
        let answers = match self.collect_voter_cursors(voters).await {
            Ok(answers) => answers,
            Err(error) => {
                tracing::debug!(%error, "could not read this node's own cursors");
                Vec::new()
            }
        };
        let others: Vec<(NodeId, Option<bool>)> = voters
            .iter()
            .filter(|peer| peer.id != self.cluster_config.node_id)
            .map(|peer| {
                let answer = answers.iter().find(|answer| answer.node_id == peer.id);
                (peer.id, answer.map(|answer| answer.looks_empty()))
            })
            .collect();
        amnesia::decide_first_start(&others)
    }

    /// Every voter is empty: this is the cluster's first start, and this
    /// node is where it starts. The group is initialized before the marker
    /// goes, so a crash in between leaves a node that asks again.
    async fn start_new_cluster(&self) -> Result<(), Error> {
        self.initialize_group().await?;
        self.leave_amnesia(
            "every other voter answered and is empty too, so the cluster is new and this node \
             has initialized it",
        )
    }

    /// Adds a learner to the cluster (for passive backup nodes).
    pub async fn add_learner(&self, id: NodeId, addr: String) -> Result<(), Error> {
        let raft = self.raft_node().ok_or_else(|| Error::Unavailable {
            message: "add_learner called before init_raft".into(),
        })?;

        // Non-blocking: waits only for the membership entry to commit on the
        // existing quorum, never for the learner's own replication progress —
        // a down or stalled learner must not hang the caller.
        raft.add_learner(id, BasicNode { addr: addr.clone() }, false)
            .await
            .map_err(|e| Error::Rejected {
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
            return Err(Error::Rejected {
                message: "native voter membership must not be empty".into(),
            });
        }

        let voters = {
            let known = self.known_peers.read();
            voter_ids
                .iter()
                .map(|id| {
                    known.get(id).cloned().ok_or_else(|| Error::Rejected {
                        message: format!("node {id} is absent from the peer directory"),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
        };
        let raft = self.raft_node().ok_or_else(|| Error::Unavailable {
            message: "change_membership called before init_raft".into(),
        })?;

        raft.change_membership(voter_ids.to_vec(), false)
            .await
            .map_err(|e| Error::Rejected {
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

/// Waits for the Raft core behind `metrics` to stop. An error it returns
/// (storage, or a clean stop) is in its last metrics; a panic leaves no
/// metrics, and shows as the channel closing when the task's state is dropped.
async fn core_stopped(
    mut metrics: tokio::sync::watch::Receiver<openraft::RaftMetrics<NodeId, BasicNode>>,
) -> String {
    loop {
        if let Err(fatal) = &metrics.borrow_and_update().running_state {
            return fatal.to_string();
        }
        if metrics.changed().await.is_err() {
            return "its task ended without reporting an error, which is what a panic does \
                    (the panic message is earlier in this log)"
                .to_string();
        }
    }
}

#[cfg(test)]
mod core_stopped_tests {
    use super::*;
    use openraft::RaftMetrics;
    use openraft::error::Fatal;
    use std::time::Duration;
    use tokio::sync::watch;

    #[tokio::test]
    async fn a_running_core_does_not_resolve() {
        let (tx, rx) = watch::channel(RaftMetrics::<NodeId, BasicNode>::new_initial(1));
        let mut running = tx.borrow().clone();
        running.current_term = 7;
        tx.send(running).unwrap();
        let stopped = tokio::time::timeout(Duration::from_millis(50), core_stopped(rx)).await;
        assert!(stopped.is_err(), "metrics of a running core ended the wait");
    }

    #[tokio::test]
    async fn a_reported_fatal_error_resolves_with_it() {
        let (tx, rx) = watch::channel(RaftMetrics::<NodeId, BasicNode>::new_initial(1));
        let waiting = tokio::spawn(core_stopped(rx));
        let mut failed = tx.borrow().clone();
        failed.running_state = Err(Fatal::Panicked);
        tx.send(failed).unwrap();
        assert_eq!(waiting.await.unwrap(), "panicked");
    }

    #[tokio::test]
    async fn a_core_that_vanishes_resolves_as_a_panic() {
        let (tx, rx) = watch::channel(RaftMetrics::<NodeId, BasicNode>::new_initial(1));
        let waiting = tokio::spawn(core_stopped(rx));
        drop(tx);
        assert!(waiting.await.unwrap().contains("panic"));
    }
}

#[cfg(test)]
mod forget_leadership_tests {
    use super::*;
    use openraft::Vote;
    use openraft::storage::RaftLogStorage;

    async fn vote_after_restart(stored: Vote<NodeId>, node_id: NodeId) -> Option<Vote<NodeId>> {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();
            RaftLogStorage::<TypeConfig>::save_vote(&mut store, &stored)
                .await
                .unwrap();
        }
        let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();
        forget_leadership(&mut store, node_id).await.unwrap();
        drop(store);
        // What the next start reads from disk.
        let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();
        RaftLogStorage::<TypeConfig>::read_vote(&mut store)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn a_restarted_leader_keeps_its_term_and_its_vote_but_not_its_leadership() {
        let vote = vote_after_restart(Vote::new_committed(7, 2), 2).await;
        assert_eq!(vote, Some(Vote::new(7, 2)));
        assert!(!vote.unwrap().is_committed());
    }

    #[tokio::test]
    async fn a_follower_keeps_the_leader_it_follows() {
        let vote = vote_after_restart(Vote::new_committed(7, 3), 2).await;
        assert_eq!(vote, Some(Vote::new_committed(7, 3)));
    }

    #[tokio::test]
    async fn a_plain_vote_is_left_alone() {
        let vote = vote_after_restart(Vote::new(7, 2), 2).await;
        assert_eq!(vote, Some(Vote::new(7, 2)));
    }

    #[tokio::test]
    async fn a_store_without_a_vote_stays_without_one() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::new(dir.path(), LogStoreConfig::default()).unwrap();
        forget_leadership(&mut store, 2).await.unwrap();
        assert_eq!(
            RaftLogStorage::<TypeConfig>::read_vote(&mut store)
                .await
                .unwrap(),
            None
        );
    }
}
