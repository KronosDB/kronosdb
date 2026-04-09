//! `ClusterManager` — the middleman between the gRPC service layer and the event store.
//!
//! In standalone mode: returns the raw `EventStoreEngine` directly.
//! In cluster mode: returns a `RaftEventStore` decorator that routes writes through Raft.
//!
//! The gRPC layer always calls `cluster_manager.get_store(context)` and gets back
//! `Arc<dyn EventStore>` — it never knows or cares whether Raft is involved.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use openraft::{BasicNode, Config, Raft};
use parking_lot::RwLock;

use kronosdb_eventstore::api::EventStore;
use kronosdb_eventstore::context::ContextManager;
use kronosdb_eventstore::error::Error;

use crate::log_store::LogStore;
use crate::network::NetworkFactory;
use crate::node::RaftEventStore;
use crate::state_machine::EventStoreStateMachine;
use crate::types::{NodeId, TypeConfig};

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
    /// All voter peers (including self if standard).
    pub voters: Vec<PeerConfig>,
    /// All learner peers (including self if passive backup).
    pub learners: Vec<PeerConfig>,
    /// Raft configuration.
    pub raft_config: Config,
}

/// Manages event store access — standalone or clustered.
///
/// This is what the gRPC service layer uses instead of ContextManager directly.
/// It returns `Arc<dyn EventStore>` which is either a raw engine or a Raft decorator.
pub struct ClusterManager {
    context_manager: Arc<ContextManager>,
    /// When clustering is enabled, per-context Raft nodes.
    raft_stores: Option<RwLock<HashMap<String, Arc<RaftEventStore>>>>,
    /// The Raft nodes themselves (for the transport service).
    raft_nodes: Option<RwLock<HashMap<String, Arc<Raft<TypeConfig>>>>>,
    cluster_config: Option<ClusterConfig>,
}

impl ClusterManager {
    /// Creates a standalone (non-clustered) manager.
    /// All stores are raw EventStoreEngines from the ContextManager.
    pub fn standalone(context_manager: Arc<ContextManager>) -> Self {
        Self {
            context_manager,
            raft_stores: None,
            raft_nodes: None,
            cluster_config: None,
        }
    }

    /// Creates a clustered manager.
    /// Call `init_context()` for each context to set up its Raft group.
    pub fn clustered(
        context_manager: Arc<ContextManager>,
        cluster_config: ClusterConfig,
    ) -> Self {
        Self {
            context_manager,
            raft_stores: Some(RwLock::new(HashMap::new())),
            raft_nodes: Some(RwLock::new(HashMap::new())),
            cluster_config: Some(cluster_config),
        }
    }

    /// Returns whether this manager is in cluster mode.
    pub fn is_clustered(&self) -> bool {
        self.cluster_config.is_some()
    }

    /// Initializes Raft for a context. Only valid in cluster mode.
    pub async fn init_context(&self, context_name: &str) -> Result<(), Error> {
        let config = self.cluster_config.as_ref().ok_or_else(|| Error::Corrupted {
            message: "init_context called in standalone mode".into(),
        })?;

        let local_engine = self.context_manager.get_context(context_name)?;

        // Create Raft log store in a subdirectory of the context.
        let raft_dir = self
            .context_manager
            .data_dir()
            .join(context_name)
            .join("raft");
        let log_store = LogStore::new(&raft_dir).map_err(Error::Io)?;

        // Create state machine wrapping the context manager.
        let state_machine =
            EventStoreStateMachine::new(Arc::clone(&self.context_manager));

        // Create Raft node.
        let raft = Raft::new(
            config.node_id,
            Arc::new(config.raft_config.clone()),
            NetworkFactory,
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| Error::Corrupted {
            message: format!("failed to create raft node: {e}"),
        })?;

        let raft = Arc::new(raft);

        // Wrap in RaftEventStore decorator.
        let raft_store = Arc::new(RaftEventStore::new(
            Arc::clone(&raft),
            local_engine,
            context_name.to_string(),
        ));

        if let Some(ref stores) = self.raft_stores {
            stores.write().insert(context_name.to_string(), raft_store);
        }
        if let Some(ref nodes) = self.raft_nodes {
            nodes.write().insert(context_name.to_string(), raft);
        }

        Ok(())
    }

    /// Gets an event store for a context.
    ///
    /// - Standalone mode: returns the raw EventStoreEngine.
    /// - Cluster mode: returns a RaftEventStore decorator.
    pub fn get_store(&self, context_name: &str) -> Result<Arc<dyn EventStore>, Error> {
        match &self.raft_stores {
            Some(stores) => {
                let stores = stores.read();
                stores
                    .get(context_name)
                    .cloned()
                    .map(|s| s as Arc<dyn EventStore>)
                    .ok_or_else(|| Error::ContextNotFound {
                        name: context_name.to_string(),
                    })
            }
            None => {
                // Standalone — return raw engine.
                self.context_manager
                    .get_context(context_name)
                    .map(|e| e as Arc<dyn EventStore>)
            }
        }
    }

    /// Gets the underlying ContextManager (for admin, snapshot store access, etc.).
    pub fn context_manager(&self) -> &Arc<ContextManager> {
        &self.context_manager
    }

    /// Gets a Raft node for a context (for building the transport service).
    /// Returns None in standalone mode or if context isn't initialized.
    pub fn get_raft_node(&self, context_name: &str) -> Option<Arc<Raft<TypeConfig>>> {
        self.raft_nodes
            .as_ref()?
            .read()
            .get(context_name)
            .cloned()
    }

    /// Gets all Raft nodes (for building the transport service).
    pub fn get_all_raft_nodes(&self) -> HashMap<String, Arc<Raft<TypeConfig>>> {
        match &self.raft_nodes {
            Some(nodes) => nodes.read().clone(),
            None => HashMap::new(),
        }
    }

    /// Returns the cluster config, if clustered.
    pub fn cluster_config(&self) -> Option<&ClusterConfig> {
        self.cluster_config.as_ref()
    }

    /// Bootstraps the Raft cluster if this is a fresh start.
    ///
    /// Should be called on the lowest-numbered voter node. Initializes the
    /// Raft membership with all configured voters. Learners are added after
    /// the cluster is initialized.
    ///
    /// Safe to call multiple times — if already initialized, this is a no-op.
    pub async fn bootstrap(&self) -> Result<(), Error> {
        let config = self.cluster_config.as_ref().ok_or_else(|| Error::Corrupted {
            message: "bootstrap called in standalone mode".into(),
        })?;

        // Only the lowest-ID voter bootstraps.
        let min_voter_id = config.voters.iter().map(|p| p.id).min().unwrap_or(0);
        if config.node_id != min_voter_id {
            return Ok(()); // Not our job to bootstrap.
        }

        // Build initial membership from voter configs.
        let mut members = BTreeMap::new();
        for peer in &config.voters {
            members.insert(peer.id, BasicNode { addr: peer.addr.clone() });
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
        let raft = self.get_raft_node(context).ok_or_else(|| Error::ContextNotFound {
            name: context.to_string(),
        })?;

        raft.add_learner(id, BasicNode { addr }, true)
            .await
            .map_err(|e| Error::Corrupted {
                message: format!("failed to add learner: {e}"),
            })?;

        Ok(())
    }

    /// Adds a voter to the cluster (for dynamic membership changes).
    pub async fn change_membership(
        &self,
        context: &str,
        voter_ids: Vec<NodeId>,
    ) -> Result<(), Error> {
        let raft = self.get_raft_node(context).ok_or_else(|| Error::ContextNotFound {
            name: context.to_string(),
        })?;

        let members: BTreeMap<NodeId, BasicNode> = voter_ids
            .into_iter()
            .map(|id| {
                // Look up address from config or existing membership.
                let addr = self
                    .cluster_config
                    .as_ref()
                    .and_then(|c| c.voters.iter().find(|p| p.id == id))
                    .map(|p| p.addr.clone())
                    .unwrap_or_default();
                (id, BasicNode { addr })
            })
            .collect();

        raft.change_membership(members.keys().copied().collect::<std::collections::BTreeSet<_>>(), false)
            .await
            .map_err(|e| Error::Corrupted {
                message: format!("failed to change membership: {e}"),
            })?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kronosdb_eventstore::event::Position;
    use kronosdb_eventstore::segment::DEFAULT_SEGMENT_SIZE;

    fn make_contexts(dir: &std::path::Path) -> Arc<ContextManager> {
        let ctx = Arc::new(ContextManager::new(dir, DEFAULT_SEGMENT_SIZE).unwrap());
        ctx.create_context("default").unwrap();
        ctx
    }

    #[test]
    fn standalone_returns_raw_engine() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());
        let cluster = ClusterManager::standalone(Arc::clone(&contexts));

        assert!(!cluster.is_clustered());

        let store = cluster.get_store("default").unwrap();
        assert_eq!(store.head(), Position(1));
    }

    #[test]
    fn standalone_context_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());
        let cluster = ClusterManager::standalone(contexts);

        let result = cluster.get_store("nonexistent");
        assert!(result.is_err());
    }

    #[test]
    fn clustered_without_init_returns_not_found() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());

        let config = ClusterConfig {
            node_id: 1,
            node_type: NodeType::Standard,
            advertise_addr: "127.0.0.1:50051".into(),
            voters: vec![PeerConfig { id: 1, addr: "127.0.0.1:50051".into() }],
            learners: vec![],
            raft_config: crate::types::default_raft_config(),
        };

        let cluster = ClusterManager::clustered(contexts, config);
        assert!(cluster.is_clustered());

        // Before init_context, get_store should fail.
        let result = cluster.get_store("default");
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn clustered_init_context_enables_get_store() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());

        let config = ClusterConfig {
            node_id: 1,
            node_type: NodeType::Standard,
            advertise_addr: "127.0.0.1:50051".into(),
            voters: vec![PeerConfig { id: 1, addr: "127.0.0.1:50051".into() }],
            learners: vec![],
            raft_config: crate::types::default_raft_config(),
        };

        let cluster = ClusterManager::clustered(Arc::clone(&contexts), config);
        cluster.init_context("default").await.unwrap();

        // Now get_store should return a RaftEventStore.
        let store = cluster.get_store("default").unwrap();
        // The store wraps the same engine — head should match.
        assert_eq!(store.head(), Position(1));
    }

    #[tokio::test]
    async fn get_raft_node_returns_node_after_init() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());

        let config = ClusterConfig {
            node_id: 1,
            node_type: NodeType::Standard,
            advertise_addr: "127.0.0.1:50051".into(),
            voters: vec![PeerConfig { id: 1, addr: "127.0.0.1:50051".into() }],
            learners: vec![],
            raft_config: crate::types::default_raft_config(),
        };

        let cluster = ClusterManager::clustered(Arc::clone(&contexts), config);

        assert!(cluster.get_raft_node("default").is_none());

        cluster.init_context("default").await.unwrap();

        assert!(cluster.get_raft_node("default").is_some());
    }

    #[test]
    fn standalone_get_raft_node_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());
        let cluster = ClusterManager::standalone(contexts);

        assert!(cluster.get_raft_node("default").is_none());
    }

    #[test]
    fn context_manager_accessible() {
        let dir = tempfile::tempdir().unwrap();
        let contexts = make_contexts(dir.path());
        let cluster = ClusterManager::standalone(Arc::clone(&contexts));

        assert!(cluster.context_manager().context_exists("default"));
    }
}
