//! The messaging hub: commands, queries, subscription queries and the
//! platform, over `kronosdb_messaging`'s buses, with no wire in sight.
//!
//! The hub is the shared state of the node's messaging: the buses with the
//! senders to their connected handlers, the clients' platform streams, the
//! processors they report. Everything it does is a function over domain
//! types — `kronosdb_messaging`'s commands, queries and results, and the
//! messages in [`types`] — so an adapter for any transport only translates
//! its frames to those and drives these functions; it holds no state and
//! makes no decisions of its own.
//!
//! What reaches handlers on other nodes goes through [`peers::Peers`]; what
//! decides where is plain functions in [`routing`].

mod bus;
mod clients;
mod commands;
mod handlers;
mod peers;
mod platform;
mod queries;
mod routing;
mod subscriptions;
#[cfg(test)]
mod tests;
mod types;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use kronosdb_eventstore::raft::cluster::ClusterManager;
use kronosdb_messaging::client::ClientRegistry;
use kronosdb_messaging::manager::MessagingManager;
use parking_lot::RwLock;

use crate::processor::ProcessorRegistry;

pub use bus::Bus;
pub use clients::ClientDirectory;
pub use peers::{BoxFuture, Peers, QueryLeg, RemoteFeed};
pub use queries::QueryStream;
pub use routing::{expected_results, routing_key_of};
pub use subscriptions::SubscriptionFeed;
pub use types::*;

#[derive(Debug, Clone)]
pub struct HubConfig {
    pub node_name: String,
    /// How long a command may take, dispatch to answer, unless it says so.
    pub command_timeout: Duration,
    pub query_timeout: Duration,
    /// How often a platform stream hears from the server.
    pub heartbeat_interval: Duration,
    /// How long a platform stream may stay silent before its client is gone.
    pub heartbeat_timeout: Duration,
}

/// What the hub keeps with the admin console: the engines behind the buses,
/// the connected clients, the processors they report.
pub struct Shared {
    pub manager: Arc<MessagingManager>,
    pub client_registry: Arc<ClientRegistry>,
    pub processors: Arc<ProcessorRegistry>,
}

pub struct Hub {
    pub config: HubConfig,
    manager: Arc<MessagingManager>,
    buses: RwLock<HashMap<String, Arc<Bus>>>,
    pub clients: ClientDirectory,
    pub client_registry: Arc<ClientRegistry>,
    pub processors: Arc<ProcessorRegistry>,
    router: routing::Router,
    cluster: Arc<ClusterManager>,
    peers: Arc<dyn Peers>,
    contexts: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
    streams: AtomicU64,
}

impl Hub {
    /// `contexts` lists the node's event store contexts, for platform info.
    pub fn new(
        config: HubConfig,
        shared: Shared,
        cluster: Arc<ClusterManager>,
        peers: Arc<dyn Peers>,
        contexts: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
    ) -> Arc<Self> {
        let Shared {
            manager,
            client_registry,
            processors,
        } = shared;
        Arc::new(Self {
            config,
            manager,
            buses: RwLock::new(HashMap::new()),
            clients: ClientDirectory::new(),
            client_registry,
            processors,
            router: routing::Router::new(cluster.handler_routing(), cluster.local_node_id()),
            cluster,
            peers,
            contexts,
            streams: AtomicU64::new(0),
        })
    }

    /// The bus called `name`, created on first use.
    pub fn bus(&self, name: &str) -> Arc<Bus> {
        if let Some(bus) = self.buses.read().get(name) {
            return Arc::clone(bus);
        }
        let mut buses = self.buses.write();
        Arc::clone(
            buses.entry(name.to_string()).or_insert_with(|| {
                Arc::new(Bus::new(name.into(), self.manager.get_or_create(name)))
            }),
        )
    }

    /// The buses that exist, for cleanups that touch them all.
    fn all_buses(&self) -> Vec<Arc<Bus>> {
        self.buses.read().values().cloned().collect()
    }

    fn next_stream(&self) -> u64 {
        self.streams.fetch_add(1, Ordering::Relaxed) + 1
    }
}
