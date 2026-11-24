//! The node's rows of the replicated routing table.
//!
//! What a client's live handler streams have subscribed on this node is the
//! truth; the table is written to match it. Opens, subscribes, unsubscribes
//! and ends do not write: they mark the client as changed, and one task per
//! client compares what its streams hold with what this node last wrote for
//! it and writes the difference, one write after another, until they match.
//! No two writes for a client are ever in flight together, so none can land
//! behind a later one.
//!
//! Every write is scoped to this node: rows another node wrote for the same
//! client id are not touched by a removal.

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use kronosdb_eventstore::error::Error;
use kronosdb_eventstore::raft::cluster::ClusterManager;
use kronosdb_eventstore::raft::handler_registry::{HandlerKind, HandlerRegistration};
use kronosdb_eventstore::raft::types::NodeId;
use parking_lot::Mutex;

use super::Hub;
use super::peers::BoxFuture;

/// The writes the hub makes to the replicated routing table.
pub trait RoutingWrites: Send + Sync {
    fn node_id(&self) -> NodeId;

    fn register(&self, registration: HandlerRegistration) -> BoxFuture<'_, Result<(), Error>>;

    fn deregister<'a>(
        &'a self,
        bus: &'a str,
        kind: HandlerKind,
        message_type: &'a str,
        client_id: &'a str,
    ) -> BoxFuture<'a, Result<(), Error>>;
}

/// The writes, as Raft control-plane entries.
pub(super) struct ClusterWrites(pub(super) Arc<ClusterManager>);

impl RoutingWrites for ClusterWrites {
    fn node_id(&self) -> NodeId {
        self.0.local_node_id()
    }

    fn register(&self, registration: HandlerRegistration) -> BoxFuture<'_, Result<(), Error>> {
        Box::pin(self.0.register_handler(registration))
    }

    fn deregister<'a>(
        &'a self,
        bus: &'a str,
        kind: HandlerKind,
        message_type: &'a str,
        client_id: &'a str,
    ) -> BoxFuture<'a, Result<(), Error>> {
        Box::pin(
            self.0
                .deregister_handler(bus, kind, message_type, client_id),
        )
    }
}

/// A row of the table: bus, kind, message type.
type Key = (String, HandlerKind, String);

/// The load factor query rows carry; query fan-out does not read it.
pub(super) const QUERY_LOAD_FACTOR: i32 = 100;

const FIRST_RETRY: Duration = Duration::from_millis(50);
const LONGEST_RETRY: Duration = Duration::from_secs(2);

#[derive(Clone)]
struct Written {
    load_factor: i32,
    /// False while the write is in flight or failed: the row may be in the
    /// table, so it is removed when no stream holds it, but it is written
    /// again until a write is known to have landed.
    confirmed: bool,
}

/// What this node has written for one client, and whether a task is on it.
#[derive(Default)]
pub(super) struct ClientRows {
    /// Something changed since the task last looked.
    dirty: bool,
    running: bool,
    written: HashMap<Key, Written>,
}

pub(super) type Rows = Mutex<HashMap<String, ClientRows>>;

impl Hub {
    /// What a client's streams on this node hold has changed: have the
    /// table made to match.
    pub(super) fn sync_routing(self: &Arc<Self>, client_id: &str) {
        let start = {
            let mut rows = self.routing.lock();
            let client = rows.entry(client_id.to_string()).or_default();
            client.dirty = true;
            !std::mem::replace(&mut client.running, true)
        };
        if start {
            tokio::spawn(reconcile(Arc::downgrade(self), client_id.to_string()));
        }
    }

    /// What the client's registered handler streams have subscribed, on
    /// every bus.
    fn held_rows(&self, client_id: &str) -> HashMap<Key, i32> {
        let mut held = HashMap::new();
        for bus in self.all_buses() {
            if let Some(link) = bus.command_links.get(client_id) {
                for (message_type, load_factor) in &link.subscribed {
                    held.insert(
                        (
                            bus.name.to_string(),
                            HandlerKind::Command,
                            message_type.clone(),
                        ),
                        *load_factor,
                    );
                }
            }
            if let Some(link) = bus.query_links.get(client_id) {
                for message_type in link.subscribed.keys() {
                    held.insert(
                        (
                            bus.name.to_string(),
                            HandlerKind::Query,
                            message_type.clone(),
                        ),
                        QUERY_LOAD_FACTOR,
                    );
                }
            }
        }
        // The table refuses a row without a bus or a message type.
        held.retain(|(bus, _, message_type), _| !bus.is_empty() && !message_type.is_empty());
        held
    }

    /// Whether the client changed since the task last looked.
    fn changed_since_looked(&self, client_id: &str) -> bool {
        self.routing.lock().get(client_id).is_some_and(|c| c.dirty)
    }

    /// Takes the client's changed mark. False ends the task: nothing is left
    /// to do, and a client with no rows written is forgotten.
    fn take_changed(&self, client_id: &str) -> bool {
        let mut rows = self.routing.lock();
        let Some(client) = rows.get_mut(client_id) else {
            return false;
        };
        if std::mem::take(&mut client.dirty) {
            return true;
        }
        if client.written.is_empty() {
            rows.remove(client_id);
        } else {
            client.running = false;
        }
        false
    }

    /// Writes what differs between the client's streams and the table, until
    /// they match or the client changes again. An error is one to retry.
    async fn write_difference(&self, client_id: &str) -> Result<(), Error> {
        let held = self.held_rows(client_id);
        let written: Vec<(Key, Written)> = self
            .routing
            .lock()
            .get(client_id)
            .map(|c| c.written.clone().into_iter().collect())
            .unwrap_or_default();
        for (key, _) in written.iter().filter(|(key, _)| !held.contains_key(key)) {
            if self.changed_since_looked(client_id) {
                return Ok(());
            }
            self.writes
                .deregister(&key.0, key.1, &key.2, client_id)
                .await?;
            if let Some(client) = self.routing.lock().get_mut(client_id) {
                client.written.remove(key);
            }
        }
        for (key, load_factor) in held {
            let landed = written
                .iter()
                .any(|(k, w)| *k == key && w.confirmed && w.load_factor == load_factor);
            if landed {
                continue;
            }
            if self.changed_since_looked(client_id) {
                return Ok(());
            }
            if let Some(client) = self.routing.lock().get_mut(client_id) {
                client.written.insert(
                    key.clone(),
                    Written {
                        load_factor,
                        confirmed: false,
                    },
                );
            }
            self.writes
                .register(HandlerRegistration {
                    bus: key.0.clone(),
                    kind: key.1,
                    message_type: key.2.clone(),
                    client_id: client_id.to_string(),
                    node_id: self.writes.node_id(),
                    load_factor,
                })
                .await?;
            if let Some(written) = self
                .routing
                .lock()
                .get_mut(client_id)
                .and_then(|client| client.written.get_mut(&key))
            {
                written.confirmed = true;
            }
        }
        Ok(())
    }
}

/// The one task of a client: makes the table match its streams until it
/// does. A write the control plane cannot take yet (no leader: the node is
/// starting, or an election is on) is retried, otherwise the handler would
/// stay invisible to every other node.
async fn reconcile(hub: Weak<Hub>, client_id: String) {
    let mut backoff = FIRST_RETRY;
    loop {
        let Some(strong) = hub.upgrade() else { return };
        if !strong.take_changed(&client_id) {
            return;
        }
        match strong.write_difference(&client_id).await {
            Ok(()) => backoff = FIRST_RETRY,
            Err(error) => {
                match error {
                    Error::Unavailable { message } => {
                        tracing::debug!(%message, "fabric: handler rows wait for the control plane");
                    }
                    error => {
                        tracing::warn!(%error, "fabric: handler row write failed; retrying");
                    }
                }
                if let Some(client) = strong.routing.lock().get_mut(&client_id) {
                    client.dirty = true;
                }
                drop(strong);
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(LONGEST_RETRY);
            }
        }
    }
}
