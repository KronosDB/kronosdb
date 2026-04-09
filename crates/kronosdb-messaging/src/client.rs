use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use parking_lot::{Mutex, RwLock};

use std::time::Duration;

use crate::types::{ClientId, ComponentName};

/// Summary info about a connected client, for display in the admin console.
pub struct ClientInfo {
    pub client_id: ClientId,
    pub component_name: ComponentName,
    pub version: String,
    pub connected_since: Duration,
    pub since_last_heartbeat: Duration,
    pub has_active_stream: bool,
}

/// Information about a connected client.
pub struct ConnectedClient {
    pub client_id: ClientId,
    pub component_name: ComponentName,
    pub version: String,
    pub tags: HashMap<String, String>,
    pub connected_at: Instant,
    /// Last time a heartbeat response was received from this client.
    last_heartbeat: Mutex<Instant>,
    /// Whether this client has an active platform stream.
    has_stream: AtomicBool,
}

impl ConnectedClient {
    fn new(
        client_id: ClientId,
        component_name: ComponentName,
        version: String,
        tags: HashMap<String, String>,
    ) -> Self {
        let now = Instant::now();
        Self {
            client_id,
            component_name,
            version,
            tags,
            connected_at: now,
            last_heartbeat: Mutex::new(now),
            has_stream: AtomicBool::new(false),
        }
    }

    /// Updates the last heartbeat time to now.
    pub fn record_heartbeat(&self) {
        *self.last_heartbeat.lock() = Instant::now();
    }

    /// Returns how long since the last heartbeat.
    pub fn since_last_heartbeat(&self) -> std::time::Duration {
        self.last_heartbeat.lock().elapsed()
    }

    /// Marks the client as having an active platform stream.
    pub fn set_stream_active(&self, active: bool) {
        self.has_stream.store(active, Ordering::Relaxed);
    }

    /// Whether the client has an active platform stream.
    pub fn has_active_stream(&self) -> bool {
        self.has_stream.load(Ordering::Relaxed)
    }
}

/// Central registry of connected clients.
///
/// Tracks all clients that have identified themselves via the PlatformService.
/// Used for heartbeat liveness checks and connection cleanup.
pub struct ClientRegistry {
    clients: RwLock<HashMap<ClientId, ConnectedClient>>,
}

impl ClientRegistry {
    pub fn new() -> Self {
        Self {
            clients: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a new client. Replaces any existing entry with the same client_id
    /// (reconnect scenario).
    pub fn register(
        &self,
        client_id: ClientId,
        component_name: ComponentName,
        version: String,
        tags: HashMap<String, String>,
    ) {
        let client = ConnectedClient::new(client_id.clone(), component_name, version, tags);
        let mut clients = self.clients.write();
        clients.insert(client_id, client);
    }

    /// Unregisters a client. Returns true if the client was found and removed.
    pub fn unregister(&self, client_id: &ClientId) -> bool {
        let mut clients = self.clients.write();
        clients.remove(client_id).is_some()
    }

    /// Records a heartbeat for a client.
    pub fn heartbeat(&self, client_id: &ClientId) {
        let clients = self.clients.read();
        if let Some(client) = clients.get(client_id) {
            client.record_heartbeat();
        }
    }

    /// Marks a client as having an active platform stream.
    pub fn set_stream_active(&self, client_id: &ClientId, active: bool) {
        let clients = self.clients.read();
        if let Some(client) = clients.get(client_id) {
            client.set_stream_active(active);
        }
    }

    /// Returns whether a client is registered.
    pub fn is_registered(&self, client_id: &ClientId) -> bool {
        let clients = self.clients.read();
        clients.contains_key(client_id)
    }

    /// Returns the number of connected clients.
    pub fn client_count(&self) -> usize {
        let clients = self.clients.read();
        clients.len()
    }

    /// Lists all connected client IDs with their component names.
    pub fn list_clients(&self) -> Vec<(ClientId, ComponentName)> {
        let clients = self.clients.read();
        clients
            .values()
            .map(|c| (c.client_id.clone(), c.component_name.clone()))
            .collect()
    }

    /// Lists all connected clients with detailed info.
    pub fn list_client_details(&self) -> Vec<ClientInfo> {
        let clients = self.clients.read();
        clients
            .values()
            .map(|c| ClientInfo {
                client_id: c.client_id.clone(),
                component_name: c.component_name.clone(),
                version: c.version.clone(),
                connected_since: c.connected_at.elapsed(),
                since_last_heartbeat: c.since_last_heartbeat(),
                has_active_stream: c.has_active_stream(),
            })
            .collect()
    }

    /// Reaps clients that haven't sent a heartbeat within the given timeout.
    /// Returns the client IDs that were removed.
    pub fn reap_dead_clients(&self, timeout: std::time::Duration) -> Vec<ClientId> {
        let mut clients = self.clients.write();
        let mut dead = Vec::new();

        clients.retain(|id, client| {
            if client.since_last_heartbeat() > timeout {
                dead.push(id.clone());
                false
            } else {
                true
            }
        });

        dead
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client(id: &str) -> ClientId {
        ClientId(id.to_string())
    }

    fn component(name: &str) -> ComponentName {
        ComponentName(name.to_string())
    }

    #[test]
    fn register_and_lookup() {
        let registry = ClientRegistry::new();

        registry.register(
            client("node-1"),
            component("order-service"),
            "1.0.0".into(),
            HashMap::new(),
        );

        assert!(registry.is_registered(&client("node-1")));
        assert!(!registry.is_registered(&client("node-2")));
        assert_eq!(registry.client_count(), 1);
    }

    #[test]
    fn unregister() {
        let registry = ClientRegistry::new();

        registry.register(
            client("node-1"),
            component("order-service"),
            "1.0.0".into(),
            HashMap::new(),
        );

        assert!(registry.unregister(&client("node-1")));
        assert!(!registry.is_registered(&client("node-1")));
        assert_eq!(registry.client_count(), 0);

        // Unregister non-existent.
        assert!(!registry.unregister(&client("node-1")));
    }

    #[test]
    fn reconnect_replaces() {
        let registry = ClientRegistry::new();

        registry.register(
            client("node-1"),
            component("order-service"),
            "1.0.0".into(),
            HashMap::new(),
        );
        registry.register(
            client("node-1"),
            component("order-service"),
            "2.0.0".into(),
            HashMap::new(),
        );

        assert_eq!(registry.client_count(), 1);
    }

    #[test]
    fn heartbeat_updates_liveness() {
        let registry = ClientRegistry::new();

        registry.register(
            client("node-1"),
            component("order-service"),
            "1.0.0".into(),
            HashMap::new(),
        );

        // Immediately after registration, heartbeat should be very recent.
        registry.heartbeat(&client("node-1"));

        let clients = registry.clients.read();
        let c = clients.get(&client("node-1")).unwrap();
        assert!(c.since_last_heartbeat() < std::time::Duration::from_secs(1));
    }

    #[test]
    fn reap_dead_clients() {
        let registry = ClientRegistry::new();

        registry.register(
            client("alive"),
            component("order-service"),
            "1.0.0".into(),
            HashMap::new(),
        );
        registry.register(
            client("dead"),
            component("order-service"),
            "1.0.0".into(),
            HashMap::new(),
        );

        // Manually set "dead" client's last heartbeat to the past.
        {
            let clients = registry.clients.read();
            let dead_client = clients.get(&client("dead")).unwrap();
            *dead_client.last_heartbeat.lock() =
                Instant::now() - std::time::Duration::from_secs(60);
        }

        // Refresh "alive" client's heartbeat.
        registry.heartbeat(&client("alive"));

        let dead = registry.reap_dead_clients(std::time::Duration::from_secs(15));
        assert_eq!(dead.len(), 1);
        assert_eq!(dead[0], client("dead"));

        assert!(registry.is_registered(&client("alive")));
        assert!(!registry.is_registered(&client("dead")));
    }

    #[test]
    fn list_clients() {
        let registry = ClientRegistry::new();

        registry.register(
            client("node-1"),
            component("order-service"),
            "1.0.0".into(),
            HashMap::new(),
        );
        registry.register(
            client("node-2"),
            component("payment-service"),
            "1.0.0".into(),
            HashMap::new(),
        );

        let list = registry.list_clients();
        assert_eq!(list.len(), 2);
    }

    #[test]
    fn stream_active_tracking() {
        let registry = ClientRegistry::new();

        registry.register(
            client("node-1"),
            component("order-service"),
            "1.0.0".into(),
            HashMap::new(),
        );

        {
            let clients = registry.clients.read();
            let c = clients.get(&client("node-1")).unwrap();
            assert!(!c.has_active_stream());
        }

        registry.set_stream_active(&client("node-1"), true);

        {
            let clients = registry.clients.read();
            let c = clients.get(&client("node-1")).unwrap();
            assert!(c.has_active_stream());
        }
    }
}
