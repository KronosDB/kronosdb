use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::types::{ClientId, ComponentName};

/// A registered handler — a connected client that can process messages.
pub struct Handler {
    /// Unique client instance identifier.
    pub client_id: ClientId,
    /// Component/application name.
    pub component_name: ComponentName,
    /// Relative load capacity. Higher = more messages routed here.
    /// 0 is treated as 100 (default).
    pub load_factor: i32,
    /// Available permits — how many messages the server can send.
    /// Decremented when a message is dispatched, incremented when
    /// the client grants more permits.
    permits: AtomicI64,
}

impl Handler {
    pub fn new(client_id: ClientId, component_name: ComponentName, load_factor: i32) -> Self {
        Self {
            client_id,
            component_name,
            load_factor: if load_factor == 0 { 100 } else { load_factor },
            permits: AtomicI64::new(0),
        }
    }

    /// Grants additional permits.
    pub fn add_permits(&self, count: i64) {
        self.permits.fetch_add(count, Ordering::Relaxed);
    }

    /// Tries to consume one permit. Returns true if successful.
    pub fn try_acquire_permit(&self) -> bool {
        loop {
            let current = self.permits.load(Ordering::Relaxed);
            if current <= 0 {
                return false;
            }
            if self
                .permits
                .compare_exchange_weak(current, current - 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                return true;
            }
        }
    }

    /// Returns the current permit count.
    pub fn available_permits(&self) -> i64 {
        self.permits.load(Ordering::Relaxed)
    }
}

/// Registry of handlers subscribed to message types.
///
/// Maps message type name → list of handlers that can process it.
/// Thread-safe via external synchronization (the command/query bus holds a RwLock).
pub struct HandlerRegistry {
    /// message_type → list of (client_id, handler)
    subscriptions: HashMap<String, Vec<HandlerEntry>>,
    /// client_id → list of message types they handle
    client_subscriptions: HashMap<ClientId, Vec<String>>,
}

/// An entry in the handler list for a message type.
pub struct HandlerEntry {
    pub handler: Handler,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            client_subscriptions: HashMap::new(),
        }
    }

    /// Registers a handler for a message type.
    pub fn subscribe(
        &mut self,
        message_type: String,
        client_id: ClientId,
        component_name: ComponentName,
        load_factor: i32,
    ) {
        let handler = Handler::new(client_id.clone(), component_name, load_factor);
        let entry = HandlerEntry { handler };

        self.subscriptions
            .entry(message_type.clone())
            .or_default()
            .push(entry);

        self.client_subscriptions
            .entry(client_id)
            .or_default()
            .push(message_type);
    }

    /// Unregisters a handler for a message type.
    pub fn unsubscribe(&mut self, message_type: &str, client_id: &ClientId) {
        if let Some(handlers) = self.subscriptions.get_mut(message_type) {
            handlers.retain(|e| &e.handler.client_id != client_id);
            if handlers.is_empty() {
                self.subscriptions.remove(message_type);
            }
        }

        if let Some(types) = self.client_subscriptions.get_mut(client_id) {
            types.retain(|t| t != message_type);
            if types.is_empty() {
                self.client_subscriptions.remove(client_id);
            }
        }
    }

    /// Removes all subscriptions for a client (e.g., on disconnect).
    pub fn remove_client(&mut self, client_id: &ClientId) {
        if let Some(types) = self.client_subscriptions.remove(client_id) {
            for message_type in types {
                if let Some(handlers) = self.subscriptions.get_mut(&message_type) {
                    handlers.retain(|e| &e.handler.client_id != client_id);
                    if handlers.is_empty() {
                        self.subscriptions.remove(&message_type);
                    }
                }
            }
        }
    }

    /// Gets the handlers for a message type.
    pub fn get_handlers(&self, message_type: &str) -> Option<&Vec<HandlerEntry>> {
        self.subscriptions.get(message_type)
    }

    /// Grants permits to a specific client across all their subscriptions.
    pub fn grant_permits(&self, client_id: &ClientId, permits: i64) {
        for handlers in self.subscriptions.values() {
            for entry in handlers {
                if &entry.handler.client_id == client_id {
                    entry.handler.add_permits(permits);
                }
            }
        }
    }

    /// Returns all registered message types.
    pub fn registered_types(&self) -> Vec<&str> {
        self.subscriptions.keys().map(|s| s.as_str()).collect()
    }

    /// Returns all connected client IDs.
    pub fn connected_clients(&self) -> Vec<&ClientId> {
        self.client_subscriptions.keys().collect()
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
    fn subscribe_and_get_handlers() {
        let mut registry = HandlerRegistry::new();
        registry.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );

        let handlers = registry.get_handlers("CreateOrder").unwrap();
        assert_eq!(handlers.len(), 1);
        assert_eq!(handlers[0].handler.client_id, client("node-1"));
    }

    #[test]
    fn multiple_handlers_for_same_type() {
        let mut registry = HandlerRegistry::new();
        registry.subscribe("CreateOrder".into(), client("node-1"), component("order-service"), 100);
        registry.subscribe("CreateOrder".into(), client("node-2"), component("order-service"), 100);

        let handlers = registry.get_handlers("CreateOrder").unwrap();
        assert_eq!(handlers.len(), 2);
    }

    #[test]
    fn unsubscribe() {
        let mut registry = HandlerRegistry::new();
        registry.subscribe("CreateOrder".into(), client("node-1"), component("order-service"), 100);
        registry.subscribe("CreateOrder".into(), client("node-2"), component("order-service"), 100);

        registry.unsubscribe("CreateOrder", &client("node-1"));

        let handlers = registry.get_handlers("CreateOrder").unwrap();
        assert_eq!(handlers.len(), 1);
        assert_eq!(handlers[0].handler.client_id, client("node-2"));
    }

    #[test]
    fn remove_client() {
        let mut registry = HandlerRegistry::new();
        registry.subscribe("CreateOrder".into(), client("node-1"), component("order-service"), 100);
        registry.subscribe("ProcessPayment".into(), client("node-1"), component("order-service"), 100);

        registry.remove_client(&client("node-1"));

        assert!(registry.get_handlers("CreateOrder").is_none());
        assert!(registry.get_handlers("ProcessPayment").is_none());
    }

    #[test]
    fn permits() {
        let mut registry = HandlerRegistry::new();
        registry.subscribe("CreateOrder".into(), client("node-1"), component("order-service"), 100);

        // No permits initially.
        let handlers = registry.get_handlers("CreateOrder").unwrap();
        assert!(!handlers[0].handler.try_acquire_permit());

        // Grant permits.
        registry.grant_permits(&client("node-1"), 5);

        let handlers = registry.get_handlers("CreateOrder").unwrap();
        assert!(handlers[0].handler.try_acquire_permit());
        assert_eq!(handlers[0].handler.available_permits(), 4);
    }
}
