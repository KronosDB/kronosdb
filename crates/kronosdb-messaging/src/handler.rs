use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use crate::ring::Ring;
use crate::types::{ClientId, ComponentName};

/// Snapshot of a single handler's state, for admin display.
#[derive(Debug, Clone)]
pub struct HandlerDetail {
    pub client_id: String,
    pub component_name: String,
    pub load_factor: i32,
    pub available_permits: i64,
}

/// Lock-free dispatch metrics for a single message type (command or query).
///
/// All fields are atomic — no locks, no allocations on the hot path.
/// Same pattern as `StoreMetrics` in the eventstore.
pub struct MessageTypeMetrics {
    pub dispatched: AtomicU64,
    pub succeeded: AtomicU64,
    pub failed: AtomicU64,
    pub no_handler: AtomicU64,
    pub no_permits: AtomicU64,
    /// Cumulative dispatch-to-response duration in microseconds.
    pub total_duration_us: AtomicU64,
}

impl Default for MessageTypeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl MessageTypeMetrics {
    pub fn new() -> Self {
        Self {
            dispatched: AtomicU64::new(0),
            succeeded: AtomicU64::new(0),
            failed: AtomicU64::new(0),
            no_handler: AtomicU64::new(0),
            no_permits: AtomicU64::new(0),
            total_duration_us: AtomicU64::new(0),
        }
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let dispatched = self.dispatched.load(Ordering::Relaxed);
        let succeeded = self.succeeded.load(Ordering::Relaxed);
        let failed = self.failed.load(Ordering::Relaxed);
        let no_handler = self.no_handler.load(Ordering::Relaxed);
        let no_permits = self.no_permits.load(Ordering::Relaxed);
        let total_duration_us = self.total_duration_us.load(Ordering::Relaxed);
        let completed = succeeded + failed;
        MetricsSnapshot {
            dispatched,
            succeeded,
            failed,
            no_handler,
            no_permits,
            avg_duration_us: total_duration_us.checked_div(completed).unwrap_or(0),
            success_rate: if completed > 0 {
                (succeeded as f64 / completed as f64) * 100.0
            } else {
                0.0
            },
        }
    }
}

/// Point-in-time snapshot of dispatch metrics for a message type.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub dispatched: u64,
    pub succeeded: u64,
    pub failed: u64,
    pub no_handler: u64,
    pub no_permits: u64,
    pub avg_duration_us: u64,
    pub success_rate: f64,
}

impl MetricsSnapshot {
    pub fn empty() -> Self {
        Self {
            dispatched: 0,
            succeeded: 0,
            failed: 0,
            no_handler: 0,
            no_permits: 0,
            avg_duration_us: 0,
            success_rate: 0.0,
        }
    }
}

/// Combined detail for a message type: handlers + dispatch metrics.
#[derive(Debug, Clone)]
pub struct MessageTypeDetail {
    pub name: String,
    pub handlers: Vec<HandlerDetail>,
    pub metrics: MetricsSnapshot,
    /// Bus this detail belongs to. Empty at the engine level; stamped by
    /// `MessagingManager` when aggregating across buses.
    pub bus: String,
}

/// A registered handler — a connected client that can process messages.
pub struct Handler {
    /// Unique client instance identifier.
    pub client_id: ClientId,
    /// Component/application name.
    pub component_name: ComponentName,
    /// Relative load capacity. Higher = more messages routed here.
    /// Non-positive values (unset or bogus wire input) are treated as
    /// 100 (default) — dispatch arithmetic requires every weight >= 1.
    pub load_factor: i32,
    /// How many more messages the client will take, of any type: one pool
    /// per client handler stream, shared by every type it subscribed.
    /// Decremented when a message is dispatched, incremented when the
    /// client grants more.
    permits: Arc<AtomicI64>,
}

impl Handler {
    /// A handler drawing on `permits`, the client's pool.
    pub fn new(
        client_id: ClientId,
        component_name: ComponentName,
        load_factor: i32,
        permits: Arc<AtomicI64>,
    ) -> Self {
        Self {
            client_id,
            component_name,
            load_factor: if load_factor <= 0 { 100 } else { load_factor },
            permits,
        }
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

    /// Returns the current permit count (the client's whole pool).
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
    /// client_id → the client's permit pool. Lives until the client is
    /// removed, so re-subscribing or unsubscribing a type keeps what the
    /// client granted.
    pools: HashMap<ClientId, Arc<AtomicI64>>,
    /// message_type → consistent-hash ring over that type's handler list.
    /// Rebuilt on every registration change (rare), read on every
    /// routing-keyed dispatch (hot). Indices are only valid against the
    /// same registry state the ring was built from — both are read under
    /// the bus's lock, so they can't drift.
    rings: HashMap<String, Ring>,
}

/// An entry in the handler list for a message type.
pub struct HandlerEntry {
    pub handler: Handler,
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl HandlerRegistry {
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            client_subscriptions: HashMap::new(),
            pools: HashMap::new(),
            rings: HashMap::new(),
        }
    }

    /// Rebuilds the consistent-hash ring for a message type after its
    /// handler list changed.
    fn rebuild_ring(&mut self, message_type: &str) {
        match self.subscriptions.get(message_type) {
            Some(handlers) if !handlers.is_empty() => {
                let ring = Ring::build(
                    handlers
                        .iter()
                        .map(|e| (e.handler.client_id.0.as_str(), e.handler.load_factor)),
                );
                self.rings.insert(message_type.to_string(), ring);
            }
            _ => {
                self.rings.remove(message_type);
            }
        }
    }

    /// Registers a handler for a message type.
    ///
    /// Re-subscribing the same (message_type, client_id) replaces the old
    /// entry instead of accumulating a duplicate — duplicates would double
    /// the client's round-robin weight. The entry draws on the client's
    /// permit pool, so subscribing never changes how many messages the
    /// client said it will take.
    pub fn subscribe(
        &mut self,
        message_type: String,
        client_id: ClientId,
        component_name: ComponentName,
        load_factor: i32,
    ) {
        let pool = Arc::clone(self.pools.entry(client_id.clone()).or_default());
        let handler = Handler::new(client_id.clone(), component_name, load_factor, pool);
        let entry = HandlerEntry { handler };

        let handlers = self.subscriptions.entry(message_type.clone()).or_default();
        if let Some(existing) = handlers
            .iter_mut()
            .find(|e| e.handler.client_id == client_id)
        {
            *existing = entry;
        } else {
            handlers.push(entry);
        }

        let types = self.client_subscriptions.entry(client_id).or_default();
        if !types.contains(&message_type) {
            types.push(message_type.clone());
        }
        self.rebuild_ring(&message_type);
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
        self.rebuild_ring(message_type);
    }

    /// Removes all subscriptions for a client (e.g., on disconnect), and
    /// its permits with them.
    pub fn remove_client(&mut self, client_id: &ClientId) {
        self.pools.remove(client_id);
        if let Some(types) = self.client_subscriptions.remove(client_id) {
            for message_type in types {
                if let Some(handlers) = self.subscriptions.get_mut(&message_type) {
                    handlers.retain(|e| &e.handler.client_id != client_id);
                    if handlers.is_empty() {
                        self.subscriptions.remove(&message_type);
                    }
                }
                self.rebuild_ring(&message_type);
            }
        }
    }

    /// Gets the handlers for a message type.
    pub fn get_handlers(&self, message_type: &str) -> Option<&Vec<HandlerEntry>> {
        self.subscriptions.get(message_type)
    }

    /// Resolves a routing key to a handler index via the message type's
    /// consistent-hash ring.
    pub fn ring_lookup(&self, message_type: &str, key: &str) -> Option<usize> {
        self.rings.get(message_type).and_then(|r| r.lookup(key))
    }

    /// Adds `permits` to the client's pool: that many more messages, of
    /// whichever types it handles. Returns those types, so the bus can wake
    /// exactly the dispatchers parked on them — or `None` when the client
    /// has no pool yet (it granted before subscribing anything), in which
    /// case the caller makes one with [`grant_permits_creating`].
    ///
    /// [`grant_permits_creating`]: Self::grant_permits_creating
    pub fn grant_permits(&self, client_id: &ClientId, permits: i64) -> Option<&[String]> {
        let pool = self.pools.get(client_id)?;
        pool.fetch_add(permits, Ordering::Relaxed);
        Some(
            self.client_subscriptions
                .get(client_id)
                .map_or(&[], Vec::as_slice),
        )
    }

    /// [`grant_permits`](Self::grant_permits) for a client without a pool:
    /// makes one, so permits granted before the first subscription count.
    pub fn grant_permits_creating(&mut self, client_id: &ClientId, permits: i64) {
        self.pools
            .entry(client_id.clone())
            .or_default()
            .fetch_add(permits, Ordering::Relaxed);
    }

    /// Returns detailed handler info per message type, for admin display.
    pub fn handler_details(&self) -> Vec<(String, Vec<HandlerDetail>)> {
        self.subscriptions
            .iter()
            .map(|(name, entries)| {
                let details = entries
                    .iter()
                    .map(|e| HandlerDetail {
                        client_id: e.handler.client_id.0.clone(),
                        component_name: e.handler.component_name.0.clone(),
                        load_factor: e.handler.load_factor,
                        available_permits: e.handler.available_permits(),
                    })
                    .collect();
                (name.clone(), details)
            })
            .collect()
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
        registry.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        registry.subscribe(
            "CreateOrder".into(),
            client("node-2"),
            component("order-service"),
            100,
        );

        let handlers = registry.get_handlers("CreateOrder").unwrap();
        assert_eq!(handlers.len(), 2);
    }

    #[test]
    fn unsubscribe() {
        let mut registry = HandlerRegistry::new();
        registry.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        registry.subscribe(
            "CreateOrder".into(),
            client("node-2"),
            component("order-service"),
            100,
        );

        registry.unsubscribe("CreateOrder", &client("node-1"));

        let handlers = registry.get_handlers("CreateOrder").unwrap();
        assert_eq!(handlers.len(), 1);
        assert_eq!(handlers[0].handler.client_id, client("node-2"));
    }

    #[test]
    fn remove_client() {
        let mut registry = HandlerRegistry::new();
        registry.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        registry.subscribe(
            "ProcessPayment".into(),
            client("node-1"),
            component("order-service"),
            100,
        );

        registry.remove_client(&client("node-1"));

        assert!(registry.get_handlers("CreateOrder").is_none());
        assert!(registry.get_handlers("ProcessPayment").is_none());
    }

    #[test]
    fn permits() {
        let mut registry = HandlerRegistry::new();
        registry.subscribe(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );

        // No permits initially.
        let handlers = registry.get_handlers("CreateOrder").unwrap();
        assert!(!handlers[0].handler.try_acquire_permit());

        // Grant permits.
        registry.grant_permits(&client("node-1"), 5);

        let handlers = registry.get_handlers("CreateOrder").unwrap();
        assert!(handlers[0].handler.try_acquire_permit());
        assert_eq!(handlers[0].handler.available_permits(), 4);
    }

    #[test]
    fn a_grant_is_one_pool_across_every_type_a_client_handles() {
        let mut registry = HandlerRegistry::new();
        for command in ["PlaceOrder", "CancelOrder"] {
            registry.subscribe(
                command.into(),
                client("node-1"),
                component("order-service"),
                100,
            );
        }
        registry.grant_permits(&client("node-1"), 3);

        // Three messages in total, whatever their types.
        let place = &registry.get_handlers("PlaceOrder").unwrap()[0].handler;
        let cancel = &registry.get_handlers("CancelOrder").unwrap()[0].handler;
        assert!(place.try_acquire_permit());
        assert!(cancel.try_acquire_permit());
        assert!(place.try_acquire_permit());
        assert!(!cancel.try_acquire_permit());
        assert!(!place.try_acquire_permit());
    }

    #[test]
    fn subscribing_keeps_what_the_client_granted() {
        let mut registry = HandlerRegistry::new();
        registry.subscribe(
            "PlaceOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        registry.grant_permits(&client("node-1"), 2);
        // A later subscription, and a re-subscription, draw on the same pool.
        for command in ["CancelOrder", "PlaceOrder"] {
            registry.subscribe(
                command.into(),
                client("node-1"),
                component("order-service"),
                100,
            );
        }
        let cancel = &registry.get_handlers("CancelOrder").unwrap()[0].handler;
        assert_eq!(cancel.available_permits(), 2);
    }

    #[test]
    fn permits_granted_before_subscribing_count() {
        let mut registry = HandlerRegistry::new();
        assert!(registry.grant_permits(&client("node-1"), 4).is_none());
        registry.grant_permits_creating(&client("node-1"), 4);
        registry.subscribe(
            "PlaceOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        let place = &registry.get_handlers("PlaceOrder").unwrap()[0].handler;
        assert_eq!(place.available_permits(), 4);
        // Removing the client takes its permits with it.
        registry.remove_client(&client("node-1"));
        assert!(registry.grant_permits(&client("node-1"), 1).is_none());
    }
}
