use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::api::MessagingPlatform;
use crate::engine::MessagingEngine;
use crate::handler::MessageTypeDetail;
use crate::subscription::SubscriptionInfo;

/// Manages named messaging buses.
///
/// A bus is an isolated messaging namespace: its own command bus, query bus,
/// and subscription registry. Buses are independent of event store contexts —
/// how many buses exist and which consumers share one is entirely up to
/// clients (routed per-RPC via the `kronosdb-bus` header). Buses are created
/// lazily on first use.
pub struct MessagingManager {
    engines: RwLock<HashMap<String, Arc<MessagingEngine>>>,
    /// Permit-wait policy applied to every engine this manager creates.
    permit_wait: bool,
}

impl Default for MessagingManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MessagingManager {
    pub fn new() -> Self {
        Self::with_permit_wait(true)
    }

    /// Creates a manager whose buses use the given permit-wait policy
    /// (`false` = fail fast when handlers are saturated).
    pub fn with_permit_wait(permit_wait: bool) -> Self {
        Self {
            engines: RwLock::new(HashMap::new()),
            permit_wait,
        }
    }

    /// Gets the messaging engine for a bus, creating one if it doesn't exist.
    pub fn get_or_create(&self, bus: &str) -> Arc<MessagingEngine> {
        // Fast path: read lock.
        {
            let engines = self.engines.read();
            if let Some(engine) = engines.get(bus) {
                return Arc::clone(engine);
            }
        }

        // Slow path: write lock + create.
        let mut engines = self.engines.write();
        engines
            .entry(bus.to_string())
            .or_insert_with(|| Arc::new(MessagingEngine::with_permit_wait(self.permit_wait)))
            .clone()
    }

    /// Gets the messaging engine for a bus as a trait object.
    pub fn get_platform(&self, bus: &str) -> Arc<dyn MessagingPlatform> {
        self.get_or_create(bus)
    }

    /// Lists all buses that have messaging engines.
    pub fn list_buses(&self) -> Vec<String> {
        let engines = self.engines.read();
        let mut names: Vec<String> = engines.keys().cloned().collect();
        names.sort();
        names
    }

    /// Returns detailed command handler info + dispatch metrics, aggregated
    /// across all buses. Each detail is stamped with its bus name.
    pub fn all_command_details(&self) -> Vec<MessageTypeDetail> {
        let engines = self.engines.read();
        let mut details = Vec::new();
        for (bus, engine) in engines.iter() {
            details.extend(engine.command_details().into_iter().map(|mut d| {
                d.bus = bus.clone();
                d
            }));
        }
        details
    }

    /// Returns detailed query handler info + dispatch metrics, aggregated
    /// across all buses. Each detail is stamped with its bus name.
    pub fn all_query_details(&self) -> Vec<MessageTypeDetail> {
        let engines = self.engines.read();
        let mut details = Vec::new();
        for (bus, engine) in engines.iter() {
            details.extend(engine.query_details().into_iter().map(|mut d| {
                d.bus = bus.clone();
                d
            }));
        }
        details
    }

    /// Returns all active subscription queries across all buses, stamped
    /// with their bus name.
    pub fn all_subscription_stats(&self) -> Vec<SubscriptionInfo> {
        let engines = self.engines.read();
        let mut stats = Vec::new();
        for (bus, engine) in engines.iter() {
            stats.extend(engine.subscription_stats().into_iter().map(|mut s| {
                s.bus = bus.clone();
                s
            }));
        }
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::CommandDispatcher;
    use crate::types::{ClientId, ComponentName};

    #[test]
    fn buses_are_isolated() {
        let manager = MessagingManager::new();

        let orders = manager.get_or_create("orders");
        let payments = manager.get_or_create("payments");

        // Register a handler on the orders bus.
        orders.subscribe_command(
            "CreateOrder".into(),
            ClientId("h1".into()),
            ComponentName("order-service".into()),
            100,
        );

        // Orders should have the handler.
        assert_eq!(orders.command_details().len(), 1);

        // Payments should be empty.
        assert_eq!(payments.command_details().len(), 0);
    }

    #[test]
    fn get_or_create_returns_same_instance() {
        let manager = MessagingManager::new();

        let first = manager.get_or_create("default");
        let second = manager.get_or_create("default");

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn aggregated_details_are_stamped_with_bus() {
        let manager = MessagingManager::new();

        manager.get_or_create("shared").subscribe_command(
            "CreateOrder".into(),
            ClientId("h1".into()),
            ComponentName("order-service".into()),
            100,
        );

        let details = manager.all_command_details();
        assert_eq!(details.len(), 1);
        assert_eq!(details[0].bus, "shared");
    }
}
