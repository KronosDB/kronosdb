use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::api::MessagingPlatform;
use crate::engine::MessagingEngine;
use crate::handler::MessageTypeDetail;
use crate::subscription::SubscriptionInfo;

/// Manages per-context messaging engines.
///
/// Each context gets its own command bus, query bus, and subscription registry.
/// Handlers registered in one context are isolated from other contexts.
pub struct MessagingManager {
    engines: RwLock<HashMap<String, Arc<MessagingEngine>>>,
}

impl Default for MessagingManager {
    fn default() -> Self {
        Self::new()
    }
}

impl MessagingManager {
    pub fn new() -> Self {
        Self {
            engines: RwLock::new(HashMap::new()),
        }
    }

    /// Gets the messaging engine for a context, creating one if it doesn't exist.
    pub fn get_or_create(&self, context: &str) -> Arc<MessagingEngine> {
        // Fast path: read lock.
        {
            let engines = self.engines.read();
            if let Some(engine) = engines.get(context) {
                return Arc::clone(engine);
            }
        }

        // Slow path: write lock + create.
        let mut engines = self.engines.write();
        engines
            .entry(context.to_string())
            .or_insert_with(|| Arc::new(MessagingEngine::new()))
            .clone()
    }

    /// Gets the messaging engine for a context as a trait object.
    pub fn get_platform(&self, context: &str) -> Arc<dyn MessagingPlatform> {
        self.get_or_create(context)
    }

    /// Lists all contexts that have messaging engines.
    pub fn list_contexts(&self) -> Vec<String> {
        let engines = self.engines.read();
        let mut names: Vec<String> = engines.keys().cloned().collect();
        names.sort();
        names
    }

    /// Returns aggregate stats across all contexts.
    pub fn all_command_stats(&self) -> Vec<(String, usize)> {
        let engines = self.engines.read();
        let mut stats = Vec::new();
        for engine in engines.values() {
            stats.extend(engine.command_stats());
        }
        stats
    }

    /// Returns aggregate stats across all contexts.
    pub fn all_query_stats(&self) -> Vec<(String, usize)> {
        let engines = self.engines.read();
        let mut stats = Vec::new();
        for engine in engines.values() {
            stats.extend(engine.query_stats());
        }
        stats
    }

    /// Returns detailed command handler info + dispatch metrics, aggregated across all contexts.
    pub fn all_command_details(&self) -> Vec<MessageTypeDetail> {
        let engines = self.engines.read();
        let mut details = Vec::new();
        for engine in engines.values() {
            details.extend(engine.command_details());
        }
        details
    }

    /// Returns detailed query handler info + dispatch metrics, aggregated across all contexts.
    pub fn all_query_details(&self) -> Vec<MessageTypeDetail> {
        let engines = self.engines.read();
        let mut details = Vec::new();
        for engine in engines.values() {
            details.extend(engine.query_details());
        }
        details
    }

    /// Returns all active subscription queries across all contexts.
    pub fn all_subscription_stats(&self) -> Vec<SubscriptionInfo> {
        let engines = self.engines.read();
        let mut stats = Vec::new();
        for engine in engines.values() {
            stats.extend(engine.subscription_stats());
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
    fn contexts_are_isolated() {
        let manager = MessagingManager::new();

        let orders = manager.get_or_create("orders");
        let payments = manager.get_or_create("payments");

        // Register a handler in orders context.
        orders.subscribe_command(
            "CreateOrder".into(),
            ClientId("h1".into()),
            ComponentName("order-service".into()),
            100,
        );

        // Orders should have the handler.
        assert_eq!(orders.command_stats().len(), 1);

        // Payments should be empty.
        assert_eq!(payments.command_stats().len(), 0);
    }

    #[test]
    fn get_or_create_returns_same_instance() {
        let manager = MessagingManager::new();

        let first = manager.get_or_create("default");
        let second = manager.get_or_create("default");

        assert!(Arc::ptr_eq(&first, &second));
    }
}
