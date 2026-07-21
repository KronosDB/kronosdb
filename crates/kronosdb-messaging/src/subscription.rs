use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::handler::HandlerRegistry;
use crate::query::PendingQuery;
use crate::types::{ClientId, ComponentName, ErrorDetail, Metadata, Payload};

/// A subscription query — initial query + ongoing updates.
#[derive(Debug, Clone)]
pub struct SubscriptionQuery {
    /// Unique subscription identifier.
    pub subscription_id: String,
    /// The query name, used for routing.
    pub query_name: String,
    /// Timestamp of query creation.
    pub timestamp: i64,
    /// The query payload.
    pub payload: Payload,
    /// Metadata.
    pub metadata: Metadata,
    /// The client that opened this subscription.
    pub client_id: ClientId,
    /// The component name of the subscriber.
    pub component_name: ComponentName,
    /// Number of update permits (how many updates the server may send before
    /// needing more permits).
    pub initial_permits: i64,
}

/// An update pushed by a handler for a subscription query.
#[derive(Debug, Clone)]
pub struct SubscriptionUpdate {
    /// The subscription this update is for.
    pub subscription_id: String,
    /// Update payload.
    pub payload: Option<Payload>,
    /// Update metadata.
    pub metadata: Metadata,
    /// Error code, if the update represents an error.
    pub error_code: Option<String>,
    /// Full error detail preserving the complete error context.
    pub error: Option<ErrorDetail>,
    /// Terminal marker: the handler completed this subscription and no
    /// further updates will follow. With `error_code` set it maps to
    /// CompleteExceptionally on the wire; without, to Complete.
    pub complete: bool,
}

/// Error related to subscription queries.
#[derive(Debug)]
pub enum SubscriptionError {
    /// No handler for this query type.
    NoHandlerAvailable { query_name: String },
    /// All handlers at capacity.
    NoPermitsAvailable { query_name: String },
    /// Subscription not found.
    SubscriptionNotFound { subscription_id: String },
    /// A subscription with this id is already active.
    DuplicateSubscription { subscription_id: String },
}

impl std::fmt::Display for SubscriptionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoHandlerAvailable { query_name } => {
                write!(
                    f,
                    "no handler available for subscription query '{query_name}'"
                )
            }
            Self::NoPermitsAvailable { query_name } => {
                write!(
                    f,
                    "all handlers at capacity for subscription query '{query_name}'"
                )
            }
            Self::SubscriptionNotFound { subscription_id } => {
                write!(f, "subscription not found: '{subscription_id}'")
            }
            Self::DuplicateSubscription { subscription_id } => {
                write!(f, "subscription '{subscription_id}' is already active")
            }
        }
    }
}

/// Summary of an active subscription query, for admin display.
#[derive(Debug, Clone)]
pub struct SubscriptionInfo {
    pub subscription_id: String,
    pub query_name: String,
    pub subscriber_client_id: ClientId,
    pub subscriber_component: ComponentName,
    pub handler_client_id: ClientId,
    pub since_opened: Duration,
}

/// An active subscription — tracks the channel for sending updates to the subscriber.
struct ActiveSubscription {
    /// The subscriber's client ID.
    client_id: ClientId,
    /// The handler's client ID that is providing updates.
    handler_client_id: ClientId,
    /// The query name this subscription is for.
    query_name: String,
    /// The component name of the subscriber.
    component_name: ComponentName,
    /// When this subscription was opened.
    opened_at: Instant,
    /// Channel to send updates to the subscriber.
    update_tx: mpsc::Sender<SubscriptionUpdate>,
    /// Update credit: how many more updates the subscriber has asked for.
    /// Seeded from `initial_permits`, replenished by FlowControl. Terminal
    /// messages (complete / error) bypass the credit check — the subscriber
    /// must always learn the subscription ended.
    permits: AtomicI64,
}

/// Registry of active subscription queries.
///
/// Manages the lifecycle of subscription queries: opening, updating,
/// completing, and cancelling.
pub struct SubscriptionRegistry {
    /// Query handler registry — shared with QueryBus so subscription queries
    /// can find handlers registered via the regular query bus.
    handlers: Arc<RwLock<HandlerRegistry>>,
    /// Active subscriptions, keyed by subscription_id.
    active: RwLock<HashMap<String, ActiveSubscription>>,
}

impl Default for SubscriptionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl SubscriptionRegistry {
    /// Creates a registry with its own handler registry (for testing).
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(RwLock::new(HandlerRegistry::new())),
            active: RwLock::new(HashMap::new()),
        }
    }

    /// Creates the registry sharing the QueryBus's handler registry.
    /// This is the correct constructor for production use — subscription queries
    /// need to find the same handlers that serve regular queries.
    pub fn with_shared_handlers(handlers: Arc<RwLock<HandlerRegistry>>) -> Self {
        Self {
            handlers,
            active: RwLock::new(HashMap::new()),
        }
    }

    /// Registers a query handler (delegates to inner registry).
    pub fn subscribe_handler(
        &self,
        query_name: String,
        client_id: ClientId,
        component_name: ComponentName,
    ) {
        let mut handlers = self.handlers.write();
        handlers.subscribe(query_name, client_id, component_name, 100);
    }

    /// Opens a subscription query.
    ///
    /// Routes to a handler and creates an update channel.
    /// Returns the pending query (to deliver to the handler) and a receiver
    /// for the subscriber to receive updates on.
    pub fn open(
        &self,
        query: SubscriptionQuery,
    ) -> Result<(PendingQuery, mpsc::Receiver<SubscriptionUpdate>), SubscriptionError> {
        // Reject duplicates before consuming a handler permit. Holding the
        // write lock across handler selection also closes the check-then-act
        // race between two opens with the same id.
        let mut active = self.active.write();
        if active.contains_key(&query.subscription_id) {
            return Err(SubscriptionError::DuplicateSubscription {
                subscription_id: query.subscription_id.clone(),
            });
        }

        let handlers = self.handlers.read();
        let handler_list = handlers.get_handlers(&query.query_name).ok_or_else(|| {
            SubscriptionError::NoHandlerAvailable {
                query_name: query.query_name.clone(),
            }
        })?;

        if handler_list.is_empty() {
            return Err(SubscriptionError::NoHandlerAvailable {
                query_name: query.query_name.clone(),
            });
        }

        // Pick the first handler with available permits.
        let handler = handler_list
            .iter()
            .find(|h| h.handler.try_acquire_permit())
            .ok_or_else(|| SubscriptionError::NoPermitsAvailable {
                query_name: query.query_name.clone(),
            })?;

        let handler_client_id = handler.handler.client_id.clone();

        // Create the update channel. The subscriber's requested permit count
        // sizes the buffer (bounded to keep a hostile value from pinning memory)
        // and seeds the update credit.
        let initial_permits = query.initial_permits.clamp(1, 1024);
        let (update_tx, update_rx) = mpsc::channel(initial_permits as usize);

        let subscription = ActiveSubscription {
            client_id: query.client_id.clone(),
            handler_client_id: handler_client_id.clone(),
            query_name: query.query_name.clone(),
            component_name: query.component_name.clone(),
            opened_at: Instant::now(),
            update_tx,
            permits: AtomicI64::new(initial_permits),
        };

        let subscription_id = query.subscription_id.clone();

        let pending = PendingQuery {
            query: crate::query::Query {
                message_id: query.subscription_id.clone(),
                name: query.query_name,
                timestamp: query.timestamp,
                payload: query.payload,
                metadata: query.metadata,
                processing_instructions: vec![],
                client_id: query.client_id,
                component_name: query.component_name,
                expected_results: 1,
            },
            target_handlers: vec![handler_client_id],
        };

        // Register the active subscription (write lock held since the
        // duplicate check above).
        active.insert(subscription_id, subscription);

        Ok((pending, update_rx))
    }

    /// Sends an update from a handler to a subscription query subscriber.
    ///
    /// Credit-based flow control: each non-terminal update consumes one permit
    /// (seeded from `initial_permits`, replenished via [`grant_permits`]).
    /// Out-of-credit updates are dropped with a warning — the subscriber
    /// stopped refilling, so it is either gone or not keeping up. Terminal
    /// updates (complete / error) bypass the credit check.
    pub fn send_update(&self, subscription_id: &str, update: SubscriptionUpdate) {
        let active = self.active.read();
        let Some(sub) = active.get(subscription_id) else {
            return;
        };

        let terminal = update.complete || update.error_code.is_some();
        if !terminal && sub.permits.fetch_sub(1, Ordering::Relaxed) <= 0 {
            sub.permits.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                subscription_id = %subscription_id,
                query = %sub.query_name,
                "subscription update dropped: subscriber out of update permits"
            );
            return;
        }

        if let Err(e) = sub.update_tx.try_send(update) {
            tracing::warn!(
                subscription_id = %subscription_id,
                query = %sub.query_name,
                reason = %e,
                "subscription update dropped: subscriber buffer full or closed"
            );
        }
    }

    /// Grants additional update permits to a subscription (FlowControl refill).
    pub fn grant_permits(&self, subscription_id: &str, permits: i64) {
        let active = self.active.read();
        if let Some(sub) = active.get(subscription_id) {
            sub.permits.fetch_add(permits.max(0), Ordering::Relaxed);
        }
    }

    /// Completes a subscription query — handler signals no more updates.
    pub fn complete(&self, subscription_id: &str) {
        let mut active = self.active.write();
        active.remove(subscription_id);
        // Dropping the ActiveSubscription closes the update_tx channel,
        // which signals the subscriber that the stream is done.
    }

    /// Cancels a subscription query — subscriber is no longer interested.
    /// Returns the handler's client id (if the subscription existed) so the
    /// caller can notify the handler to stop emitting updates.
    pub fn cancel(&self, subscription_id: &str) -> Option<ClientId> {
        let mut active = self.active.write();
        active
            .remove(subscription_id)
            .map(|sub| sub.handler_client_id)
    }

    /// Removes all subscriptions for a disconnected client
    /// (both as subscriber and as handler).
    pub fn remove_client(&self, client_id: &ClientId) {
        let mut active = self.active.write();

        // Remove as subscriber — the subscriber is gone, nothing to notify.
        active.retain(|_, sub| &sub.client_id != client_id);

        // Remove subscriptions whose HANDLER disconnected, telling each
        // subscriber the stream ended abnormally — a bare channel close
        // would look like normal completion.
        active.retain(|sub_id, sub| {
            if &sub.handler_client_id != client_id {
                return true;
            }
            let _ = sub.update_tx.try_send(SubscriptionUpdate {
                subscription_id: sub_id.clone(),
                payload: None,
                metadata: HashMap::new(),
                error_code: Some(crate::error_codes::CONNECTION_TO_HANDLER_LOST.to_string()),
                error: Some(crate::types::ErrorDetail {
                    message: "the handler serving this subscription disconnected".to_string(),
                    location: "kronosdb".to_string(),
                    details: vec![],
                    error_code: crate::error_codes::CONNECTION_TO_HANDLER_LOST.to_string(),
                }),
                complete: true,
            });
            false
        });

        drop(active);

        // Remove as handler.
        let mut handlers = self.handlers.write();
        handlers.remove_client(client_id);
    }

    /// Returns the number of active subscriptions.
    pub fn active_count(&self) -> usize {
        let active = self.active.read();
        active.len()
    }

    /// Returns details of all active subscriptions, for admin display.
    pub fn list_active(&self) -> Vec<SubscriptionInfo> {
        let now = Instant::now();
        let active = self.active.read();
        active
            .iter()
            .map(|(id, sub)| SubscriptionInfo {
                subscription_id: id.clone(),
                query_name: sub.query_name.clone(),
                subscriber_client_id: sub.client_id.clone(),
                subscriber_component: sub.component_name.clone(),
                handler_client_id: sub.handler_client_id.clone(),
                since_opened: now.duration_since(sub.opened_at),
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

    fn make_subscription(id: &str, query_name: &str) -> SubscriptionQuery {
        SubscriptionQuery {
            subscription_id: id.to_string(),
            query_name: query_name.to_string(),
            timestamp: 0,
            payload: Payload {
                payload_type: query_name.to_string(),
                revision: "1".to_string(),
                data: vec![],
            },
            metadata: std::collections::HashMap::new(),
            client_id: client("subscriber"),
            component_name: component("dashboard"),
            initial_permits: 10,
        }
    }

    #[test]
    fn open_subscription() {
        let registry = SubscriptionRegistry::new();
        registry.subscribe_handler(
            "GetOrderCount".into(),
            client("handler-1"),
            component("order-service"),
        );

        // Grant permits to the handler.
        {
            let handlers = registry.handlers.read();
            handlers.grant_permits(&client("handler-1"), 10);
        }

        let sub = make_subscription("sub-1", "GetOrderCount");
        let (pending, _rx) = registry.open(sub).unwrap();

        assert_eq!(pending.target_handlers.len(), 1);
        assert_eq!(pending.target_handlers[0], client("handler-1"));
        assert_eq!(registry.active_count(), 1);
    }

    #[test]
    fn send_update() {
        let registry = SubscriptionRegistry::new();
        registry.subscribe_handler(
            "GetOrderCount".into(),
            client("handler-1"),
            component("order-service"),
        );
        {
            let handlers = registry.handlers.read();
            handlers.grant_permits(&client("handler-1"), 10);
        }

        let sub = make_subscription("sub-1", "GetOrderCount");
        let (_pending, mut rx) = registry.open(sub).unwrap();

        // Handler sends an update.
        registry.send_update(
            "sub-1",
            SubscriptionUpdate {
                subscription_id: "sub-1".into(),
                payload: Some(Payload {
                    payload_type: "OrderCount".into(),
                    revision: "1".into(),
                    data: b"42".to_vec(),
                }),
                metadata: std::collections::HashMap::new(),
                error_code: None,
                error: None,
                complete: false,
            },
        );

        // Subscriber receives it.
        let update = rx.try_recv().unwrap();
        assert_eq!(update.subscription_id, "sub-1");
    }

    #[test]
    fn complete_subscription_closes_channel() {
        let registry = SubscriptionRegistry::new();
        registry.subscribe_handler(
            "GetOrderCount".into(),
            client("handler-1"),
            component("order-service"),
        );
        {
            let handlers = registry.handlers.read();
            handlers.grant_permits(&client("handler-1"), 10);
        }

        let sub = make_subscription("sub-1", "GetOrderCount");
        let (_pending, mut rx) = registry.open(sub).unwrap();

        registry.complete("sub-1");
        assert_eq!(registry.active_count(), 0);

        // Channel is closed — try_recv returns an error.
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn cancel_subscription() {
        let registry = SubscriptionRegistry::new();
        registry.subscribe_handler(
            "GetOrderCount".into(),
            client("handler-1"),
            component("order-service"),
        );
        {
            let handlers = registry.handlers.read();
            handlers.grant_permits(&client("handler-1"), 10);
        }

        let sub = make_subscription("sub-1", "GetOrderCount");
        let (_pending, _rx) = registry.open(sub).unwrap();

        registry.cancel("sub-1");
        assert_eq!(registry.active_count(), 0);
    }

    #[test]
    fn no_handler_returns_error() {
        let registry = SubscriptionRegistry::new();
        let sub = make_subscription("sub-1", "NonExistent");
        let result = registry.open(sub);
        assert!(matches!(
            result,
            Err(SubscriptionError::NoHandlerAvailable { .. })
        ));
    }

    #[test]
    fn remove_client_cleans_up_subscriptions() {
        let registry = SubscriptionRegistry::new();
        registry.subscribe_handler(
            "GetOrderCount".into(),
            client("handler-1"),
            component("order-service"),
        );
        {
            let handlers = registry.handlers.read();
            handlers.grant_permits(&client("handler-1"), 10);
        }

        let sub = make_subscription("sub-1", "GetOrderCount");
        let (_pending, _rx) = registry.open(sub).unwrap();

        // Subscriber disconnects.
        registry.remove_client(&client("subscriber"));
        assert_eq!(registry.active_count(), 0);
    }
}
