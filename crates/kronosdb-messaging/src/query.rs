use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;
use parking_lot::RwLock;

use crate::handler::{HandlerRegistry, MessageTypeDetail, MessageTypeMetrics, MetricsSnapshot};
use crate::types::{
    ClientId, ComponentName, ErrorDetail, Metadata, Payload, ProcessingInstruction,
};

/// A query to be dispatched.
#[derive(Debug, Clone)]
pub struct Query {
    /// Unique message identifier.
    pub message_id: String,
    /// The query name, used for routing.
    pub name: String,
    /// Timestamp of query creation.
    pub timestamp: i64,
    /// The query payload.
    pub payload: Payload,
    /// Metadata — opaque to KronosDB, transported losslessly.
    pub metadata: Metadata,
    /// Processing instructions.
    pub processing_instructions: Vec<ProcessingInstruction>,
    /// The client that dispatched this query.
    pub client_id: ClientId,
    /// The component name that dispatched this query.
    pub component_name: ComponentName,
    /// Number of expected results. -1 for unlimited (scatter-gather to all handlers).
    /// 1 for point-to-point (single handler).
    pub expected_results: i32,
}

/// A query result from a handler.
#[derive(Debug, Clone)]
pub struct QueryResult {
    /// Unique response identifier.
    pub message_id: String,
    /// The query this is a response to.
    pub request_id: String,
    /// Error code, if failed (top-level, for quick checks).
    pub error_code: Option<String>,
    /// Full error detail preserving the complete error context.
    pub error: Option<ErrorDetail>,
    /// Result payload, if any.
    pub payload: Option<Payload>,
    /// Response metadata — transported losslessly.
    pub metadata: Metadata,
    /// Processing instructions on the response.
    pub processing_instructions: Vec<ProcessingInstruction>,
}

/// Error dispatching a query.
#[derive(Debug)]
pub enum QueryError {
    /// No handler registered for this query type.
    NoHandlerAvailable { query_name: String },
    /// All handlers are at capacity.
    NoPermitsAvailable { query_name: String },
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoHandlerAvailable { query_name } => {
                write!(f, "no handler available for query '{query_name}'")
            }
            Self::NoPermitsAvailable { query_name } => {
                write!(f, "all handlers at capacity for query '{query_name}'")
            }
        }
    }
}

/// A query dispatched to one or more handlers, collecting responses.
pub struct PendingQuery {
    /// The query to deliver to handlers.
    pub query: Query,
    /// Client IDs of the handlers that should receive this query.
    pub target_handlers: Vec<ClientId>,
}

/// The query bus. Routes queries to registered handlers.
///
/// Supports two dispatch modes:
/// - **Point-to-point**: query goes to one handler (expected_results = 1)
/// - **Scatter-gather**: query goes to all handlers (expected_results = -1)
pub struct QueryBus {
    handlers: Arc<RwLock<HandlerRegistry>>,
    /// Round-robin counter distributing point-to-point queries across handlers.
    dispatch_counter: AtomicU64,
    /// Per-query-type dispatch metrics. Lock-free atomic counters inside the
    /// value; DashMap shards key-level insert contention (same pattern as
    /// CommandBus).
    metrics: DashMap<String, MessageTypeMetrics>,
}

impl Default for QueryBus {
    fn default() -> Self {
        Self::new()
    }
}

impl QueryBus {
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(RwLock::new(HandlerRegistry::new())),
            dispatch_counter: AtomicU64::new(0),
            metrics: DashMap::new(),
        }
    }

    /// Returns a shared reference to the handler registry.
    /// Used by SubscriptionRegistry to share the same handlers.
    pub fn shared_handlers(&self) -> Arc<RwLock<HandlerRegistry>> {
        Arc::clone(&self.handlers)
    }

    /// Registers a query handler.
    pub fn subscribe(
        &self,
        query_name: String,
        client_id: ClientId,
        component_name: ComponentName,
    ) {
        let mut handlers = self.handlers.write();
        // Query handlers don't have load_factor — scatter-gather sends to all.
        handlers.subscribe(query_name, client_id, component_name, 100);
    }

    /// Unregisters a query handler.
    pub fn unsubscribe(&self, query_name: &str, client_id: &ClientId) {
        let mut handlers = self.handlers.write();
        handlers.unsubscribe(query_name, client_id);
    }

    /// Removes all subscriptions for a disconnected client.
    pub fn remove_client(&self, client_id: &ClientId) {
        let mut handlers = self.handlers.write();
        handlers.remove_client(client_id);
    }

    /// Grants flow control permits to a client.
    pub fn grant_permits(&self, client_id: &ClientId, permits: i64) {
        let handlers = self.handlers.read();
        handlers.grant_permits(client_id, permits);
    }

    /// Returns detailed handler info + dispatch metrics per query type.
    pub fn handler_details(&self) -> Vec<MessageTypeDetail> {
        let handlers = self.handlers.read();
        let details = handlers.handler_details();
        details
            .into_iter()
            .map(|(name, handlers)| {
                let snapshot = self
                    .metrics
                    .get(&name)
                    .map(|m| m.snapshot())
                    .unwrap_or_else(MetricsSnapshot::empty);
                MessageTypeDetail {
                    name,
                    handlers,
                    metrics: snapshot,
                }
            })
            .collect()
    }

    /// Dispatches a query.
    ///
    /// Returns a `PendingQuery` with the target handler client IDs.
    /// The gRPC layer is responsible for sending the query to each handler's
    /// stream and collecting responses.
    ///
    /// - `expected_results = 1`: point-to-point (first available handler)
    /// - `expected_results = -1`: scatter-gather (all handlers)
    pub fn dispatch(&self, query: Query) -> Result<PendingQuery, QueryError> {
        let query_name = query.name.clone();
        let handlers = self.handlers.read();
        let handler_list = handlers.get_handlers(&query.name).ok_or_else(|| {
            self.record_no_handler(&query_name);
            QueryError::NoHandlerAvailable {
                query_name: query.name.clone(),
            }
        })?;

        if handler_list.is_empty() {
            self.record_no_handler(&query_name);
            return Err(QueryError::NoHandlerAvailable {
                query_name: query.name.clone(),
            });
        }

        let target_handlers = if query.expected_results == 1 {
            // Point-to-point: round-robin start offset so load spreads across
            // handlers instead of always hammering the first; walk the ring
            // until one has a permit.
            let start = self.dispatch_counter.fetch_add(1, Ordering::Relaxed) as usize;
            let handler = (0..handler_list.len())
                .map(|i| &handler_list[(start + i) % handler_list.len()])
                .find(|h| h.handler.try_acquire_permit())
                .ok_or_else(|| {
                    self.record_no_permits(&query_name);
                    QueryError::NoPermitsAvailable {
                        query_name: query.name.clone(),
                    }
                })?;
            vec![handler.handler.client_id.clone()]
        } else {
            // Scatter-gather: send to all handlers with available permits.
            let mut targets = Vec::new();
            for entry in handler_list {
                if entry.handler.try_acquire_permit() {
                    targets.push(entry.handler.client_id.clone());
                }
            }
            if targets.is_empty() {
                self.record_no_permits(&query_name);
                return Err(QueryError::NoPermitsAvailable {
                    query_name: query.name.clone(),
                });
            }
            targets
        };

        // Record dispatch.
        self.record_dispatched(&query_name);

        Ok(PendingQuery {
            query,
            target_handlers,
        })
    }

    // ── Metrics helpers ─────────────────────────────────────────────

    fn record_dispatched(&self, name: &str) {
        self.metrics
            .entry(name.to_string())
            .or_default()
            .dispatched
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_no_handler(&self, name: &str) {
        self.metrics
            .entry(name.to_string())
            .or_default()
            .no_handler
            .fetch_add(1, Ordering::Relaxed);
    }

    fn record_no_permits(&self, name: &str) {
        self.metrics
            .entry(name.to_string())
            .or_default()
            .no_permits
            .fetch_add(1, Ordering::Relaxed);
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

    fn make_query(name: &str, expected_results: i32) -> Query {
        Query {
            message_id: format!("q-{name}"),
            name: name.to_string(),
            timestamp: 0,
            payload: Payload {
                payload_type: name.to_string(),
                revision: "1".to_string(),
                data: vec![],
            },
            metadata: std::collections::HashMap::new(),
            processing_instructions: vec![],
            client_id: client("dispatcher"),
            component_name: component("test"),
            expected_results,
        }
    }

    #[test]
    fn scatter_gather_dispatches_to_all() {
        let bus = QueryBus::new();
        bus.subscribe(
            "GetOrders".into(),
            client("node-1"),
            component("order-service"),
        );
        bus.subscribe(
            "GetOrders".into(),
            client("node-2"),
            component("order-service"),
        );
        bus.grant_permits(&client("node-1"), 10);
        bus.grant_permits(&client("node-2"), 10);

        let query = make_query("GetOrders", -1);
        let pending = bus.dispatch(query).unwrap();

        assert_eq!(pending.target_handlers.len(), 2);
    }

    #[test]
    fn point_to_point_dispatches_to_one() {
        let bus = QueryBus::new();
        bus.subscribe(
            "GetOrder".into(),
            client("node-1"),
            component("order-service"),
        );
        bus.subscribe(
            "GetOrder".into(),
            client("node-2"),
            component("order-service"),
        );
        bus.grant_permits(&client("node-1"), 10);
        bus.grant_permits(&client("node-2"), 10);

        let query = make_query("GetOrder", 1);
        let pending = bus.dispatch(query).unwrap();

        assert_eq!(pending.target_handlers.len(), 1);
    }

    #[test]
    fn no_handler_returns_error() {
        let bus = QueryBus::new();
        let query = make_query("NonExistent", 1);
        let result = bus.dispatch(query);
        assert!(matches!(result, Err(QueryError::NoHandlerAvailable { .. })));
    }

    #[test]
    fn no_permits_returns_error() {
        let bus = QueryBus::new();
        bus.subscribe(
            "GetOrders".into(),
            client("node-1"),
            component("order-service"),
        );
        // No permits.

        let query = make_query("GetOrders", -1);
        let result = bus.dispatch(query);
        assert!(matches!(result, Err(QueryError::NoPermitsAvailable { .. })));
    }

    #[test]
    fn remove_client_cleans_up() {
        let bus = QueryBus::new();
        bus.subscribe(
            "GetOrders".into(),
            client("node-1"),
            component("order-service"),
        );
        bus.grant_permits(&client("node-1"), 10);

        bus.remove_client(&client("node-1"));

        let query = make_query("GetOrders", 1);
        let result = bus.dispatch(query);
        assert!(matches!(result, Err(QueryError::NoHandlerAvailable { .. })));
    }
}
