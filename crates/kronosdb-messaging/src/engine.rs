use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::api::{
    CommandDispatcher, MessagingPlatform, QueryDispatcher, SubscriptionQueryDispatcher,
};
use crate::command::{Command, CommandBus, CommandError, CommandResult, PendingCommand};
use crate::handler::MessageTypeDetail;
use crate::query::{PendingQuery, Query, QueryBus, QueryError};
use crate::subscription::{
    SubscriptionError, SubscriptionInfo, SubscriptionQuery, SubscriptionRegistry,
    SubscriptionUpdate,
};
use crate::types::{ClientId, ComponentName};

/// The messaging platform engine.
///
/// Combines the command bus, query bus, and subscription registry
/// into a single concrete implementation of `MessagingPlatform`.
pub struct MessagingEngine {
    command_bus: CommandBus,
    query_bus: QueryBus,
    subscriptions: SubscriptionRegistry,
}

impl Default for MessagingEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl MessagingEngine {
    pub fn new() -> Self {
        let query_bus = QueryBus::new();
        let subscriptions = SubscriptionRegistry::with_shared_handlers(query_bus.shared_handlers());
        Self {
            command_bus: CommandBus::new(),
            query_bus,
            subscriptions,
        }
    }

    /// Returns detailed command handler info + dispatch metrics per command type.
    pub fn command_details(&self) -> Vec<MessageTypeDetail> {
        self.command_bus.handler_details()
    }

    /// Returns detailed query handler info + dispatch metrics per query type.
    pub fn query_details(&self) -> Vec<MessageTypeDetail> {
        self.query_bus.handler_details()
    }

    /// Returns details of all active subscription queries.
    pub fn subscription_stats(&self) -> Vec<SubscriptionInfo> {
        self.subscriptions.list_active()
    }
}

impl CommandDispatcher for MessagingEngine {
    fn subscribe_command(
        &self,
        command_name: String,
        client_id: ClientId,
        component_name: ComponentName,
        load_factor: i32,
    ) {
        self.command_bus
            .subscribe(command_name, client_id, component_name, load_factor);
    }

    fn unsubscribe_command(&self, command_name: &str, client_id: &ClientId) {
        self.command_bus.unsubscribe(command_name, client_id);
    }

    fn remove_command_client(&self, client_id: &ClientId) -> Vec<String> {
        self.command_bus.remove_client(client_id)
    }

    fn grant_command_permits(&self, client_id: &ClientId, permits: i64) {
        self.command_bus.grant_permits(client_id, permits);
    }

    fn dispatch_command(
        &self,
        command: Command,
    ) -> Result<(PendingCommand, oneshot::Receiver<CommandResult>), CommandError> {
        self.command_bus.dispatch(command)
    }

    fn complete_command(&self, request_id: &str, result: CommandResult) {
        self.command_bus.complete(request_id, result);
    }

    fn cancel_in_flight_command(&self, message_id: &str) {
        self.command_bus.cancel_in_flight(message_id);
    }

    fn sweep_command_timeouts(&self, timeout: Duration) -> Vec<String> {
        self.command_bus.sweep_timeouts(timeout)
    }
}

impl QueryDispatcher for MessagingEngine {
    fn subscribe_query(
        &self,
        query_name: String,
        client_id: ClientId,
        component_name: ComponentName,
    ) {
        self.query_bus
            .subscribe(query_name, client_id, component_name);
    }

    fn unsubscribe_query(&self, query_name: &str, client_id: &ClientId) {
        self.query_bus.unsubscribe(query_name, client_id);
    }

    fn remove_query_client(&self, client_id: &ClientId) {
        self.query_bus.remove_client(client_id);
    }

    fn grant_query_permits(&self, client_id: &ClientId, permits: i64) {
        self.query_bus.grant_permits(client_id, permits);
    }

    fn dispatch_query(&self, query: Query) -> Result<PendingQuery, QueryError> {
        self.query_bus.dispatch(query)
    }
}

impl SubscriptionQueryDispatcher for MessagingEngine {
    fn subscribe(
        &self,
        query: SubscriptionQuery,
    ) -> Result<(PendingQuery, mpsc::Receiver<SubscriptionUpdate>), SubscriptionError> {
        self.subscriptions.open(query)
    }

    fn send_update(&self, subscription_id: &str, update: SubscriptionUpdate) {
        self.subscriptions.send_update(subscription_id, update);
    }

    fn grant_subscription_permits(&self, subscription_id: &str, permits: i64) {
        self.subscriptions.grant_permits(subscription_id, permits);
    }

    fn complete_subscription(&self, subscription_id: &str) {
        self.subscriptions.complete(subscription_id);
    }

    fn cancel_subscription(&self, subscription_id: &str) -> Option<ClientId> {
        self.subscriptions.cancel(subscription_id)
    }
}

impl MessagingPlatform for MessagingEngine {
    fn remove_client(&self, client_id: &ClientId) -> Vec<String> {
        let cancelled = self.command_bus.remove_client(client_id);
        self.query_bus.remove_client(client_id);
        self.subscriptions.remove_client(client_id);
        cancelled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Payload;

    fn client(id: &str) -> ClientId {
        ClientId(id.to_string())
    }

    fn component(name: &str) -> ComponentName {
        ComponentName(name.to_string())
    }

    #[test]
    fn messaging_engine_dispatches_commands() {
        let engine = MessagingEngine::new();

        engine.subscribe_command(
            "CreateOrder".into(),
            client("handler-1"),
            component("order-service"),
            100,
        );
        engine.grant_command_permits(&client("handler-1"), 10);

        let cmd = Command {
            message_id: "cmd-1".into(),
            name: "CreateOrder".into(),
            timestamp: 0,
            payload: Payload {
                payload_type: "CreateOrder".into(),
                revision: "1".into(),
                data: vec![],
            },
            metadata: std::collections::HashMap::new(),
            processing_instructions: vec![],
            routing_key: None,
            client_id: client("dispatcher"),
            component_name: component("test"),
        };

        let result = engine.dispatch_command(cmd);
        assert!(result.is_ok());
    }

    #[test]
    fn messaging_engine_dispatches_queries() {
        let engine = MessagingEngine::new();

        engine.subscribe_query(
            "GetOrders".into(),
            client("handler-1"),
            component("order-service"),
        );
        engine.grant_query_permits(&client("handler-1"), 10);

        let query = Query {
            message_id: "q-1".into(),
            name: "GetOrders".into(),
            timestamp: 0,
            payload: Payload {
                payload_type: "GetOrders".into(),
                revision: "1".into(),
                data: vec![],
            },
            metadata: std::collections::HashMap::new(),
            processing_instructions: vec![],
            client_id: client("dispatcher"),
            component_name: component("test"),
            expected_results: -1,
        };

        let result = engine.dispatch_query(query);
        assert!(result.is_ok());
    }

    #[test]
    fn remove_client_cleans_up_everything() {
        let engine = MessagingEngine::new();

        engine.subscribe_command(
            "CreateOrder".into(),
            client("node-1"),
            component("order-service"),
            100,
        );
        engine.subscribe_query(
            "GetOrders".into(),
            client("node-1"),
            component("order-service"),
        );

        engine.remove_client(&client("node-1"));

        let cmd = Command {
            message_id: "cmd-1".into(),
            name: "CreateOrder".into(),
            timestamp: 0,
            payload: Payload {
                payload_type: "CreateOrder".into(),
                revision: "1".into(),
                data: vec![],
            },
            metadata: std::collections::HashMap::new(),
            processing_instructions: vec![],
            routing_key: None,
            client_id: client("dispatcher"),
            component_name: component("test"),
        };
        assert!(matches!(
            engine.dispatch_command(cmd),
            Err(CommandError::NoHandlerAvailable { .. })
        ));

        let query = Query {
            message_id: "q-1".into(),
            name: "GetOrders".into(),
            timestamp: 0,
            payload: Payload {
                payload_type: "GetOrders".into(),
                revision: "1".into(),
                data: vec![],
            },
            metadata: std::collections::HashMap::new(),
            processing_instructions: vec![],
            client_id: client("dispatcher"),
            component_name: component("test"),
            expected_results: 1,
        };
        assert!(matches!(
            engine.dispatch_query(query),
            Err(QueryError::NoHandlerAvailable { .. })
        ));
    }
}
