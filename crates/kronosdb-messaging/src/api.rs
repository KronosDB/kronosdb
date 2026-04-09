use tokio::sync::{mpsc, oneshot};

use crate::command::{Command, CommandError, CommandResult, PendingCommand};
use crate::query::{PendingQuery, Query, QueryError};
use crate::subscription::{SubscriptionError, SubscriptionQuery, SubscriptionUpdate};
use crate::types::{ClientId, ComponentName};

/// The command bus interface.
///
/// Routes commands to exactly one registered handler, load-balanced.
/// Extensions can decorate this trait to add logging, metrics, auth, etc.
pub trait CommandDispatcher: Send + Sync {
    /// Registers a command handler.
    fn subscribe_command(
        &self,
        command_name: String,
        client_id: ClientId,
        component_name: ComponentName,
        load_factor: i32,
    );

    /// Unregisters a command handler.
    fn unsubscribe_command(&self, command_name: &str, client_id: &ClientId);

    /// Removes all command subscriptions for a disconnected client.
    fn remove_command_client(&self, client_id: &ClientId);

    /// Grants flow control permits to a command handler.
    fn grant_command_permits(&self, client_id: &ClientId, permits: i64);

    /// Dispatches a command to a handler.
    /// Returns the pending command (to deliver to the handler) and a receiver for the response.
    fn dispatch_command(
        &self,
        command: Command,
    ) -> Result<(PendingCommand, oneshot::Receiver<CommandResult>), CommandError>;
}

/// The query bus interface.
///
/// Routes queries to one or all registered handlers.
/// Extensions can decorate this trait.
pub trait QueryDispatcher: Send + Sync {
    /// Registers a query handler.
    fn subscribe_query(
        &self,
        query_name: String,
        client_id: ClientId,
        component_name: ComponentName,
    );

    /// Unregisters a query handler.
    fn unsubscribe_query(&self, query_name: &str, client_id: &ClientId);

    /// Removes all query subscriptions for a disconnected client.
    fn remove_query_client(&self, client_id: &ClientId);

    /// Grants flow control permits to a query handler.
    fn grant_query_permits(&self, client_id: &ClientId, permits: i64);

    /// Dispatches a query.
    /// Returns the pending query with target handler client IDs.
    fn dispatch_query(&self, query: Query) -> Result<PendingQuery, QueryError>;
}

/// The subscription query interface.
///
/// Manages long-lived query subscriptions where handlers push updates
/// to subscribers whenever the answer changes.
pub trait SubscriptionQueryDispatcher: Send + Sync {
    /// Opens a subscription query. Routes to a handler and returns
    /// the pending query to deliver and a receiver for streaming updates.
    fn subscribe(
        &self,
        query: SubscriptionQuery,
    ) -> Result<(PendingQuery, mpsc::Receiver<SubscriptionUpdate>), SubscriptionError>;

    /// Sends an update from a handler to a subscription query subscriber.
    fn send_update(&self, subscription_id: &str, update: SubscriptionUpdate);

    /// Completes a subscription query (no more updates).
    fn complete_subscription(&self, subscription_id: &str);

    /// Cancels a subscription query (subscriber no longer interested).
    fn cancel_subscription(&self, subscription_id: &str);
}

/// Combined messaging platform interface.
///
/// The gRPC layer programs against this trait. Implementations can be
/// the direct engine or a cluster-aware decorator that forwards to remote nodes.
pub trait MessagingPlatform: CommandDispatcher + QueryDispatcher + SubscriptionQueryDispatcher {
    /// Removes all subscriptions for a disconnected client (commands, queries, subscriptions).
    fn remove_client(&self, client_id: &ClientId);
}
