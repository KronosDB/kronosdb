use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::command::{Command, CommandError, CommandResult, PendingCommand};
use crate::query::{PendingQuery, Query, QueryError};
use crate::subscription::{SubscriptionError, SubscriptionQuery, SubscriptionUpdate};
use crate::types::{ClientId, ComponentName};

/// Boxed future returned by [`CommandDispatcher::dispatch_command_wait`].
///
/// Hand-rolled instead of `async fn` so the trait stays object-safe — the
/// gRPC layer programs against `Arc<dyn MessagingPlatform>`.
pub type DispatchFuture<'a> = std::pin::Pin<
    Box<
        dyn std::future::Future<
                Output = Result<(PendingCommand, oneshot::Receiver<CommandResult>), CommandError>,
            > + Send
            + 'a,
    >,
>;

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

    /// Removes all command subscriptions for a disconnected client and
    /// cancels any in-flight commands routed to it. Returns the cancelled
    /// command message_ids for logging.
    fn remove_command_client(&self, client_id: &ClientId) -> Vec<String>;

    /// Grants flow control permits to a command handler.
    fn grant_command_permits(&self, client_id: &ClientId, permits: i64);

    /// Dispatches a command to a handler.
    /// Returns the pending command (to deliver to the handler) and a receiver for the response.
    fn dispatch_command(
        &self,
        command: Command,
    ) -> Result<(PendingCommand, oneshot::Receiver<CommandResult>), CommandError>;

    /// Dispatches a command, waiting up to `max_wait` for a flow-control
    /// grant when the target handler is out of permits. Sticky
    /// (routing-keyed) commands wait on their ring-selected handler
    /// specifically — they are never re-routed. With permit-wait disabled
    /// in the bus config this behaves exactly like [`dispatch_command`].
    ///
    /// [`dispatch_command`]: CommandDispatcher::dispatch_command
    fn dispatch_command_wait(&self, command: Command, max_wait: Duration) -> DispatchFuture<'_>;

    /// Dispatches a command to a specific handler instance — the fabric
    /// path (ADR-0007): selection happened on the dispatching node, this
    /// node just delivers. Bounded permit wait, never re-selects.
    fn dispatch_command_to_wait(
        &self,
        command: Command,
        target: ClientId,
        max_wait: Duration,
    ) -> DispatchFuture<'_>;

    /// Completes a pending command with a response from the handler.
    fn complete_command(&self, request_id: &str, result: CommandResult);

    /// Cancels a single in-flight command (caller-side timeout / abandon).
    fn cancel_in_flight_command(&self, message_id: &str);

    /// Sweeps in-flight commands older than `timeout`. Each receives a
    /// `KRONOSDB-4005` failure. Returns the swept message_ids.
    fn sweep_command_timeouts(&self, timeout: Duration) -> Vec<String>;
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

    /// Dispatches a query to specific handler instances — the fabric path
    /// (ADR-0007): selection happened on the dispatching node, this node
    /// just delivers to the named local handlers.
    fn dispatch_query_to(
        &self,
        query: Query,
        targets: &[ClientId],
    ) -> Result<PendingQuery, QueryError>;

    /// Dispatches a query, waiting (bounded by `max_wait`) for a
    /// flow-control grant when no handler can accept.
    fn dispatch_query_wait(&self, query: Query, max_wait: Duration) -> QueryDispatchFuture<'_>;

    /// [`dispatch_query_to`] with a bounded permit wait.
    ///
    /// [`dispatch_query_to`]: QueryDispatcher::dispatch_query_to
    fn dispatch_query_to_wait(
        &self,
        query: Query,
        targets: Vec<ClientId>,
        max_wait: Duration,
    ) -> QueryDispatchFuture<'_>;
}

/// Boxed future returned by the waiting query-dispatch variants; hand-
/// rolled for object safety, like [`DispatchFuture`].
pub type QueryDispatchFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<PendingQuery, QueryError>> + Send + 'a>,
>;

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

    /// Opens a subscription against a specific handler instance — the
    /// fabric path (ADR-0007): selection happened on the subscriber's node.
    fn subscribe_to(
        &self,
        query: SubscriptionQuery,
        target: &ClientId,
    ) -> Result<(PendingQuery, mpsc::Receiver<SubscriptionUpdate>), SubscriptionError>;

    /// Sends an update from a handler to a subscription query subscriber.
    fn send_update(&self, subscription_id: &str, update: SubscriptionUpdate);

    /// Grants additional update permits to a subscription (FlowControl refill).
    fn grant_subscription_permits(&self, subscription_id: &str, permits: i64);

    /// Completes a subscription query (no more updates).
    fn complete_subscription(&self, subscription_id: &str);

    /// Cancels a subscription query (subscriber no longer interested).
    /// Returns the handler's client id if the subscription existed, so the
    /// caller can notify that handler to stop emitting updates.
    fn cancel_subscription(&self, subscription_id: &str) -> Option<ClientId>;
}

/// Combined messaging platform interface.
///
/// The gRPC layer programs against this trait. Implementations can be
/// the direct engine or a cluster-aware decorator that forwards to remote nodes.
pub trait MessagingPlatform:
    CommandDispatcher + QueryDispatcher + SubscriptionQueryDispatcher
{
    /// Removes all subscriptions for a disconnected client (commands, queries,
    /// subscriptions) and cancels any in-flight commands routed to it.
    /// Returns the cancelled command message_ids.
    fn remove_client(&self, client_id: &ClientId) -> Vec<String>;
}
