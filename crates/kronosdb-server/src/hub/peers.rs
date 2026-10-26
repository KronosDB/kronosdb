//! How the hub reaches handlers connected to other nodes.
//!
//! The hub decides where a message goes; what carries it there is behind
//! this trait, so the hub knows nothing about connections or frames. The
//! native implementation lives with the other adapters.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use kronosdb_messaging::command::{Command, CommandResult};
use kronosdb_messaging::query::{Query, QueryResult};
use kronosdb_messaging::subscription::SubscriptionQuery;
use tokio::sync::mpsc;

use super::types::{Fault, SubscriptionEvent};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// A subscription relayed to the node that owns its handler.
pub struct RemoteFeed {
    /// What the handler produced, in order. Closes when the relay ends.
    pub events: mpsc::Receiver<Result<SubscriptionEvent, Fault>>,
    /// Update credits the subscriber granted, to pass on to the owner.
    pub credits: mpsc::UnboundedSender<u64>,
}

/// One node's share of a query.
pub struct QueryLeg<'a> {
    pub node: u64,
    pub bus: &'a str,
    pub target_client_ids: Vec<String>,
    pub query: Query,
    pub timeout: Duration,
    /// Whether a failure goes into the sink: only when this leg is the
    /// query's single source of results.
    pub report_failure: bool,
}

pub trait Peers: Send + Sync {
    /// Delivers a command to `target_client_id` on `node` and waits for its
    /// result. `timeout` is the whole budget; the owning node enforces it.
    fn forward_command<'a>(
        &'a self,
        node: u64,
        bus: &'a str,
        target_client_id: String,
        command: Command,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<CommandResult, Fault>>;

    /// Delivers a query to the named handlers on a node and pushes their
    /// results into `sink` until they are all in, the timeout passes, or the
    /// sink is dropped.
    fn forward_query<'a>(
        &'a self,
        leg: QueryLeg<'a>,
        sink: mpsc::Sender<Result<QueryResult, Fault>>,
    ) -> BoxFuture<'a, ()>;

    /// Opens a subscription on the node that owns `target_client_id`.
    /// Dropping the feed ends the relay.
    fn relay_subscription<'a>(
        &'a self,
        node: u64,
        bus: &'a str,
        target_client_id: String,
        subscription: SubscriptionQuery,
    ) -> BoxFuture<'a, Result<RemoteFeed, Fault>>;
}

/// Peers for a node that has none: every forward is unavailable. Used in
/// tests of the hub that never leave the process.
#[cfg(test)]
pub struct NoPeers;

#[cfg(test)]
impl Peers for NoPeers {
    fn forward_command<'a>(
        &'a self,
        node: u64,
        _bus: &'a str,
        _target_client_id: String,
        _command: Command,
        _timeout: Duration,
    ) -> BoxFuture<'a, Result<CommandResult, Fault>> {
        Box::pin(async move { Err(Fault::unavailable(format!("no route to node {node}"))) })
    }

    fn forward_query<'a>(
        &'a self,
        leg: QueryLeg<'a>,
        sink: mpsc::Sender<Result<QueryResult, Fault>>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            if leg.report_failure {
                let _ = sink
                    .send(Err(Fault::unavailable(format!(
                        "no route to node {}",
                        leg.node
                    ))))
                    .await;
            }
        })
    }

    fn relay_subscription<'a>(
        &'a self,
        node: u64,
        _bus: &'a str,
        _target_client_id: String,
        _subscription: SubscriptionQuery,
    ) -> BoxFuture<'a, Result<RemoteFeed, Fault>> {
        Box::pin(async move { Err(Fault::unavailable(format!("no route to node {node}"))) })
    }
}
