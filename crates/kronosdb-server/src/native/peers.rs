//! Reaching other nodes' handlers over native peer connections.
//!
//! Everything the hub forwards — commands, queries, subscription relays — is
//! a stream on one connection per peer, from a pool of the fabric's own so
//! that messaging never queues behind replication or forwarded appends.

use std::sync::Arc;
use std::time::Duration;

use kronosdb_eventstore::raft::cluster::ClusterManager;
use kronosdb_eventstore::replication::peer::PeerPool;
use kronosdb_messaging::command::{Command, CommandResult};
use kronosdb_messaging::query::QueryResult;
use kronosdb_messaging::subscription::SubscriptionQuery;
use kronosdb_protocol as p;
use kronosdb_protocol::net::NetError;
use kronosdb_protocol::net::client::{ClientStream, Connection};
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::wire::{self, Source};
use crate::hub::{BoxFuture, Fault, Peers, QueryLeg, RemoteFeed};

/// Time beyond a forwarded command's budget to wait for the owning node's
/// answer, which it gives (in-band) when the budget runs out.
const REPLY_GRACE: Duration = Duration::from_secs(2);

pub struct NativePeers {
    cluster: Arc<ClusterManager>,
    pool: PeerPool,
}

impl NativePeers {
    pub fn new(cluster: Arc<ClusterManager>) -> Self {
        let pool = PeerPool::new(cluster.cluster_config().peer_transport.clone());
        Self { cluster, pool }
    }

    /// A live connection to `node`, opened if there is none.
    async fn connection(&self, node: u64) -> Result<Connection, Fault> {
        let address = self
            .cluster
            .peer_address(node)
            .ok_or_else(|| Fault::unavailable(format!("no address for node {node}")))?;
        self.pool
            .connection(&address)
            .await
            .map_err(|e| Fault::unavailable(format!("fabric peer connect to node {node}: {e}")))
    }
}

fn net_fault(error: NetError) -> Fault {
    match error {
        NetError::Failed(failure) => wire::fault(&failure),
        other => Fault::unavailable(format!("fabric peer: {other}")),
    }
}

impl Peers for NativePeers {
    fn forward_command<'a>(
        &'a self,
        node: u64,
        bus: &'a str,
        target_client_id: String,
        command: Command,
        timeout: Duration,
    ) -> BoxFuture<'a, Result<CommandResult, Fault>> {
        Box::pin(async move {
            let connection = self.connection(node).await?;
            let request = p::Frame::ForwardCommand(p::ForwardCommand {
                bus,
                target_client_id: &target_client_id,
                command: wire::wire_command(&command),
                timeout_ms: timeout.as_millis() as u64,
            });
            let reply = tokio::time::timeout(timeout + REPLY_GRACE, connection.request(&request))
                .await
                .map_err(|_| Fault::unavailable(format!("node {node} did not answer in time")))?
                .map_err(net_fault)?;
            match reply.decode() {
                Ok(p::Frame::CommandResponse(r)) => {
                    Ok(wire::command_result(Source(reply.bytes()), &r.result))
                }
                _ => Err(Fault::unavailable(format!(
                    "node {node} answered a forwarded command with an unexpected frame"
                ))),
            }
        })
    }

    fn forward_query<'a>(
        &'a self,
        leg: QueryLeg<'a>,
        sink: mpsc::Sender<Result<QueryResult, Fault>>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let outcome = self.query_leg(&leg, &sink).await;
            if let Err(fault) = outcome {
                tracing::warn!(node = leg.node, error = %fault, "fabric: query forward failed");
                // A single-target route surfaces the failure instead of
                // an empty stream.
                if leg.report_failure {
                    let _ = sink.send(Err(fault)).await;
                }
            }
        })
    }

    fn relay_subscription<'a>(
        &'a self,
        node: u64,
        bus: &'a str,
        target_client_id: String,
        subscription: SubscriptionQuery,
    ) -> BoxFuture<'a, Result<RemoteFeed, Fault>> {
        Box::pin(async move {
            let connection = self.connection(node).await?;
            let stream = connection
                .open(
                    &p::Frame::RelaySubscription(p::RelaySubscription {
                        bus,
                        target_client_id: &target_client_id,
                        subscription: wire::wire_subscription(&subscription),
                    }),
                    false,
                )
                .await
                .map_err(net_fault)?;
            let (events_tx, events) = mpsc::channel(64);
            let (permits, permits_rx) = mpsc::unbounded_channel();
            tokio::spawn(pump_relay(stream, events_tx, permits_rx));
            Ok(RemoteFeed { events, permits })
        })
    }
}

impl NativePeers {
    /// Asks one node's handlers and pushes their results into `sink` until
    /// the node says it is done, the budget is spent, or the sink is gone
    /// (dropping the stream cancels the query over there).
    async fn query_leg(
        &self,
        leg: &QueryLeg<'_>,
        sink: &mpsc::Sender<Result<QueryResult, Fault>>,
    ) -> Result<(), Fault> {
        let connection = self.connection(leg.node).await?;
        let mut stream = connection
            .open(
                &p::Frame::ForwardQuery(p::ForwardQuery {
                    bus: leg.bus,
                    target_client_ids: leg.target_client_ids.iter().map(String::as_str).collect(),
                    query: wire::wire_query(&leg.query),
                    timeout_ms: leg.timeout.as_millis() as u64,
                }),
                true,
            )
            .await
            .map_err(net_fault)?;
        let deadline = Instant::now() + leg.timeout;
        loop {
            tokio::select! {
                _ = sink.closed() => return Ok(()),
                next = tokio::time::timeout_at(deadline, stream.next()) => match next {
                    Err(_) | Ok(Ok(None)) => return Ok(()),
                    Ok(Err(e)) => return Err(net_fault(e)),
                    Ok(Ok(Some(frame))) => match frame.decode() {
                        Ok(p::Frame::QueryResponse(r)) => {
                            let result = wire::query_result(Source(frame.bytes()), &r.result);
                            if sink.send(Ok(result)).await.is_err() {
                                return Ok(());
                            }
                        }
                        Ok(p::Frame::QueryEnd(_)) => return Ok(()),
                        _ => {}
                    },
                }
            }
        }
    }
}

/// Moves a relayed subscription's events from the owning node to the feed,
/// and the subscriber's permits the other way. Ends with the first of: the
/// terminal event, the feed being dropped, the connection failing.
async fn pump_relay(
    mut stream: ClientStream,
    events: mpsc::Sender<Result<crate::hub::SubscriptionEvent, Fault>>,
    mut permits: mpsc::UnboundedReceiver<u64>,
) {
    loop {
        tokio::select! {
            _ = events.closed() => return,
            grant = permits.recv() => {
                let Some(mut grant) = grant else { return };
                while grant > 0 {
                    let chunk = grant.min(u32::MAX as u64);
                    grant -= chunk;
                    let permits = p::Frame::Permits(p::Permits { count: chunk as u32 });
                    if stream.send(&permits).await.is_err() {
                        return;
                    }
                }
            }
            frame = stream.next() => match frame {
                Ok(Some(frame)) => {
                    let Ok(p::Frame::SubscriptionQueryEvent(e)) = frame.decode() else {
                        continue;
                    };
                    let event = wire::subscription_event(Source(frame.bytes()), e.subscription_id, &e.event);
                    let terminal = event.is_terminal();
                    if events.send(Ok(event)).await.is_err() || terminal {
                        return;
                    }
                }
                Ok(None) => return,
                Err(e) => {
                    let _ = events.send(Err(net_fault(e))).await;
                    return;
                }
            },
        }
    }
}
