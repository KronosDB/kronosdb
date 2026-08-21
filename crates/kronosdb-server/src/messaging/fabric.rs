//! Messaging fabric (ADR-0007 Tier 2): location-aware dispatch over the
//! Raft-replicated handler routing table, plus the internode forwarding
//! service.
//!
//! Selection happens on the dispatching node — the ring and weighted
//! round-robin run against the replicated table, which every node holds
//! identically, so any node resolves the same owner for the same key.
//! The forward then names the chosen handler explicitly; the owning node
//! only acquires the permit and delivers.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use kronosdb_eventstore::raft::cluster::ClusterManager;
use kronosdb_eventstore::raft::handler_registry::{HandlerKind, RegisteredHandler};
use kronosdb_messaging::manager::MessagingManager;
use kronosdb_messaging::ring::Ring;
use kronosdb_messaging::types::ClientId;

use super::command_service::{
    from_proto_command, to_proto_command_inbound, to_proto_command_response,
};
use super::query_service::from_proto_query;
use crate::proto::kronosdb::command as command_pb;
use crate::proto::kronosdb::fabric as pb;
use crate::proto::kronosdb::query as query_pb;

/// Outbound delivery channels for locally-connected command handlers,
/// shared between `CommandServiceImpl` (which registers them) and the
/// fabric service (which delivers forwarded commands on them).
pub type CommandHandlerStreams =
    Arc<DashMap<String, mpsc::Sender<Result<command_pb::CommandHandlerInbound, Status>>>>;

/// Outbound delivery channels for locally-connected query handlers,
/// shared between `QueryServiceImpl` and the fabric service.
pub type QueryHandlerStreams =
    Arc<DashMap<String, mpsc::Sender<Result<query_pb::QueryHandlerInbound, Status>>>>;

/// A pending query's response collector. Handler responses arriving on
/// query-handler streams route here by request id; the entry retires when
/// every targeted handler has sent QueryComplete (counted, so a fast
/// handler's completion no longer drops a slower handler's responses).
pub struct PendingQueryEntry {
    pub sender: mpsc::Sender<query_pb::QueryResponse>,
    pub remaining: AtomicUsize,
}

/// request_id → pending entry, shared between `QueryServiceImpl` (local
/// queries + handler-response routing) and the fabric service (forwarded
/// queries).
pub type PendingQueries = Arc<DashMap<String, Arc<PendingQueryEntry>>>;

/// Where a query should fan out, per the replicated routing table.
pub struct QueryFanout {
    /// Attempt local dispatch (local handlers exist, or no replicated rows
    /// at all — the legacy/bootstrap fallback).
    pub local: bool,
    /// Remote deliveries: owning node → handler instances on that node.
    pub remote: Vec<(u64, Vec<String>)>,
}

/// Where a command should go, per the replicated routing table.
pub enum CommandRoute {
    /// No replicated rows for this (bus, type) — use the node-local path.
    /// Covers single-node bootstrap and registration-write lag.
    LocalFallback,
    /// Keyed command whose ring owner is connected to this node.
    Local { client_id: String },
    /// The selected handler is connected to a remote node.
    Remote { client_id: String, node_id: u64 },
    /// Unkeyed command with at least one local handler: let the local bus
    /// load-balance (locality-preferred).
    LocalAny,
}

struct CachedRing {
    generation: u64,
    ring: Arc<Ring>,
}

/// Routing decisions against the replicated handler table, with a
/// per-(bus, type) ring cache keyed by table generation.
pub struct FabricRouter {
    cluster: Arc<ClusterManager>,
    rings: RwLock<HashMap<(String, String), CachedRing>>,
    rr_counter: AtomicU64,
}

impl FabricRouter {
    pub fn new(cluster: Arc<ClusterManager>) -> Self {
        Self {
            cluster,
            rings: RwLock::new(HashMap::new()),
            rr_counter: AtomicU64::new(0),
        }
    }

    pub fn route_command(
        &self,
        bus: &str,
        message_type: &str,
        routing_key: Option<&str>,
    ) -> CommandRoute {
        let table = self.cluster.handler_routing();
        let generation = table.generation();
        let rows = table.lookup(bus, HandlerKind::Command, message_type);
        if rows.is_empty() {
            return CommandRoute::LocalFallback;
        }
        let local = self.cluster.local_node_id();

        match routing_key {
            Some(key) => {
                let ring = self.ring_for(bus, message_type, generation, &rows);
                let idx = ring.lookup(key).unwrap_or(0);
                // Defensive clamp: a table mutation between the generation
                // read and the row fetch can leave the cached ring one
                // generation stale.
                let row = rows.get(idx).unwrap_or(&rows[0]);
                if row.node_id == local {
                    CommandRoute::Local {
                        client_id: row.client_id.clone(),
                    }
                } else {
                    CommandRoute::Remote {
                        client_id: row.client_id.clone(),
                        node_id: row.node_id,
                    }
                }
            }
            None => {
                if rows.iter().any(|r| r.node_id == local) {
                    return CommandRoute::LocalAny;
                }
                let chosen = self.weighted_pick(&rows);
                CommandRoute::Remote {
                    client_id: chosen.client_id.clone(),
                    node_id: chosen.node_id,
                }
            }
        }
    }

    /// Fan-out plan for a query. Scatter-gather goes to every node with
    /// registered handlers (local delivery for local rows); point-to-point
    /// is locality-preferred, falling back to a weighted pick over remote
    /// instances when no local handler exists.
    pub fn route_query(&self, bus: &str, message_type: &str, point_to_point: bool) -> QueryFanout {
        let table = self.cluster.handler_routing();
        let rows = table.lookup(bus, HandlerKind::Query, message_type);
        if rows.is_empty() {
            return QueryFanout {
                local: true,
                remote: Vec::new(),
            };
        }
        let local_node = self.cluster.local_node_id();
        let has_local = rows.iter().any(|r| r.node_id == local_node);

        if point_to_point {
            if has_local {
                return QueryFanout {
                    local: true,
                    remote: Vec::new(),
                };
            }
            let chosen = self.weighted_pick(&rows);
            return QueryFanout {
                local: false,
                remote: vec![(chosen.node_id, vec![chosen.client_id.clone()])],
            };
        }

        let mut remote: HashMap<u64, Vec<String>> = HashMap::new();
        for row in rows.iter().filter(|r| r.node_id != local_node) {
            remote
                .entry(row.node_id)
                .or_default()
                .push(row.client_id.clone());
        }
        let mut remote: Vec<(u64, Vec<String>)> = remote.into_iter().collect();
        remote.sort_by_key(|(node, _)| *node);
        QueryFanout {
            // Attempt local delivery even without local rows when remote is
            // empty is handled above; with remote present, only dispatch
            // locally when local handlers are known — a local NoHandler is
            // tolerated by the caller either way.
            local: has_local,
            remote,
        }
    }

    fn weighted_pick<'a>(&self, rows: &'a [RegisteredHandler]) -> &'a RegisteredHandler {
        let total: u64 = rows.iter().map(|r| r.load_factor.max(1) as u64).sum();
        let target = self.rr_counter.fetch_add(1, Ordering::Relaxed) % total.max(1);
        let mut cumulative = 0u64;
        for row in rows {
            cumulative += row.load_factor.max(1) as u64;
            if target < cumulative {
                return row;
            }
        }
        &rows[0]
    }

    /// Ring over the sorted global handler rows. Rows come pre-sorted by
    /// client_id from the table, and the hash is deterministic across
    /// builds, so every node derives the same ring at the same generation.
    fn ring_for(
        &self,
        bus: &str,
        message_type: &str,
        generation: u64,
        rows: &[RegisteredHandler],
    ) -> Arc<Ring> {
        let key = (bus.to_string(), message_type.to_string());
        {
            let rings = self.rings.read();
            if let Some(cached) = rings.get(&key)
                && cached.generation == generation
            {
                return Arc::clone(&cached.ring);
            }
        }
        let ring = Arc::new(Ring::build(
            rows.iter().map(|r| (r.client_id.as_str(), r.load_factor)),
        ));
        self.rings.write().insert(
            key,
            CachedRing {
                generation,
                ring: Arc::clone(&ring),
            },
        );
        ring
    }
}

/// Forwards a command to the node its selected handler is connected to.
pub async fn forward_command(
    cluster: &ClusterManager,
    node_id: u64,
    bus: &str,
    target_client_id: String,
    command: command_pb::Command,
    timeout: Duration,
) -> Result<command_pb::CommandResponse, Status> {
    let address = cluster
        .peer_address(node_id)
        .ok_or_else(|| Status::unavailable(format!("no address for node {node_id}")))?;
    let channel = cluster
        .peer_channel(&address)
        .await
        .map_err(|e| Status::unavailable(format!("fabric peer connect: {e}")))?;
    let request = cluster
        .peer_request(pb::ForwardCommandRequest {
            bus: bus.to_string(),
            target_client_id,
            command: Some(command),
            timeout_ms: timeout.as_millis() as u64,
        })
        .map_err(|e| Status::internal(e.to_string()))?;
    let mut client = pb::messaging_fabric_client::MessagingFabricClient::new(channel);
    Ok(client.forward_command(request).await?.into_inner())
}

/// Forwards a query to a node holding handler instances for it; returns
/// the response stream from that node.
pub async fn forward_query(
    cluster: &ClusterManager,
    node_id: u64,
    bus: &str,
    target_client_ids: Vec<String>,
    query: query_pb::QueryRequest,
    timeout: Duration,
) -> Result<tonic::Streaming<query_pb::QueryResponse>, Status> {
    let address = cluster
        .peer_address(node_id)
        .ok_or_else(|| Status::unavailable(format!("no address for node {node_id}")))?;
    let channel = cluster
        .peer_channel(&address)
        .await
        .map_err(|e| Status::unavailable(format!("fabric peer connect: {e}")))?;
    let request = cluster
        .peer_request(pb::ForwardQueryRequest {
            bus: bus.to_string(),
            target_client_ids,
            query: Some(query),
            timeout_ms: timeout.as_millis() as u64,
        })
        .map_err(|e| Status::internal(e.to_string()))?;
    let mut client = pb::messaging_fabric_client::MessagingFabricClient::new(channel);
    Ok(client.forward_query(request).await?.into_inner())
}

/// Receiving side of the fabric: delivers forwarded commands and queries
/// to handlers connected to this node.
pub struct FabricServiceImpl {
    messaging: Arc<MessagingManager>,
    handler_streams: CommandHandlerStreams,
    query_handler_streams: QueryHandlerStreams,
    pending_queries: PendingQueries,
}

impl FabricServiceImpl {
    pub fn new(
        messaging: Arc<MessagingManager>,
        handler_streams: CommandHandlerStreams,
        query_handler_streams: QueryHandlerStreams,
        pending_queries: PendingQueries,
    ) -> Self {
        Self {
            messaging,
            handler_streams,
            query_handler_streams,
            pending_queries,
        }
    }
}

#[tonic::async_trait]
impl pb::messaging_fabric_server::MessagingFabric for FabricServiceImpl {
    async fn forward_command(
        &self,
        request: Request<pb::ForwardCommandRequest>,
    ) -> Result<Response<command_pb::CommandResponse>, Status> {
        let req = request.into_inner();
        let cmd = req
            .command
            .ok_or_else(|| Status::invalid_argument("missing command"))?;
        let command = from_proto_command(cmd);
        let message_id = command.message_id.clone();
        let budget = Duration::from_millis(req.timeout_ms.max(1));
        let started = tokio::time::Instant::now();

        let platform = self.messaging.get_platform(&req.bus);
        let (pending_cmd, response_rx) = platform
            .dispatch_command_to_wait(command, ClientId(req.target_client_id), budget)
            .await
            .map_err(|e| Status::unavailable(e.to_string()))?;

        let handler_tx = self
            .handler_streams
            .get(&pending_cmd.target_handler.0)
            .map(|r| r.value().clone())
            .ok_or_else(|| {
                platform.cancel_in_flight_command(&message_id);
                Status::unavailable("handler stream not found on owning node")
            })?;

        let inbound = to_proto_command_inbound(&pending_cmd.command);
        if handler_tx.send(Ok(inbound)).await.is_err() {
            platform.cancel_in_flight_command(&message_id);
            return Err(Status::unavailable("handler disconnected"));
        }

        let remaining = budget.saturating_sub(started.elapsed());
        match tokio::time::timeout(remaining, response_rx).await {
            Ok(Ok(result)) => Ok(Response::new(to_proto_command_response(result))),
            Ok(Err(_)) => Err(Status::unavailable(
                "handler disconnected before responding",
            )),
            Err(_) => {
                platform.cancel_in_flight_command(&message_id);
                Err(Status::deadline_exceeded("forwarded command timed out"))
            }
        }
    }

    type ForwardQueryStream = ReceiverStream<Result<query_pb::QueryResponse, Status>>;

    async fn forward_query(
        &self,
        request: Request<pb::ForwardQueryRequest>,
    ) -> Result<Response<Self::ForwardQueryStream>, Status> {
        let req = request.into_inner();
        let query_proto = req
            .query
            .ok_or_else(|| Status::invalid_argument("missing query"))?;
        let message_id = query_proto.message_identifier.clone();
        let budget = Duration::from_millis(req.timeout_ms.max(1));
        let deadline = tokio::time::Instant::now() + budget;

        let platform = self.messaging.get_platform(&req.bus);
        let query = from_proto_query(query_proto.clone());
        let targets: Vec<ClientId> = req.target_client_ids.into_iter().map(ClientId).collect();
        let pending = platform
            .dispatch_query_to(query, &targets)
            .map_err(|e| Status::unavailable(e.to_string()))?;

        // Register the response collector BEFORE delivery so no handler
        // response can race past it. Retired when every accepted handler
        // completes, or at the deadline.
        let (response_tx, mut response_rx) = mpsc::channel::<query_pb::QueryResponse>(64);
        self.pending_queries.insert(
            message_id.clone(),
            Arc::new(PendingQueryEntry {
                sender: response_tx,
                remaining: AtomicUsize::new(pending.target_handlers.len()),
            }),
        );

        // Deliver to each accepted local handler; the original proto is the
        // wire-exact QueryRequest the handler expects.
        for target in &pending.target_handlers {
            let handler_tx = self
                .query_handler_streams
                .get(&target.0)
                .map(|r| r.value().clone());
            if let Some(tx) = handler_tx {
                let inbound = query_pb::QueryHandlerInbound {
                    request: Some(query_pb::query_handler_inbound::Request::Query(
                        query_proto.clone(),
                    )),
                    instruction_id: String::new(),
                };
                let _ = tx.send(Ok(inbound)).await;
            }
        }

        // Relay collected responses onto the fabric stream until every
        // handler completed (entry removed → channel closes) or deadline.
        let (out_tx, out_rx) = mpsc::channel::<Result<query_pb::QueryResponse, Status>>(64);
        let pending_queries = Arc::clone(&self.pending_queries);
        tokio::spawn(async move {
            loop {
                match tokio::time::timeout_at(deadline, response_rx.recv()).await {
                    Ok(Some(resp)) => {
                        if out_tx.send(Ok(resp)).await.is_err() {
                            break; // Dispatching node hung up.
                        }
                    }
                    Ok(None) => break, // All handlers completed.
                    Err(_) => break,   // Deadline.
                }
            }
            pending_queries.remove(&message_id);
        });

        Ok(Response::new(ReceiverStream::new(out_rx)))
    }
}
