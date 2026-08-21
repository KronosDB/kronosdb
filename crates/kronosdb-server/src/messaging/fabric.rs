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
use std::sync::atomic::{AtomicU64, Ordering};
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
use crate::proto::kronosdb::command as command_pb;
use crate::proto::kronosdb::fabric as pb;
use crate::proto::kronosdb::query as query_pb;

/// Outbound delivery channels for locally-connected command handlers,
/// shared between `CommandServiceImpl` (which registers them) and the
/// fabric service (which delivers forwarded commands on them).
pub type CommandHandlerStreams =
    Arc<DashMap<String, mpsc::Sender<Result<command_pb::CommandHandlerInbound, Status>>>>;

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

/// Receiving side of the fabric: delivers forwarded commands to handlers
/// connected to this node.
pub struct FabricServiceImpl {
    messaging: Arc<MessagingManager>,
    handler_streams: CommandHandlerStreams,
}

impl FabricServiceImpl {
    pub fn new(messaging: Arc<MessagingManager>, handler_streams: CommandHandlerStreams) -> Self {
        Self {
            messaging,
            handler_streams,
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
        _request: Request<pb::ForwardQueryRequest>,
    ) -> Result<Response<Self::ForwardQueryStream>, Status> {
        // Query fan-out lands with the next fabric stage (ADR-0007).
        Err(Status::unimplemented(
            "fabric query forwarding not yet implemented",
        ))
    }
}
