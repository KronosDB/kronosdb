use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::RwLock;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use kronosdb_messaging::api::MessagingPlatform;
use kronosdb_messaging::client::ClientRegistry;
use kronosdb_messaging::manager::MessagingManager;
use kronosdb_messaging::types::{ClientId, ComponentName};

use crate::handler_registry::HandlerStreamRegistry;
use crate::processor::ProcessorRegistry;
use crate::proto::kronosdb::platform as pb;
use crate::proto::kronosdb::platform::platform_service_server::PlatformServiceServer as GrpcPlatformServiceServer;

/// Timeout for unacknowledged instructions.
const INSTRUCTION_ACK_TIMEOUT: Duration = Duration::from_secs(10);

/// A pending instruction awaiting acknowledgement from a client.
struct PendingInstruction {
    sent_at: Instant,
    ack_tx: oneshot::Sender<InstructionAckResult>,
}

/// Result of an instruction ack.
pub struct InstructionAckResult {
    pub success: bool,
    pub error: Option<String>,
}

/// Registry of outbound channels for connected platform clients.
///
/// Allows external code (e.g. graceful shutdown, cluster rebalancing)
/// to send instructions like `RequestReconnect` to specific clients
/// or broadcast to all connected clients.
pub struct ClientChannelRegistry {
    channels: RwLock<HashMap<String, mpsc::Sender<Result<pb::PlatformOutbound, Status>>>>,
    pending_instructions: RwLock<HashMap<String, PendingInstruction>>,
    instruction_counter: std::sync::atomic::AtomicU64,
}

impl ClientChannelRegistry {
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
            pending_instructions: RwLock::new(HashMap::new()),
            instruction_counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn register(&self, client_id: &str, tx: mpsc::Sender<Result<pb::PlatformOutbound, Status>>) {
        self.channels.write().insert(client_id.to_string(), tx);
    }

    fn unregister(&self, client_id: &str) {
        self.channels.write().remove(client_id);
    }

    fn next_instruction_id(&self) -> String {
        let id = self
            .instruction_counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        format!("instr-{id}")
    }

    /// Resolves a pending instruction ack. Returns true if found.
    fn resolve_ack(&self, instruction_id: &str, success: bool, error: Option<String>) -> bool {
        let pending = self.pending_instructions.write().remove(instruction_id);
        if let Some(entry) = pending {
            let _ = entry.ack_tx.send(InstructionAckResult { success, error });
            true
        } else {
            false
        }
    }

    /// Drops senders whose deadline has passed; the caller observes RecvError.
    fn sweep_timed_out_instructions(&self) {
        let mut pending = self.pending_instructions.write();
        pending.retain(|_id, entry| entry.sent_at.elapsed() <= INSTRUCTION_ACK_TIMEOUT);
    }

    /// Sends a `RequestReconnect` to a specific client. Returns true if sent.
    pub async fn request_reconnect(&self, client_id: &str) -> bool {
        let tx = {
            let channels = self.channels.read();
            channels.get(client_id).cloned()
        };
        if let Some(tx) = tx {
            let msg = pb::PlatformOutbound {
                request: Some(pb::platform_outbound::Request::RequestReconnect(
                    pb::RequestReconnect {},
                )),
                instruction_id: String::new(),
            };
            tx.send(Ok(msg)).await.is_ok()
        } else {
            false
        }
    }

    /// Sends `RequestReconnect` to all connected clients (e.g. graceful shutdown).
    pub async fn request_reconnect_all(&self) {
        let senders: Vec<_> = {
            let channels = self.channels.read();
            channels.values().cloned().collect()
        };
        for tx in senders {
            let msg = pb::PlatformOutbound {
                request: Some(pb::platform_outbound::Request::RequestReconnect(
                    pb::RequestReconnect {},
                )),
                instruction_id: String::new(),
            };
            let _ = tx.send(Ok(msg)).await;
        }
    }

    /// Sends a processor instruction to a specific client.
    /// Returns true if the message was sent.
    pub async fn send_instruction(
        &self,
        client_id: &str,
        instruction_id: String,
        request: pb::platform_outbound::Request,
    ) -> bool {
        let tx = {
            let channels = self.channels.read();
            channels.get(client_id).cloned()
        };
        if let Some(tx) = tx {
            let msg = pb::PlatformOutbound {
                request: Some(request),
                instruction_id,
            };
            tx.send(Ok(msg)).await.is_ok()
        } else {
            false
        }
    }

    /// Sends a processor control instruction that requires an ack.
    /// Returns a receiver for the ack result, or None if the client is not connected.
    /// The instruction will time out after `INSTRUCTION_ACK_TIMEOUT`.
    pub async fn send_tracked_instruction(
        &self,
        client_id: &str,
        request: pb::platform_outbound::Request,
    ) -> Option<oneshot::Receiver<InstructionAckResult>> {
        let instruction_id = self.next_instruction_id();
        let (ack_tx, ack_rx) = oneshot::channel();

        // Register pending before sending so an immediate ack can't race us.
        {
            let mut pending = self.pending_instructions.write();
            pending.insert(
                instruction_id.clone(),
                PendingInstruction {
                    sent_at: Instant::now(),
                    ack_tx,
                },
            );
        }

        let sent = self
            .send_instruction(client_id, instruction_id.clone(), request)
            .await;
        if !sent {
            self.pending_instructions.write().remove(&instruction_id);
            return None;
        }

        Some(ack_rx)
    }

    /// Broadcasts a topology notification to all connected platform clients.
    pub async fn broadcast_topology_notification(&self, notification: pb::TopologyNotification) {
        let senders: Vec<_> = {
            let channels = self.channels.read();
            channels.values().cloned().collect()
        };
        for tx in senders {
            let msg = pb::PlatformOutbound {
                request: Some(pb::platform_outbound::Request::TopologyNotification(
                    notification.clone(),
                )),
                instruction_id: String::new(),
            };
            let _ = tx.send(Ok(msg)).await;
        }
    }
}

/// gRPC service for client connection lifecycle.
pub struct PlatformServiceImpl {
    client_registry: Arc<ClientRegistry>,
    channel_registry: Arc<ClientChannelRegistry>,
    processor_registry: Arc<ProcessorRegistry>,
    messaging: Arc<MessagingManager>,
    handler_stream_registry: Arc<HandlerStreamRegistry>,
    platform: Arc<dyn MessagingPlatform>,
    context_names: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
    node_name: String,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
}

impl PlatformServiceImpl {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        client_registry: Arc<ClientRegistry>,
        channel_registry: Arc<ClientChannelRegistry>,
        processor_registry: Arc<ProcessorRegistry>,
        messaging: Arc<MessagingManager>,
        handler_stream_registry: Arc<HandlerStreamRegistry>,
        platform: Arc<dyn MessagingPlatform>,
        context_names: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
        node_name: String,
        heartbeat_interval: Duration,
        heartbeat_timeout: Duration,
    ) -> Self {
        Self {
            client_registry,
            channel_registry,
            processor_registry,
            messaging,
            handler_stream_registry,
            platform,
            context_names,
            node_name,
            heartbeat_interval,
            heartbeat_timeout,
        }
    }

    pub fn into_server(self) -> GrpcPlatformServiceServer<Self> {
        GrpcPlatformServiceServer::new(self)
    }

    fn make_platform_info(&self) -> pb::PlatformInfo {
        pb::PlatformInfo {
            node_name: self.node_name.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            contexts: (self.context_names)(),
        }
    }
}

#[tonic::async_trait]
impl pb::platform_service_server::PlatformService for PlatformServiceImpl {
    type OpenStreamStream = ReceiverStream<Result<pb::PlatformOutbound, Status>>;

    async fn get_platform_server(
        &self,
        request: Request<pb::ClientIdentification>,
    ) -> Result<Response<pb::PlatformInfo>, Status> {
        let id = request.into_inner();

        if id.client_id.is_empty() {
            return Err(Status::invalid_argument("client_id is required"));
        }
        if id.component_name.is_empty() {
            return Err(Status::invalid_argument("component_name is required"));
        }

        self.client_registry.register(
            ClientId(id.client_id.clone()),
            ComponentName(id.component_name),
            id.version,
            id.tags,
        );

        Ok(Response::new(self.make_platform_info()))
    }

    async fn open_stream(
        &self,
        request: Request<Streaming<pb::PlatformInbound>>,
    ) -> Result<Response<Self::OpenStreamStream>, Status> {
        let mut inbound = request.into_inner();
        let (outbound_tx, outbound_rx) = mpsc::channel::<Result<pb::PlatformOutbound, Status>>(256);

        let client_registry = Arc::clone(&self.client_registry);
        let channel_registry = Arc::clone(&self.channel_registry);
        let processor_registry = Arc::clone(&self.processor_registry);
        let messaging = Arc::clone(&self.messaging);
        let handler_stream_registry = Arc::clone(&self.handler_stream_registry);
        let platform = Arc::clone(&self.platform);
        let platform_info = self.make_platform_info();
        let heartbeat_interval = self.heartbeat_interval;
        let heartbeat_timeout = self.heartbeat_timeout;

        tokio::spawn(async move {
            // Wait for the first message — must be a register.
            let client_id = match inbound.message().await {
                Ok(Some(msg)) => match msg.request {
                    Some(pb::platform_inbound::Request::Register(id)) => {
                        if id.client_id.is_empty() {
                            let _ = outbound_tx
                                .send(Err(Status::invalid_argument("client_id is required")))
                                .await;
                            return;
                        }

                        client_registry.register(
                            ClientId(id.client_id.clone()),
                            ComponentName(id.component_name),
                            id.version,
                            id.tags,
                        );
                        client_registry.set_stream_active(&ClientId(id.client_id.clone()), true);

                        channel_registry.register(&id.client_id, outbound_tx.clone());

                        let _ = outbound_tx
                            .send(Ok(pb::PlatformOutbound {
                                request: Some(pb::platform_outbound::Request::NodeNotification(
                                    platform_info,
                                )),
                                instruction_id: String::new(),
                            }))
                            .await;

                        id.client_id
                    }
                    _ => {
                        let _ = outbound_tx
                            .send(Err(Status::invalid_argument(
                                "first message must be a register",
                            )))
                            .await;
                        return;
                    }
                },
                _ => return,
            };

            let cid = ClientId(client_id.clone());

            // Spawn heartbeat sender.
            let heartbeat_tx = outbound_tx.clone();
            let heartbeat_handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(heartbeat_interval);
                loop {
                    interval.tick().await;
                    let msg = pb::PlatformOutbound {
                        request: Some(pb::platform_outbound::Request::Heartbeat(pb::Heartbeat {})),
                        instruction_id: String::new(),
                    };
                    if heartbeat_tx.send(Ok(msg)).await.is_err() {
                        break;
                    }
                }
            });

            // Spawn instruction ack timeout sweeper.
            let sweep_registry = Arc::clone(&channel_registry);
            let sweep_handle = tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5));
                loop {
                    interval.tick().await;
                    sweep_registry.sweep_timed_out_instructions();
                }
            });

            let component_name = client_registry.get_component_name(&cid).unwrap_or_default();

            loop {
                match tokio::time::timeout(heartbeat_timeout, inbound.message()).await {
                    Ok(Ok(Some(msg))) => match msg.request {
                        Some(pb::platform_inbound::Request::Heartbeat(_)) => {
                            client_registry.heartbeat(&cid);
                        }
                        Some(pb::platform_inbound::Request::Ack(ack)) => {
                            let error_msg = ack.error.map(|e| e.message);
                            channel_registry.resolve_ack(
                                &ack.instruction_id,
                                ack.success,
                                error_msg,
                            );
                        }
                        Some(pb::platform_inbound::Request::Register(_)) => {
                            client_registry.heartbeat(&cid);
                        }
                        Some(pb::platform_inbound::Request::EventProcessorInfo(info)) => {
                            processor_registry.report(&client_id, &component_name, &info);
                        }
                        Some(pb::platform_inbound::Request::Result(result)) => {
                            let error_msg = result.error.map(|e| e.message);
                            channel_registry.resolve_ack(
                                &result.instruction_id,
                                result.success,
                                error_msg,
                            );
                        }
                        None => {}
                    },
                    Ok(Ok(None)) => {
                        tracing::info!(client_id = %client_id, "platform stream: client closed cleanly");
                        break;
                    }
                    Ok(Err(e)) => {
                        tracing::warn!(client_id = %client_id, error = %e, "platform stream: transport error");
                        break;
                    }
                    Err(_) => {
                        tracing::warn!(
                            client_id = %client_id,
                            timeout_secs = ?heartbeat_timeout,
                            "platform stream: heartbeat timeout — cascading disconnect"
                        );
                        break;
                    }
                }
            }

            // Cascade disconnect to all handler streams: cancels server-side
            // handler tasks and surfaces CANCELLED to clients so they reconnect.
            heartbeat_handle.abort();
            sweep_handle.abort();
            handler_stream_registry.close_client_streams(&client_id);

            channel_registry.unregister(&client_id);
            processor_registry.remove_client(&client_id);
            client_registry.set_stream_active(&cid, false);
            client_registry.unregister(&cid);

            platform.remove_client(&cid);
            for context in messaging.list_contexts() {
                let ctx_platform = messaging.get_platform(&context);
                ctx_platform.remove_client(&cid);
            }
        });

        Ok(Response::new(ReceiverStream::new(outbound_rx)))
    }
}

/// Spawns a background task that periodically reaps dead clients from the
/// registry and cleans up their messaging subscriptions.
pub fn spawn_reaper(
    client_registry: Arc<ClientRegistry>,
    platform: Arc<dyn MessagingPlatform>,
    timeout: Duration,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(timeout);
        loop {
            interval.tick().await;
            let dead = client_registry.reap_dead_clients(timeout);
            for cid in dead {
                platform.remove_client(&cid);
            }
        }
    })
}
