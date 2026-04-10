use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use kronosdb_messaging::api::MessagingPlatform;
use kronosdb_messaging::client::ClientRegistry;
use kronosdb_messaging::types::{ClientId, ComponentName};

use crate::processor::ProcessorRegistry;
use crate::proto::kronosdb::platform as pb;
use crate::proto::kronosdb::platform::platform_service_server::PlatformServiceServer as GrpcPlatformServiceServer;

/// Registry of outbound channels for connected platform clients.
///
/// Allows external code (e.g. graceful shutdown, cluster rebalancing)
/// to send instructions like `RequestReconnect` to specific clients
/// or broadcast to all connected clients.
pub struct ClientChannelRegistry {
    channels: RwLock<HashMap<String, mpsc::Sender<Result<pb::PlatformOutbound, Status>>>>,
}

impl ClientChannelRegistry {
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
        }
    }

    fn register(&self, client_id: &str, tx: mpsc::Sender<Result<pb::PlatformOutbound, Status>>) {
        self.channels.write().insert(client_id.to_string(), tx);
    }

    fn unregister(&self, client_id: &str) {
        self.channels.write().remove(client_id);
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
}

/// gRPC service for client connection lifecycle.
pub struct PlatformServiceImpl {
    client_registry: Arc<ClientRegistry>,
    channel_registry: Arc<ClientChannelRegistry>,
    processor_registry: Arc<ProcessorRegistry>,
    platform: Arc<dyn MessagingPlatform>,
    /// Context names, for inclusion in PlatformInfo.
    context_names: Arc<dyn Fn() -> Vec<String> + Send + Sync>,
    node_name: String,
    heartbeat_interval: Duration,
    heartbeat_timeout: Duration,
}

impl PlatformServiceImpl {
    pub fn new(
        client_registry: Arc<ClientRegistry>,
        channel_registry: Arc<ClientChannelRegistry>,
        processor_registry: Arc<ProcessorRegistry>,
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

impl ClientChannelRegistry {
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
        let (outbound_tx, outbound_rx) = mpsc::channel::<Result<pb::PlatformOutbound, Status>>(32);

        let client_registry = Arc::clone(&self.client_registry);
        let channel_registry = Arc::clone(&self.channel_registry);
        let processor_registry = Arc::clone(&self.processor_registry);
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

                        // Register (or re-register) the client.
                        client_registry.register(
                            ClientId(id.client_id.clone()),
                            ComponentName(id.component_name),
                            id.version,
                            id.tags,
                        );
                        client_registry.set_stream_active(&ClientId(id.client_id.clone()), true);

                        // Register the outbound channel for RequestReconnect etc.
                        channel_registry.register(&id.client_id, outbound_tx.clone());

                        // Send node info.
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
                        break; // Outbound channel closed.
                    }
                }
            });

            // Look up component name for processor registry reports.
            let component_name = client_registry.get_component_name(&cid).unwrap_or_default();

            // Process inbound messages.
            loop {
                match tokio::time::timeout(heartbeat_timeout, inbound.message()).await {
                    Ok(Ok(Some(msg))) => match msg.request {
                        Some(pb::platform_inbound::Request::Heartbeat(_)) => {
                            client_registry.heartbeat(&cid);
                        }
                        Some(pb::platform_inbound::Request::Ack(_ack)) => {
                            // Instruction acknowledgement — currently no-op.
                        }
                        Some(pb::platform_inbound::Request::Register(_)) => {
                            // Re-register on an existing stream — just update heartbeat.
                            client_registry.heartbeat(&cid);
                        }
                        Some(pb::platform_inbound::Request::EventProcessorInfo(info)) => {
                            processor_registry.report(&client_id, &component_name, &info);
                        }
                        Some(pb::platform_inbound::Request::Result(_result)) => {
                            // Instruction result — currently no-op (logged for future use).
                        }
                        None => {}
                    },
                    Ok(Ok(None)) => break, // Stream closed cleanly.
                    Ok(Err(_)) => break,   // Stream error.
                    Err(_) => break,       // Heartbeat timeout — client is dead.
                }
            }

            // Cleanup.
            heartbeat_handle.abort();
            channel_registry.unregister(&client_id);
            processor_registry.remove_client(&client_id);
            client_registry.set_stream_active(&cid, false);
            client_registry.unregister(&cid);
            platform.remove_client(&cid);
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
