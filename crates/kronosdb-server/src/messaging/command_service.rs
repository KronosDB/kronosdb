use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use kronosdb_eventstore::raft::cluster::ClusterManager;
use kronosdb_eventstore::raft::handler_registry::{HandlerKind, HandlerRegistration};
use kronosdb_messaging::command::{Command, CommandResult};
use kronosdb_messaging::manager::MessagingManager;
use kronosdb_messaging::types::{ClientId, ComponentName, Payload, RoutingKey};

use super::convert::{
    effective_timeout, internal_metadata_to_proto, internal_pi_to_proto,
    proto_metadata_to_internal, proto_pi_to_internal,
};
use super::fabric::{CommandHandlerStreams, CommandRoute, FabricRouter, forward_command};
use crate::handler_registry::HandlerStreamRegistry;
use crate::platform::service::ClientChannelRegistry;
use crate::proto::kronosdb::command as pb;
use crate::proto::kronosdb::platform as platform_pb;

/// gRPC service implementation for the command bus.
///
/// Routes handlers to named messaging buses via the `kronosdb-bus` header.
/// `handler_streams` is sharded so concurrent dispatches don't contend; the
/// caller-side response channel lives in the messaging engine's in-flight map
/// rather than a parallel gRPC-side map. Dispatch is location-aware
/// (ADR-0007): handlers on remote nodes are reached through the fabric.
pub struct CommandServiceImpl {
    messaging: Arc<MessagingManager>,
    handler_streams: CommandHandlerStreams,
    command_timeout: Duration,
    channel_registry: Arc<ClientChannelRegistry>,
    handler_stream_registry: Arc<HandlerStreamRegistry>,
    cluster: Arc<ClusterManager>,
    fabric: Arc<FabricRouter>,
}

impl CommandServiceImpl {
    pub fn new(
        messaging: Arc<MessagingManager>,
        command_timeout: Duration,
        channel_registry: Arc<ClientChannelRegistry>,
        handler_stream_registry: Arc<HandlerStreamRegistry>,
        handler_streams: CommandHandlerStreams,
        cluster: Arc<ClusterManager>,
        fabric: Arc<FabricRouter>,
    ) -> Self {
        Self {
            messaging,
            handler_streams,
            command_timeout,
            channel_registry,
            handler_stream_registry,
            cluster,
            fabric,
        }
    }
}

#[tonic::async_trait]
impl pb::command_service_server::CommandService for CommandServiceImpl {
    type OpenStreamStream = ReceiverStream<Result<pb::CommandHandlerInbound, Status>>;

    async fn open_stream(
        &self,
        request: Request<Streaming<pb::CommandHandlerOutbound>>,
    ) -> Result<Response<Self::OpenStreamStream>, Status> {
        let bus = super::bus_from_metadata(request.metadata());
        tracing::info!(bus = %bus, "Command OpenStream opened");
        let platform = self.messaging.get_platform(&bus);

        let mut inbound = request.into_inner();
        // 4096 absorbs dispatch bursts without blocking the send.await path.
        let (handler_tx, handler_rx) =
            mpsc::channel::<Result<pb::CommandHandlerInbound, Status>>(4096);

        let handler_streams = Arc::clone(&self.handler_streams);
        let channel_registry = Arc::clone(&self.channel_registry);
        let handler_stream_registry = Arc::clone(&self.handler_stream_registry);
        let cluster = Arc::clone(&self.cluster);
        let reg_bus = bus.clone();
        let mut client_id: Option<String> = None;

        tokio::spawn(async move {
            let mut cancel_token: Option<tokio_util::sync::CancellationToken> = None;
            let mut subscribed_commands: Vec<String> = Vec::new();

            loop {
                let msg_result = if let Some(ref token) = cancel_token {
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => {
                            tracing::info!(client_id = ?client_id, "command handler: cancelled by platform disconnect");
                            break;
                        }
                        result = inbound.message() => result,
                    }
                } else {
                    inbound.message().await
                };

                let msg = match msg_result {
                    Ok(Some(msg)) => msg,
                    Ok(None) => {
                        tracing::info!(client_id = ?client_id, "command handler stream: client closed send-side");
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(client_id = ?client_id, error = %e, "command handler stream: transport error");
                        break;
                    }
                };

                let instruction_id = if msg.instruction_id.is_empty() {
                    None
                } else {
                    Some(msg.instruction_id.clone())
                };

                match msg.request {
                    Some(pb::command_handler_outbound::Request::Subscribe(sub)) => {
                        tracing::info!(
                            command = %sub.command,
                            client_id = %sub.client_id,
                            component = %sub.component_name,
                            "Command handler subscribing"
                        );
                        let command_name = sub.command.clone();
                        let sub_client_id = sub.client_id.clone();
                        let sub_component = sub.component_name.clone();
                        client_id = Some(sub.client_id.clone());
                        subscribed_commands.push(sub.command.clone());

                        if cancel_token.is_none() {
                            cancel_token = Some(
                                handler_stream_registry.get_cancellation_token(&sub.client_id),
                            );
                        }

                        handler_streams.insert(sub.client_id.clone(), handler_tx.clone());
                        handler_stream_registry.register(&sub.client_id, handler_tx.clone());

                        platform.subscribe_command(
                            sub.command,
                            ClientId(sub.client_id),
                            ComponentName(sub.component_name),
                            sub.load_factor,
                        );

                        // Publish to the replicated routing table so every
                        // node can route to this handler (ADR-0007). Written
                        // off the stream loop; dispatch falls back to the
                        // local path until it lands.
                        let registration = HandlerRegistration {
                            bus: reg_bus.clone(),
                            kind: HandlerKind::Command,
                            message_type: command_name.clone(),
                            client_id: sub_client_id.clone(),
                            node_id: cluster.local_node_id(),
                            load_factor: sub.load_factor,
                        };
                        let reg_cluster = Arc::clone(&cluster);
                        tokio::spawn(async move {
                            if let Err(e) = reg_cluster.register_handler(registration).await {
                                tracing::warn!(error = %e, "fabric: handler registration write failed");
                            }
                        });

                        channel_registry
                            .broadcast_topology_notification(platform_pb::TopologyNotification {
                                change_type: "handler_registered".to_string(),
                                message_type: command_name,
                                handler_kind: "command".to_string(),
                                client_id: sub_client_id,
                                component_name: sub_component,
                            })
                            .await;

                        if let Some(id) = instruction_id {
                            let ack = pb::CommandHandlerInbound {
                                request: Some(pb::command_handler_inbound::Request::Ack(
                                    crate::proto::kronosdb::InstructionAck {
                                        instruction_id: id,
                                        success: true,
                                        error: None,
                                    },
                                )),
                                instruction_id: String::new(),
                            };
                            let _ = handler_tx.send(Ok(ack)).await;
                        }
                    }
                    Some(pb::command_handler_outbound::Request::Unsubscribe(sub)) => {
                        let command_name = sub.command.clone();
                        let unsub_client_id = sub.client_id.clone();
                        platform.unsubscribe_command(&sub.command, &ClientId(sub.client_id));

                        let dereg_cluster = Arc::clone(&cluster);
                        let dereg_bus = reg_bus.clone();
                        let dereg_type = command_name.clone();
                        let dereg_client = unsub_client_id.clone();
                        tokio::spawn(async move {
                            if let Err(e) = dereg_cluster
                                .deregister_handler(
                                    &dereg_bus,
                                    HandlerKind::Command,
                                    &dereg_type,
                                    &dereg_client,
                                )
                                .await
                            {
                                tracing::warn!(error = %e, "fabric: handler deregistration write failed");
                            }
                        });

                        channel_registry
                            .broadcast_topology_notification(platform_pb::TopologyNotification {
                                change_type: "handler_deregistered".to_string(),
                                message_type: command_name,
                                handler_kind: "command".to_string(),
                                client_id: unsub_client_id,
                                component_name: String::new(),
                            })
                            .await;
                    }
                    Some(pb::command_handler_outbound::Request::FlowControl(fc)) => {
                        platform.grant_command_permits(&ClientId(fc.client_id), fc.permits);
                    }
                    Some(pb::command_handler_outbound::Request::CommandResponse(resp)) => {
                        let result = from_proto_command_response(resp);
                        let request_id = result.request_id.clone();
                        // The bus owns the response sender (in its in-flight map);
                        // complete_command extracts the entry, sends, and updates metrics.
                        platform.complete_command(&request_id, result);
                    }
                    Some(pb::command_handler_outbound::Request::Ack(_)) => {}
                    None => {}
                }
            }

            // Handler disconnected. Skip cleanup if the platform stream already
            // cascaded the disconnect — it's responsible for remove_client.
            let was_cancelled = cancel_token.as_ref().is_some_and(|t| t.is_cancelled());

            if let Some(cid) = client_id {
                handler_streams.remove(&cid);

                if !was_cancelled {
                    let client = ClientId(cid.clone());
                    // Removes all subscriptions and cancels in-flight commands
                    // (each receives a KRONOSDB-4006 failure on its caller).
                    let dereg_cluster = Arc::clone(&cluster);
                    let dereg_client = cid.clone();
                    tokio::spawn(async move {
                        if let Err(e) = dereg_cluster
                            .deregister_client_handlers(&dereg_client)
                            .await
                        {
                            tracing::warn!(error = %e, "fabric: client deregistration write failed");
                        }
                    });
                    let cancelled = platform.remove_command_client(&client);
                    tracing::info!(
                        client_id = %cid,
                        commands = subscribed_commands.len(),
                        cancelled_in_flight = cancelled.len(),
                        "command handler: unsubscribed and cancelled in-flight commands"
                    );

                    channel_registry
                        .broadcast_topology_notification(platform_pb::TopologyNotification {
                            change_type: "handler_deregistered".to_string(),
                            message_type: String::new(),
                            handler_kind: "command".to_string(),
                            client_id: cid.clone(),
                            component_name: String::new(),
                        })
                        .await;
                } else {
                    tracing::info!(
                        client_id = %cid,
                        "command handler: skipping cleanup (platform stream cascaded)"
                    );
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(handler_rx)))
    }

    async fn dispatch(
        &self,
        request: Request<pb::Command>,
    ) -> Result<Response<pb::CommandResponse>, Status> {
        let bus = super::bus_from_metadata(request.metadata());
        let platform = self.messaging.get_platform(&bus);

        let cmd = request.into_inner();
        let routing_key = routing_key_of(&cmd);
        let instructions = proto_pi_to_internal(cmd.processing_instructions.clone());
        let timeout = effective_timeout(&instructions, self.command_timeout);

        // Location-aware routing (ADR-0007): selection runs against the
        // replicated handler table. A remote owner means the command never
        // touches the local bus — it forwards to the owning node, which
        // acquires the permit and delivers.
        let route = self
            .fabric
            .route_command(&bus, &cmd.name, routing_key.as_deref());
        if let CommandRoute::Remote { client_id, node_id } = route {
            return forward_command(&self.cluster, node_id, &bus, client_id, cmd, timeout)
                .await
                .map(Response::new);
        }

        let command = from_proto_command(cmd);
        let message_id = command.message_id.clone();
        let dispatch_started = tokio::time::Instant::now();

        // Dispatch — selects a handler, acquires a permit, and registers the
        // in-flight tracking entry holding the response sender. When the
        // target handler is saturated this waits (bounded by the command
        // timeout) for a flow-control grant rather than failing immediately.
        let dispatched = match route {
            // Keyed command whose ring owner is local: deliver to exactly
            // that handler — the global ring decision, not local re-selection.
            CommandRoute::Local { client_id } => {
                platform
                    .dispatch_command_to_wait(command, ClientId(client_id), timeout)
                    .await
            }
            _ => platform.dispatch_command_wait(command, timeout).await,
        };
        let (pending_cmd, response_rx) =
            dispatched.map_err(|e| Status::unavailable(e.to_string()))?;

        // Find the selected handler's stream and deliver the command.
        let target_id = &pending_cmd.target_handler.0;
        let handler_tx = self
            .handler_streams
            .get(target_id)
            .map(|r| r.value().clone());

        let handler_tx = match handler_tx {
            Some(tx) => tx,
            None => {
                platform.cancel_in_flight_command(&message_id);
                return Err(Status::unavailable(format!(
                    "handler '{target_id}' stream not found"
                )));
            }
        };

        let inbound_cmd = to_proto_command_inbound(&pending_cmd.command);
        if handler_tx.send(Ok(inbound_cmd)).await.is_err() {
            platform.cancel_in_flight_command(&message_id);
            return Err(Status::unavailable("handler disconnected"));
        }

        // Wait for the handler's response. The command timeout is one
        // budget covering dispatch (including any permit wait) plus the
        // handler's response — deduct what dispatch consumed. The bus also
        // runs a background sweep as a safety net, but the per-request
        // deadline is enforced here so callers see deadline_exceeded.
        let timeout = timeout.saturating_sub(dispatch_started.elapsed());
        match tokio::time::timeout(timeout, response_rx).await {
            Ok(Ok(result)) => Ok(Response::new(to_proto_command_response(result))),
            Ok(Err(_)) => {
                // Sender dropped without sending — the bus extracted the entry
                // (e.g. handler disconnect cascade dropped the entry before
                // sending). Treat as unavailable.
                Err(Status::unavailable(
                    "handler disconnected before responding",
                ))
            }
            Err(_) => {
                platform.cancel_in_flight_command(&message_id);
                Err(Status::deadline_exceeded("command dispatch timed out"))
            }
        }
    }
}

/// Extracts the routing key from a proto command's processing instructions.
pub(crate) fn routing_key_of(cmd: &pb::Command) -> Option<String> {
    cmd.processing_instructions.iter().find_map(|pi| {
        if pi.key == crate::proto::kronosdb::ProcessingKey::RoutingKey as i32 {
            pi.value.as_ref().and_then(|v| match &v.data {
                Some(crate::proto::kronosdb::metadata_value::Data::TextValue(s)) => Some(s.clone()),
                _ => None,
            })
        } else {
            None
        }
    })
}

pub(crate) fn from_proto_command(cmd: pb::Command) -> Command {
    let routing_key = routing_key_of(&cmd).map(RoutingKey);

    let processing_instructions = proto_pi_to_internal(cmd.processing_instructions);

    Command {
        message_id: cmd.message_identifier,
        name: cmd.name,
        timestamp: cmd.timestamp,
        payload: Payload {
            payload_type: cmd
                .payload
                .as_ref()
                .map(|p| p.r#type.clone())
                .unwrap_or_default(),
            revision: cmd
                .payload
                .as_ref()
                .map(|p| p.revision.clone())
                .unwrap_or_default(),
            data: cmd.payload.map(|p| p.data).unwrap_or_default(),
        },
        metadata: proto_metadata_to_internal(cmd.metadata),
        processing_instructions,
        routing_key,
        client_id: ClientId(cmd.client_id),
        component_name: ComponentName(cmd.component_name),
    }
}

fn from_proto_command_response(resp: pb::CommandResponse) -> CommandResult {
    CommandResult {
        message_id: resp.message_identifier,
        request_id: resp.request_identifier,
        error_code: if resp.error_code.is_empty() {
            None
        } else {
            Some(resp.error_code)
        },
        error: resp
            .error_message
            .map(|e| kronosdb_messaging::types::ErrorDetail {
                message: e.message,
                location: e.location,
                details: e.details,
                error_code: e.error_code,
            }),
        payload: resp.payload.map(|p| Payload {
            payload_type: p.r#type,
            revision: p.revision,
            data: p.data,
        }),
        metadata: proto_metadata_to_internal(resp.metadata),
        processing_instructions: proto_pi_to_internal(resp.processing_instructions),
    }
}

pub(crate) fn to_proto_command_inbound(cmd: &Command) -> pb::CommandHandlerInbound {
    pb::CommandHandlerInbound {
        request: Some(pb::command_handler_inbound::Request::Command(pb::Command {
            message_identifier: cmd.message_id.clone(),
            name: cmd.name.clone(),
            timestamp: cmd.timestamp,
            payload: Some(crate::proto::kronosdb::SerializedObject {
                r#type: cmd.payload.payload_type.clone(),
                revision: cmd.payload.revision.clone(),
                data: cmd.payload.data.clone(),
            }),
            metadata: internal_metadata_to_proto(&cmd.metadata),
            processing_instructions: internal_pi_to_proto(&cmd.processing_instructions),
            client_id: cmd.client_id.0.clone(),
            component_name: cmd.component_name.0.clone(),
        })),
        instruction_id: String::new(),
    }
}

pub(crate) fn to_proto_command_response(result: CommandResult) -> pb::CommandResponse {
    pb::CommandResponse {
        message_identifier: result.message_id,
        error_code: result.error_code.unwrap_or_default(),
        error_message: result.error.map(|e| crate::proto::kronosdb::ErrorMessage {
            message: e.message,
            location: e.location,
            details: e.details,
            error_code: e.error_code,
        }),
        payload: result
            .payload
            .map(|p| crate::proto::kronosdb::SerializedObject {
                r#type: p.payload_type,
                revision: p.revision,
                data: p.data,
            }),
        metadata: internal_metadata_to_proto(&result.metadata),
        processing_instructions: internal_pi_to_proto(&result.processing_instructions),
        request_identifier: result.request_id,
    }
}
