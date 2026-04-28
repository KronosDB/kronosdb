use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use kronosdb_messaging::command::{Command, CommandResult};
use kronosdb_messaging::manager::MessagingManager;
use kronosdb_messaging::types::{
    ClientId, ComponentName, MetadataValue, Payload, ProcessingInstruction, ProcessingKey,
    RoutingKey,
};

use crate::handler_registry::HandlerStreamRegistry;
use crate::platform::service::ClientChannelRegistry;
use crate::proto::kronosdb::command as pb;
use crate::proto::kronosdb::command::command_service_server::CommandServiceServer as GrpcCommandServiceServer;
use crate::proto::kronosdb::platform as platform_pb;

/// gRPC metadata header for context routing.
const CONTEXT_HEADER: &str = "kronosdb-context";
const DEFAULT_CONTEXT: &str = "default";

type HandlerSender = mpsc::Sender<Result<pb::CommandHandlerInbound, Status>>;

/// gRPC service implementation for the command bus.
///
/// Routes handlers to per-context messaging engines via `kronosdb-context` header.
/// `handler_streams` is sharded so concurrent dispatches don't contend; the
/// caller-side response channel lives in the messaging engine's in-flight map
/// rather than a parallel gRPC-side map.
pub struct CommandServiceImpl {
    messaging: Arc<MessagingManager>,
    handler_streams: Arc<DashMap<String, HandlerSender>>,
    command_timeout: Duration,
    channel_registry: Arc<ClientChannelRegistry>,
    handler_stream_registry: Arc<HandlerStreamRegistry>,
}

impl CommandServiceImpl {
    pub fn new(
        messaging: Arc<MessagingManager>,
        command_timeout: Duration,
        channel_registry: Arc<ClientChannelRegistry>,
        handler_stream_registry: Arc<HandlerStreamRegistry>,
    ) -> Self {
        Self {
            messaging,
            handler_streams: Arc::new(DashMap::new()),
            command_timeout,
            channel_registry,
            handler_stream_registry,
        }
    }

    pub fn into_server(self) -> GrpcCommandServiceServer<Self> {
        GrpcCommandServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl pb::command_service_server::CommandService for CommandServiceImpl {
    type OpenStreamStream = ReceiverStream<Result<pb::CommandHandlerInbound, Status>>;

    async fn open_stream(
        &self,
        request: Request<Streaming<pb::CommandHandlerOutbound>>,
    ) -> Result<Response<Self::OpenStreamStream>, Status> {
        let context = request
            .metadata()
            .get(CONTEXT_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(DEFAULT_CONTEXT)
            .to_string();
        tracing::info!(context = %context, "Command OpenStream opened");
        let platform = self.messaging.get_platform(&context);

        let mut inbound = request.into_inner();
        // 4096 absorbs dispatch bursts without blocking the send.await path.
        let (handler_tx, handler_rx) =
            mpsc::channel::<Result<pb::CommandHandlerInbound, Status>>(4096);

        let handler_streams = Arc::clone(&self.handler_streams);
        let channel_registry = Arc::clone(&self.channel_registry);
        let handler_stream_registry = Arc::clone(&self.handler_stream_registry);
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
        let context = request
            .metadata()
            .get(CONTEXT_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(DEFAULT_CONTEXT)
            .to_string();
        let platform = self.messaging.get_platform(&context);

        let cmd = request.into_inner();
        let command = from_proto_command(cmd);
        let message_id = command.message_id.clone();

        // Dispatch — selects a handler, acquires a permit, and registers the
        // in-flight tracking entry holding the response sender.
        let (pending_cmd, response_rx) = platform
            .dispatch_command(command)
            .map_err(|e| Status::unavailable(e.to_string()))?;

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

        // Wait for the handler's response, with a per-request timeout. The
        // bus also runs a background sweep as a safety net, but the per-
        // request deadline is enforced here so callers see deadline_exceeded.
        match tokio::time::timeout(self.command_timeout, response_rx).await {
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

// --- Proto conversions ---

fn proto_mv_to_internal(v: crate::proto::kronosdb::MetadataValue) -> MetadataValue {
    match v.data {
        Some(crate::proto::kronosdb::metadata_value::Data::TextValue(s)) => MetadataValue::Text(s),
        Some(crate::proto::kronosdb::metadata_value::Data::NumberValue(n)) => {
            MetadataValue::Number(n)
        }
        Some(crate::proto::kronosdb::metadata_value::Data::BooleanValue(b)) => {
            MetadataValue::Boolean(b)
        }
        Some(crate::proto::kronosdb::metadata_value::Data::DoubleValue(d)) => {
            MetadataValue::Double(d)
        }
        Some(crate::proto::kronosdb::metadata_value::Data::BytesValue(obj)) => {
            MetadataValue::Bytes(Payload {
                payload_type: obj.r#type,
                revision: obj.revision,
                data: obj.data,
            })
        }
        None => MetadataValue::Text(String::new()),
    }
}

fn internal_mv_to_proto(v: &MetadataValue) -> crate::proto::kronosdb::MetadataValue {
    let data = match v {
        MetadataValue::Text(s) => Some(crate::proto::kronosdb::metadata_value::Data::TextValue(
            s.clone(),
        )),
        MetadataValue::Number(n) => Some(
            crate::proto::kronosdb::metadata_value::Data::NumberValue(*n),
        ),
        MetadataValue::Boolean(b) => Some(
            crate::proto::kronosdb::metadata_value::Data::BooleanValue(*b),
        ),
        MetadataValue::Double(d) => Some(
            crate::proto::kronosdb::metadata_value::Data::DoubleValue(*d),
        ),
        MetadataValue::Bytes(p) => Some(crate::proto::kronosdb::metadata_value::Data::BytesValue(
            crate::proto::kronosdb::SerializedObject {
                r#type: p.payload_type.clone(),
                revision: p.revision.clone(),
                data: p.data.clone(),
            },
        )),
    };
    crate::proto::kronosdb::MetadataValue { data }
}

fn proto_metadata_to_internal(
    meta: HashMap<String, crate::proto::kronosdb::MetadataValue>,
) -> kronosdb_messaging::types::Metadata {
    meta.into_iter()
        .map(|(k, v)| (k, proto_mv_to_internal(v)))
        .collect()
}

fn internal_metadata_to_proto(
    meta: &kronosdb_messaging::types::Metadata,
) -> HashMap<String, crate::proto::kronosdb::MetadataValue> {
    meta.iter()
        .map(|(k, v)| (k.clone(), internal_mv_to_proto(v)))
        .collect()
}

fn proto_pk_to_internal(key: i32) -> ProcessingKey {
    match key {
        1 => ProcessingKey::Priority,
        2 => ProcessingKey::Timeout,
        3 => ProcessingKey::NrOfResults,
        _ => ProcessingKey::RoutingKey, // 0 and unknown
    }
}

fn internal_pk_to_proto(key: ProcessingKey) -> i32 {
    match key {
        ProcessingKey::RoutingKey => 0,
        ProcessingKey::Priority => 1,
        ProcessingKey::Timeout => 2,
        ProcessingKey::NrOfResults => 3,
    }
}

fn proto_pi_to_internal(
    pis: Vec<crate::proto::kronosdb::ProcessingInstruction>,
) -> Vec<ProcessingInstruction> {
    pis.into_iter()
        .map(|pi| ProcessingInstruction {
            key: proto_pk_to_internal(pi.key),
            value: pi.value.map(proto_mv_to_internal),
        })
        .collect()
}

fn internal_pi_to_proto(
    pis: &[ProcessingInstruction],
) -> Vec<crate::proto::kronosdb::ProcessingInstruction> {
    pis.iter()
        .map(|pi| crate::proto::kronosdb::ProcessingInstruction {
            key: internal_pk_to_proto(pi.key),
            value: pi.value.as_ref().map(internal_mv_to_proto),
        })
        .collect()
}

fn from_proto_command(cmd: pb::Command) -> Command {
    let routing_key = cmd.processing_instructions.iter().find_map(|pi| {
        if pi.key == crate::proto::kronosdb::ProcessingKey::RoutingKey as i32 {
            pi.value.as_ref().and_then(|v| match &v.data {
                Some(crate::proto::kronosdb::metadata_value::Data::TextValue(s)) => {
                    Some(RoutingKey(s.clone()))
                }
                _ => None,
            })
        } else {
            None
        }
    });

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

fn to_proto_command_inbound(cmd: &Command) -> pb::CommandHandlerInbound {
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

fn to_proto_command_response(result: CommandResult) -> pb::CommandResponse {
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
