use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use kronosdb_messaging::api::MessagingPlatform;
use kronosdb_messaging::command::{Command, CommandResult};
use kronosdb_messaging::types::{ClientId, ComponentName, Payload, RoutingKey};

use crate::proto::kronosdb::command as pb;
use crate::proto::kronosdb::command::command_service_server::CommandServiceServer as GrpcCommandServiceServer;

type HandlerSender = mpsc::Sender<Result<pb::CommandHandlerInbound, Status>>;
type PendingResponses = Arc<Mutex<HashMap<String, oneshot::Sender<CommandResult>>>>;

/// gRPC service implementation for the command bus.
pub struct CommandServiceImpl {
    platform: Arc<dyn MessagingPlatform>,
    /// Active handler streams: client_id → channel to deliver commands to that handler.
    handler_streams: Arc<Mutex<HashMap<String, HandlerSender>>>,
    /// Pending command responses: message_id → oneshot sender to complete the dispatch call.
    pending: PendingResponses,
}

impl CommandServiceImpl {
    pub fn new(platform: Arc<dyn MessagingPlatform>) -> Self {
        Self {
            platform,
            handler_streams: Arc::new(Mutex::new(HashMap::new())),
            pending: Arc::new(Mutex::new(HashMap::new())),
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
        let mut inbound = request.into_inner();
        let (handler_tx, handler_rx) = mpsc::channel::<Result<pb::CommandHandlerInbound, Status>>(128);

        let platform = Arc::clone(&self.platform);
        let handler_streams = Arc::clone(&self.handler_streams);
        let pending = Arc::clone(&self.pending);
        let mut client_id: Option<String> = None;

        tokio::spawn(async move {
            while let Ok(Some(msg)) = inbound.message().await {
                let instruction_id = if msg.instruction_id.is_empty() {
                    None
                } else {
                    Some(msg.instruction_id.clone())
                };

                match msg.request {
                    Some(pb::command_handler_outbound::Request::Subscribe(sub)) => {
                        client_id = Some(sub.client_id.clone());

                        {
                            let mut streams = handler_streams.lock();
                            streams.insert(sub.client_id.clone(), handler_tx.clone());
                        }

                        platform.subscribe_command(
                            sub.command,
                            ClientId(sub.client_id),
                            ComponentName(sub.component_name),
                            sub.load_factor,
                        );

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
                        platform.unsubscribe_command(&sub.command, &ClientId(sub.client_id));
                    }
                    Some(pb::command_handler_outbound::Request::FlowControl(fc)) => {
                        platform.grant_command_permits(&ClientId(fc.client_id), fc.permits);
                    }
                    Some(pb::command_handler_outbound::Request::CommandResponse(resp)) => {
                        let result = from_proto_command_response(resp);
                        let request_id = result.request_id.clone();

                        // Complete the pending dispatch call.
                        let mut pending_map = pending.lock();
                        if let Some(tx) = pending_map.remove(&request_id) {
                            let _ = tx.send(result);
                        }
                    }
                    Some(pb::command_handler_outbound::Request::Ack(_)) => {}
                    None => {}
                }
            }

            // Handler disconnected — clean up.
            if let Some(cid) = client_id {
                platform.remove_client(&ClientId(cid.clone()));
                handler_streams.lock().remove(&cid);

                // Fail all pending commands for this handler.
                // (In practice, the oneshot receivers will get a RecvError
                // when the handler_streams entry is removed and the dispatch
                // path can't send the command.)
            }
        });

        Ok(Response::new(ReceiverStream::new(handler_rx)))
    }

    async fn dispatch(
        &self,
        request: Request<pb::Command>,
    ) -> Result<Response<pb::CommandResponse>, Status> {
        let cmd = request.into_inner();
        let command = from_proto_command(cmd);
        let message_id = command.message_id.clone();

        // Dispatch — selects a handler and acquires a permit.
        let (pending_cmd, response_rx) = self
            .platform
            .dispatch_command(command)
            .map_err(|e| Status::unavailable(e.to_string()))?;

        // Store the response channel so the handler stream can complete it.
        {
            let mut pending_map = self.pending.lock();
            pending_map.insert(message_id.clone(), pending_cmd.response_tx);
        }

        // Find the selected handler's stream and deliver the command.
        let target_id = &pending_cmd.target_handler.0;
        let handler_tx = {
            let streams = self.handler_streams.lock();
            streams.get(target_id).cloned()
        };

        let handler_tx = handler_tx
            .ok_or_else(|| Status::unavailable(format!("handler '{}' stream not found", target_id)))?;

        let inbound_cmd = to_proto_command_inbound(&pending_cmd.command);
        handler_tx
            .send(Ok(inbound_cmd))
            .await
            .map_err(|_| Status::unavailable("handler disconnected"))?;

        // Wait for the handler's response.
        let result = response_rx
            .await
            .map_err(|_| Status::unavailable("handler disconnected before responding"))?;

        Ok(Response::new(to_proto_command_response(result)))
    }
}

// --- Proto conversions ---

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

    Command {
        message_id: cmd.message_identifier,
        name: cmd.name,
        timestamp: cmd.timestamp,
        payload: Payload {
            payload_type: cmd.payload.as_ref().map(|p| p.r#type.clone()).unwrap_or_default(),
            revision: cmd.payload.as_ref().map(|p| p.revision.clone()).unwrap_or_default(),
            data: cmd.payload.map(|p| p.data).unwrap_or_default(),
        },
        metadata: extract_text_metadata(cmd.metadata),
        routing_key,
        client_id: ClientId(cmd.client_id),
        component_name: ComponentName(cmd.component_name),
    }
}

fn from_proto_command_response(resp: pb::CommandResponse) -> CommandResult {
    CommandResult {
        message_id: resp.message_identifier,
        request_id: resp.request_identifier,
        error_code: if resp.error_code.is_empty() { None } else { Some(resp.error_code) },
        error_message: resp.error_message.map(|e| e.message),
        payload: resp.payload.map(|p| Payload {
            payload_type: p.r#type,
            revision: p.revision,
            data: p.data,
        }),
        metadata: extract_text_metadata(resp.metadata),
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
            metadata: Default::default(),
            processing_instructions: vec![],
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
        error_message: result.error_message.map(|msg| crate::proto::kronosdb::ErrorMessage {
            message: msg,
            location: String::new(),
            details: vec![],
            error_code: String::new(),
        }),
        payload: result.payload.map(|p| crate::proto::kronosdb::SerializedObject {
            r#type: p.payload_type,
            revision: p.revision,
            data: p.data,
        }),
        metadata: Default::default(),
        processing_instructions: vec![],
        request_identifier: result.request_id,
    }
}

fn extract_text_metadata(
    metadata: HashMap<String, crate::proto::kronosdb::MetadataValue>,
) -> Vec<(String, String)> {
    metadata
        .into_iter()
        .filter_map(|(k, v)| match v.data {
            Some(crate::proto::kronosdb::metadata_value::Data::TextValue(s)) => Some((k, s)),
            _ => None,
        })
        .collect()
}
