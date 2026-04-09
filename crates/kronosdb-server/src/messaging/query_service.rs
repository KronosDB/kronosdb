use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use kronosdb_messaging::manager::MessagingManager;
use kronosdb_messaging::query::Query;
use kronosdb_messaging::subscription::{SubscriptionQuery, SubscriptionUpdate};
use kronosdb_messaging::types::{ClientId, ComponentName, ErrorDetail, MetadataValue, Payload, ProcessingInstruction, ProcessingKey};

use crate::proto::kronosdb::query as pb;
use crate::proto::kronosdb::query::query_service_server::QueryServiceServer as GrpcQueryServiceServer;

const CONTEXT_HEADER: &str = "kronosdb-context";
const DEFAULT_CONTEXT: &str = "default";

type HandlerSender = mpsc::Sender<Result<pb::QueryHandlerInbound, Status>>;

/// Pending query response collectors: request_id → channel to send results to the caller.
type QueryResponseSender = mpsc::Sender<pb::QueryResponse>;
type PendingQueries = Arc<Mutex<HashMap<String, QueryResponseSender>>>;

/// gRPC service implementation for the query bus.
///
/// Routes handlers to per-context messaging engines via `kronosdb-context` header.
pub struct QueryServiceImpl {
    messaging: Arc<MessagingManager>,
    handler_streams: Arc<Mutex<HashMap<String, HandlerSender>>>,
    pending_queries: PendingQueries,
    query_timeout: Duration,
}

impl QueryServiceImpl {
    pub fn new(messaging: Arc<MessagingManager>, query_timeout: Duration) -> Self {
        Self {
            messaging,
            handler_streams: Arc::new(Mutex::new(HashMap::new())),
            pending_queries: Arc::new(Mutex::new(HashMap::new())),
            query_timeout,
        }
    }

    pub fn into_server(self) -> GrpcQueryServiceServer<Self> {
        GrpcQueryServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl pb::query_service_server::QueryService for QueryServiceImpl {
    type OpenStreamStream = ReceiverStream<Result<pb::QueryHandlerInbound, Status>>;
    type QueryStream = ReceiverStream<Result<pb::QueryResponse, Status>>;
    type SubscriptionStream = ReceiverStream<Result<pb::SubscriptionQueryResponse, Status>>;

    async fn open_stream(
        &self,
        request: Request<Streaming<pb::QueryHandlerOutbound>>,
    ) -> Result<Response<Self::OpenStreamStream>, Status> {
        let context = request.metadata()
            .get(CONTEXT_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(DEFAULT_CONTEXT)
            .to_string();
        let platform = self.messaging.get_platform(&context);

        let mut inbound = request.into_inner();
        let (handler_tx, handler_rx) = mpsc::channel::<Result<pb::QueryHandlerInbound, Status>>(128);

        let handler_streams = Arc::clone(&self.handler_streams);
        let pending_queries = Arc::clone(&self.pending_queries);
        let mut client_id: Option<String> = None;

        tokio::spawn(async move {
            while let Ok(Some(msg)) = inbound.message().await {
                let instruction_id = if msg.instruction_id.is_empty() {
                    None
                } else {
                    Some(msg.instruction_id.clone())
                };

                match msg.request {
                    Some(pb::query_handler_outbound::Request::Subscribe(sub)) => {
                        client_id = Some(sub.client_id.clone());

                        {
                            let mut streams = handler_streams.lock();
                            streams.insert(sub.client_id.clone(), handler_tx.clone());
                        }

                        platform.subscribe_query(
                            sub.query,
                            ClientId(sub.client_id),
                            ComponentName(sub.component_name),
                        );

                        if let Some(id) = instruction_id {
                            let ack = pb::QueryHandlerInbound {
                                request: Some(pb::query_handler_inbound::Request::Ack(
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
                    Some(pb::query_handler_outbound::Request::Unsubscribe(sub)) => {
                        platform.unsubscribe_query(&sub.query, &ClientId(sub.client_id));
                    }
                    Some(pb::query_handler_outbound::Request::FlowControl(fc)) => {
                        platform.grant_query_permits(&ClientId(fc.client_id), fc.permits);
                    }
                    Some(pb::query_handler_outbound::Request::QueryResponse(resp)) => {
                        // Route the response back to the waiting caller.
                        let request_id = resp.request_identifier.clone();
                        let pending = pending_queries.lock();
                        if let Some(tx) = pending.get(&request_id) {
                            let _ = tx.try_send(resp);
                        }
                    }
                    Some(pb::query_handler_outbound::Request::QueryComplete(complete)) => {
                        // All results sent for this query — remove from pending.
                        let mut pending = pending_queries.lock();
                        pending.remove(&complete.request_id);
                        // Dropping the sender closes the channel, signaling the caller.
                    }
                    Some(pb::query_handler_outbound::Request::SubscriptionQueryResponse(resp)) => {
                        // Route subscription update through the registry so it
                        // reaches the subscriber's update receiver.
                        let sub_id = resp.subscription_identifier.clone();
                        let update = proto_subscription_response_to_update(resp);
                        platform.send_update(&sub_id, update);
                    }
                    Some(pb::query_handler_outbound::Request::Ack(_)) => {}
                    None => {}
                }
            }

            // Handler disconnected.
            if let Some(cid) = client_id {
                platform.remove_client(&ClientId(cid.clone()));
                handler_streams.lock().remove(&cid);
            }
        });

        Ok(Response::new(ReceiverStream::new(handler_rx)))
    }

    async fn query(
        &self,
        request: Request<pb::QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let context = request.metadata()
            .get(CONTEXT_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(DEFAULT_CONTEXT)
            .to_string();
        let platform = self.messaging.get_platform(&context);

        let req = request.into_inner();
        let message_id = req.message_identifier.clone();

        let query = Query {
            message_id: req.message_identifier,
            name: req.query.clone(),
            timestamp: req.timestamp,
            payload: Payload {
                payload_type: req.payload.as_ref().map(|p| p.r#type.clone()).unwrap_or_default(),
                revision: req.payload.as_ref().map(|p| p.revision.clone()).unwrap_or_default(),
                data: req.payload.map(|p| p.data).unwrap_or_default(),
            },
            metadata: proto_metadata_to_internal(req.metadata),
            processing_instructions: proto_pi_to_internal(req.processing_instructions),
            client_id: ClientId(req.client_id),
            component_name: ComponentName(req.component_name),
            expected_results: -1,
        };

        let query_name = query.name.clone();
        let pending = platform
            .dispatch_query(query)
            .map_err(|e| Status::unavailable(e.to_string()))?;

        // Create a channel for collecting responses from handlers.
        let (response_tx, mut response_rx) = mpsc::channel::<pb::QueryResponse>(64);

        // Register in pending map so handler responses route here.
        {
            let mut pending_map = self.pending_queries.lock();
            pending_map.insert(message_id.clone(), response_tx);
        }

        // Deliver the query to each target handler.
        for target_client_id in &pending.target_handlers {
            let handler_tx = {
                let streams = self.handler_streams.lock();
                streams.get(&target_client_id.0).cloned()
            };

            if let Some(tx) = handler_tx {
                let inbound_query = pb::QueryHandlerInbound {
                    request: Some(pb::query_handler_inbound::Request::Query(pb::QueryRequest {
                        message_identifier: message_id.clone(),
                        query: query_name.clone(),
                        timestamp: pending.query.timestamp,
                        payload: Some(crate::proto::kronosdb::SerializedObject {
                            r#type: pending.query.payload.payload_type.clone(),
                            revision: pending.query.payload.revision.clone(),
                            data: pending.query.payload.data.clone(),
                        }),
                        metadata: internal_metadata_to_proto(&pending.query.metadata),
                        processing_instructions: internal_pi_to_proto(&pending.query.processing_instructions),
                        client_id: pending.query.client_id.0.clone(),
                        component_name: pending.query.component_name.0.clone(),
                    })),
                    instruction_id: String::new(),
                };
                let _ = tx.send(Ok(inbound_query)).await;
            }
        }

        // Stream responses back to the caller.
        let (caller_tx, caller_rx) = mpsc::channel(64);
        let pending_queries = Arc::clone(&self.pending_queries);
        let msg_id = message_id.clone();
        let query_timeout = self.query_timeout;

        tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + query_timeout;
            loop {
                match tokio::time::timeout_at(deadline, response_rx.recv()).await {
                    Ok(Some(resp)) => {
                        if caller_tx.send(Ok(resp)).await.is_err() {
                            break; // Caller disconnected.
                        }
                    }
                    Ok(None) => break, // All handlers done (channel closed).
                    Err(_) => {
                        // Timeout — send error and stop collecting.
                        let _ = caller_tx
                            .send(Err(Status::deadline_exceeded("query response timed out")))
                            .await;
                        break;
                    }
                }
            }
            // Clean up pending entry.
            pending_queries.lock().remove(&msg_id);
        });

        Ok(Response::new(ReceiverStream::new(caller_rx)))
    }

    async fn subscription(
        &self,
        request: Request<Streaming<pb::SubscriptionQueryRequest>>,
    ) -> Result<Response<Self::SubscriptionStream>, Status> {
        let context = request.metadata()
            .get(CONTEXT_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(DEFAULT_CONTEXT)
            .to_string();
        let platform = self.messaging.get_platform(&context);

        let mut inbound = request.into_inner();
        let (sub_tx, sub_rx) = mpsc::channel::<Result<pb::SubscriptionQueryResponse, Status>>(64);
        let handler_streams = Arc::clone(&self.handler_streams);

        tokio::spawn(async move {
            while let Ok(Some(msg)) = inbound.message().await {
                match msg.request {
                    Some(pb::subscription_query_request::Request::Subscribe(sub)) => {
                        let sub_id = sub.subscription_identifier.clone();

                        let subscription = SubscriptionQuery {
                            subscription_id: sub_id.clone(),
                            query_name: sub.query_request.as_ref().map(|q| q.query.clone()).unwrap_or_default(),
                            timestamp: sub.query_request.as_ref().map(|q| q.timestamp).unwrap_or(0),
                            payload: Payload {
                                payload_type: sub.query_request.as_ref()
                                    .and_then(|q| q.payload.as_ref())
                                    .map(|p| p.r#type.clone())
                                    .unwrap_or_default(),
                                revision: sub.query_request.as_ref()
                                    .and_then(|q| q.payload.as_ref())
                                    .map(|p| p.revision.clone())
                                    .unwrap_or_default(),
                                data: sub.query_request.as_ref()
                                    .and_then(|q| q.payload.as_ref())
                                    .map(|p| p.data.clone())
                                    .unwrap_or_default(),
                            },
                            metadata: sub.query_request.as_ref()
                                .map(|q| proto_metadata_to_internal(q.metadata.clone()))
                                .unwrap_or_default(),
                            client_id: ClientId(sub.query_request.as_ref().map(|q| q.client_id.clone()).unwrap_or_default()),
                            component_name: ComponentName(sub.query_request.as_ref().map(|q| q.component_name.clone()).unwrap_or_default()),
                            initial_permits: sub.number_of_permits,
                        };

                        match platform.subscribe(subscription) {
                            Ok((pending, mut update_rx)) => {
                                // Deliver the initial query to the handler.
                                let handler_tx = {
                                    let streams = handler_streams.lock();
                                    pending.target_handlers.first()
                                        .and_then(|id| streams.get(&id.0).cloned())
                                };

                                if let Some(tx) = handler_tx {
                                    let inbound_query = pb::QueryHandlerInbound {
                                        request: Some(pb::query_handler_inbound::Request::Query(pb::QueryRequest {
                                            message_identifier: sub_id.clone(),
                                            query: pending.query.name.clone(),
                                            timestamp: pending.query.timestamp,
                                            payload: Some(crate::proto::kronosdb::SerializedObject {
                                                r#type: pending.query.payload.payload_type.clone(),
                                                revision: pending.query.payload.revision.clone(),
                                                data: pending.query.payload.data.clone(),
                                            }),
                                            metadata: internal_metadata_to_proto(&pending.query.metadata),
                                            processing_instructions: internal_pi_to_proto(&pending.query.processing_instructions),
                                            client_id: pending.query.client_id.0.clone(),
                                            component_name: pending.query.component_name.0.clone(),
                                        })),
                                        instruction_id: String::new(),
                                    };
                                    let _ = tx.send(Ok(inbound_query)).await;
                                }

                                // Spawn a task to drain updates from the registry
                                // and forward them to the gRPC subscriber stream.
                                let sub_tx = sub_tx.clone();
                                tokio::spawn(async move {
                                    while let Some(update) = update_rx.recv().await {
                                        let resp = subscription_update_to_proto(update);
                                        if sub_tx.send(Ok(resp)).await.is_err() {
                                            break; // Subscriber disconnected.
                                        }
                                    }
                                });
                            }
                            Err(e) => {
                                let _ = sub_tx
                                    .send(Err(Status::unavailable(e.to_string())))
                                    .await;
                            }
                        }
                    }
                    Some(pb::subscription_query_request::Request::Unsubscribe(sub)) => {
                        let sub_id = sub.subscription_identifier;
                        platform.cancel_subscription(&sub_id);
                    }
                    Some(pb::subscription_query_request::Request::FlowControl(_fc)) => {
                        // Grant more update permits — currently unused since we
                        // use channel backpressure instead.
                    }
                    None => {}
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(sub_rx)))
    }
}

fn proto_error_to_detail(e: crate::proto::kronosdb::ErrorMessage) -> ErrorDetail {
    ErrorDetail {
        message: e.message,
        location: e.location,
        details: e.details,
        error_code: e.error_code,
    }
}

fn detail_to_proto_error(e: &ErrorDetail) -> crate::proto::kronosdb::ErrorMessage {
    crate::proto::kronosdb::ErrorMessage {
        message: e.message.clone(),
        location: e.location.clone(),
        details: e.details.clone(),
        error_code: e.error_code.clone(),
    }
}

fn proto_subscription_response_to_update(resp: pb::SubscriptionQueryResponse) -> SubscriptionUpdate {
    let sub_id = resp.subscription_identifier;
    match resp.response {
        Some(pb::subscription_query_response::Response::Update(upd)) => SubscriptionUpdate {
            subscription_id: sub_id,
            payload: upd.payload.map(|p| Payload {
                payload_type: p.r#type,
                revision: p.revision,
                data: p.data,
            }),
            metadata: proto_metadata_to_internal(upd.metadata),
            error_code: if upd.error_code.is_empty() { None } else { Some(upd.error_code) },
            error: upd.error_message.map(proto_error_to_detail),
        },
        Some(pb::subscription_query_response::Response::InitialResult(result)) => SubscriptionUpdate {
            subscription_id: sub_id,
            payload: result.payload.map(|p| Payload {
                payload_type: p.r#type,
                revision: p.revision,
                data: p.data,
            }),
            metadata: proto_metadata_to_internal(result.metadata),
            error_code: if result.error_code.is_empty() { None } else { Some(result.error_code) },
            error: result.error_message.map(proto_error_to_detail),
        },
        Some(pb::subscription_query_response::Response::CompleteExceptionally(err)) => SubscriptionUpdate {
            subscription_id: sub_id,
            payload: None,
            metadata: HashMap::new(),
            error_code: Some(err.error_code),
            error: err.error_message.map(proto_error_to_detail),
        },
        Some(pb::subscription_query_response::Response::Complete(_)) | None => SubscriptionUpdate {
            subscription_id: sub_id,
            payload: None,
            metadata: HashMap::new(),
            error_code: None,
            error: None,
        },
    }
}

fn subscription_update_to_proto(update: SubscriptionUpdate) -> pb::SubscriptionQueryResponse {
    let response = if let Some(ref error_code) = update.error_code {
        Some(pb::subscription_query_response::Response::CompleteExceptionally(
            pb::QueryUpdateCompleteExceptionally {
                client_id: String::new(),
                component_name: String::new(),
                error_code: error_code.clone(),
                error_message: update.error.as_ref().map(detail_to_proto_error),
            },
        ))
    } else {
        Some(pb::subscription_query_response::Response::Update(
            pb::QueryUpdate {
                message_identifier: update.subscription_id.clone(),
                payload: update.payload.map(|p| crate::proto::kronosdb::SerializedObject {
                    r#type: p.payload_type,
                    revision: p.revision,
                    data: p.data,
                }),
                metadata: internal_metadata_to_proto(&update.metadata),
                client_id: String::new(),
                component_name: String::new(),
                error_code: String::new(),
                error_message: None,
            },
        ))
    };

    pb::SubscriptionQueryResponse {
        message_identifier: String::new(),
        subscription_identifier: update.subscription_id,
        response,
    }
}

// --- Proto conversion helpers ---

fn proto_mv_to_internal(v: crate::proto::kronosdb::MetadataValue) -> MetadataValue {
    match v.data {
        Some(crate::proto::kronosdb::metadata_value::Data::TextValue(s)) => MetadataValue::Text(s),
        Some(crate::proto::kronosdb::metadata_value::Data::NumberValue(n)) => MetadataValue::Number(n),
        Some(crate::proto::kronosdb::metadata_value::Data::BooleanValue(b)) => MetadataValue::Boolean(b),
        Some(crate::proto::kronosdb::metadata_value::Data::DoubleValue(d)) => MetadataValue::Double(d),
        Some(crate::proto::kronosdb::metadata_value::Data::BytesValue(obj)) => MetadataValue::Bytes(Payload {
            payload_type: obj.r#type,
            revision: obj.revision,
            data: obj.data,
        }),
        None => MetadataValue::Text(String::new()),
    }
}

fn internal_mv_to_proto(v: &MetadataValue) -> crate::proto::kronosdb::MetadataValue {
    let data = match v {
        MetadataValue::Text(s) => Some(crate::proto::kronosdb::metadata_value::Data::TextValue(s.clone())),
        MetadataValue::Number(n) => Some(crate::proto::kronosdb::metadata_value::Data::NumberValue(*n)),
        MetadataValue::Boolean(b) => Some(crate::proto::kronosdb::metadata_value::Data::BooleanValue(*b)),
        MetadataValue::Double(d) => Some(crate::proto::kronosdb::metadata_value::Data::DoubleValue(*d)),
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
        _ => ProcessingKey::RoutingKey,
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
