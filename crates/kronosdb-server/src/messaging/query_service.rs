use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use kronosdb_messaging::manager::MessagingManager;
use kronosdb_messaging::query::Query;
use kronosdb_messaging::subscription::{SubscriptionQuery, SubscriptionUpdate};
use kronosdb_messaging::types::{
    ClientId, ComponentName, ErrorDetail, MetadataValue, Payload, ProcessingInstruction,
    ProcessingKey,
};

use crate::handler_registry::HandlerStreamRegistry;
use crate::platform::service::ClientChannelRegistry;
use crate::proto::kronosdb::platform as platform_pb;
use crate::proto::kronosdb::query as pb;
use crate::proto::kronosdb::query::query_service_server::QueryServiceServer as GrpcQueryServiceServer;

const CONTEXT_HEADER: &str = "kronosdb-context";
const DEFAULT_CONTEXT: &str = "default";

type HandlerSender = mpsc::Sender<Result<pb::QueryHandlerInbound, Status>>;

/// Pending query response collectors: request_id → channel to send results to the caller.
type QueryResponseSender = mpsc::Sender<pb::QueryResponse>;
type PendingQueries = Arc<DashMap<String, QueryResponseSender>>;

/// Pending subscription-query initial results: subscription_id → sender on the
/// subscription stream. Routed when a regular QueryResponse comes back tagged
/// with a subscription's id, so the caller receives it as
/// `SubscriptionQueryResponse::InitialResult` on the same stream as updates.
type SubscriptionInitialSender = mpsc::Sender<Result<pb::SubscriptionQueryResponse, Status>>;
type PendingSubscriptionInitials = Arc<DashMap<String, SubscriptionInitialSender>>;

/// gRPC service implementation for the query bus.
///
/// Routes handlers to per-context messaging engines via `kronosdb-context` header.
/// `handler_streams` and `pending_queries` are sharded maps so concurrent
/// queries and response completions don't contend on a single mutex.
pub struct QueryServiceImpl {
    messaging: Arc<MessagingManager>,
    handler_streams: Arc<DashMap<String, HandlerSender>>,
    pending_queries: PendingQueries,
    pending_sub_initials: PendingSubscriptionInitials,
    query_timeout: Duration,
    channel_registry: Arc<ClientChannelRegistry>,
    handler_stream_registry: Arc<HandlerStreamRegistry>,
}

impl QueryServiceImpl {
    pub fn new(
        messaging: Arc<MessagingManager>,
        query_timeout: Duration,
        channel_registry: Arc<ClientChannelRegistry>,
        handler_stream_registry: Arc<HandlerStreamRegistry>,
    ) -> Self {
        Self {
            messaging,
            handler_streams: Arc::new(DashMap::new()),
            pending_queries: Arc::new(DashMap::new()),
            pending_sub_initials: Arc::new(DashMap::new()),
            query_timeout,
            channel_registry,
            handler_stream_registry,
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
        let context = request
            .metadata()
            .get(CONTEXT_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(DEFAULT_CONTEXT)
            .to_string();
        tracing::info!(context = %context, "Query OpenStream opened");
        let platform = self.messaging.get_platform(&context);

        let mut inbound = request.into_inner();
        // 4096 absorbs dispatch bursts without blocking the send.await path.
        let (handler_tx, handler_rx) =
            mpsc::channel::<Result<pb::QueryHandlerInbound, Status>>(4096);

        let handler_streams = Arc::clone(&self.handler_streams);
        let pending_queries = Arc::clone(&self.pending_queries);
        let pending_sub_initials = Arc::clone(&self.pending_sub_initials);
        let channel_registry = Arc::clone(&self.channel_registry);
        let handler_stream_registry = Arc::clone(&self.handler_stream_registry);
        let mut client_id: Option<String> = None;

        tokio::spawn(async move {
            let mut cancel_token: Option<tokio_util::sync::CancellationToken> = None;
            let mut subscribed_queries: Vec<String> = Vec::new();

            loop {
                let msg_result = if let Some(ref token) = cancel_token {
                    tokio::select! {
                        biased;
                        _ = token.cancelled() => {
                            tracing::info!(client_id = ?client_id, "query handler: cancelled by platform disconnect");
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
                        tracing::info!(client_id = ?client_id, "query handler stream: client closed send-side");
                        break;
                    }
                    Err(e) => {
                        tracing::warn!(client_id = ?client_id, error = %e, "query handler stream: transport error");
                        break;
                    }
                };

                let instruction_id = if msg.instruction_id.is_empty() {
                    None
                } else {
                    Some(msg.instruction_id.clone())
                };

                match msg.request {
                    Some(pb::query_handler_outbound::Request::Subscribe(sub)) => {
                        tracing::info!(
                            query = %sub.query,
                            client_id = %sub.client_id,
                            component = %sub.component_name,
                            "Query handler subscribing"
                        );
                        let query_name = sub.query.clone();
                        let sub_client_id = sub.client_id.clone();
                        let sub_component = sub.component_name.clone();
                        client_id = Some(sub.client_id.clone());
                        subscribed_queries.push(sub.query.clone());

                        if cancel_token.is_none() {
                            cancel_token = Some(
                                handler_stream_registry.get_cancellation_token(&sub.client_id),
                            );
                        }

                        handler_streams.insert(sub.client_id.clone(), handler_tx.clone());
                        handler_stream_registry.register(&sub.client_id, handler_tx.clone());

                        platform.subscribe_query(
                            sub.query,
                            ClientId(sub.client_id),
                            ComponentName(sub.component_name),
                        );

                        channel_registry
                            .broadcast_topology_notification(platform_pb::TopologyNotification {
                                change_type: "handler_registered".to_string(),
                                message_type: query_name,
                                handler_kind: "query".to_string(),
                                client_id: sub_client_id,
                                component_name: sub_component,
                            })
                            .await;

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
                        let query_name = sub.query.clone();
                        let unsub_client_id = sub.client_id.clone();
                        platform.unsubscribe_query(&sub.query, &ClientId(sub.client_id));

                        channel_registry
                            .broadcast_topology_notification(platform_pb::TopologyNotification {
                                change_type: "handler_deregistered".to_string(),
                                message_type: query_name,
                                handler_kind: "query".to_string(),
                                client_id: unsub_client_id,
                                component_name: String::new(),
                            })
                            .await;
                    }
                    Some(pb::query_handler_outbound::Request::FlowControl(fc)) => {
                        platform.grant_query_permits(&ClientId(fc.client_id), fc.permits);
                    }
                    Some(pb::query_handler_outbound::Request::QueryResponse(resp)) => {
                        let request_id = resp.request_identifier.clone();

                        // First try regular pending queries.
                        let routed = if let Some(entry) = pending_queries.get(&request_id) {
                            let _ = entry.value().try_send(resp.clone());
                            true
                        } else {
                            false
                        };

                        // Otherwise this may be the initial result for a subscription
                        // query — wrap and forward on the subscription stream.
                        if !routed && let Some(entry) = pending_sub_initials.get(&request_id) {
                            let initial_result = pb::SubscriptionQueryResponse {
                                message_identifier: String::new(),
                                subscription_identifier: request_id.clone(),
                                response: Some(
                                    pb::subscription_query_response::Response::InitialResult(
                                        pb::QueryResponse {
                                            message_identifier: resp.message_identifier,
                                            error_code: resp.error_code,
                                            error_message: resp.error_message,
                                            payload: resp.payload,
                                            metadata: resp.metadata,
                                            processing_instructions: resp.processing_instructions,
                                            request_identifier: resp.request_identifier,
                                        },
                                    ),
                                ),
                            };
                            let _ = entry.value().try_send(Ok(initial_result));
                        }
                    }
                    Some(pb::query_handler_outbound::Request::QueryComplete(complete)) => {
                        pending_queries.remove(&complete.request_id);
                        // Initial result delivered (or never coming); drop the subscriber sender.
                        pending_sub_initials.remove(&complete.request_id);
                    }
                    Some(pb::query_handler_outbound::Request::SubscriptionQueryResponse(resp)) => {
                        let sub_id = resp.subscription_identifier.clone();
                        let update = proto_subscription_response_to_update(resp);
                        platform.send_update(&sub_id, update);
                    }
                    Some(pb::query_handler_outbound::Request::Ack(_)) => {}
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
                    for query in &subscribed_queries {
                        platform.unsubscribe_query(query, &client);
                    }
                    tracing::info!(
                        client_id = %cid,
                        queries = subscribed_queries.len(),
                        "query handler: unsubscribed queries for this stream"
                    );

                    channel_registry
                        .broadcast_topology_notification(platform_pb::TopologyNotification {
                            change_type: "handler_deregistered".to_string(),
                            message_type: String::new(),
                            handler_kind: "query".to_string(),
                            client_id: cid.clone(),
                            component_name: String::new(),
                        })
                        .await;
                } else {
                    tracing::info!(
                        client_id = %cid,
                        "query handler: skipping cleanup (platform stream cascaded)"
                    );
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(handler_rx)))
    }

    async fn query(
        &self,
        request: Request<pb::QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let context = request
            .metadata()
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
                payload_type: req
                    .payload
                    .as_ref()
                    .map(|p| p.r#type.clone())
                    .unwrap_or_default(),
                revision: req
                    .payload
                    .as_ref()
                    .map(|p| p.revision.clone())
                    .unwrap_or_default(),
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
        self.pending_queries.insert(message_id.clone(), response_tx);

        // Deliver the query to each target handler.
        for target_client_id in &pending.target_handlers {
            let handler_tx = self
                .handler_streams
                .get(&target_client_id.0)
                .map(|r| r.value().clone());

            if let Some(tx) = handler_tx {
                let inbound_query = pb::QueryHandlerInbound {
                    request: Some(pb::query_handler_inbound::Request::Query(
                        pb::QueryRequest {
                            message_identifier: message_id.clone(),
                            query: query_name.clone(),
                            timestamp: pending.query.timestamp,
                            payload: Some(crate::proto::kronosdb::SerializedObject {
                                r#type: pending.query.payload.payload_type.clone(),
                                revision: pending.query.payload.revision.clone(),
                                data: pending.query.payload.data.clone(),
                            }),
                            metadata: internal_metadata_to_proto(&pending.query.metadata),
                            processing_instructions: internal_pi_to_proto(
                                &pending.query.processing_instructions,
                            ),
                            client_id: pending.query.client_id.0.clone(),
                            component_name: pending.query.component_name.0.clone(),
                        },
                    )),
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
            pending_queries.remove(&msg_id);
        });

        Ok(Response::new(ReceiverStream::new(caller_rx)))
    }

    async fn subscription(
        &self,
        request: Request<Streaming<pb::SubscriptionQueryRequest>>,
    ) -> Result<Response<Self::SubscriptionStream>, Status> {
        let context = request
            .metadata()
            .get(CONTEXT_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(DEFAULT_CONTEXT)
            .to_string();
        let platform = self.messaging.get_platform(&context);

        let mut inbound = request.into_inner();
        let (sub_tx, sub_rx) = mpsc::channel::<Result<pb::SubscriptionQueryResponse, Status>>(64);
        let handler_streams = Arc::clone(&self.handler_streams);
        let pending_sub_initials = Arc::clone(&self.pending_sub_initials);

        tokio::spawn(async move {
            while let Ok(Some(msg)) = inbound.message().await {
                match msg.request {
                    Some(pb::subscription_query_request::Request::Subscribe(sub)) => {
                        let sub_id = sub.subscription_identifier.clone();
                        let sub_permits = sub.number_of_permits;

                        let subscription = SubscriptionQuery {
                            subscription_id: sub_id.clone(),
                            query_name: sub
                                .query_request
                                .as_ref()
                                .map(|q| q.query.clone())
                                .unwrap_or_default(),
                            timestamp: sub.query_request.as_ref().map(|q| q.timestamp).unwrap_or(0),
                            payload: Payload {
                                payload_type: sub
                                    .query_request
                                    .as_ref()
                                    .and_then(|q| q.payload.as_ref())
                                    .map(|p| p.r#type.clone())
                                    .unwrap_or_default(),
                                revision: sub
                                    .query_request
                                    .as_ref()
                                    .and_then(|q| q.payload.as_ref())
                                    .map(|p| p.revision.clone())
                                    .unwrap_or_default(),
                                data: sub
                                    .query_request
                                    .as_ref()
                                    .and_then(|q| q.payload.as_ref())
                                    .map(|p| p.data.clone())
                                    .unwrap_or_default(),
                            },
                            metadata: sub
                                .query_request
                                .as_ref()
                                .map(|q| proto_metadata_to_internal(q.metadata.clone()))
                                .unwrap_or_default(),
                            client_id: ClientId(
                                sub.query_request
                                    .as_ref()
                                    .map(|q| q.client_id.clone())
                                    .unwrap_or_default(),
                            ),
                            component_name: ComponentName(
                                sub.query_request
                                    .as_ref()
                                    .map(|q| q.component_name.clone())
                                    .unwrap_or_default(),
                            ),
                            initial_permits: sub.number_of_permits,
                        };

                        match platform.subscribe(subscription) {
                            Ok((pending, mut update_rx)) => {
                                // Register so the initial QueryResponse from the handler
                                // can be re-routed onto this subscription stream as
                                // SubscriptionQueryResponse::InitialResult.
                                pending_sub_initials.insert(sub_id.clone(), sub_tx.clone());

                                // Deliver the initial query to the handler.
                                let handler_tx = pending.target_handlers.first().and_then(|id| {
                                    handler_streams.get(&id.0).map(|r| r.value().clone())
                                });

                                if let Some(tx) = handler_tx {
                                    // Send as a SubscriptionQueryRequest so the handler
                                    // knows to register for updates (not just a regular query).
                                    let inbound_sub = pb::QueryHandlerInbound {
                                        request: Some(pb::query_handler_inbound::Request::SubscriptionQueryRequest(
                                            pb::SubscriptionQueryRequest {
                                                request: Some(pb::subscription_query_request::Request::Subscribe(
                                                    pb::SubscriptionQuery {
                                                        subscription_identifier: sub_id.clone(),
                                                        number_of_permits: sub_permits,
                                                        query_request: Some(pb::QueryRequest {
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
                                                        }),
                                                    }
                                                )),
                                            }
                                        )),
                                        instruction_id: String::new(),
                                    };
                                    let _ = tx.send(Ok(inbound_sub)).await;
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
                                let _ = sub_tx.send(Err(Status::unavailable(e.to_string()))).await;
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

fn proto_subscription_response_to_update(
    resp: pb::SubscriptionQueryResponse,
) -> SubscriptionUpdate {
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
            error_code: if upd.error_code.is_empty() {
                None
            } else {
                Some(upd.error_code)
            },
            error: upd.error_message.map(proto_error_to_detail),
        },
        Some(pb::subscription_query_response::Response::InitialResult(result)) => {
            SubscriptionUpdate {
                subscription_id: sub_id,
                payload: result.payload.map(|p| Payload {
                    payload_type: p.r#type,
                    revision: p.revision,
                    data: p.data,
                }),
                metadata: proto_metadata_to_internal(result.metadata),
                error_code: if result.error_code.is_empty() {
                    None
                } else {
                    Some(result.error_code)
                },
                error: result.error_message.map(proto_error_to_detail),
            }
        }
        Some(pb::subscription_query_response::Response::CompleteExceptionally(err)) => {
            SubscriptionUpdate {
                subscription_id: sub_id,
                payload: None,
                metadata: HashMap::new(),
                error_code: Some(err.error_code),
                error: err.error_message.map(proto_error_to_detail),
            }
        }
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
        Some(
            pb::subscription_query_response::Response::CompleteExceptionally(
                pb::QueryUpdateCompleteExceptionally {
                    client_id: String::new(),
                    component_name: String::new(),
                    error_code: error_code.clone(),
                    error_message: update.error.as_ref().map(detail_to_proto_error),
                },
            ),
        )
    } else {
        Some(pb::subscription_query_response::Response::Update(
            pb::QueryUpdate {
                message_identifier: update.subscription_id.clone(),
                payload: update
                    .payload
                    .map(|p| crate::proto::kronosdb::SerializedObject {
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
