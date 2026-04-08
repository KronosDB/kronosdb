use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use kronosdb_messaging::api::MessagingPlatform;
use kronosdb_messaging::query::Query;
use kronosdb_messaging::subscription::SubscriptionQuery;
use kronosdb_messaging::types::{ClientId, ComponentName, Payload};

use crate::proto::kronosdb::query as pb;
use crate::proto::kronosdb::query::query_service_server::QueryServiceServer as GrpcQueryServiceServer;

type HandlerSender = mpsc::Sender<Result<pb::QueryHandlerInbound, Status>>;

/// Pending query response collectors: request_id → channel to send results to the caller.
type QueryResponseSender = mpsc::Sender<pb::QueryResponse>;
type PendingQueries = Arc<Mutex<HashMap<String, QueryResponseSender>>>;

/// gRPC service implementation for the query bus.
pub struct QueryServiceImpl {
    platform: Arc<dyn MessagingPlatform>,
    handler_streams: Arc<Mutex<HashMap<String, HandlerSender>>>,
    /// Pending query dispatches: request_id → channel for collecting handler responses.
    pending_queries: PendingQueries,
    /// Active subscription queries: subscription_id → channel for sending updates to subscriber.
    active_subscriptions: Arc<Mutex<HashMap<String, mpsc::Sender<Result<pb::SubscriptionQueryResponse, Status>>>>>,
}

impl QueryServiceImpl {
    pub fn new(platform: Arc<dyn MessagingPlatform>) -> Self {
        Self {
            platform,
            handler_streams: Arc::new(Mutex::new(HashMap::new())),
            pending_queries: Arc::new(Mutex::new(HashMap::new())),
            active_subscriptions: Arc::new(Mutex::new(HashMap::new())),
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
        let mut inbound = request.into_inner();
        let (handler_tx, handler_rx) = mpsc::channel::<Result<pb::QueryHandlerInbound, Status>>(128);

        let platform = Arc::clone(&self.platform);
        let handler_streams = Arc::clone(&self.handler_streams);
        let pending_queries = Arc::clone(&self.pending_queries);
        let active_subscriptions = Arc::clone(&self.active_subscriptions);
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
                        // Route subscription update to the subscriber.
                        let sub_id = resp.subscription_identifier.clone();
                        let subs = active_subscriptions.lock();
                        if let Some(tx) = subs.get(&sub_id) {
                            let _ = tx.try_send(Ok(resp));
                        }
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
            metadata: extract_text_metadata(req.metadata),
            client_id: ClientId(req.client_id),
            component_name: ComponentName(req.component_name),
            expected_results: -1,
        };

        let query_name = query.name.clone();
        let pending = self
            .platform
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
                        metadata: Default::default(),
                        processing_instructions: vec![],
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

        tokio::spawn(async move {
            while let Some(resp) = response_rx.recv().await {
                if caller_tx.send(Ok(resp)).await.is_err() {
                    break; // Caller disconnected.
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
        let mut inbound = request.into_inner();
        let (sub_tx, sub_rx) = mpsc::channel::<Result<pb::SubscriptionQueryResponse, Status>>(64);

        let platform = Arc::clone(&self.platform);
        let active_subscriptions = Arc::clone(&self.active_subscriptions);

        tokio::spawn(async move {
            while let Ok(Some(msg)) = inbound.message().await {
                match msg.request {
                    Some(pb::subscription_query_request::Request::Subscribe(sub)) => {
                        let sub_id = sub.subscription_identifier.clone();

                        // Register the subscriber's channel.
                        {
                            let mut subs = active_subscriptions.lock();
                            subs.insert(sub_id.clone(), sub_tx.clone());
                        }

                        // Open the subscription via the platform.
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
                            metadata: vec![],
                            client_id: ClientId(sub.query_request.as_ref().map(|q| q.client_id.clone()).unwrap_or_default()),
                            component_name: ComponentName(sub.query_request.as_ref().map(|q| q.component_name.clone()).unwrap_or_default()),
                            initial_permits: sub.number_of_permits,
                        };

                        if let Err(e) = platform.subscribe(subscription) {
                            let _ = sub_tx
                                .send(Err(Status::unavailable(e.to_string())))
                                .await;
                        }
                    }
                    Some(pb::subscription_query_request::Request::Unsubscribe(sub)) => {
                        let sub_id = sub.subscription_identifier;
                        platform.cancel_subscription(&sub_id);
                        active_subscriptions.lock().remove(&sub_id);
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
