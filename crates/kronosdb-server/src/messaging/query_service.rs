use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use kronosdb_eventstore::raft::cluster::ClusterManager;
use kronosdb_eventstore::raft::handler_registry::{HandlerKind, HandlerRegistration};
use kronosdb_messaging::manager::MessagingManager;
use kronosdb_messaging::query::Query;
use kronosdb_messaging::subscription::{SubscriptionQuery, SubscriptionUpdate};
use kronosdb_messaging::types::{ClientId, ComponentName, Payload};

use super::convert::{
    detail_to_proto_error, effective_timeout, expected_results, internal_metadata_to_proto,
    internal_pi_to_proto, proto_error_to_detail, proto_metadata_to_internal, proto_pi_to_internal,
};
use super::fabric::{
    FabricRouter, PendingQueries, PendingQueryEntry, QueryHandlerStreams, forward_query,
};
use crate::handler_registry::HandlerStreamRegistry;
use crate::platform::service::ClientChannelRegistry;
use crate::proto::kronosdb::platform as platform_pb;
use crate::proto::kronosdb::query as pb;

/// Pending subscription-query initial results: subscription_id → sender on the
/// subscription stream. Routed when a regular QueryResponse comes back tagged
/// with a subscription's id, so the caller receives it as
/// `SubscriptionQueryResponse::InitialResult` on the same stream as updates.
type SubscriptionInitialSender = mpsc::Sender<Result<pb::SubscriptionQueryResponse, Status>>;
type PendingSubscriptionInitials = Arc<DashMap<String, SubscriptionInitialSender>>;

/// gRPC service implementation for the query bus.
///
/// Routes handlers to named messaging buses via the `kronosdb-bus` header.
/// `handler_streams` and `pending_queries` are sharded maps so concurrent
/// queries and response completions don't contend on a single mutex.
pub struct QueryServiceImpl {
    messaging: Arc<MessagingManager>,
    handler_streams: QueryHandlerStreams,
    pending_queries: PendingQueries,
    pending_sub_initials: PendingSubscriptionInitials,
    query_timeout: Duration,
    channel_registry: Arc<ClientChannelRegistry>,
    handler_stream_registry: Arc<HandlerStreamRegistry>,
    cluster: Arc<ClusterManager>,
    fabric: Arc<FabricRouter>,
}

impl QueryServiceImpl {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        messaging: Arc<MessagingManager>,
        query_timeout: Duration,
        channel_registry: Arc<ClientChannelRegistry>,
        handler_stream_registry: Arc<HandlerStreamRegistry>,
        handler_streams: QueryHandlerStreams,
        pending_queries: PendingQueries,
        cluster: Arc<ClusterManager>,
        fabric: Arc<FabricRouter>,
    ) -> Self {
        Self {
            messaging,
            handler_streams,
            pending_queries,
            pending_sub_initials: Arc::new(DashMap::new()),
            query_timeout,
            channel_registry,
            handler_stream_registry,
            cluster,
            fabric,
        }
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
        let bus = super::bus_from_metadata(request.metadata());
        tracing::info!(bus = %bus, "Query OpenStream opened");
        let platform = self.messaging.get_platform(&bus);

        let mut inbound = request.into_inner();
        // 4096 absorbs dispatch bursts without blocking the send.await path.
        let (handler_tx, handler_rx) =
            mpsc::channel::<Result<pb::QueryHandlerInbound, Status>>(4096);

        let handler_streams = Arc::clone(&self.handler_streams);
        let pending_queries = Arc::clone(&self.pending_queries);
        let pending_sub_initials = Arc::clone(&self.pending_sub_initials);
        let channel_registry = Arc::clone(&self.channel_registry);
        let handler_stream_registry = Arc::clone(&self.handler_stream_registry);
        let cluster = Arc::clone(&self.cluster);
        let reg_bus = bus.clone();
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

                        // Replicated routing-table row (ADR-0007); query
                        // fan-out consumes these in the next fabric stage.
                        let registration = HandlerRegistration {
                            bus: reg_bus.clone(),
                            kind: HandlerKind::Query,
                            message_type: query_name.clone(),
                            client_id: sub_client_id.clone(),
                            node_id: cluster.local_node_id(),
                            load_factor: 100,
                        };
                        let reg_cluster = Arc::clone(&cluster);
                        tokio::spawn(async move {
                            if let Err(e) = reg_cluster.register_handler(registration).await {
                                tracing::warn!(error = %e, "fabric: query handler registration write failed");
                            }
                        });

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

                        let dereg_cluster = Arc::clone(&cluster);
                        let dereg_bus = reg_bus.clone();
                        let dereg_type = query_name.clone();
                        let dereg_client = unsub_client_id.clone();
                        tokio::spawn(async move {
                            if let Err(e) = dereg_cluster
                                .deregister_handler(
                                    &dereg_bus,
                                    HandlerKind::Query,
                                    &dereg_type,
                                    &dereg_client,
                                )
                                .await
                            {
                                tracing::warn!(error = %e, "fabric: query handler deregistration write failed");
                            }
                        });

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
                            if let Err(e) = entry.value().sender.try_send(resp.clone()) {
                                tracing::warn!(
                                    request_id = %request_id,
                                    reason = %e,
                                    "query response dropped: caller buffer full or closed"
                                );
                            }
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
                            if let Err(e) = entry.value().try_send(Ok(initial_result)) {
                                tracing::warn!(
                                    request_id = %request_id,
                                    reason = %e,
                                    "subscription initial result dropped: subscriber buffer full or closed"
                                );
                            }
                        }
                    }
                    Some(pb::query_handler_outbound::Request::QueryComplete(complete)) => {
                        // Retire the pending entry only when EVERY targeted
                        // handler has completed — a fast handler's completion
                        // must not drop a slower handler's responses.
                        let retire = pending_queries
                            .get(&complete.request_id)
                            .map(|entry| entry.remaining.fetch_sub(1, Ordering::AcqRel) <= 1)
                            .unwrap_or(false);
                        if retire {
                            pending_queries.remove(&complete.request_id);
                        }
                        // Initial result delivered (or never coming); drop the subscriber sender.
                        pending_sub_initials.remove(&complete.request_id);
                    }
                    Some(pb::query_handler_outbound::Request::SubscriptionQueryResponse(resp)) => {
                        let sub_id = resp.subscription_identifier.clone();
                        let update = proto_subscription_response_to_update(resp);
                        let terminal = update.complete || update.error_code.is_some();
                        platform.send_update(&sub_id, update);
                        if terminal {
                            // Handler signalled Complete/CompleteExceptionally:
                            // the terminal message is in the subscriber's buffer,
                            // now retire the subscription so the update channel
                            // closes and nothing leaks.
                            platform.complete_subscription(&sub_id);
                        }
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
        let bus = super::bus_from_metadata(request.metadata());
        let platform = self.messaging.get_platform(&bus);

        let req = request.into_inner();
        let message_id = req.message_identifier.clone();

        // Fan-out plan from the replicated routing table (ADR-0007):
        // scatter-gather spans every node with registered handlers;
        // point-to-point is locality-preferred.
        let instructions = proto_pi_to_internal(req.processing_instructions.clone());
        let point_to_point = expected_results(&instructions) == 1;
        let fanout = self.fabric.route_query(&bus, &req.query, point_to_point);
        tracing::debug!(
            query = %req.query,
            local = fanout.local,
            remote_nodes = fanout.remote.len(),
            "query fanout"
        );
        let remote_proto = if fanout.remote.is_empty() {
            None
        } else {
            Some(req.clone())
        };
        let query_timeout = effective_timeout(&instructions, self.query_timeout);

        let query = from_proto_query(req);
        let query_name = query.name.clone();

        // Local dispatch. A local failure (no handler / no permits) is
        // tolerated when remote targets exist — scatter-gather is
        // best-effort per handler, and point-to-point remote-only routes
        // skip the local bus entirely.
        let local_pending = if fanout.local || fanout.remote.is_empty() {
            match platform.dispatch_query(query) {
                Ok(pending) => Some(pending),
                Err(e) if !fanout.remote.is_empty() => {
                    tracing::debug!(query = %query_name, error = %e, "local query dispatch skipped");
                    None
                }
                Err(e) => return Err(Status::unavailable(e.to_string())),
            }
        } else {
            None
        };

        // Local response collector, registered before delivery. The map
        // holds the only sender, so the channel closes when the entry
        // retires (all local handlers completed).
        let (response_tx, mut response_rx) = mpsc::channel::<pb::QueryResponse>(64);
        if let Some(ref pending) = local_pending {
            self.pending_queries.insert(
                message_id.clone(),
                Arc::new(PendingQueryEntry {
                    sender: response_tx,
                    remaining: AtomicUsize::new(pending.target_handlers.len()),
                }),
            );

            // Deliver the query to each local target handler.
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
        }
        // No local dispatch → the collector channel is already closed.

        // Merge local + remote responses into the caller's stream. The
        // stream ends when every source finishes (all caller_tx clones
        // dropped) — each source under the shared deadline.
        let (caller_tx, caller_rx) = mpsc::channel(64);
        let deadline = tokio::time::Instant::now() + query_timeout;

        for (node_id, targets) in fanout.remote {
            let caller_tx = caller_tx.clone();
            let cluster = Arc::clone(&self.cluster);
            let forward_bus = bus.clone();
            let proto = remote_proto.clone().expect("remote fanout implies proto");
            tokio::spawn(async move {
                let stream = forward_query(
                    &cluster,
                    node_id,
                    &forward_bus,
                    targets,
                    proto,
                    query_timeout,
                )
                .await;
                let mut stream = match stream {
                    Ok(stream) => stream,
                    Err(status) => {
                        tracing::warn!(node = node_id, error = %status, "fabric: query forward failed");
                        if point_to_point {
                            // Single-target route: surface the failure
                            // instead of an empty stream.
                            let _ = caller_tx.send(Err(status)).await;
                        }
                        return;
                    }
                };
                loop {
                    match tokio::time::timeout_at(deadline, stream.message()).await {
                        Ok(Ok(Some(resp))) => {
                            if caller_tx.send(Ok(resp)).await.is_err() {
                                break; // Caller disconnected.
                            }
                        }
                        Ok(Ok(None)) => break, // Remote node done.
                        Ok(Err(status)) => {
                            tracing::warn!(node = node_id, error = %status, "fabric: query forward stream error");
                            if point_to_point {
                                let _ = caller_tx.send(Err(status)).await;
                            }
                            break;
                        }
                        Err(_) => break, // Deadline.
                    }
                }
            });
        }

        let pending_queries = Arc::clone(&self.pending_queries);
        let msg_id = message_id.clone();
        let local_active = local_pending.is_some();
        tokio::spawn(async move {
            loop {
                match tokio::time::timeout_at(deadline, response_rx.recv()).await {
                    Ok(Some(resp)) => {
                        if caller_tx.send(Ok(resp)).await.is_err() {
                            break; // Caller disconnected.
                        }
                    }
                    Ok(None) => break, // All local handlers done (channel closed).
                    Err(_) => {
                        // Timeout — send error and stop collecting.
                        if local_active {
                            let _ = caller_tx
                                .send(Err(Status::deadline_exceeded("query response timed out")))
                                .await;
                        }
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
        let bus = super::bus_from_metadata(request.metadata());
        let platform = self.messaging.get_platform(&bus);

        let mut inbound = request.into_inner();
        let (sub_tx, sub_rx) = mpsc::channel::<Result<pb::SubscriptionQueryResponse, Status>>(64);
        let handler_streams = Arc::clone(&self.handler_streams);
        let pending_sub_initials = Arc::clone(&self.pending_sub_initials);

        tokio::spawn(async move {
            // Subscriptions opened on THIS gRPC stream — cancelled when the
            // subscriber's stream closes, so handler-side registrations and
            // pending-initial entries don't outlive the subscriber.
            let mut stream_subs: Vec<String> = Vec::new();
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
                                stream_subs.push(sub_id.clone());
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
                                        let terminal =
                                            update.complete || update.error_code.is_some();
                                        let resp = subscription_update_to_proto(update);
                                        if sub_tx.send(Ok(resp)).await.is_err() {
                                            break; // Subscriber disconnected.
                                        }
                                        if terminal {
                                            break; // Complete/CompleteExceptionally sent.
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
                        stream_subs.retain(|s| s != &sub_id);
                        pending_sub_initials.remove(&sub_id);
                        if let Some(handler_id) = platform.cancel_subscription(&sub_id) {
                            notify_handler_unsubscribe(&handler_streams, &handler_id.0, &sub_id)
                                .await;
                        }
                    }
                    Some(pb::subscription_query_request::Request::FlowControl(fc)) => {
                        platform.grant_subscription_permits(
                            &fc.subscription_identifier,
                            fc.number_of_permits,
                        );
                    }
                    None => {}
                }
            }

            // Subscriber stream closed: retire everything it opened so the
            // handler stops emitting into a void and no pending-initial
            // senders leak.
            for sub_id in stream_subs {
                pending_sub_initials.remove(&sub_id);
                if let Some(handler_id) = platform.cancel_subscription(&sub_id) {
                    notify_handler_unsubscribe(&handler_streams, &handler_id.0, &sub_id).await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(sub_rx)))
    }
}

/// Converts a proto query request into the internal representation.
pub(crate) fn from_proto_query(req: pb::QueryRequest) -> Query {
    let processing_instructions = proto_pi_to_internal(req.processing_instructions);
    Query {
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
        client_id: ClientId(req.client_id),
        component_name: ComponentName(req.component_name),
        // NrOfResults == 1 selects point-to-point routing; everything
        // else is scatter-gather.
        expected_results: expected_results(&processing_instructions),
        processing_instructions,
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
            complete: false,
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
                complete: false,
            }
        }
        Some(pb::subscription_query_response::Response::CompleteExceptionally(err)) => {
            SubscriptionUpdate {
                subscription_id: sub_id,
                payload: None,
                metadata: HashMap::new(),
                error_code: Some(err.error_code),
                error: err.error_message.map(proto_error_to_detail),
                complete: true,
            }
        }
        Some(pb::subscription_query_response::Response::Complete(_)) => SubscriptionUpdate {
            subscription_id: sub_id,
            payload: None,
            metadata: HashMap::new(),
            error_code: None,
            error: None,
            complete: true,
        },
        None => SubscriptionUpdate {
            subscription_id: sub_id,
            payload: None,
            metadata: HashMap::new(),
            error_code: None,
            error: None,
            complete: false,
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
    } else if update.complete {
        Some(pb::subscription_query_response::Response::Complete(
            pb::QueryUpdateComplete {
                client_id: String::new(),
                component_name: String::new(),
            },
        ))
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

/// Forwards an Unsubscribe to the handler's open stream so its Axon-side
/// update-emitter registration can be closed (the subscriber is gone).
async fn notify_handler_unsubscribe(
    handler_streams: &DashMap<String, mpsc::Sender<Result<pb::QueryHandlerInbound, Status>>>,
    handler_client_id: &str,
    subscription_id: &str,
) {
    let tx = handler_streams
        .get(handler_client_id)
        .map(|r| r.value().clone());
    if let Some(tx) = tx {
        let msg = pb::QueryHandlerInbound {
            request: Some(
                pb::query_handler_inbound::Request::SubscriptionQueryRequest(
                    pb::SubscriptionQueryRequest {
                        request: Some(pb::subscription_query_request::Request::Unsubscribe(
                            pb::SubscriptionQuery {
                                subscription_identifier: subscription_id.to_string(),
                                number_of_permits: 0,
                                query_request: None,
                            },
                        )),
                    },
                ),
            ),
            instruction_id: String::new(),
        };
        let _ = tx.send(Ok(msg)).await;
    }
}
