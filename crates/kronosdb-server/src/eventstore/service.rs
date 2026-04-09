use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use kronosdb_eventstore::api::EventStore;
use kronosdb_eventstore::append::{AppendCondition, AppendRequest};
use kronosdb_eventstore::criteria::{Criterion, SourcingCondition};
use kronosdb_eventstore::event::{AppendEvent, Position, Tag};
use kronosdb_eventstore::error::Error;
use kronosdb_raft::cluster::ClusterManager;

use crate::proto::kronosdb::eventstore as pb;
use crate::proto::kronosdb::eventstore::event_store_server::EventStoreServer as GrpcEventStoreServer;

/// Default context name when no `kronosdb-context` header is provided.
const DEFAULT_CONTEXT: &str = "default";

/// gRPC metadata header key for context routing.
const CONTEXT_HEADER: &str = "kronosdb-context";

/// gRPC service implementation for the event store.
///
/// Routes requests to the correct context based on the `kronosdb-context`
/// gRPC metadata header. Defaults to "default" if not provided.
///
/// Uses `ClusterManager` to get the event store — which returns either a raw
/// `EventStoreEngine` (standalone) or a `RaftEventStore` decorator (clustered).
///
/// All engine calls are dispatched to `spawn_blocking` to avoid
/// blocking the tokio async worker threads with synchronous file I/O.
pub struct EventStoreService {
    cluster: Arc<ClusterManager>,
}

impl EventStoreService {
    pub fn new(cluster: Arc<ClusterManager>) -> Self {
        Self { cluster }
    }

    pub fn into_server(self) -> GrpcEventStoreServer<Self> {
        GrpcEventStoreServer::new(self)
    }

    /// Extracts the context name from gRPC request metadata.
    fn extract_context<T>(request: &Request<T>) -> &str {
        request
            .metadata()
            .get(CONTEXT_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or(DEFAULT_CONTEXT)
    }

    /// Gets an event store for the context (raw engine or Raft decorator).
    fn get_store(&self, context_name: &str) -> Result<Arc<dyn EventStore>, Status> {
        self.cluster
            .get_store(context_name)
            .map_err(to_status)
    }
}

#[tonic::async_trait]
impl pb::event_store_server::EventStore for EventStoreService {
    type SourceStream = ReceiverStream<Result<pb::SourceResponse, Status>>;
    type StreamStream = ReceiverStream<Result<pb::StreamResponse, Status>>;

    async fn append(
        &self,
        request: Request<Streaming<pb::AppendRequest>>,
    ) -> Result<Response<pb::AppendResponse>, Status> {
        let context_name = Self::extract_context(&request).to_string();
        let mut stream = request.into_inner();
        let mut all_events = Vec::new();
        let mut condition = None;

        while let Some(msg) = stream.message().await? {
            if msg.condition.is_some() && condition.is_none() {
                condition = msg.condition.map(|c| from_proto_condition(c));
            }
            for tagged_event in msg.events {
                all_events.push(from_proto_tagged_event(tagged_event));
            }
        }

        let request = AppendRequest {
            condition,
            events: all_events,
        };

        let store = self.get_store(&context_name)?;
        let response = tokio::task::spawn_blocking(move || {
            store.append(request)
        })
        .await
        .map_err(|e| Status::internal(format!("task join error: {e}")))?
        .map_err(to_status)?;

        Ok(Response::new(pb::AppendResponse {
            first_sequence: response.first_position.0 as i64,
            count: response.count as i32,
            consistency_marker: response.consistency_marker.0 as i64,
        }))
    }

    async fn source(
        &self,
        request: Request<pb::SourceRequest>,
    ) -> Result<Response<Self::SourceStream>, Status> {
        let context_name = Self::extract_context(&request).to_string();
        let req = request.into_inner();
        let from_position = Position(req.from_sequence as u64);
        let condition = from_proto_criteria(req.criteria);

        let store = self.get_store(&context_name)?;
        let condition_clone = condition.clone();
        let (events, committed) = tokio::task::spawn_blocking(move || {
            let events = store.source(from_position, &condition_clone)?;
            let head = store.head();
            let committed = if head.0 > 0 { head.0 - 1 } else { 0 };
            Ok::<_, Error>((events, committed))
        })
        .await
        .map_err(|e| Status::internal(format!("task join error: {e}")))?
        .map_err(to_status)?;

        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            for event in events {
                let response = pb::SourceResponse {
                    result: Some(pb::source_response::Result::Event(to_proto_sequenced_event(
                        &event,
                    ))),
                };
                if tx.send(Ok(response)).await.is_err() {
                    return;
                }
            }
            let response = pb::SourceResponse {
                result: Some(pb::source_response::Result::ConsistencyMarker(
                    committed as i64,
                )),
            };
            let _ = tx.send(Ok(response)).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn stream(
        &self,
        request: Request<pb::StreamRequest>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let context_name = Self::extract_context(&request).to_string();
        let req = request.into_inner();
        let from_position = Position(req.from_sequence as u64);
        let condition = from_proto_criteria(req.criteria);

        let store = self.get_store(&context_name)?;
        let mut event_stream = store.subscribe(from_position, condition.clone());

        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            // First: send historical events.
            let historical = {
                let store2 = Arc::clone(&store);
                let condition = condition.clone();
                let cursor = event_stream.cursor;
                tokio::task::spawn_blocking(move || {
                    store2.source(cursor, &condition)
                })
                .await
            };

            match historical {
                Ok(Ok(events)) => {
                    for event in &events {
                        let response = pb::StreamResponse {
                            event: Some(to_proto_sequenced_event(event)),
                        };
                        if tx.send(Ok(response)).await.is_err() {
                            return;
                        }
                    }
                    if let Some(last) = events.last() {
                        event_stream.advance_cursor(Position(last.position.0 + 1));
                    }
                }
                Ok(Err(e)) => {
                    let _ = tx.send(Err(to_status(e))).await;
                    return;
                }
                Err(e) => {
                    let _ = tx.send(Err(Status::internal(format!("task join error: {e}")))).await;
                    return;
                }
            }

            // Then: live tail.
            loop {
                let _committed = event_stream.wait_for_new_events().await;

                let new_events = {
                    let store2 = Arc::clone(&store);
                    let condition = condition.clone();
                    let cursor = event_stream.cursor;
                    tokio::task::spawn_blocking(move || {
                        store2.source(cursor, &condition)
                    })
                    .await
                };

                match new_events {
                    Ok(Ok(events)) => {
                        for event in &events {
                            let response = pb::StreamResponse {
                                event: Some(to_proto_sequenced_event(event)),
                            };
                            if tx.send(Ok(response)).await.is_err() {
                                return;
                            }
                        }
                        if let Some(last) = events.last() {
                            event_stream.advance_cursor(Position(last.position.0 + 1));
                        }
                    }
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(to_status(e))).await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(Status::internal(format!("task join error: {e}")))).await;
                        return;
                    }
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_head(
        &self,
        request: Request<pb::GetHeadRequest>,
    ) -> Result<Response<pb::GetHeadResponse>, Status> {
        let context_name = Self::extract_context(&request).to_string();
        let store = self.get_store(&context_name)?;
        let head = tokio::task::spawn_blocking(move || {
            store.head()
        })
        .await
        .map_err(|e| Status::internal(format!("task join error: {e}")))?;

        Ok(Response::new(pb::GetHeadResponse {
            sequence: head.0 as i64,
        }))
    }

    async fn get_tail(
        &self,
        request: Request<pb::GetTailRequest>,
    ) -> Result<Response<pb::GetTailResponse>, Status> {
        let context_name = Self::extract_context(&request).to_string();
        let store = self.get_store(&context_name)?;
        let tail = tokio::task::spawn_blocking(move || {
            store.tail()
        })
        .await
        .map_err(|e| Status::internal(format!("task join error: {e}")))?;

        Ok(Response::new(pb::GetTailResponse {
            sequence: tail.0 as i64,
        }))
    }

    async fn get_tags(
        &self,
        request: Request<pb::GetTagsRequest>,
    ) -> Result<Response<pb::GetTagsResponse>, Status> {
        let context_name = Self::extract_context(&request).to_string();
        let req = request.into_inner();
        let position = Position(req.sequence as u64);

        let store = self.get_store(&context_name)?;
        let tags = tokio::task::spawn_blocking(move || {
            store.get_tags(position)
        })
        .await
        .map_err(|e| Status::internal(format!("task join error: {e}")))?
        .map_err(to_status)?;

        Ok(Response::new(pb::GetTagsResponse {
            tags: tags.into_iter().map(to_proto_tag).collect(),
        }))
    }
}

// --- Type conversions: proto → engine ---

fn from_proto_condition(c: pb::ConsistencyCondition) -> AppendCondition {
    AppendCondition {
        consistency_marker: Position(c.consistency_marker as u64),
        criteria: from_proto_criteria(c.criteria),
    }
}

fn from_proto_criteria(criteria: Vec<pb::Criterion>) -> SourcingCondition {
    SourcingCondition {
        criteria: criteria.into_iter().map(from_proto_criterion).collect(),
    }
}

fn from_proto_criterion(c: pb::Criterion) -> Criterion {
    Criterion {
        names: c.names,
        tags: c.tags.into_iter().map(from_proto_tag).collect(),
    }
}

fn from_proto_tag(t: pb::Tag) -> Tag {
    Tag {
        key: t.key,
        value: t.value,
    }
}

fn from_proto_tagged_event(te: pb::TaggedEvent) -> AppendEvent {
    let event = te.event.unwrap_or_default();
    AppendEvent {
        identifier: event.identifier,
        name: event.name,
        version: event.version,
        timestamp: event.timestamp,
        payload: event.payload,
        metadata: event.metadata.into_iter().collect(),
        tags: te.tags.into_iter().map(from_proto_tag).collect(),
    }
}

// --- Type conversions: engine → proto ---

fn to_proto_sequenced_event(e: &kronosdb_eventstore::event::SequencedEvent) -> pb::SequencedEvent {
    pb::SequencedEvent {
        sequence: e.position.0 as i64,
        event: Some(pb::Event {
            identifier: e.identifier.clone(),
            timestamp: e.timestamp,
            name: e.name.clone(),
            version: e.version.clone(),
            payload: e.payload.clone(),
            metadata: e.metadata.iter().cloned().collect(),
        }),
    }
}

fn to_proto_tag(t: Tag) -> pb::Tag {
    pb::Tag {
        key: t.key,
        value: t.value,
    }
}

// --- Error conversion ---

fn to_status(e: Error) -> Status {
    match e {
        Error::ConsistencyConditionViolated { conflicting_position } => Status::aborted(format!(
            "consistency condition violated: conflicting event at position {}",
            conflicting_position.0
        )),
        Error::Io(err) => Status::internal(format!("I/O error: {err}")),
        Error::Corrupted { message } => Status::internal(format!("data corrupted: {message}")),
        Error::ContextNotFound { name } => Status::not_found(format!("context not found: {name}")),
        Error::ContextAlreadyExists { name } => {
            Status::already_exists(format!("context already exists: {name}"))
        }
        Error::InvalidContextName { name, reason } => {
            Status::invalid_argument(format!("invalid context name '{name}': {reason}"))
        }
        Error::SnapshotNotFound { key } => {
            Status::not_found(format!("snapshot not found: {key}"))
        }
    }
}
