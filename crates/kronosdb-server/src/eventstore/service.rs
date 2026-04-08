use std::sync::Arc;

use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use kronosdb_eventstore::api::EventStore;
use kronosdb_eventstore::append::{AppendCondition, AppendRequest};
use kronosdb_eventstore::criteria::{Criterion, SourcingCondition};
use kronosdb_eventstore::event::{AppendEvent, Position, Tag};
use kronosdb_eventstore::error::Error;

use crate::proto::kronosdb::eventstore as pb;
use crate::proto::kronosdb::eventstore::event_store_server::EventStoreServer as GrpcEventStoreServer;

/// gRPC service implementation for the event store.
///
/// Programs against the `EventStore` trait, not the concrete engine.
/// The inner store is behind Arc<Mutex<>> because append requires &mut self
/// and gRPC handlers are called concurrently.
pub struct EventStoreService {
    store: Arc<Mutex<Box<dyn EventStore>>>,
}

impl EventStoreService {
    pub fn new(store: Box<dyn EventStore>) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
        }
    }

    pub fn into_server(self) -> GrpcEventStoreServer<Self> {
        GrpcEventStoreServer::new(self)
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
        // Collect the streaming request into a single append.
        // The proto defines Append as a client stream, but typically
        // a single AppendRequest contains all events for one transaction.
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

        let response = {
            let mut store = self.store.lock().await;
            store.append(request).map_err(to_status)?
        };

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
        let req = request.into_inner();
        let from_position = Position(req.from_sequence as u64);
        let condition = from_proto_criteria(req.criteria);

        let (events, committed) = {
            let store = self.store.lock().await;
            let events = store.source(from_position, &condition).map_err(to_status)?;
            let head = store.head();
            let committed = if head.0 > 0 { head.0 - 1 } else { 0 };
            (events, committed)
        };

        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            for event in events {
                let response = pb::SourceResponse {
                    result: Some(pb::source_response::Result::Event(to_proto_sequenced_event(
                        &event,
                    ))),
                };
                if tx.send(Ok(response)).await.is_err() {
                    return; // Client disconnected.
                }
            }
            // Send the final consistency marker.
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
        let req = request.into_inner();
        let from_position = Position(req.from_sequence as u64);
        let condition = from_proto_criteria(req.criteria);

        let mut event_stream = {
            let store = self.store.lock().await;
            store.subscribe(from_position, condition.clone())
        };

        let store = Arc::clone(&self.store);
        let (tx, rx) = mpsc::channel(128);

        tokio::spawn(async move {
            // First: send historical events.
            {
                let s = store.lock().await;
                match s.source(event_stream.cursor, &condition) {
                    Ok(events) => {
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
                    Err(e) => {
                        let _ = tx.send(Err(to_status(e))).await;
                        return;
                    }
                }
            }

            // Then: live tail.
            loop {
                let _committed = event_stream.wait_for_new_events().await;

                let events = {
                    let s = store.lock().await;
                    match s.source(event_stream.cursor, &condition) {
                        Ok(events) => events,
                        Err(e) => {
                            let _ = tx.send(Err(to_status(e))).await;
                            return;
                        }
                    }
                };

                for event in &events {
                    let response = pb::StreamResponse {
                        event: Some(to_proto_sequenced_event(event)),
                    };
                    if tx.send(Ok(response)).await.is_err() {
                        return; // Client disconnected.
                    }
                }

                if let Some(last) = events.last() {
                    event_stream.advance_cursor(Position(last.position.0 + 1));
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_head(
        &self,
        _request: Request<pb::GetHeadRequest>,
    ) -> Result<Response<pb::GetHeadResponse>, Status> {
        let store = self.store.lock().await;
        let head = store.head();
        Ok(Response::new(pb::GetHeadResponse {
            sequence: head.0 as i64,
        }))
    }

    async fn get_tail(
        &self,
        _request: Request<pb::GetTailRequest>,
    ) -> Result<Response<pb::GetTailResponse>, Status> {
        let store = self.store.lock().await;
        let tail = store.tail();
        Ok(Response::new(pb::GetTailResponse {
            sequence: tail.0 as i64,
        }))
    }

    async fn get_tags(
        &self,
        request: Request<pb::GetTagsRequest>,
    ) -> Result<Response<pb::GetTagsResponse>, Status> {
        let req = request.into_inner();
        let position = Position(req.sequence as u64);

        let store = self.store.lock().await;
        let tags = store.get_tags(position).map_err(to_status)?;

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
    }
}
