use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use kronosdb_eventstore::api::EventStore;
use kronosdb_eventstore::append::{AppendCondition, AppendRequest};
use kronosdb_eventstore::criteria::{Criterion, SourcingCondition};
use kronosdb_eventstore::error::Error;
use kronosdb_eventstore::event::{AppendEvent, Position, Tag};
use kronosdb_eventstore::raft::cluster::ClusterManager;

use crate::proto::kronosdb::eventstore as pb;

/// Default context name when no `kronosdb-context` header is provided.
const DEFAULT_CONTEXT: &str = "default";

/// gRPC metadata header key for context routing.
const CONTEXT_HEADER: &str = "kronosdb-context";

/// One commit range, converted to protobuf ONCE and shared (via `Arc`)
/// across every match-all subscriber on the context. Without this, N
/// subscribers each re-read the engine and re-convert the same events on
/// every commit — the dominant CPU cost at high fan-out.
struct HubBatch {
    /// First position covered by this batch (events may start later if the
    /// range ends with non-events, but never earlier).
    first: u64,
    /// Next-exclusive position after this batch: subscribers advance their
    /// cursor here after consuming.
    next: u64,
    events: Vec<pb::SequencedEvent>,
}

type HubSender = tokio::sync::broadcast::Sender<Arc<HubBatch>>;

/// Buffered hub batches per subscriber before it counts as lagged and
/// falls back to engine catch-up. Batches are Arc-shared, so capacity
/// costs pointers, not event copies.
const HUB_CAPACITY: usize = 64;

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
    /// Per-context fan-out hubs (created lazily by the first match-all
    /// subscriber). The hub task removes its entry and drops its sender on
    /// engine shutdown, which propagates `Closed` to subscribers.
    hubs: Arc<std::sync::Mutex<std::collections::HashMap<String, HubSender>>>,
}

impl EventStoreService {
    pub fn new(cluster: Arc<ClusterManager>) -> Self {
        Self {
            cluster,
            hubs: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
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
    // tonic's `Status` fixes the large error type.
    #[allow(clippy::result_large_err)]
    fn get_store(&self, context_name: &str) -> Result<Arc<dyn EventStore>, Status> {
        self.cluster.get_store(context_name).map_err(to_status)
    }

    /// Returns the context's fan-out hub, spawning it on first use.
    fn get_or_spawn_hub(&self, context_name: &str, store: &Arc<dyn EventStore>) -> HubSender {
        let mut hubs = self.hubs.lock().expect("hub map poisoned");
        if let Some(tx) = hubs.get(context_name) {
            return tx.clone();
        }
        let (tx, _) = tokio::sync::broadcast::channel(HUB_CAPACITY);
        hubs.insert(context_name.to_string(), tx.clone());
        tokio::spawn(run_fanout_hub(
            Arc::clone(store),
            tx.clone(),
            Arc::clone(&self.hubs),
            context_name.to_string(),
        ));
        tx
    }
}

/// The per-context fan-out hub: tails the engine from its creation head and
/// publishes each commit range as ONE protobuf conversion, shared by all
/// match-all subscribers. With zero subscribers it skips the read+convert
/// entirely (cursor jump only), so an idle hub costs nothing per commit.
async fn run_fanout_hub(
    store: Arc<dyn EventStore>,
    tx: HubSender,
    hubs: Arc<std::sync::Mutex<std::collections::HashMap<String, HubSender>>>,
    context_name: String,
) {
    const PAGE: usize = 8192;
    let condition = from_proto_read_criteria_empty();
    let mut stream = store.subscribe(store.head(), condition.clone());
    loop {
        let bound = Position(stream.wait_for_new_events().await);
        if bound.0 <= stream.cursor.0 {
            // Non-advancing bound: engine commit channel closed (shutdown).
            hubs.lock().expect("hub map poisoned").remove(&context_name);
            return; // Dropping tx closes subscriber receivers.
        }
        if tx.receiver_count() == 0 {
            stream.advance_cursor(bound);
            continue;
        }
        while stream.cursor.0 < bound.0 {
            let cursor = stream.cursor;
            let page = {
                let store2 = Arc::clone(&store);
                let condition = condition.clone();
                tokio::task::spawn_blocking(move || {
                    store2.source_page(cursor, &condition, bound, PAGE)
                })
                .await
            };
            let events = match page {
                Ok(Ok(events)) => events,
                _ => {
                    // Engine read failure mid-tail: drop the hub; subscribers
                    // see Closed and their RPCs end (clients reconnect).
                    hubs.lock().expect("hub map poisoned").remove(&context_name);
                    return;
                }
            };
            let page_len = events.len();
            let next = match events.last() {
                Some(last) if page_len == PAGE => Position(last.position.0 + 1),
                _ => bound,
            };
            let protos: Vec<pb::SequencedEvent> =
                events.iter().map(to_proto_sequenced_event).collect();
            let _ = tx.send(Arc::new(HubBatch {
                first: cursor.0,
                next: next.0,
                events: protos,
            }));
            stream.advance_cursor(next);
        }
    }
}

/// Match-all sourcing condition in the engine's normalized form (one empty
/// criterion — an empty criteria LIST matches nothing).
fn from_proto_read_criteria_empty() -> SourcingCondition {
    SourcingCondition {
        criteria: vec![Criterion {
            names: vec![],
            tags: vec![],
        }],
    }
}

#[tonic::async_trait]
impl pb::event_store_server::EventStore for EventStoreService {
    type SourceStream = ReceiverStream<Result<pb::SourceResponse, Status>>;
    type StreamStream = ReceiverStream<Result<pb::StreamResponse, Status>>;

    async fn append(
        &self,
        request: Request<pb::AppendRequest>,
    ) -> Result<Response<pb::AppendResponse>, Status> {
        let context_name = Self::extract_context(&request).to_string();
        let msg = request.into_inner();

        let request = AppendRequest {
            condition: msg.condition.map(from_proto_condition),
            events: msg
                .events
                .into_iter()
                .map(from_proto_tagged_event)
                .collect(),
        };

        let store = self.get_store(&context_name)?;
        let response = store.append(request).await.map_err(to_status)?;

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
        let batch_size = match req.batch_size as usize {
            0 => 1024, // Server default; 0 means "let the server pick".
            n => n,
        };
        let condition = from_proto_read_criteria(req.criteria);
        // Per-request logging stays below the default level; the criteria
        // strings are only formatted when debug logging is enabled.
        tracing::debug!(
            from = from_position.0,
            criteria_count = condition.criteria.len(),
            criteria = ?condition.criteria.iter().map(|c| format!(
                "names={:?} tags={:?}",
                c.names,
                c.tags.iter().map(|t| format!(
                    "{}={}",
                    String::from_utf8_lossy(&t.key),
                    String::from_utf8_lossy(&t.value)
                )).collect::<Vec<_>>()
            )).collect::<Vec<_>>(),
            "Source request"
        );

        let store = self.get_store(&context_name)?;

        // Freeze the read bound (and thus the consistency marker) up front:
        // head is next-exclusive — exactly the marker the client should use
        // for a subsequent DCB-conditioned append. `head()` reads an atomic;
        // no writer-lock contention on the read path.
        let marker = store.head().0;

        const PAGE: usize = 8192;

        // Fast path for the common shallow read (aggregate rehydration):
        // read the first page inline and, if it both completes the result
        // and fits one message, answer with a single marker-carrying batch —
        // no pager task, no channel round-trips, no extra frames.
        let first_page = {
            let store_page = Arc::clone(&store);
            let condition_page = condition.clone();
            tokio::task::spawn_blocking(move || {
                store_page.source_page(from_position, &condition_page, Position(marker), PAGE)
            })
            .await
            .map_err(|e| Status::internal(format!("task join error: {e}")))?
            .map_err(to_status)?
        };

        if first_page.len() < PAGE && first_page.len() <= batch_size {
            let (tx, rx) = mpsc::channel(1);
            let response = pb::SourceResponse {
                batch: Some(pb::SequencedEventBatch {
                    events: first_page.iter().map(to_proto_sequenced_event).collect(),
                    consistency_marker: Some(marker as i64),
                }),
            };
            // Capacity 1 and exactly one message: this send cannot block.
            let _ = tx.send(Ok(response)).await;
            return Ok(Response::new(ReceiverStream::new(rx)));
        }

        // Bound in-flight memory by EVENTS, not messages: a message is a
        // whole batch, so a fixed message count would let a slow client pin
        // capacity × batch_size events (~250MB at batch 5000) per stream.
        // ~16k buffered events ≈ a few MB regardless of batch size.
        let channel_capacity = (16384 / batch_size).clamp(2, 128);
        let (tx, rx) = mpsc::channel(channel_capacity);

        // Stream in bounded pages instead of materializing the whole result:
        // memory stays flat regardless of log size, and a client dropping
        // the stream stops the remaining work at the next page boundary
        // (an abandoned whole-log scan can't keep burning CPU).
        tokio::spawn(async move {
            let mut page = first_page;
            loop {
                let next_cursor = page.last().map(|e| Position(e.position.0 + 1));
                let page_len = page.len();
                let is_final = page_len < PAGE;
                // Pack what we already have in hand into up-to-batch_size
                // messages. A short batch at the end of a page is sent
                // immediately — never held back for fill. The last chunk of
                // the final page carries the consistency marker; an empty
                // final page sends one empty marker-carrying batch.
                let mut chunks = page.chunks(batch_size).peekable();
                if is_final && chunks.peek().is_none() {
                    let response = pb::SourceResponse {
                        batch: Some(pb::SequencedEventBatch {
                            events: Vec::new(),
                            consistency_marker: Some(marker as i64),
                        }),
                    };
                    let _ = tx.send(Ok(response)).await;
                    return;
                }
                while let Some(chunk) = chunks.next() {
                    let last_of_stream = is_final && chunks.peek().is_none();
                    let response = pb::SourceResponse {
                        batch: Some(pb::SequencedEventBatch {
                            events: chunk.iter().map(to_proto_sequenced_event).collect(),
                            consistency_marker: last_of_stream.then_some(marker as i64),
                        }),
                    };
                    if tx.send(Ok(response)).await.is_err() {
                        return; // Client went away — stop reading further pages.
                    }
                }
                if is_final {
                    return;
                }

                let cursor = match next_cursor {
                    Some(next) => next,
                    None => return, // Unreachable: a full page has a last event.
                };
                let store_page = Arc::clone(&store);
                let condition_page = condition.clone();
                page = match tokio::task::spawn_blocking(move || {
                    store_page.source_page(cursor, &condition_page, Position(marker), PAGE)
                })
                .await
                {
                    Ok(Ok(page)) => page,
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(to_status(e))).await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!("task join error: {e}"))))
                            .await;
                        return;
                    }
                };
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn stream(
        &self,
        request: Request<Streaming<pb::StreamControl>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let context_name = Self::extract_context(&request).to_string();
        let mut inbound = request.into_inner();

        // First inbound message MUST be a Subscribe; protocol violation otherwise.
        let first = inbound
            .message()
            .await
            .map_err(|e| Status::internal(format!("stream recv: {e}")))?
            .ok_or_else(|| Status::invalid_argument("stream closed before subscribe"))?;
        let subscribe = match first.request {
            Some(pb::stream_control::Request::Subscribe(s)) => s,
            Some(pb::stream_control::Request::Permits(_)) => {
                return Err(Status::invalid_argument(
                    "first stream message must be subscribe, got permits",
                ));
            }
            None => return Err(Status::invalid_argument("empty stream control message")),
        };
        if subscribe.initial_permits <= 0 {
            return Err(Status::invalid_argument("initial_permits must be > 0"));
        }

        let from_position = Position(subscribe.from_sequence as u64);
        let condition = from_proto_read_criteria(subscribe.criteria);
        let blacklist: std::collections::HashSet<String> =
            subscribe.blacklisted_names.into_iter().collect();
        let batch_size = match subscribe.batch_size as usize {
            0 => 1024, // Server default; 0 means "let the server pick".
            n => n,
        };
        let store = self.get_store(&context_name)?;
        let mut event_stream = store.subscribe(from_position, condition.clone());

        // Bound in-flight memory by events, not messages (see `source`).
        let channel_capacity = (16384 / batch_size).clamp(2, 128);
        let (tx, rx) = mpsc::channel(channel_capacity);
        let permits = Arc::new(tokio::sync::Semaphore::new(
            subscribe.initial_permits as usize,
        ));

        // Task: drain inbound StreamControl messages and add permits.
        let permits_in = Arc::clone(&permits);
        let tx_in = tx.clone();
        tokio::spawn(async move {
            loop {
                match inbound.message().await {
                    Ok(Some(msg)) => match msg.request {
                        Some(pb::stream_control::Request::Permits(p)) if p.permits > 0 => {
                            permits_in.add_permits(p.permits as usize);
                        }
                        Some(pb::stream_control::Request::Permits(_)) => {} // <=0 ignored
                        Some(pb::stream_control::Request::Subscribe(_)) => {
                            let _ = tx_in
                                .send(Err(Status::invalid_argument(
                                    "duplicate subscribe on active stream",
                                )))
                                .await;
                            return;
                        }
                        None => {} // unknown oneof variant — ignore
                    },
                    Ok(None) => return, // client half-closed
                    Err(_) => return,   // transport error
                }
            }
        });

        // Task: 15s heartbeat keep-alive. Spurious heartbeats during event flow
        // are explicitly allowed by the protocol; clients ignore them.
        let tx_hb = tx.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(15));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            tick.tick().await; // first tick fires immediately — skip it
            loop {
                tick.tick().await;
                let response = pb::StreamResponse {
                    result: Some(pb::stream_response::Result::Heartbeat(
                        pb::StreamHeartbeat {},
                    )),
                };
                if tx_hb.send(Ok(response)).await.is_err() {
                    return;
                }
            }
        });

        // Match-all subscriptions (the common event-processor case) ride the
        // shared fan-out hub: the engine read + protobuf conversion happens
        // once per commit for the whole context instead of once per
        // subscriber. Tag/name-criteria subscriptions keep the per-subscriber
        // engine path (criteria can't be evaluated on the wire form — it has
        // no tags).
        let is_match_all = condition
            .criteria
            .iter()
            .all(|c| c.names.is_empty() && c.tags.is_empty());
        let hub_rx = if is_match_all {
            // Subscribe BEFORE snapshotting the replay bound so no commit
            // can fall between replay end and hub attach (overlap is fine —
            // the cursor dedups).
            Some(self.get_or_spawn_hub(&context_name, &store).subscribe())
        } else {
            None
        };

        // Task: producer — paged historical replay, then live tail. Permits
        // count events: each batch acquires one permit per event before send,
        // greedily — the first event of a batch awaits a permit, the rest
        // take only what's immediately available, so a batch never waits to
        // fill. Blacklisted events are dropped without consuming a permit.
        let permits_p = Arc::clone(&permits);
        tokio::spawn(async move {
            // Sends pre-converted events as permitted batches. Returns false
            // when the client is gone (semaphore closed or channel dropped).
            async fn send_protos(
                protos: Vec<pb::SequencedEvent>,
                permits: &tokio::sync::Semaphore,
                tx: &mpsc::Sender<Result<pb::StreamResponse, Status>>,
                batch_size: usize,
            ) -> bool {
                let mut i = 0;
                while i < protos.len() {
                    let Ok(first) = permits.acquire().await else {
                        return false;
                    };
                    first.forget();
                    let cap = batch_size.min(protos.len() - i);
                    let extra = permits.available_permits().min(cap - 1);
                    let n = if extra > 0 {
                        match permits.try_acquire_many(extra as u32) {
                            Ok(p) => {
                                p.forget();
                                1 + extra
                            }
                            Err(_) => 1,
                        }
                    } else {
                        1
                    };
                    let response = pb::StreamResponse {
                        result: Some(pb::stream_response::Result::Batch(
                            pb::SequencedEventBatch {
                                events: protos[i..i + n].to_vec(),
                                // Only final Source batches carry a marker.
                                consistency_marker: None,
                            },
                        )),
                    };
                    if tx.send(Ok(response)).await.is_err() {
                        return false;
                    }
                    i += n;
                }
                true
            }

            async fn send_batched(
                events: &[kronosdb_eventstore::event::SequencedEvent],
                blacklist: &std::collections::HashSet<String>,
                permits: &tokio::sync::Semaphore,
                tx: &mpsc::Sender<Result<pb::StreamResponse, Status>>,
                batch_size: usize,
            ) -> bool {
                let protos: Vec<pb::SequencedEvent> = events
                    .iter()
                    .filter(|e| !blacklist.contains(&e.name))
                    .map(to_proto_sequenced_event)
                    .collect();
                send_protos(protos, permits, tx, batch_size).await
            }

            // Historical replay, paged: memory stays bounded no matter how
            // far behind the subscriber starts, and a dropped subscriber
            // stops the work at the next page.
            const PAGE: usize = 8192;
            let replay_bound = store.head().0;
            loop {
                let cursor = event_stream.cursor;
                if cursor.0 >= replay_bound {
                    break;
                }
                let page = {
                    let store2 = Arc::clone(&store);
                    let condition = condition.clone();
                    tokio::task::spawn_blocking(move || {
                        store2.source_page(cursor, &condition, Position(replay_bound), PAGE)
                    })
                    .await
                };
                let events = match page {
                    Ok(Ok(events)) => events,
                    Ok(Err(e)) => {
                        let _ = tx.send(Err(to_status(e))).await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx
                            .send(Err(Status::internal(format!("task join error: {e}"))))
                            .await;
                        return;
                    }
                };
                let page_len = events.len();
                if !send_batched(&events, &blacklist, &permits_p, &tx, batch_size).await {
                    return;
                }
                match events.last() {
                    Some(last) if page_len == PAGE => {
                        event_stream.advance_cursor(Position(last.position.0 + 1));
                    }
                    Some(last) => {
                        event_stream.advance_cursor(Position(last.position.0 + 1));
                        break; // Short page — replay caught up to the bound.
                    }
                    None => break,
                }
            }

            // Pages [cursor, bound) from the engine and sends it, advancing
            // the cursor. Used by the engine live tail and by hub-lag
            // recovery. Returns false when the stream should end (client
            // gone or error already sent).
            #[allow(clippy::too_many_arguments)]
            async fn engine_catch_up(
                store: &Arc<dyn EventStore>,
                condition: &kronosdb_eventstore::criteria::SourcingCondition,
                event_stream: &mut kronosdb_eventstore::stream::EventStream,
                bound: Position,
                blacklist: &std::collections::HashSet<String>,
                permits: &tokio::sync::Semaphore,
                tx: &mpsc::Sender<Result<pb::StreamResponse, Status>>,
                batch_size: usize,
            ) -> bool {
                const PAGE: usize = 8192;
                while event_stream.cursor.0 < bound.0 {
                    let cursor = event_stream.cursor;
                    let page = {
                        let store2 = Arc::clone(store);
                        let condition = condition.clone();
                        tokio::task::spawn_blocking(move || {
                            store2.source_page(cursor, &condition, bound, PAGE)
                        })
                        .await
                    };
                    let events = match page {
                        Ok(Ok(events)) => events,
                        Ok(Err(e)) => {
                            let _ = tx.send(Err(to_status(e))).await;
                            return false;
                        }
                        Err(e) => {
                            let _ = tx
                                .send(Err(Status::internal(format!("task join error: {e}"))))
                                .await;
                            return false;
                        }
                    };
                    let page_len = events.len();
                    if !send_batched(&events, blacklist, permits, tx, batch_size).await {
                        return false;
                    }
                    match events.last() {
                        Some(last) if page_len == PAGE => {
                            event_stream.advance_cursor(Position(last.position.0 + 1));
                        }
                        _ => {
                            // Short/empty page: everything below the bound is
                            // delivered (or doesn't match). Jump the cursor to
                            // the bound so the gap isn't re-scanned.
                            event_stream.advance_cursor(bound);
                        }
                    }
                }
                true
            }

            if let Some(mut hub_rx) = hub_rx {
                // Live tail via the shared fan-out hub: events arrive already
                // converted; this subscriber only filters (cursor dedup +
                // blacklist), clones its wire copies, and sends within its
                // permit budget. A subscriber that falls > HUB_CAPACITY
                // batches behind gets Lagged and catches up from the engine.
                loop {
                    use tokio::sync::broadcast::error::RecvError;
                    match hub_rx.recv().await {
                        Ok(batch) => {
                            if batch.next <= event_stream.cursor.0 {
                                continue; // Entirely covered by replay/catch-up.
                            }
                            if batch.first > event_stream.cursor.0 {
                                // Safety net: shouldn't happen (rx attaches
                                // before the replay bound is read), but a gap
                                // must never skip events silently.
                                let to = Position(batch.first);
                                if !engine_catch_up(
                                    &store,
                                    &condition,
                                    &mut event_stream,
                                    to,
                                    &blacklist,
                                    &permits_p,
                                    &tx,
                                    batch_size,
                                )
                                .await
                                {
                                    return;
                                }
                            }
                            let cursor = event_stream.cursor.0;
                            let protos: Vec<pb::SequencedEvent> = batch
                                .events
                                .iter()
                                .filter(|e| e.sequence as u64 >= cursor)
                                .filter(|e| {
                                    e.event
                                        .as_ref()
                                        .is_none_or(|ev| !blacklist.contains(&ev.name))
                                })
                                .cloned()
                                .collect();
                            if !send_protos(protos, &permits_p, &tx, batch_size).await {
                                return;
                            }
                            event_stream.advance_cursor(Position(batch.next));
                        }
                        Err(RecvError::Lagged(_)) => {
                            let bound = store.head();
                            if !engine_catch_up(
                                &store,
                                &condition,
                                &mut event_stream,
                                bound,
                                &blacklist,
                                &permits_p,
                                &tx,
                                batch_size,
                            )
                            .await
                            {
                                return;
                            }
                        }
                        Err(RecvError::Closed) => return, // Hub gone (shutdown).
                    }
                }
            }

            // Live tail via per-subscriber engine reads (criteria
            // subscriptions — the engine's tag index does the filtering).
            // Paged like the replay: a permit-starved subscriber whose
            // cursor fell far behind must not force one giant
            // materialization when it finally drains permits.
            loop {
                let bound = Position(event_stream.wait_for_new_events().await);
                if bound.0 <= event_stream.cursor.0 {
                    // wait_for_new_events only returns a non-advancing bound
                    // when the engine's commit channel closed (shutdown).
                    return;
                }
                if !engine_catch_up(
                    &store,
                    &condition,
                    &mut event_stream,
                    bound,
                    &blacklist,
                    &permits_p,
                    &tx,
                    batch_size,
                )
                .await
                {
                    return;
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
        // The visible head, not the true one: clients compare their own
        // cursor against this, and positions they can never read would show
        // as permanent lag and stall any poll-until-caught-up loop.
        let head = tokio::task::spawn_blocking(move || store.visible_head())
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
        let tail = tokio::task::spawn_blocking(move || store.tail())
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
        let tags = tokio::task::spawn_blocking(move || store.get_tags(position))
            .await
            .map_err(|e| Status::internal(format!("task join error: {e}")))?
            .map_err(to_status)?;

        Ok(Response::new(pb::GetTagsResponse {
            tags: tags.into_iter().map(to_proto_tag).collect(),
        }))
    }

    async fn get_sequence_at(
        &self,
        request: Request<pb::GetSequenceAtRequest>,
    ) -> Result<Response<pb::GetSequenceAtResponse>, Status> {
        let context_name = Self::extract_context(&request).to_string();
        let req = request.into_inner();
        let timestamp_millis = req.timestamp;

        let store = self.get_store(&context_name)?;
        let position = tokio::task::spawn_blocking(move || store.get_sequence_at(timestamp_millis))
            .await
            .map_err(|e| Status::internal(format!("task join error: {e}")))?
            .map_err(to_status)?;

        Ok(Response::new(pb::GetSequenceAtResponse {
            sequence: position.map(|p| p.0 as i64).unwrap_or(-1),
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

/// Criteria conversion for reads (Source/Stream), where the proto contract is
/// "if empty, all events are returned". The engine resolves an empty criteria
/// list to no matches, so an empty read filter is normalized to a single empty
/// criterion (no names, no tags), which every index path treats as match-all.
/// Append conditions must NOT use this: an absent/empty condition there means
/// "no conflict check", which the unnormalized conversion already provides.
fn from_proto_read_criteria(criteria: Vec<pb::Criterion>) -> SourcingCondition {
    if criteria.is_empty() {
        return SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![],
            }],
        };
    }
    from_proto_criteria(criteria)
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
        Error::ConsistencyConditionViolated {
            conflicting_position,
        } => Status::aborted(format!(
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
        Error::SnapshotNotFound { key } => Status::not_found(format!("snapshot not found: {key}")),
        Error::ReservedNamespace { detail } => {
            Status::invalid_argument(format!("reserved namespace: {detail}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The Source/Stream proto contract says "if criteria is empty, all events
    /// are returned", but the engine resolves an empty criteria list to zero
    /// matches. Reads must normalize to a single match-all criterion.
    #[test]
    fn empty_read_criteria_normalizes_to_match_all() {
        let cond = from_proto_read_criteria(vec![]);
        assert_eq!(cond.criteria.len(), 1);
        assert!(cond.criteria[0].names.is_empty());
        assert!(cond.criteria[0].tags.is_empty());
    }

    /// Append conditions keep the unnormalized conversion: empty criteria
    /// means "no conflict check", not "conflict with everything".
    #[test]
    fn empty_append_condition_criteria_stays_empty() {
        let cond = from_proto_condition(pb::ConsistencyCondition {
            consistency_marker: 7,
            criteria: vec![],
        });
        assert!(cond.criteria.criteria.is_empty());
        assert_eq!(cond.consistency_marker.0, 7);
    }

    #[test]
    fn non_empty_read_criteria_passes_through() {
        let cond = from_proto_read_criteria(vec![pb::Criterion {
            names: vec!["OrderPlaced".into()],
            tags: vec![],
        }]);
        assert_eq!(cond.criteria.len(), 1);
        assert_eq!(cond.criteria[0].names, vec!["OrderPlaced".to_string()]);
    }
}
