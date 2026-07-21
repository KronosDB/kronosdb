//! Leader-side implementation of the SegmentReplication Tail service.

use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use tokio::sync::{Notify, mpsc};
use tokio_stream::Stream;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::append::{AppendCondition, AppendRequest};
use crate::context::ContextManager;
use crate::criteria::{Criterion, SourcingCondition};
use crate::event::{AppendEvent, Position, Tag};
use crate::replication::control::ReplicationControl;
use crate::replication::dispatcher::{LiveFrame, WaveSlice};
use crate::replication::{MAX_RECORD_BYTES_PER_FRAME, PEER_MAX_MESSAGE_BYTES};
use crate::segment::{self, record};

use super::proto;
use super::proto::segment_replication_server::{SegmentReplication, SegmentReplicationServer};

const OUTPUT_BUFFER: usize = 8;

pub struct SegmentReplicationService {
    contexts: Arc<ContextManager>,
    control: Arc<ReplicationControl>,
    node_id: u64,
    max_inflight_bytes: u64,
}

impl SegmentReplicationService {
    pub fn new(
        contexts: Arc<ContextManager>,
        control: Arc<ReplicationControl>,
        max_inflight_bytes: u64,
    ) -> Self {
        let node_id = control.node_id();
        Self {
            contexts,
            control,
            node_id,
            max_inflight_bytes: max_inflight_bytes.max(1),
        }
    }

    pub fn into_server(self) -> SegmentReplicationServer<Self> {
        SegmentReplicationServer::new(self)
            .max_decoding_message_size(PEER_MAX_MESSAGE_BYTES)
            .max_encoding_message_size(PEER_MAX_MESSAGE_BYTES)
    }
}

#[tonic::async_trait]
impl SegmentReplication for SegmentReplicationService {
    type TailStream =
        Pin<Box<dyn Stream<Item = Result<proto::TailFrame, Status>> + Send + 'static>>;

    async fn get_cursors(
        &self,
        _request: Request<proto::GetCursorsRequest>,
    ) -> Result<Response<proto::GetCursorsResponse>, Status> {
        let mut contexts = Vec::new();
        let mut epoch = None;
        for context in self.contexts.list_contexts() {
            let engine = self
                .contexts
                .get_context(&context)
                .map_err(|error| Status::internal(error.to_string()))?;
            let context_epoch = engine.replication_epoch();
            epoch = Some(
                epoch
                    .map(|current: u64| current.min(context_epoch))
                    .unwrap_or(context_epoch),
            );
            let physical = engine
                .replication_cursor()
                .map_err(|error| Status::internal(error.to_string()))?;
            contexts.push(proto::ContextCursor {
                context,
                durable_position: physical.position.0,
                watermark: engine.head().0,
                segment_base: physical.segment_base,
                byte_offset: physical.byte_offset,
                last_record_crc: physical.last_record_crc,
            });
        }
        Ok(Response::new(proto::GetCursorsResponse {
            node_id: self.node_id,
            epoch: epoch.unwrap_or(0),
            contexts,
            control_epoch: self.control.claim().map(|claim| claim.epoch).unwrap_or(0),
        }))
    }

    async fn forward_append(
        &self,
        request: Request<proto::ForwardAppendRequest>,
    ) -> Result<Response<proto::ForwardAppendResponse>, Status> {
        let request = request.into_inner();
        if !self
            .control
            .is_local_writable(request.epoch, request.leader_id)
        {
            return Ok(Response::new(retry_response(
                "node is not the active claimed leader",
            )));
        }
        let engine = self
            .contexts
            .get_context(&request.context)
            .map_err(|error| Status::not_found(error.to_string()))?;
        if engine.replication_epoch() != request.epoch {
            return Ok(Response::new(retry_response(
                "context is not installed in the active leader epoch",
            )));
        }
        let request_epoch = request.epoch;
        let request_leader = request.leader_id;
        let append = decode_forward_append(request)?;
        let result = tokio::task::spawn_blocking(move || engine.append(append))
            .await
            .map_err(|error| Status::internal(format!("append task panicked: {error}")))?;
        if !self
            .control
            .is_local_writable(request_epoch, request_leader)
        {
            return Ok(Response::new(retry_response(
                "leader fence changed before append acknowledgement",
            )));
        }

        let result = match result {
            Ok(response) => {
                proto::forward_append_response::Result::Success(proto::ForwardAppendSuccess {
                    first_position: response.first_position.0,
                    count: response.count,
                    consistency_marker: response.consistency_marker.0,
                    epoch: self.control.claim().map(|claim| claim.epoch).unwrap_or(0),
                })
            }
            Err(crate::error::Error::ConsistencyConditionViolated {
                conflicting_position,
            }) => proto::forward_append_response::Result::Rejected(proto::ForwardAppendRejected {
                conflicting_position: conflicting_position.0,
            }),
            Err(error) => {
                proto::forward_append_response::Result::Retry(proto::ForwardAppendRetry {
                    reason: error.to_string(),
                })
            }
        };
        Ok(Response::new(proto::ForwardAppendResponse {
            result: Some(result),
        }))
    }

    async fn tail(
        &self,
        request: Request<tonic::Streaming<proto::TailRequest>>,
    ) -> Result<Response<Self::TailStream>, Status> {
        let mut inbound = request.into_inner();
        let first = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("Tail stream ended before TailOpen"))?;
        let open = match first.request {
            Some(proto::tail_request::Request::Open(open)) => open,
            _ => {
                return Err(Status::invalid_argument(
                    "first Tail request must be TailOpen",
                ));
            }
        };

        let engine = self
            .contexts
            .get_context(&open.context)
            .map_err(|error| Status::not_found(error.to_string()))?;
        let session_claim = self
            .control
            .claim()
            .ok_or_else(|| Status::failed_precondition("no committed leader claim"))?;
        if !self.control.is_replica(open.follower_id) {
            return Err(Status::permission_denied(
                "Tail requester is not a cluster member",
            ));
        }
        if open.recovery_source {
            if open.claim_epoch != session_claim.epoch
                || open.claim_leader_id != session_claim.leader_id
                || open.follower_id != session_claim.leader_id
                || engine.replication_epoch() != open.epoch_seen
                || open.epoch_seen >= session_claim.epoch
            {
                return Err(Status::failed_precondition(
                    "recovery Tail does not match the active fencing claim",
                ));
            }
        } else if session_claim.leader_id != self.node_id
            || engine.replication_epoch() != session_claim.epoch
        {
            return Err(Status::failed_precondition(
                "node is not serving this context in the active claim epoch",
            ));
        }
        let recovery_source = open.recovery_source;
        let mut claim_updates = self.control.claim_updates();
        let epoch = engine.replication_epoch();
        let leader_tail = engine.local_tail().0;
        let available_from = engine.tail().0;
        let mut catchup_from = open.from_position;
        let mut initial_truncate = None;
        let mut exact_cursor = false;
        if open.watermark > leader_tail {
            let (out_tx, out_rx) = mpsc::channel(1);
            let _ = out_tx.try_send(Ok(proto::TailFrame {
                frame: Some(proto::tail_frame::Frame::NeedSnapshot(
                    proto::NeedSnapshot {
                        context: open.context,
                        available_from: leader_tail,
                        watermark: open.watermark,
                        reason: "source is missing bytes already committed on the follower".into(),
                    },
                )),
            }));
            return Ok(Response::new(Box::pin(ReceiverStream::new(out_rx))));
        }
        if open.from_position > leader_tail {
            catchup_from = leader_tail;
            let prev = engine
                .replication_boundary_prev(Position(leader_tail))
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            initial_truncate = Some((leader_tail, prev));
        } else if engine
            .verify_replication_probe(open.segment_base, open.byte_offset, open.last_record_crc)
            .map_err(|error| Status::failed_precondition(error.to_string()))?
        {
            exact_cursor = true;
        } else {
            // The follower's own watermark is the destructive floor: bytes
            // above it were never externally visible there and may be replaced
            // by the new leader's committed prefix. The leader watermark is not
            // a truncation floor because those bytes are about to be re-sent.
            if open.watermark < available_from {
                let (out_tx, out_rx) = mpsc::channel(1);
                let _ = out_tx.try_send(Ok(proto::TailFrame {
                    frame: Some(proto::tail_frame::Frame::NeedSnapshot(
                        proto::NeedSnapshot {
                            context: open.context,
                            available_from,
                            watermark: open.watermark,
                            reason: "required committed prefix is no longer retained".into(),
                        },
                    )),
                }));
                return Ok(Response::new(Box::pin(ReceiverStream::new(out_rx))));
            }
            catchup_from = open.watermark;
            let prev = engine
                .replication_boundary_prev(Position(open.watermark))
                .map_err(|error| Status::failed_precondition(error.to_string()))?;
            initial_truncate = Some((open.watermark, prev));
        }

        // Subscribe before snapshotting catch-up ranges. Waves sealed after
        // the snapshot are buffered here; waves wholly covered by catch-up
        // are discarded by their next_position below.
        let mut live = engine.subscribe_replication();
        let catchup = if exact_cursor {
            engine.replication_catchup_from_cursor(crate::store::ReplicationCursor {
                position: Position(open.from_position),
                segment_base: open.segment_base,
                byte_offset: open.byte_offset,
                last_record_crc: open.last_record_crc,
            })
        } else {
            engine.replication_catchup_slices(Position(catchup_from))
        };
        let (catchup_tail, slices) =
            catchup.map_err(|error| Status::failed_precondition(error.to_string()))?;
        let catchup_byte_ends: BTreeMap<u64, u64> = slices
            .iter()
            .map(|slice| (slice.segment_base, slice.byte_end))
            .collect();

        let (out_tx, out_rx) = mpsc::channel(OUTPUT_BUFFER);
        let acked_bytes = Arc::new(AtomicU64::new(0));
        let acked_position = Arc::new(AtomicU64::new(open.from_position));
        let sent_boundaries = Arc::new(tokio::sync::Mutex::new(BTreeMap::new()));
        let ack_notify = Arc::new(Notify::new());
        let follower_id = open.follower_id;
        let context = open.context.clone();

        // Consume durable acknowledgements independently from outbound
        // sourcing so the byte window can open while the stream is active.
        {
            let acked_bytes = Arc::clone(&acked_bytes);
            let acked_position = Arc::clone(&acked_position);
            let sent_boundaries = Arc::clone(&sent_boundaries);
            let ack_notify = Arc::clone(&ack_notify);
            let leader_engine = Arc::clone(&engine);
            let session_control = Arc::clone(&self.control);
            let mut inbound_claim_updates = claim_updates.clone();
            let watermark_tx = out_tx.clone();
            tokio::spawn(async move {
                loop {
                    let message = tokio::select! {
                        message = inbound.message() => message,
                        changed = inbound_claim_updates.changed() => {
                            if changed.is_err()
                                || !claim_matches(&session_control, session_claim)
                            {
                                break;
                            }
                            continue;
                        }
                    };
                    match message {
                        Ok(Some(request)) => {
                            if let Some(proto::tail_request::Request::Ack(ack)) = request.request {
                                if ack.follower_id != follower_id
                                    || ack.context != context
                                    || ack.epoch != epoch
                                {
                                    tracing::warn!(
                                        expected_follower = follower_id,
                                        actual_follower = ack.follower_id,
                                        "ignoring TailAck for a different session"
                                    );
                                    continue;
                                }
                                let current_bytes = acked_bytes.load(Ordering::Acquire);
                                let current_position = acked_position.load(Ordering::Acquire);
                                if ack.durable_bytes < current_bytes
                                    || (ack.durable_bytes == current_bytes
                                        && ack.durable_position != current_position)
                                {
                                    let _ = watermark_tx
                                        .send(Err(Status::invalid_argument(
                                            "TailAck regressed or changed its durable boundary",
                                        )))
                                        .await;
                                    break;
                                }
                                if ack.durable_bytes > current_bytes {
                                    let mut boundaries = sent_boundaries.lock().await;
                                    if boundaries.get(&ack.durable_bytes).copied()
                                        != Some(ack.durable_position)
                                    {
                                        let _ = watermark_tx
                                            .send(Err(Status::invalid_argument(
                                                "TailAck does not match a sent record boundary",
                                            )))
                                            .await;
                                        break;
                                    }
                                    boundaries.retain(|bytes, _| *bytes > ack.durable_bytes);
                                    acked_position.store(ack.durable_position, Ordering::Release);
                                    acked_bytes.store(ack.durable_bytes, Ordering::Release);
                                }
                                if !recovery_source {
                                    session_control.record_progress(
                                        &context,
                                        ack.follower_id,
                                        ack.durable_position,
                                    );
                                    if let Some(watermark) = leader_engine.acknowledge_replica(
                                        ack.follower_id,
                                        ack.epoch,
                                        Position(ack.durable_position),
                                    ) {
                                        let _ = send_frame(
                                            &watermark_tx,
                                            proto::tail_frame::Frame::Watermark(proto::Watermark {
                                                epoch: ack.epoch,
                                                position: watermark.0,
                                            }),
                                        )
                                        .await;
                                    }
                                }
                                ack_notify.notify_waiters();
                            }
                        }
                        Ok(None) => break,
                        Err(error) => {
                            tracing::debug!(%error, follower_id, "Tail request stream closed");
                            break;
                        }
                    }
                }
            });
        }

        let max_inflight = self.max_inflight_bytes;
        let outbound_control = Arc::clone(&self.control);
        let leader_id = self.node_id;
        tokio::spawn(async move {
            let mut sent_bytes = 0u64;
            let mut current_base: Option<u64> = None;

            if open.epoch_seen != epoch
                && send_frame(
                    &out_tx,
                    proto::tail_frame::Frame::EpochChange(proto::EpochChange {
                        epoch,
                        leader_id,
                        start_position: open.from_position,
                    }),
                )
                .await
                .is_err()
            {
                return;
            }

            if let Some((position, prev)) = initial_truncate
                && send_frame(
                    &out_tx,
                    proto::tail_frame::Frame::Truncate(proto::Truncate {
                        epoch,
                        position,
                        has_boundary: true,
                        prev_record_crc: prev.unwrap_or(0),
                        prev_at_segment_start: prev.is_none(),
                    }),
                )
                .await
                .is_err()
            {
                return;
            }

            for slice in slices {
                if current_base != Some(slice.segment_base)
                    && slice.first_position == slice.segment_base
                    && send_frame(
                        &out_tx,
                        proto::tail_frame::Frame::Rotate(proto::Rotate {
                            epoch,
                            new_segment_base: slice.segment_base,
                        }),
                    )
                    .await
                    .is_err()
                {
                    return;
                }
                current_base = Some(slice.segment_base);

                let (chunk_tx, mut chunk_rx) = mpsc::channel(2);
                let reader = tokio::task::spawn_blocking(move || source_slice(slice, chunk_tx));
                while let Some(chunk) = chunk_rx.recv().await {
                    if !claim_matches(&outbound_control, session_claim)
                        || engine.replication_epoch() != epoch
                    {
                        let _ = out_tx
                            .send(Err(Status::aborted("native fencing claim changed")))
                            .await;
                        return;
                    }
                    if wait_for_window(
                        &acked_bytes,
                        &ack_notify,
                        sent_bytes,
                        chunk.data.len() as u64,
                        max_inflight,
                    )
                    .await
                    .is_err()
                    {
                        return;
                    }
                    sent_bytes += chunk.data.len() as u64;
                    if send_records(
                        &out_tx,
                        epoch,
                        chunk.segment_base,
                        chunk.first_position,
                        chunk.next_position,
                        chunk.data,
                        sent_bytes,
                        &sent_boundaries,
                    )
                    .await
                    .is_err()
                    {
                        return;
                    }
                }
                match reader.await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        let _ = out_tx.send(Err(Status::internal(error.to_string()))).await;
                        return;
                    }
                    Err(error) => {
                        let _ = out_tx
                            .send(Err(Status::internal(format!(
                                "catch-up reader panicked: {error}"
                            ))))
                            .await;
                        return;
                    }
                }
            }

            if !claim_matches(&outbound_control, session_claim)
                || engine.replication_epoch() != epoch
            {
                let _ = out_tx
                    .send(Err(Status::aborted("native fencing claim changed")))
                    .await;
                return;
            }
            if send_frame(
                &out_tx,
                proto::tail_frame::Frame::Watermark(proto::Watermark {
                    epoch,
                    position: engine.head().0,
                }),
            )
            .await
            .is_err()
            {
                return;
            }

            // Live handoff. Frames already covered by the catch-up snapshot
            // are duplicates and are dropped. Partial overlap indicates a
            // broken wave boundary; force a cursor-based reconnect.
            loop {
                let live_frame = tokio::select! {
                    frame = live.recv() => frame,
                    changed = claim_updates.changed() => {
                        if changed.is_err()
                            || !claim_matches(&outbound_control, session_claim)
                        {
                            let _ = out_tx
                                .send(Err(Status::aborted("native fencing claim changed")))
                                .await;
                            return;
                        }
                        continue;
                    }
                };
                if engine.replication_epoch() != epoch {
                    let _ = out_tx
                        .send(Err(Status::aborted("source context epoch changed")))
                        .await;
                    return;
                }
                match live_frame {
                    Ok(LiveFrame::Rotate {
                        epoch,
                        new_segment_base,
                    }) => {
                        if new_segment_base < catchup_tail.0 {
                            continue;
                        }
                        if send_frame(
                            &out_tx,
                            proto::tail_frame::Frame::Rotate(proto::Rotate {
                                epoch,
                                new_segment_base,
                            }),
                        )
                        .await
                        .is_err()
                        {
                            return;
                        }
                    }
                    Ok(LiveFrame::Records {
                        epoch,
                        segment_base,
                        byte_start,
                        byte_end,
                        first_position,
                        next_position,
                        data,
                        ..
                    }) => {
                        if let Some(catchup_end) = catchup_byte_ends.get(&segment_base).copied() {
                            if byte_end <= catchup_end {
                                continue;
                            }
                            if byte_start < catchup_end {
                                let _ = out_tx
                                    .send(Err(Status::aborted(
                                        "live wave overlaps catch-up physical boundary; reopen from durable cursor",
                                    )))
                                    .await;
                                return;
                            }
                        }
                        if first_position < catchup_tail.0 {
                            let _ = out_tx
                                .send(Err(Status::aborted(
                                    "live wave overlaps catch-up boundary; reopen from durable cursor",
                                )))
                                .await;
                            return;
                        }
                        let chunks = match chunk_record_bytes(
                            data,
                            segment_base,
                            first_position,
                            next_position,
                        ) {
                            Ok(chunks) => chunks,
                            Err(error) => {
                                let _ = out_tx.send(Err(Status::internal(error.to_string()))).await;
                                return;
                            }
                        };
                        for chunk in chunks {
                            if wait_for_window(
                                &acked_bytes,
                                &ack_notify,
                                sent_bytes,
                                chunk.data.len() as u64,
                                max_inflight,
                            )
                            .await
                            .is_err()
                            {
                                return;
                            }
                            sent_bytes += chunk.data.len() as u64;
                            if send_records(
                                &out_tx,
                                epoch,
                                chunk.segment_base,
                                chunk.first_position,
                                chunk.next_position,
                                chunk.data,
                                sent_bytes,
                                &sent_boundaries,
                            )
                            .await
                            .is_err()
                            {
                                return;
                            }
                        }
                        if send_frame(
                            &out_tx,
                            proto::tail_frame::Frame::Watermark(proto::Watermark {
                                epoch,
                                position: engine.head().0,
                            }),
                        )
                        .await
                        .is_err()
                        {
                            return;
                        }
                    }
                    Ok(LiveFrame::Reset { epoch: reset_epoch }) => {
                        let _ = out_tx
                            .send(Err(Status::aborted(format!(
                                "live replication reset in epoch {reset_epoch}; reopen from durable cursor"
                            ))))
                            .await;
                        return;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let _ = out_tx
                            .send(Err(Status::aborted(
                                "Tail session lagged live dispatch; reopen from durable cursor",
                            )))
                            .await;
                        return;
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        Ok(Response::new(Box::pin(ReceiverStream::new(out_rx))))
    }
}

fn claim_matches(
    control: &ReplicationControl,
    expected: crate::replication::control::ActiveClaim,
) -> bool {
    control.voter_generation() == expected.voter_generation
        && control
            .claim()
            .map(|current| {
                current.epoch == expected.epoch
                    && current.leader_id == expected.leader_id
                    && current.term == expected.term
                    && current.voter_generation == expected.voter_generation
            })
            .unwrap_or(false)
}

fn retry_response(reason: impl Into<String>) -> proto::ForwardAppendResponse {
    proto::ForwardAppendResponse {
        result: Some(proto::forward_append_response::Result::Retry(
            proto::ForwardAppendRetry {
                reason: reason.into(),
            },
        )),
    }
}

// tonic's `Status` fixes the large error type.
#[allow(clippy::result_large_err)]
fn decode_forward_append(request: proto::ForwardAppendRequest) -> Result<AppendRequest, Status> {
    let events = request
        .events
        .into_iter()
        .map(|event| AppendEvent {
            identifier: event.identifier,
            name: event.name,
            version: event.version,
            timestamp: event.timestamp,
            payload: event.payload.to_vec(),
            metadata: event
                .metadata
                .into_iter()
                .map(|entry| (entry.key, entry.value))
                .collect(),
            tags: event
                .tags
                .into_iter()
                .map(|tag| Tag {
                    key: tag.key.to_vec(),
                    value: tag.value.to_vec(),
                })
                .collect(),
        })
        .collect();
    let condition = request.condition.map(|condition| AppendCondition {
        consistency_marker: Position(condition.consistency_marker),
        criteria: SourcingCondition {
            criteria: condition
                .criteria
                .into_iter()
                .map(|criterion| Criterion {
                    names: criterion.names,
                    tags: criterion
                        .tags
                        .into_iter()
                        .map(|tag| Tag {
                            key: tag.key.to_vec(),
                            value: tag.value.to_vec(),
                        })
                        .collect(),
                })
                .collect(),
        },
    });
    Ok(AppendRequest { condition, events })
}

async fn wait_for_window(
    acked: &AtomicU64,
    notify: &Notify,
    sent: u64,
    next_len: u64,
    max: u64,
) -> Result<(), ()> {
    loop {
        // Register before checking the atomic so an acknowledgement racing this
        // check leaves a permit instead of becoming a lost wakeup.
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let durable = acked.load(Ordering::Acquire);
        let inflight = sent.saturating_sub(durable);
        if inflight == 0 || inflight.saturating_add(next_len) <= max {
            return Ok(());
        }
        notified.await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn send_records(
    tx: &mpsc::Sender<Result<proto::TailFrame, Status>>,
    epoch: u64,
    segment_base: u64,
    first_position: u64,
    next_position: u64,
    data: Bytes,
    stream_bytes_end: u64,
    sent_boundaries: &tokio::sync::Mutex<BTreeMap<u64, u64>>,
) -> Result<(), ()> {
    sent_boundaries
        .lock()
        .await
        .insert(stream_bytes_end, next_position);
    send_frame(
        tx,
        proto::tail_frame::Frame::Records(proto::Records {
            epoch,
            segment_base,
            first_position,
            data,
            stream_bytes_end,
            next_position,
        }),
    )
    .await
}

async fn send_frame(
    tx: &mpsc::Sender<Result<proto::TailFrame, Status>>,
    frame: proto::tail_frame::Frame,
) -> Result<(), ()> {
    tx.send(Ok(proto::TailFrame { frame: Some(frame) }))
        .await
        .map_err(|_| ())
}

struct CatchupChunk {
    segment_base: u64,
    first_position: u64,
    next_position: u64,
    data: Bytes,
}

fn chunk_record_bytes(
    data: Bytes,
    segment_base: u64,
    first_position: u64,
    expected_next: u64,
) -> Result<Vec<CatchupChunk>, crate::error::Error> {
    let mut chunks = Vec::new();
    let mut offset = 0usize;
    let mut frame_start = 0usize;
    let mut frame_first = first_position;
    let mut position = first_position;

    while offset < data.len() {
        if data.len() - offset < segment::RECORD_HEADER_SIZE {
            return Err(crate::error::Error::Corrupted {
                message: "live replication wave ends inside a record header".into(),
            });
        }
        let header_bytes: &[u8; segment::RECORD_HEADER_SIZE] = data
            [offset..offset + segment::RECORD_HEADER_SIZE]
            .try_into()
            .unwrap();
        let header =
            record::parse_header(header_bytes)?.ok_or_else(|| crate::error::Error::Corrupted {
                message: "zero-length record in live replication wave".into(),
            })?;
        let total = header.total_len();
        if total > MAX_RECORD_BYTES_PER_FRAME {
            return Err(crate::error::Error::Corrupted {
                message: format!(
                    "replication record is {total} bytes (frame limit {MAX_RECORD_BYTES_PER_FRAME})"
                ),
            });
        }
        let end = offset
            .checked_add(total)
            .ok_or_else(|| crate::error::Error::Corrupted {
                message: "record length overflow in live replication wave".into(),
            })?;
        if end > data.len() {
            return Err(crate::error::Error::Corrupted {
                message: "live replication wave ends inside a record payload".into(),
            });
        }
        if offset > frame_start && end - frame_start > MAX_RECORD_BYTES_PER_FRAME {
            chunks.push(CatchupChunk {
                segment_base,
                first_position: frame_first,
                next_position: position,
                data: data.slice(frame_start..offset),
            });
            frame_start = offset;
            frame_first = position;
        }

        let payload = &data[offset + segment::RECORD_HEADER_SIZE..end];
        if !record::validate_crc(header, payload) {
            return Err(crate::error::Error::Corrupted {
                message: "CRC mismatch in live replication wave".into(),
            });
        }
        match record::decode_native(header, payload)? {
            record::NativeRecord::Event { position: actual } => {
                if actual != position {
                    return Err(crate::error::Error::Corrupted {
                        message: format!(
                            "live replication event position {actual} does not continue {position}"
                        ),
                    });
                }
                position += 1;
            }
            record::NativeRecord::Control(_) => {}
        }
        offset = end;
    }

    if frame_start < data.len() {
        chunks.push(CatchupChunk {
            segment_base,
            first_position: frame_first,
            next_position: position,
            data: data.slice(frame_start..),
        });
    }
    if position != expected_next {
        return Err(crate::error::Error::Corrupted {
            message: format!(
                "live replication wave positions {first_position}..{expected_next} decoded to {position}"
            ),
        });
    }
    Ok(chunks)
}

/// Streams one segment range as record-boundary-aligned frames through a
/// bounded channel so catch-up memory does not scale with retained history.
fn source_slice(
    slice: WaveSlice,
    tx: mpsc::Sender<CatchupChunk>,
) -> Result<(), crate::error::Error> {
    let mut file = File::open(&slice.path)?;
    file.seek(SeekFrom::Start(slice.byte_start))?;
    let mut remaining = slice.byte_end - slice.byte_start;
    let mut cursor_position = slice.first_position;
    let mut frame_first = cursor_position;
    let mut frame = Vec::new();

    while remaining > 0 {
        let (unit, native) = read_record_unit(&mut file, &mut remaining)?;
        let event_count = match native {
            record::NativeRecord::Event { position } if position == cursor_position => 1,
            record::NativeRecord::Event { position } => {
                return Err(crate::error::Error::Corrupted {
                    message: format!(
                        "replication event position {position} does not continue {cursor_position}"
                    ),
                });
            }
            record::NativeRecord::Control(_) => 0,
        };
        if unit.len() > MAX_RECORD_BYTES_PER_FRAME {
            return Err(crate::error::Error::Corrupted {
                message: format!(
                    "replication record group is {} bytes (frame limit {})",
                    unit.len(),
                    MAX_RECORD_BYTES_PER_FRAME
                ),
            });
        }
        if !frame.is_empty() && frame.len() + unit.len() > MAX_RECORD_BYTES_PER_FRAME {
            tx.blocking_send(CatchupChunk {
                segment_base: slice.segment_base,
                first_position: frame_first,
                next_position: cursor_position,
                data: Bytes::from(std::mem::take(&mut frame)),
            })
            .map_err(|_| {
                crate::error::Error::Io(std::io::Error::other("Tail catch-up consumer closed"))
            })?;
            frame_first = cursor_position;
        }
        frame.extend_from_slice(&unit);
        cursor_position += event_count as u64;
    }

    if !frame.is_empty() {
        tx.blocking_send(CatchupChunk {
            segment_base: slice.segment_base,
            first_position: frame_first,
            next_position: cursor_position,
            data: Bytes::from(frame),
        })
        .map_err(|_| {
            crate::error::Error::Io(std::io::Error::other("Tail catch-up consumer closed"))
        })?;
    }
    if cursor_position != slice.next_position {
        return Err(crate::error::Error::Corrupted {
            message: format!(
                "replication slice positions {}..{} decoded to {}",
                slice.first_position, slice.next_position, cursor_position
            ),
        });
    }
    Ok(())
}

/// Reads one independently durable native event or control record.
fn read_record_unit<R: Read>(
    file: &mut R,
    remaining: &mut u64,
) -> Result<(Vec<u8>, record::NativeRecord), crate::error::Error> {
    let (bytes, header, payload) = read_one_record(file, remaining)?;
    let native = record::decode_native(header, &payload)?;
    Ok((bytes, native))
}

fn read_one_record<R: Read>(
    file: &mut R,
    remaining: &mut u64,
) -> Result<(Vec<u8>, record::RecordHeader, Vec<u8>), crate::error::Error> {
    if *remaining < segment::RECORD_HEADER_SIZE as u64 {
        return Err(crate::error::Error::Corrupted {
            message: "replication range ends inside record header".into(),
        });
    }
    let mut raw_header = [0u8; segment::RECORD_HEADER_SIZE];
    file.read_exact(&mut raw_header)?;
    let header =
        record::parse_header(&raw_header)?.ok_or_else(|| crate::error::Error::Corrupted {
            message: "zero-length record in replication range".into(),
        })?;
    let total = header.total_len();
    if total as u64 > *remaining {
        return Err(crate::error::Error::Corrupted {
            message: "replication range ends inside record payload".into(),
        });
    }
    let mut payload = vec![0u8; header.payload_len];
    file.read_exact(&mut payload)?;
    if !record::validate_crc(header, &payload) {
        return Err(crate::error::Error::Corrupted {
            message: "CRC mismatch in replication range".into(),
        });
    }
    *remaining -= total as u64;

    let mut bytes = Vec::with_capacity(total);
    bytes.extend_from_slice(&raw_header);
    bytes.extend_from_slice(&payload);
    Ok((bytes, header, payload))
}
