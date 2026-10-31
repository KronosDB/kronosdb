//! Leader side of native segment replication: the Tail stream a follower
//! opens per context, forwarded appends, and cursor queries. Each is a
//! function over the request's [`ServerStream`]; the server only routes.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use kronosdb_protocol as p;
use kronosdb_protocol::Frame;
use kronosdb_protocol::net::server::{Inbound, ServerStream, StreamSender};
use parking_lot::Mutex;
use tokio::sync::{Notify, broadcast, mpsc, watch};
use tokio::task::JoinHandle;

use crate::append::{AppendRequest, RecordAppend};
use crate::context::ContextManager;
use crate::error::Error;
use crate::event::Position;
use crate::replication::MAX_RECORD_BYTES_PER_FRAME;
use crate::replication::control::{ActiveClaim, ReplicationControl};
use crate::replication::dispatcher::{LiveFrame, WaveSlice};
use crate::replication::epochs::EpochExtent;
use crate::segment::{self, record};
use crate::store::EventStoreEngine;
use crate::wire;

/// What a TailRecords frame carries besides its bytes: the frame header and
/// the fixed fields on either side of them.
const TAIL_RECORDS_OVERHEAD: usize = 64;

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

    /// Answers `GetCursors` with where each context's log stands on this node.
    pub async fn get_cursors(&self, stream: ServerStream) -> Result<(), Error> {
        let names = self.contexts.list_contexts();
        let mut cursors = Vec::with_capacity(names.len());
        let mut epoch: Option<u64> = None;
        for name in &names {
            let engine = self.contexts.get_context(name)?;
            let context_epoch = engine.replication_epoch();
            epoch = Some(epoch.map_or(context_epoch, |current| current.min(context_epoch)));
            let physical = engine.replication_cursor()?;
            cursors.push(p::ContextCursor {
                context: name,
                durable_position: physical.position.0,
                watermark: engine.head().0,
                segment_base: physical.segment_base,
                byte_offset: physical.byte_offset,
                last_record_crc: physical.last_record_crc,
                log_epoch: engine.log_epoch(),
            });
        }
        let _ = stream
            .out
            .finish(&Frame::Cursors(p::Cursors {
                node_id: self.node_id,
                epoch: epoch.unwrap_or(0),
                control_epoch: self.control.claim().map(|claim| claim.epoch).unwrap_or(0),
                contexts: cursors,
            }))
            .await;
        Ok(())
    }

    /// Appends on behalf of a follower that received the client's request,
    /// if this node is still the claimed leader the follower thinks it is.
    pub async fn forward_append(&self, stream: ServerStream) -> Result<(), Error> {
        let out = &stream.out;
        let Ok(Frame::ForwardAppend(request)) = stream.request.decode() else {
            return Err(Error::Rejected {
                message: "malformed ForwardAppend".into(),
            });
        };
        if !self
            .control
            .is_local_writable(request.epoch, request.leader_id)
        {
            return retry(out, "node is not the active claimed leader").await;
        }
        let engine = self.contexts.get_context(request.context)?;
        if engine.replication_epoch() != request.epoch {
            return retry(out, "context is not installed in the active leader epoch").await;
        }
        let (request_epoch, request_leader, system) =
            (request.epoch, request.leader_id, request.system);
        let condition = request.condition.as_ref().map(wire::append_condition);
        // Staging takes the writer lock for microseconds; the durability
        // wait is awaited so no thread is pinned through it.
        let result = if system {
            // The server's own events are framed (stamped with the `$`
            // marker) on the leader, as events.
            let append = AppendRequest {
                condition,
                events: wire::append_events(&request.events)
                    .map_err(|bad| Error::Rejected { message: bad.0 })?,
            };
            engine.append_async(append, true).await
        } else {
            // A client's records are checked where they stand and written as
            // they are.
            let events = wire::record_batch(&request.events, stream.request.bytes())
                .map_err(|bad| Error::Rejected { message: bad.0 })?;
            engine
                .append_records_async(RecordAppend { condition, events })
                .await
        };
        if !self
            .control
            .is_local_writable(request_epoch, request_leader)
        {
            return retry(out, "leader fence changed before append acknowledgement").await;
        }

        let _ = match result {
            Ok(response) => {
                out.finish(&Frame::ForwardAppended(p::ForwardAppended {
                    first_position: response.first_position.0,
                    count: response.count,
                    marker: response.consistency_marker.0,
                    epoch: self.control.claim().map(|claim| claim.epoch).unwrap_or(0),
                }))
                .await
            }
            Err(Error::ConsistencyConditionViolated {
                conflicting_position,
            }) => {
                out.finish(&Frame::ForwardAppendConflict(p::ForwardAppendConflict {
                    conflicting_position: conflicting_position.0,
                }))
                .await
            }
            Err(error) => {
                out.finish(&Frame::ForwardAppendRetry(p::ForwardAppendRetry {
                    reason: &error.to_string(),
                }))
                .await
            }
        };
        Ok(())
    }

    /// Serves one follower's Tail: the stream's first frame is its TailOpen,
    /// its acknowledgements follow, and the leader answers with the log's
    /// bytes from where the follower stands. The session ends when the
    /// follower leaves or the fencing claim it was opened under changes; the
    /// follower then reopens from its durable cursor.
    pub async fn tail(&self, stream: ServerStream) -> Result<(), Error> {
        let ServerStream {
            request,
            inbound,
            out,
        } = stream;
        let Ok(Frame::TailOpen(open)) = request.decode() else {
            return Err(Error::Rejected {
                message: "the first frame of a Tail must be TailOpen".into(),
            });
        };

        let engine = self.contexts.get_context(open.context)?;
        let session_claim = self.control.claim().ok_or_else(|| Error::Unavailable {
            message: "no leader has claimed the write path yet".into(),
        })?;
        if !self.control.is_replica(open.follower_id) {
            return Err(Error::Rejected {
                message: "Tail requester is not a cluster member".into(),
            });
        }
        if open.recovery_source {
            // A claimant pulls from the voter it ranked best, which holds at
            // least the epoch the claimant's own log ends in. A source that
            // is behind it, or that has already moved on to the new claim,
            // must not be used.
            if open.claim_epoch != session_claim.epoch
                || open.claim_leader_id != session_claim.leader_id
                || open.follower_id != session_claim.leader_id
                || engine.log_epoch() < open.epoch_seen
                || engine.replication_epoch() >= session_claim.epoch
                || open.epoch_seen >= session_claim.epoch
            {
                return Err(Error::Unavailable {
                    message: "recovery Tail does not match the active fencing claim, or this \
                              node's log is behind the claimant's"
                        .into(),
                });
            }
        } else if session_claim.leader_id != self.node_id
            || engine.replication_epoch() != session_claim.epoch
        {
            return Err(Error::Unavailable {
                message: "node is not serving this context in the active claim epoch".into(),
            });
        }
        let recovery_source = open.recovery_source;
        let claim_updates = self.control.claim_updates();
        let epoch = engine.replication_epoch();
        let start = plan_tail_start(
            &engine,
            &FollowerLog {
                from_position: open.from_position,
                segment_base: open.segment_base,
                byte_offset: open.byte_offset,
                last_record_crc: open.last_record_crc,
                epoch_seen: open.epoch_seen,
                watermark: open.watermark,
            },
        )?;
        let (catchup_from, initial_truncate, exact_cursor) = match start {
            TailStart::NeedSnapshot {
                available_from,
                reason,
            } => {
                return need_snapshot(&out, open.context, available_from, open.watermark, reason)
                    .await;
            }
            TailStart::ExactCursor => (open.from_position, None, true),
            TailStart::Truncate { position, prev } => (position, Some((position, prev)), false),
        };

        // Subscribe before snapshotting catch-up ranges. Waves sealed after
        // the snapshot are buffered here; waves wholly covered by catch-up
        // are discarded by their next_position below.
        let live = engine.subscribe_replication();
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
        let (catchup_tail, slices) = catchup?;
        let catchup_byte_ends: BTreeMap<u64, u64> = slices
            .iter()
            .map(|slice| (slice.segment_base, slice.byte_end))
            .collect();

        tracing::info!(
            follower = open.follower_id,
            context = open.context,
            epoch,
            follower_position = open.from_position,
            follower_segment = open.segment_base,
            follower_offset = open.byte_offset,
            follower_watermark = open.watermark,
            leader_tail = catchup_tail.0,
            follower_epoch = open.epoch_seen,
            path = if exact_cursor {
                "exact cursor"
            } else {
                "truncate then position catch-up"
            },
            truncate_to = initial_truncate.map(|(position, _)| position),
            catchup_slices = slices.len(),
            "Tail session opened"
        );

        let window = Arc::new(Window::new(open.from_position));

        // Durable acknowledgements are consumed apart from outbound sourcing
        // so the byte window can open while the stream is active.
        let consumer = AckConsumer {
            inbound,
            out: out.clone(),
            window: Arc::clone(&window),
            engine: Arc::clone(&engine),
            control: Arc::clone(&self.control),
            claim: session_claim,
            claim_updates: claim_updates.clone(),
            follower_id: open.follower_id,
            context: open.context.to_string(),
            epoch,
            recovery_source,
        };
        let mut consumer = AbortOnDrop(tokio::spawn(consumer.run()));

        let sender = Sender {
            out,
            engine,
            control: Arc::clone(&self.control),
            claim: session_claim,
            claim_updates,
            live,
            window,
            max_inflight: self.max_inflight_bytes,
            epoch,
            leader_id: self.node_id,
            epoch_seen: open.epoch_seen,
            recovery_source,
            from_position: open.from_position,
            initial_truncate,
            slices,
            catchup_tail,
            catchup_byte_ends,
        };
        let ended = tokio::select! {
            ended = sender.run() => ended,
            ended = &mut consumer.0 => ended.unwrap_or_else(|error| Error::Internal {
                message: format!("Tail acknowledgement task panicked: {error}"),
            }),
        };
        Err(ended)
    }
}

/// What a follower says about its log when it opens a Tail.
pub(crate) struct FollowerLog {
    /// Its tail: the next event position.
    pub from_position: u64,
    /// Its physical end: the segment it appends to, the byte offset in it, and
    /// the stored CRC of the record before that offset.
    pub segment_base: u64,
    pub byte_offset: u64,
    pub last_record_crc: u32,
    /// The epoch its log's newest `EpochChange` names. A claimant recovering
    /// from a peer reports that too: it is where the cut is aligned.
    pub epoch_seen: u64,
    /// The highest position it knows to be quorum-committed.
    pub watermark: u64,
}

/// Where a Tail session starts.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum TailStart {
    /// The follower must restore instead of tailing.
    NeedSnapshot {
        available_from: u64,
        reason: &'static str,
    },
    /// The follower's log is a byte prefix of this one: send what follows its
    /// physical end.
    ExactCursor,
    /// The follower holds bytes this log does not: cut it back to `position`,
    /// checking that its last event before the cut is the one `prev` names
    /// (the stored CRC of this log's last event there; `None` when this log
    /// has none), then send everything this log holds after that event. Control
    /// records are not compared: either side may hold some the other lacks.
    Truncate { position: u64, prev: Option<u32> },
}

/// Decides where to resume a follower, which is the point where its history
/// stops agreeing with this log's.
///
/// A follower whose log is a byte prefix of this one resumes at its physical
/// end. Otherwise it holds records this log does not, and the cut has to
/// keep everything they share and nothing more:
///
/// - **The follower's epoch is in this log's history.** The follower and this
///   log hold the same bytes for that epoch up to the shorter of the two (one
///   leader wrote them), so they agree up to where the epoch ends here: the
///   position the next epoch took over at, which is the start of a segment.
///   The follower is cut back to that point if it is past it, and its last
///   event before the cut is compared with this log's. What lies before the
///   cut is shared history, so a mismatch there is real corruption.
/// - **It is not** (the follower led an epoch whose `EpochChange` never got
///   here, or the epoch was purged). Nothing ties the follower's uncommitted
///   suffix to this log, so the only agreed point is the follower's own
///   watermark: committed bytes are in every later leader's log.
///
/// A follower in this log's newest epoch is not ahead of a leader that wrote
/// that epoch's bytes, so there is no divergence to align; those sessions keep
/// the watermark floor, or a cut at this log's tail when the follower is past
/// it.
pub(crate) fn plan_tail_start(
    engine: &EventStoreEngine,
    follower: &FollowerLog,
) -> Result<TailStart, Error> {
    let leader_tail = engine.local_tail().0;
    let available_from = engine.tail().0;
    if follower.watermark > leader_tail {
        return Ok(TailStart::NeedSnapshot {
            available_from: leader_tail,
            reason: "source is missing bytes already committed on the follower",
        });
    }
    if follower.from_position <= leader_tail
        && engine.verify_replication_probe(
            follower.segment_base,
            follower.byte_offset,
            follower.last_record_crc,
        )?
    {
        return Ok(TailStart::ExactCursor);
    }

    match engine.epoch_extent(follower.epoch_seen) {
        EpochExtent::EndsAt(end) if follower.from_position >= end => {
            if end < follower.watermark {
                return Ok(TailStart::NeedSnapshot {
                    available_from: end,
                    reason: "follower holds committed bytes past where its epoch ends here",
                });
            }
            if end < available_from {
                return Ok(TailStart::NeedSnapshot {
                    available_from,
                    reason: "required committed prefix is no longer retained",
                });
            }
            return truncate_start(engine, end);
        }
        EpochExtent::Open if follower.from_position > leader_tail => {
            return truncate_start(engine, leader_tail);
        }
        _ => {}
    }

    // The follower's own watermark is the destructive floor: bytes above it
    // were never externally visible there and may be replaced by the new
    // leader's committed prefix. The leader watermark is not a truncation
    // floor because those bytes are about to be re-sent.
    if follower.watermark < available_from {
        return Ok(TailStart::NeedSnapshot {
            available_from,
            reason: "required committed prefix is no longer retained",
        });
    }
    truncate_start(engine, follower.watermark)
}

fn truncate_start(engine: &EventStoreEngine, position: u64) -> Result<TailStart, Error> {
    Ok(TailStart::Truncate {
        position,
        prev: engine.replication_boundary_event(Position(position))?,
    })
}

/// Stops a task when its owner is done, however that happens.
struct AbortOnDrop(JoinHandle<Error>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

async fn retry(out: &StreamSender, reason: &str) -> Result<(), Error> {
    let _ = out
        .finish(&Frame::ForwardAppendRetry(p::ForwardAppendRetry { reason }))
        .await;
    Ok(())
}

/// The follower is behind what this node still has, or ahead of it: it must
/// restore instead of tail. Ends the stream.
async fn need_snapshot(
    out: &StreamSender,
    context: &str,
    available_from: u64,
    watermark: u64,
    reason: &str,
) -> Result<(), Error> {
    let _ = out
        .finish(&Frame::TailNeedSnapshot(p::TailNeedSnapshot {
            context,
            available_from,
            watermark,
            reason,
        }))
        .await;
    Ok(())
}

fn follower_gone() -> Error {
    Error::Unavailable {
        message: "the follower closed the Tail".into(),
    }
}

fn fence_changed() -> Error {
    Error::Unavailable {
        message: "native fencing claim changed".into(),
    }
}

async fn send(out: &StreamSender, frame: Frame<'_>) -> Result<(), Error> {
    out.send(&frame).await.map_err(|_| follower_gone())
}

/// What a session's sender and its acknowledgement consumer share: how many
/// bytes the follower has made durable, and the record boundaries the sender
/// has crossed, which an acknowledgement must land on.
struct Window {
    acked_bytes: AtomicU64,
    acked_position: AtomicU64,
    sent_boundaries: Mutex<BTreeMap<u64, u64>>,
    ack_notify: Notify,
}

impl Window {
    fn new(from_position: u64) -> Self {
        Self {
            acked_bytes: AtomicU64::new(0),
            acked_position: AtomicU64::new(from_position),
            sent_boundaries: Mutex::new(BTreeMap::new()),
            ack_notify: Notify::new(),
        }
    }
}

/// Reads a follower's TailAcks, advancing the window and the quorum watermark.
struct AckConsumer {
    inbound: Inbound,
    out: StreamSender,
    window: Arc<Window>,
    engine: Arc<EventStoreEngine>,
    control: Arc<ReplicationControl>,
    claim: ActiveClaim,
    claim_updates: watch::Receiver<Option<ActiveClaim>>,
    follower_id: u64,
    context: String,
    epoch: u64,
    recovery_source: bool,
}

impl AckConsumer {
    /// Runs until the follower leaves, the claim changes, or an
    /// acknowledgement breaks the rules; returns why.
    async fn run(mut self) -> Error {
        loop {
            let frame = tokio::select! {
                frame = self.inbound.next() => frame,
                changed = self.claim_updates.changed() => {
                    if changed.is_err() || !claim_matches(&self.control, self.claim) {
                        return fence_changed();
                    }
                    continue;
                }
            };
            let Some(frame) = frame else {
                return follower_gone();
            };
            if let Ok(Frame::TailAck(ack)) = frame.decode()
                && let Err(error) = self.ack(&ack).await
            {
                return error;
            }
        }
    }

    async fn ack(&self, ack: &p::TailAck<'_>) -> Result<(), Error> {
        if ack.follower_id != self.follower_id
            || ack.context != self.context
            || ack.epoch != self.epoch
        {
            tracing::warn!(
                expected_follower = self.follower_id,
                actual_follower = ack.follower_id,
                "ignoring TailAck for a different session"
            );
            return Ok(());
        }
        let window = &self.window;
        let current_bytes = window.acked_bytes.load(Ordering::Acquire);
        let current_position = window.acked_position.load(Ordering::Acquire);
        if ack.durable_bytes < current_bytes
            || (ack.durable_bytes == current_bytes && ack.durable_position != current_position)
        {
            return Err(Error::Rejected {
                message: "TailAck regressed or changed its durable boundary".into(),
            });
        }
        if ack.durable_bytes > current_bytes {
            let mut boundaries = window.sent_boundaries.lock();
            if boundaries.get(&ack.durable_bytes).copied() != Some(ack.durable_position) {
                return Err(Error::Rejected {
                    message: "TailAck does not match a sent record boundary".into(),
                });
            }
            boundaries.retain(|bytes, _| *bytes > ack.durable_bytes);
            drop(boundaries);
            window
                .acked_position
                .store(ack.durable_position, Ordering::Release);
            window
                .acked_bytes
                .store(ack.durable_bytes, Ordering::Release);
        }
        if !self.recovery_source {
            self.control
                .record_progress(&self.context, ack.follower_id, ack.durable_position);
            if let Some(watermark) = self.engine.acknowledge_replica(
                ack.follower_id,
                ack.epoch,
                Position(ack.durable_position),
            ) {
                let _ = send(
                    &self.out,
                    Frame::TailWatermark(p::TailWatermark {
                        epoch: ack.epoch,
                        position: watermark.0,
                    }),
                )
                .await;
            }
        }
        window.ack_notify.notify_waiters();
        Ok(())
    }
}

/// Everything a session streams to its follower: the catch-up the follower
/// needs, then the leader's live waves as they seal.
struct Sender {
    out: StreamSender,
    engine: Arc<EventStoreEngine>,
    control: Arc<ReplicationControl>,
    claim: ActiveClaim,
    claim_updates: watch::Receiver<Option<ActiveClaim>>,
    live: broadcast::Receiver<LiveFrame>,
    window: Arc<Window>,
    max_inflight: u64,
    epoch: u64,
    leader_id: u64,
    epoch_seen: u64,
    recovery_source: bool,
    from_position: u64,
    initial_truncate: Option<(u64, Option<u32>)>,
    slices: Vec<WaveSlice>,
    catchup_tail: Position,
    catchup_byte_ends: BTreeMap<u64, u64>,
}

impl Sender {
    /// Streams until something ends the session; returns what.
    async fn run(mut self) -> Error {
        match self.stream().await {
            Ok(never) => match never {},
            Err(ended) => ended,
        }
    }

    fn check_fence(&self) -> Result<(), Error> {
        if claim_matches(&self.control, self.claim) && self.engine.replication_epoch() == self.epoch
        {
            Ok(())
        } else {
            Err(fence_changed())
        }
    }

    async fn stream(&mut self) -> Result<Infallible, Error> {
        let epoch = self.epoch;
        let mut sent_bytes = 0u64;
        let mut current_base: Option<u64> = None;

        // A claimant recovering from this node is not adopting a leader's
        // epoch: it has already installed the one it is claiming under.
        if self.epoch_seen != epoch && !self.recovery_source {
            send(
                &self.out,
                Frame::TailEpochChange(p::TailEpochChange {
                    epoch,
                    leader_id: self.leader_id,
                    start_position: self.from_position,
                }),
            )
            .await?;
        }

        if let Some((position, prev)) = self.initial_truncate {
            send(
                &self.out,
                Frame::TailTruncate(p::TailTruncate {
                    epoch,
                    position,
                    has_boundary: true,
                    prev_record_crc: prev.unwrap_or(0),
                    prev_at_segment_start: prev.is_none(),
                }),
            )
            .await?;
        }

        for slice in std::mem::take(&mut self.slices) {
            if current_base != Some(slice.segment_base)
                && slice.first_position == slice.segment_base
            {
                send(
                    &self.out,
                    Frame::TailRotate(p::TailRotate {
                        epoch,
                        new_segment_base: slice.segment_base,
                    }),
                )
                .await?;
            }
            current_base = Some(slice.segment_base);

            let (chunk_tx, mut chunk_rx) = mpsc::channel(2);
            let reader = tokio::task::spawn_blocking(move || source_slice(slice, chunk_tx));
            while let Some(chunk) = chunk_rx.recv().await {
                self.check_fence()?;
                self.wait_for_window(sent_bytes, chunk.data.len() as u64)
                    .await;
                sent_bytes += chunk.data.len() as u64;
                self.send_records(epoch, chunk, sent_bytes).await?;
            }
            match reader.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => return Err(error),
                Err(error) => {
                    return Err(Error::Internal {
                        message: format!("catch-up reader panicked: {error}"),
                    });
                }
            }
        }

        self.check_fence()?;
        send(
            &self.out,
            Frame::TailWatermark(p::TailWatermark {
                epoch,
                position: self.engine.head().0,
            }),
        )
        .await?;

        // Live handoff. Frames already covered by the catch-up snapshot
        // are duplicates and are dropped. Partial overlap indicates a
        // broken wave boundary; force a cursor-based reconnect.
        loop {
            let live_frame = tokio::select! {
                frame = self.live.recv() => frame,
                changed = self.claim_updates.changed() => {
                    if changed.is_err() || !claim_matches(&self.control, self.claim) {
                        return Err(fence_changed());
                    }
                    continue;
                }
            };
            if self.engine.replication_epoch() != self.epoch {
                return Err(Error::Unavailable {
                    message: "source context epoch changed".into(),
                });
            }
            match live_frame {
                Ok(LiveFrame::Rotate {
                    epoch,
                    new_segment_base,
                }) => {
                    if new_segment_base < self.catchup_tail.0 {
                        continue;
                    }
                    send(
                        &self.out,
                        Frame::TailRotate(p::TailRotate {
                            epoch,
                            new_segment_base,
                        }),
                    )
                    .await?;
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
                    if let Some(catchup_end) = self.catchup_byte_ends.get(&segment_base).copied() {
                        if byte_end <= catchup_end {
                            continue;
                        }
                        if byte_start < catchup_end {
                            return Err(Error::Unavailable {
                                message: "live wave overlaps catch-up physical boundary; reopen \
                                          from durable cursor"
                                    .into(),
                            });
                        }
                    }
                    if first_position < self.catchup_tail.0 {
                        return Err(Error::Unavailable {
                            message:
                                "live wave overlaps catch-up boundary; reopen from durable cursor"
                                    .into(),
                        });
                    }
                    for chunk in
                        chunk_record_bytes(data, segment_base, first_position, next_position)?
                    {
                        self.wait_for_window(sent_bytes, chunk.data.len() as u64)
                            .await;
                        sent_bytes += chunk.data.len() as u64;
                        self.send_records(epoch, chunk, sent_bytes).await?;
                    }
                    send(
                        &self.out,
                        Frame::TailWatermark(p::TailWatermark {
                            epoch,
                            position: self.engine.head().0,
                        }),
                    )
                    .await?;
                }
                Ok(LiveFrame::Reset { epoch: reset_epoch }) => {
                    return Err(Error::Unavailable {
                        message: format!(
                            "live replication reset in epoch {reset_epoch}; reopen from durable cursor"
                        ),
                    });
                }
                Err(broadcast::error::RecvError::Lagged(_)) => {
                    return Err(Error::Unavailable {
                        message: "Tail session lagged live dispatch; reopen from durable cursor"
                            .into(),
                    });
                }
                Err(broadcast::error::RecvError::Closed) => return Err(follower_gone()),
            }
        }
    }

    async fn wait_for_window(&self, sent: u64, next_len: u64) {
        wait_for_window(
            &self.window.acked_bytes,
            &self.window.ack_notify,
            sent,
            next_len,
            self.max_inflight,
        )
        .await
    }

    async fn send_records(
        &self,
        epoch: u64,
        chunk: CatchupChunk,
        stream_bytes_end: u64,
    ) -> Result<(), Error> {
        self.window
            .sent_boundaries
            .lock()
            .insert(stream_bytes_end, chunk.next_position);
        let frame = Frame::TailRecords(p::TailRecords {
            epoch,
            segment_base: chunk.segment_base,
            first_position: chunk.first_position,
            data: &chunk.data,
            stream_bytes_end,
            next_position: chunk.next_position,
        });
        // The frame is its bytes plus a few fixed fields: sized up front, so
        // the bytes are copied into the frame once.
        self.out
            .send_sized(chunk.data.len() + TAIL_RECORDS_OVERHEAD, &frame)
            .await
            .map_err(|_| follower_gone())
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

async fn wait_for_window(acked: &AtomicU64, notify: &Notify, sent: u64, next_len: u64, max: u64) {
    loop {
        // Register before checking the atomic so an acknowledgement racing this
        // check leaves a permit instead of becoming a lost wakeup.
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let durable = acked.load(Ordering::Acquire);
        let inflight = sent.saturating_sub(durable);
        if inflight == 0 || inflight.saturating_add(next_len) <= max {
            return;
        }
        notified.await;
    }
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

/// The rejoin handshake end to end, below the network: a leader's log, a
/// returning follower's log, the plan the leader makes, and the follower
/// applying it the way the Tail client does.
#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::*;
    use crate::event::{AppendEvent, Tag};
    use crate::store::StoreOptions;

    /// Small enough that preallocated active segments are cheap to copy.
    const SEGMENT_SIZE: u64 = 64 * 1024;

    fn options(voters: &[u64]) -> StoreOptions {
        StoreOptions {
            max_segment_size: SEGMENT_SIZE,
            voters: voters.to_vec(),
            ..Default::default()
        }
    }

    /// A single-voter engine: every append is committed as soon as it is
    /// durable, so tests can write without a quorum.
    fn open_alone(dir: &Path) -> EventStoreEngine {
        EventStoreEngine::open_with_store_options(dir, &options(&[0])).unwrap()
    }

    /// The same log opened as a member of a three-node cluster, the way a
    /// restarted node comes back: its watermark is the last checkpoint's.
    fn open_in_cluster(dir: &Path) -> EventStoreEngine {
        EventStoreEngine::open_with_store_options(dir, &options(&[0, 1, 2])).unwrap()
    }

    fn event(name: &str, payload: &str) -> AppendEvent {
        AppendEvent {
            identifier: format!("id-{name}"),
            name: name.into(),
            version: "1.0".into(),
            timestamp: 1_712_345_678_000,
            payload: payload.as_bytes().to_vec(),
            metadata: vec![],
            tags: vec![Tag::from_str("agg", "agg-0")],
        }
    }

    fn append(engine: &EventStoreEngine, name: &str, payload: &str) {
        engine
            .append(AppendRequest {
                condition: None,
                events: vec![event(name, payload)],
            })
            .unwrap();
    }

    /// What the coordinator does on a claim: adopt the epoch, then write its
    /// `EpochChange` at a fresh segment.
    fn take_over(engine: &EventStoreEngine, epoch: u64) {
        engine.begin_replication_epoch(epoch, [0]).unwrap();
        engine.persist_epoch_change(epoch, 0).unwrap();
    }

    /// Epoch 1's log, which every node shares: an `EpochChange`, five events,
    /// and (when asked) a checkpoint that commits them. `fifth` is the payload
    /// of the last event, so a test can make one node's history differ.
    fn shared_history(dir: &Path, checkpoint: bool, fifth: &str) {
        let engine = EventStoreEngine::create_with_store_options(dir, &options(&[0])).unwrap();
        take_over(&engine, 1);
        for i in 0..4 {
            append(&engine, &format!("e{i}"), "shared");
        }
        append(&engine, "e4", fifth);
        if checkpoint {
            engine.persist_watermark_checkpoint().unwrap();
        }
        engine.shutdown();
    }

    /// Copies a node's files, its index directory included. A rotated
    /// segment's index files are written in the background as `*.tmp` and
    /// renamed, which `shutdown` does not wait for: those are skipped, and a
    /// copy without them rebuilds them at open.
    fn copy_log(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            if entry.path().extension().is_some_and(|ext| ext == "tmp") {
                continue;
            }
            if entry.file_type().unwrap().is_dir() {
                copy_log(&entry.path(), &to.join(entry.file_name()));
                continue;
            }
            std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
        }
    }

    struct Logs {
        _tmp: tempfile::TempDir,
        leader: PathBuf,
        follower: PathBuf,
    }

    /// Two copies of the same history, one per node.
    fn logs(checkpoint: bool) -> Logs {
        let tmp = tempfile::tempdir().unwrap();
        let base = tmp.path().join("base");
        shared_history(&base, checkpoint, "shared");
        let (leader, follower) = (tmp.path().join("leader"), tmp.path().join("follower"));
        copy_log(&base, &leader);
        copy_log(&base, &follower);
        Logs {
            _tmp: tmp,
            leader,
            follower,
        }
    }

    /// The deposed leader: events 5 and 6 only it holds, written in the
    /// epoch it still believes in (or a later one only it knew of).
    fn write_unreplicated_suffix(follower: &Path, epoch: Option<u64>) {
        let engine = open_alone(follower);
        if let Some(epoch) = epoch {
            take_over(&engine, epoch);
        }
        append(&engine, "e5", "unreplicated-5");
        append(&engine, "e6", "unreplicated-6");
        engine.shutdown();
    }

    fn describe(engine: &EventStoreEngine) -> FollowerLog {
        let cursor = engine.replication_cursor().unwrap();
        FollowerLog {
            from_position: cursor.position.0,
            segment_base: cursor.segment_base,
            byte_offset: cursor.byte_offset,
            last_record_crc: cursor.last_record_crc,
            epoch_seen: engine.log_epoch(),
            watermark: engine.head().0,
        }
    }

    /// The valid bytes of every segment, in order.
    fn log_bytes(dir: &Path) -> Vec<(u64, Vec<u8>)> {
        segment::list_segment_files(dir)
            .unwrap()
            .into_iter()
            .map(|base| {
                let bytes = std::fs::read(segment::segment_path(dir, base)).unwrap();
                let mut end = segment::SEGMENT_HEADER_SIZE;
                while end + segment::RECORD_HEADER_SIZE <= bytes.len() {
                    let header: &[u8; segment::RECORD_HEADER_SIZE] = bytes
                        [end..end + segment::RECORD_HEADER_SIZE]
                        .try_into()
                        .unwrap();
                    match record::parse_header(header).unwrap() {
                        Some(header) => end += header.total_len(),
                        None => break,
                    }
                }
                (base, bytes[..end].to_vec())
            })
            .collect()
    }

    /// What the Tail client does with the leader's answer, as far as the log
    /// is concerned: cut back, adopt the leader's epoch, then take its bytes
    /// (rotating where the leader did) until the follower holds all of them.
    fn follow(
        leader: &EventStoreEngine,
        follower: &EventStoreEngine,
        start: &TailStart,
    ) -> Result<(), Error> {
        let slices = match start {
            TailStart::ExactCursor => {
                let cursor = follower.replication_cursor()?;
                leader.replication_catchup_from_cursor(cursor)?.1
            }
            TailStart::Truncate { position, prev } => {
                follower.truncate_to_matching(Position(*position), Some(*prev))?;
                leader.replication_catchup_slices(Position(*position))?.1
            }
            TailStart::NeedSnapshot { .. } => panic!("the follower was told to restore"),
        };
        let epoch = leader.replication_epoch();
        if follower.replication_epoch() < epoch {
            follower.begin_replication_epoch(epoch, [0, 1, 2])?;
        }
        for slice in slices {
            if slice.first_position == slice.segment_base {
                follower.rotate_replicated(epoch, Position(slice.segment_base))?;
            }
            let mut file = File::open(&slice.path)?;
            file.seek(SeekFrom::Start(slice.byte_start))?;
            let mut bytes = vec![0u8; (slice.byte_end - slice.byte_start) as usize];
            file.read_exact(&mut bytes)?;
            let write = follower.apply_replicated_records(
                epoch,
                slice.segment_base,
                Position(slice.first_position),
                &bytes,
            )?;
            follower.wait_replicated_durable(write)?;
        }
        Ok(())
    }

    fn assert_converged(leader: &Path, follower: &Path) {
        assert_eq!(
            log_bytes(follower),
            log_bytes(leader),
            "the follower is not byte-identical to the leader"
        );
        assert_indexes_identical(leader, follower);
    }

    /// The sealed segments' indexes are byte-identical too: the follower
    /// built its live index from replicated frames, the leader from its own
    /// appends, and both wrote the same `.idx` and `.keys` at the seal. Seals land in
    /// the background, so this waits for them.
    fn assert_indexes_identical(leader: &Path, follower: &Path) {
        let mut bases = segment::list_segment_files(leader).unwrap();
        bases.pop(); // the active segment
        let read = |dir: &Path, base: u64| {
            std::fs::read(segment::segment_path(dir, base).with_extension("idx")).ok()
        };
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        for base in bases {
            let (ours, theirs) = loop {
                match (read(leader, base), read(follower, base)) {
                    (Some(ours), Some(theirs)) => break (ours, theirs),
                    _ if std::time::Instant::now() < deadline => {
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                    other => panic!(
                        "segment {base} sealed without an index: {:?}",
                        (other.0.is_some(), other.1.is_some())
                    ),
                }
            };
            assert!(
                ours == theirs,
                "segment {base}: follower's index differs from the leader's"
            );
            // The key table too, unless a merge has already taken it in.
            let keys = |dir: &Path| {
                std::fs::read(segment::segment_path(dir, base).with_extension("keys")).ok()
            };
            if let (Some(ours), Some(theirs)) = (keys(leader), keys(follower)) {
                assert!(
                    ours == theirs,
                    "segment {base}: follower's key table differs from the leader's"
                );
            }
        }
    }

    /// The reported bug: the returning node holds two events the new leader
    /// never saw, the new leader wrote one event and a checkpoint, so the
    /// follower's position (7) is past the leader's tail (6). The cut has to
    /// be where the two histories diverge (5), not the leader's tail.
    #[test]
    fn a_node_ahead_of_a_leader_that_ends_in_a_checkpoint_is_cut_where_the_histories_split() {
        let logs = logs(true);
        write_unreplicated_suffix(&logs.follower, None);

        let leader = open_alone(&logs.leader);
        take_over(&leader, 2);
        append(&leader, "e5", "leader-5");
        leader.persist_watermark_checkpoint().unwrap();
        let follower = open_in_cluster(&logs.follower);
        assert_eq!(follower.local_tail().0, 7);
        assert_eq!(
            follower.head().0,
            5,
            "only the checkpointed events are committed"
        );
        assert_eq!(follower.log_epoch(), 1);

        let start = plan_tail_start(&leader, &describe(&follower)).unwrap();
        let TailStart::Truncate { position, .. } = start else {
            panic!("expected a cut, got {start:?}");
        };
        assert_eq!(position, 5, "epoch 1 ends where epoch 2 took over");

        follow(&leader, &follower, &start).unwrap();
        assert_eq!(follower.local_tail().0, 6);
        assert_eq!(follower.log_epoch(), 2);
        assert_eq!(follower.epoch_extent(1), EpochExtent::EndsAt(5));
        assert_converged(&logs.leader, &logs.follower);
    }

    /// The same overlap with an event, not a checkpoint, last in the leader's
    /// log: the record before the leader's tail is one the follower never had.
    #[test]
    fn a_node_ahead_of_a_leader_that_ends_in_an_event_is_cut_where_the_histories_split() {
        let logs = logs(true);
        write_unreplicated_suffix(&logs.follower, None);

        let leader = open_alone(&logs.leader);
        take_over(&leader, 2);
        append(&leader, "e5", "leader-5");
        let follower = open_in_cluster(&logs.follower);

        let start = plan_tail_start(&leader, &describe(&follower)).unwrap();
        assert!(matches!(start, TailStart::Truncate { position: 5, .. }));
        follow(&leader, &follower, &start).unwrap();
        assert_converged(&logs.leader, &logs.follower);
    }

    /// The leader has been through several failovers while the node was away,
    /// and the node's epoch ended at the first one.
    #[test]
    fn a_node_several_epochs_behind_is_cut_at_the_end_of_its_own_epoch() {
        let logs = logs(true);
        write_unreplicated_suffix(&logs.follower, None);

        let leader = open_alone(&logs.leader);
        take_over(&leader, 2);
        append(&leader, "e5", "leader-5");
        take_over(&leader, 3);
        append(&leader, "e6", "leader-6");
        append(&leader, "e7", "leader-7");
        take_over(&leader, 5);
        let follower = open_in_cluster(&logs.follower);

        let start = plan_tail_start(&leader, &describe(&follower)).unwrap();
        assert!(
            matches!(start, TailStart::Truncate { position: 5, .. }),
            "epoch 1 ended at 5 however many epochs followed: {start:?}"
        );
        follow(&leader, &follower, &start).unwrap();
        assert_eq!(follower.log_epoch(), 5);
        assert_eq!(follower.local_tail().0, 8);
        assert_converged(&logs.leader, &logs.follower);
    }

    /// A node that led an epoch whose `EpochChange` never reached the leader
    /// (or was purged from it) has nothing tying its suffix to the leader's
    /// history. It falls back to its watermark, and what it wrote in the
    /// unknown epoch goes with the cut.
    #[test]
    fn a_node_whose_epoch_the_leader_never_saw_falls_back_to_its_watermark() {
        let logs = logs(true);
        write_unreplicated_suffix(&logs.follower, Some(3));

        let leader = open_alone(&logs.leader);
        take_over(&leader, 2);
        append(&leader, "e5", "leader-5");
        take_over(&leader, 4);
        append(&leader, "e6", "leader-6");
        let follower = open_in_cluster(&logs.follower);
        assert_eq!(follower.log_epoch(), 3);
        assert_eq!(leader.epoch_extent(3), EpochExtent::Unknown);

        let start = plan_tail_start(&leader, &describe(&follower)).unwrap();
        assert!(
            matches!(start, TailStart::Truncate { position: 5, .. }),
            "the follower's watermark is the agreed point: {start:?}"
        );
        follow(&leader, &follower, &start).unwrap();
        assert_eq!(follower.epoch_extent(3), EpochExtent::Unknown);
        assert_eq!(follower.log_epoch(), 4);
        assert_converged(&logs.leader, &logs.follower);
    }

    /// With the same shape but a follower that cannot be cut to its watermark
    /// because the leader no longer holds that prefix, the follower restores.
    #[test]
    fn a_follower_committed_past_the_end_of_its_epoch_is_told_to_restore() {
        let logs = logs(true);
        {
            // The follower committed its two extra events too.
            let engine = open_alone(&logs.follower);
            append(&engine, "e5", "committed-5");
            append(&engine, "e6", "committed-6");
            engine.persist_watermark_checkpoint().unwrap();
            engine.shutdown();
        }
        let leader = open_alone(&logs.leader);
        take_over(&leader, 2);
        for i in 5..8 {
            append(&leader, &format!("e{i}"), "leader");
        }
        let follower = open_in_cluster(&logs.follower);
        assert_eq!(follower.head().0, 7);

        // Committed bytes past where the leader's history of the epoch ends
        // cannot happen between honest nodes: it is never cut for.
        let start = plan_tail_start(&leader, &describe(&follower)).unwrap();
        assert!(
            matches!(start, TailStart::NeedSnapshot { .. }),
            "expected a restore, got {start:?}"
        );
    }

    /// Bytes below the agreed point that differ are not a rejoin to repair:
    /// the cut's boundary record does not match, and that stays a corruption.
    #[test]
    fn a_different_history_below_the_cut_is_still_corrupted() {
        let tmp = tempfile::tempdir().unwrap();
        let (leader_dir, follower_dir) = (tmp.path().join("leader"), tmp.path().join("follower"));
        // Same shape, but the follower's fifth event is not the leader's.
        shared_history(&leader_dir, false, "shared");
        shared_history(&follower_dir, false, "forged");
        write_unreplicated_suffix(&follower_dir, None);

        let leader = open_alone(&leader_dir);
        take_over(&leader, 2);
        append(&leader, "e5", "leader-5");
        let follower = open_in_cluster(&follower_dir);

        let start = plan_tail_start(&leader, &describe(&follower)).unwrap();
        assert!(matches!(start, TailStart::Truncate { position: 5, .. }));
        let error = follow(&leader, &follower, &start).unwrap_err();
        assert!(
            matches!(&error, Error::Corrupted { message } if message.contains("diverges")),
            "expected a divergence, got {error:?}"
        );
        assert_eq!(
            follower.local_tail().0,
            7,
            "a log that failed the check is left as it was"
        );
    }

    /// Epoch 1 without a checkpoint at its end, on two nodes, then a
    /// checkpoint only the leader wrote: the leader's last record before the
    /// events the follower is missing is one the follower never had.
    fn logs_with_a_checkpoint_only_the_leader_holds() -> Logs {
        let logs = logs(false);
        let engine = open_alone(&logs.leader);
        engine.persist_watermark_checkpoint().unwrap();
        engine.shutdown();
        logs
    }

    /// The follower led an epoch the leader never saw, so it is cut back to
    /// its own watermark (5), a position. The leader holds a control record
    /// between its event 4 and event 5 that the follower lacks, and the two
    /// must still agree they share events 0..5: records that carry no event
    /// are not history to compare.
    #[test]
    fn a_control_record_only_the_leader_holds_at_the_cut_is_not_a_divergence() {
        let logs = logs_with_a_checkpoint_only_the_leader_holds();
        write_unreplicated_suffix(&logs.follower, Some(3));

        let leader = open_alone(&logs.leader);
        take_over(&leader, 4);
        append(&leader, "e5", "leader-5");
        let follower = open_in_cluster(&logs.follower);
        follower.adopt_watermark(follower.replication_epoch(), Position(5));
        assert_eq!(follower.head().0, 5);
        assert_eq!(
            leader.epoch_extent(follower.log_epoch()),
            EpochExtent::Unknown
        );

        let start = plan_tail_start(&leader, &describe(&follower)).unwrap();
        assert!(matches!(start, TailStart::Truncate { position: 5, .. }));
        follow(&leader, &follower, &start).unwrap();
        assert_eq!(follower.local_tail().0, 6);
        assert_converged(&logs.leader, &logs.follower);
    }

    /// The same gap inside one segment: the leader's checkpoint sits between
    /// its events 4 and 5 in the segment the follower's event 5 is in too.
    #[test]
    fn a_control_record_only_the_leader_holds_inside_a_segment_is_not_a_divergence() {
        let logs = logs(false);
        {
            let engine = open_alone(&logs.leader);
            engine.persist_watermark_checkpoint().unwrap();
            append(&engine, "e5", "leader-5");
            append(&engine, "e6", "leader-6");
            engine.shutdown();
        }
        {
            let engine = open_alone(&logs.follower);
            append(&engine, "e5", "unreplicated-5");
            engine.shutdown();
        }
        let leader = open_alone(&logs.leader);
        let follower = open_in_cluster(&logs.follower);
        follower.adopt_watermark(follower.replication_epoch(), Position(5));
        assert_eq!(follower.head().0, 5);

        let start = plan_tail_start(&leader, &describe(&follower)).unwrap();
        assert!(matches!(start, TailStart::Truncate { position: 5, .. }));
        follow(&leader, &follower, &start).unwrap();
        assert_eq!(follower.local_tail().0, 7);
        assert_converged(&logs.leader, &logs.follower);
    }

    /// Both nodes wrote a checkpoint after event 4, but not the same one: the
    /// follower's goes, the leader's is sent, and the logs end up identical.
    #[test]
    fn a_different_control_record_at_the_cut_is_replaced_by_the_leaders() {
        let logs = logs(false);
        {
            let engine = open_alone(&logs.leader);
            engine.persist_watermark_checkpoint().unwrap();
            engine.shutdown();
        }
        {
            // Committed only up to 4 when it wrote its checkpoint.
            let engine = open_in_cluster(&logs.follower);
            engine.adopt_watermark(engine.replication_epoch(), Position(4));
            engine.persist_watermark_checkpoint().unwrap();
            engine.shutdown();
        }
        write_unreplicated_suffix(&logs.follower, Some(3));
        let leader = open_alone(&logs.leader);
        take_over(&leader, 4);
        append(&leader, "e5", "leader-5");
        let follower = open_in_cluster(&logs.follower);
        follower.adopt_watermark(follower.replication_epoch(), Position(5));

        let start = plan_tail_start(&leader, &describe(&follower)).unwrap();
        assert!(matches!(start, TailStart::Truncate { position: 5, .. }));
        follow(&leader, &follower, &start).unwrap();
        assert_converged(&logs.leader, &logs.follower);
    }

    /// A checkpoint only the follower holds, at the end of the segment its
    /// own epoch began after: dropping it means reopening a segment that was
    /// already sealed, and the leader's bytes carry on from there.
    #[test]
    fn a_control_record_only_the_follower_holds_at_the_end_of_a_sealed_segment_is_dropped() {
        let logs = logs(false);
        {
            let engine = open_alone(&logs.follower);
            engine.persist_watermark_checkpoint().unwrap();
            engine.shutdown();
        }
        write_unreplicated_suffix(&logs.follower, Some(3));
        let leader = open_alone(&logs.leader);
        take_over(&leader, 4);
        append(&leader, "e5", "leader-5");
        let follower = open_in_cluster(&logs.follower);
        assert_eq!(follower.head().0, 5);

        let start = plan_tail_start(&leader, &describe(&follower)).unwrap();
        assert!(matches!(start, TailStart::Truncate { position: 5, .. }));
        follow(&leader, &follower, &start).unwrap();
        assert_eq!(follower.local_tail().0, 6);
        assert_converged(&logs.leader, &logs.follower);
    }

    /// The gap must not hide a real difference: with the leader's extra
    /// checkpoint in the way, a follower whose event 4 is not the leader's
    /// still fails loudly, and keeps its bytes.
    #[test]
    fn a_different_event_before_a_control_record_gap_is_still_corrupted() {
        let tmp = tempfile::tempdir().unwrap();
        let (leader_dir, follower_dir) = (tmp.path().join("leader"), tmp.path().join("follower"));
        shared_history(&leader_dir, false, "shared");
        shared_history(&follower_dir, false, "forged");
        {
            let engine = open_alone(&leader_dir);
            engine.persist_watermark_checkpoint().unwrap();
            engine.shutdown();
        }
        write_unreplicated_suffix(&follower_dir, Some(3));
        let leader = open_alone(&leader_dir);
        take_over(&leader, 4);
        append(&leader, "e5", "leader-5");
        let follower = open_in_cluster(&follower_dir);
        follower.adopt_watermark(follower.replication_epoch(), Position(5));
        let before = log_bytes(&follower_dir);

        let start = plan_tail_start(&leader, &describe(&follower)).unwrap();
        assert!(matches!(start, TailStart::Truncate { position: 5, .. }));
        let error = follow(&leader, &follower, &start).unwrap_err();
        assert!(
            matches!(&error, Error::Corrupted { message } if message.contains("diverges")),
            "expected a divergence, got {error:?}"
        );
        assert_eq!(log_bytes(&follower_dir), before, "nothing was cut");
    }

    /// A follower whose log is a byte prefix of the leader's resumes where it
    /// stands, whatever the leader's epochs.
    #[test]
    fn a_follower_that_is_a_prefix_of_the_leader_resumes_exactly() {
        let logs = logs(true);
        let leader = open_alone(&logs.leader);
        take_over(&leader, 2);
        append(&leader, "e5", "leader-5");
        let follower = open_in_cluster(&logs.follower);

        let start = plan_tail_start(&leader, &describe(&follower)).unwrap();
        assert_eq!(start, TailStart::ExactCursor);
        follow(&leader, &follower, &start).unwrap();
        assert_converged(&logs.leader, &logs.follower);
    }

    /// Epoch alignment is for a leader's *past* epochs. A node in the
    /// leader's newest epoch that holds more than the leader keeps the cut at
    /// the leader's tail.
    #[test]
    fn a_node_ahead_in_the_leaders_newest_epoch_is_cut_to_the_leaders_tail() {
        let logs = logs(true);
        {
            let engine = open_alone(&logs.leader);
            take_over(&engine, 2);
            append(&engine, "e5", "leader-5");
            engine.shutdown();
        }
        copy_log(&logs.leader, &logs.follower);
        {
            let engine = open_alone(&logs.follower);
            append(&engine, "e6", "follower-6");
            engine.shutdown();
        }
        let leader = open_alone(&logs.leader);
        let follower = open_in_cluster(&logs.follower);
        assert_eq!(leader.epoch_extent(follower.log_epoch()), EpochExtent::Open);

        let start = plan_tail_start(&leader, &describe(&follower)).unwrap();
        assert!(
            matches!(start, TailStart::Truncate { position: 6, .. }),
            "expected a cut at the leader's tail, got {start:?}"
        );
    }
}
