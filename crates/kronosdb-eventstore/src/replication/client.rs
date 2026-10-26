//! Follower-side Tail client for the authoritative native segment log.

use std::sync::Arc;

use kronosdb_protocol as p;
use kronosdb_protocol::Frame;
use tokio::sync::watch;

use crate::error::Error;
use crate::event::Position;
use crate::store::{EventStoreEngine, ReplicatedWrite};
use crate::wire;

use super::peer::PeerPool;

#[derive(Debug, Clone)]
pub struct TailClientConfig {
    pub context: String,
    pub follower_id: u64,
    pub leader_id: u64,
    pub leader_addr: String,
    pub epoch: u64,
    pub voters: Vec<u64>,
    pub recovery_claim: Option<(u64, u64)>,
    pub peers: Arc<PeerPool>,
}

/// Runs one Tail session until the leader closes it or an error requires the
/// coordinator to re-resolve leadership and reconnect from the engine's last
/// durable cursor.
pub async fn run_tail_session(
    engine: Arc<EventStoreEngine>,
    config: TailClientConfig,
) -> Result<(), Error> {
    run_tail(engine, config, None).await
}

/// Pulls a bounded suffix from a peer before a newly elected coordinator
/// claims leadership. The peer is only a byte source here; no write gate is
/// opened until the subsequent metadata claim and durable EpochChange.
pub async fn catch_up_to(
    engine: Arc<EventStoreEngine>,
    config: TailClientConfig,
    target: Position,
) -> Result<(), Error> {
    run_tail(engine, config, Some(target)).await
}

async fn run_tail(
    engine: Arc<EventStoreEngine>,
    config: TailClientConfig,
    stop_at: Option<Position>,
) -> Result<(), Error> {
    if engine.replication_epoch() > config.epoch {
        return Err(Error::Unavailable {
            message: format!(
                "Tail epoch {} is older than engine epoch {}",
                config.epoch,
                engine.replication_epoch()
            ),
        });
    }

    // A connection of its own: a replication frame can be tens of megabytes
    // and must not hold up Raft heartbeats or forwarded appends behind it.
    let connection = config.peers.dedicated(&config.leader_addr).await?;
    let cursor = engine.replication_cursor()?;
    // A follower says which epoch its log's bytes were written under, not the
    // one it has adopted: the leader aligns the cut of a diverged log to the
    // end of that epoch. A node adopts an epoch before the leader's bytes (and
    // its truncation) arrive, so after a dropped session its adopted epoch is
    // ahead of its log. A recovering claimant says the same thing.
    let epoch_seen = engine.log_epoch();
    let recovery = config.recovery_claim.is_some();
    let mut stream = connection
        .open(
            &Frame::TailOpen(p::TailOpen {
                context: &config.context,
                follower_id: config.follower_id,
                from_position: cursor.position.0,
                last_record_crc: cursor.last_record_crc,
                epoch_seen,
                segment_base: cursor.segment_base,
                byte_offset: cursor.byte_offset,
                watermark: engine.head().0,
                recovery_source: config.recovery_claim.is_some(),
                claim_epoch: config.recovery_claim.map(|claim| claim.0).unwrap_or(0),
                claim_leader_id: config.recovery_claim.map(|claim| claim.1).unwrap_or(0),
            }),
            false,
        )
        .await
        .map_err(wire::net_error)?;

    // Acknowledgements are decoupled from frame processing. Applying frames
    // stays serial (ordering matters), but the durability wait + ack runs in
    // its own task fed latest-wins, so acks coalesce to durability waves
    // instead of gating the stream on one wait+ack round-trip per frame —
    // at high wave rates that serial loop saturates and every frame queues.
    let (pending_tx, mut pending_rx) = watch::channel::<Option<PendingAck>>(None);
    let (acked_tx, mut acked_rx) = watch::channel::<u64>(0);
    let mut ack_task = {
        let engine = Arc::clone(&engine);
        let acks = stream.writer();
        let context = config.context.clone();
        let follower_id = config.follower_id;
        tokio::spawn(async move {
            while pending_rx.changed().await.is_ok() {
                let Some(ack) = *pending_rx.borrow_and_update() else {
                    continue;
                };
                if let Some(write) = ack.write
                    && (!crate::written_acks(engine.voter_count())
                        || ack.position.saturating_sub(engine.durable_tail().0)
                            > crate::ack_lag_limit())
                {
                    // Durable mode always waits; replicated mode waits only
                    // when acks have run past the lag limit (disk-stall
                    // backpressure — the leader then sees follower cursors
                    // advance at fsync pace until the disk catches up).
                    wait_durable(Arc::clone(&engine), write).await?;
                }
                acks.send(&Frame::TailAck(p::TailAck {
                    context: &context,
                    follower_id,
                    epoch: ack.epoch,
                    durable_position: ack.position,
                    durable_bytes: ack.bytes,
                }))
                .await
                .map_err(wire::net_error)?;
                let _ = acked_tx.send(ack.position);
            }
            Ok::<(), Error>(())
        })
    };

    // A normal session runs in the epoch of the leader it was opened under.
    // A claimant's source is a voter, not a leader: it stamps the epoch it
    // has adopted (never past the one the claimant adopted), and every frame
    // of the session has to agree on it.
    let mut session_epoch = (!recovery).then_some(config.epoch);
    let mut last_ack_position = 0u64;
    let mut check_epoch = |actual: u64| -> Result<u64, Error> {
        let expected = *session_epoch.get_or_insert(actual);
        if actual != expected || actual > config.epoch {
            return Err(stale_epoch(actual, expected));
        }
        Ok(actual)
    };

    // The epoch the local engine is written under: the leader's for a normal
    // session, the claimant's own adopted one for a recovering claimant.
    let apply_epoch = |engine: &EventStoreEngine, frame_epoch: u64| {
        if recovery {
            engine.replication_epoch()
        } else {
            frame_epoch
        }
    };

    while let Some(frame) = stream.next().await.map_err(wire::net_error)? {
        match frame.decode().map_err(|error| Error::Internal {
            message: format!("malformed Tail frame: {error}"),
        })? {
            Frame::TailEpochChange(change) => {
                if change.leader_id != config.leader_id || change.epoch != config.epoch {
                    return Err(Error::Unavailable {
                        message: format!(
                            "Tail leader/epoch changed to {}/{}",
                            change.leader_id, change.epoch
                        ),
                    });
                }
                if engine.replication_epoch() < change.epoch {
                    engine.begin_replication_epoch(change.epoch, config.voters.clone())?;
                }
            }
            Frame::TailRotate(rotate) => {
                let frame_epoch = check_epoch(rotate.epoch)?;
                engine.rotate_replicated(
                    apply_epoch(&engine, frame_epoch),
                    Position(rotate.new_segment_base),
                )?;
            }
            Frame::TailRecords(records) => {
                let frame_epoch = check_epoch(records.epoch)?;
                let local_tail = engine.local_tail().0;
                if records.next_position < local_tail
                    || (records.next_position == local_tail
                        && records.first_position < records.next_position)
                {
                    // Fully duplicated catch-up/live overlap. Bytes are
                    // already durable; acknowledge this session byte cursor.
                    last_ack_position = local_tail;
                    pending_tx.send_replace(Some(PendingAck {
                        write: None,
                        epoch: frame_epoch,
                        position: local_tail,
                        bytes: records.stream_bytes_end,
                    }));
                    continue;
                }
                if records.first_position != local_tail {
                    return Err(Error::Internal {
                        message: format!(
                            "Tail frame positions {}..{} do not continue local tail {}",
                            records.first_position, records.next_position, local_tail
                        ),
                    });
                }

                let write = engine.apply_replicated_records(
                    apply_epoch(&engine, frame_epoch),
                    records.segment_base,
                    Position(records.first_position),
                    records.data,
                )?;
                if write.durable_position.0 != records.next_position {
                    return Err(Error::Corrupted {
                        message: format!(
                            "Tail frame declared next position {}, decoded {}",
                            records.next_position, write.durable_position.0
                        ),
                    });
                }
                if ack_task.is_finished() {
                    return match (&mut ack_task).await {
                        Ok(Ok(())) => Err(Error::Internal {
                            message: "Tail ack task stopped unexpectedly".into(),
                        }),
                        Ok(Err(error)) => Err(error),
                        Err(error) => Err(Error::Internal {
                            message: format!("Tail ack task failed: {error}"),
                        }),
                    };
                }
                last_ack_position = write.durable_position.0;
                pending_tx.send_replace(Some(PendingAck {
                    write: Some(write),
                    epoch: frame_epoch,
                    position: write.durable_position.0,
                    bytes: records.stream_bytes_end,
                }));
            }
            Frame::TailWatermark(watermark) => {
                let frame_epoch = check_epoch(watermark.epoch)?;
                engine.adopt_watermark(
                    apply_epoch(&engine, frame_epoch),
                    Position(watermark.position),
                );
                // The leader sends this once the catch-up it computed is
                // out, so a claimant that stops here has every byte of the
                // source's suffix (control records included), not just the
                // events up to its target.
                if stop_at
                    .map(|target| engine.local_tail().0 >= target.0)
                    .unwrap_or(false)
                {
                    flush_acks(&mut acked_rx, last_ack_position).await?;
                    return Ok(());
                }
            }
            Frame::TailTruncate(truncate) => {
                check_epoch(truncate.epoch)?;
                let expected_event = truncate.has_boundary.then_some(
                    (!truncate.prev_at_segment_start).then_some(truncate.prev_record_crc),
                );
                let before = engine.local_tail().0;
                engine.truncate_to_matching(Position(truncate.position), expected_event)?;
                tracing::info!(
                    context = %config.context,
                    boundary = truncate.position,
                    tail_before = before,
                    "Tail truncated to the leader's boundary"
                );
            }
            Frame::TailNeedSnapshot(snapshot) => {
                return Err(Error::Rejected {
                    message: format!(
                        "Tail requires snapshot from {} (watermark {}): {}",
                        snapshot.available_from, snapshot.watermark, snapshot.reason
                    ),
                });
            }
            other => {
                return Err(Error::Internal {
                    message: format!("unexpected {} frame on a Tail", other.name()),
                });
            }
        }
    }

    Err(Error::Unavailable {
        message: "leader closed Tail stream".into(),
    })
}

async fn wait_durable(engine: Arc<EventStoreEngine>, write: ReplicatedWrite) -> Result<(), Error> {
    tokio::task::spawn_blocking(move || engine.wait_replicated_durable(write))
        .await
        .map_err(|error| Error::Io(std::io::Error::other(error.to_string())))?
}

/// Latest-wins ack handed to the ack task. `write` is None when the acked
/// range is already durable locally (duplicated catch-up/live overlap).
#[derive(Clone, Copy)]
struct PendingAck {
    write: Option<ReplicatedWrite>,
    /// The epoch the session runs in, which the source expects on its acks.
    epoch: u64,
    position: u64,
    bytes: u64,
}

/// Blocks a stopping Tail until the ack task has acknowledged `target`, so
/// recovery sessions never return before their final cursor is on the wire.
async fn flush_acks(acked_rx: &mut watch::Receiver<u64>, target: u64) -> Result<(), Error> {
    while *acked_rx.borrow() < target {
        acked_rx
            .changed()
            .await
            .map_err(|_| Error::Io(std::io::Error::other("Tail ack task stopped")))?;
    }
    Ok(())
}

fn stale_epoch(actual: u64, expected: u64) -> Error {
    Error::Unavailable {
        message: format!("stale Tail epoch {actual}; expected {expected}"),
    }
}
