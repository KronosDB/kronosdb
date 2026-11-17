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

    // A catch-up from the archive that a given-up session started still
    // runs on its blocking thread: stop it before reading where the log ends.
    {
        let engine = Arc::clone(&engine);
        tokio::task::spawn_blocking(move || engine.end_archive_catchup())
            .await
            .map_err(|error| Error::Internal {
                message: format!("stopping a catch-up from the archive: {error}"),
            })?;
    }

    // A connection of its own: a replication frame can be tens of megabytes
    // and must not hold up Raft heartbeats or forwarded appends behind it.
    let connection = config.peers.dedicated(&config.leader_addr).await?;
    // Waits for the group-commit fsync and reads the last record from disk.
    let cursor = {
        let engine = Arc::clone(&engine);
        tokio::task::spawn_blocking(move || engine.replication_cursor())
            .await
            .map_err(|error| Error::Internal {
                message: format!("reading the replication cursor: {error}"),
            })??
    };
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
    // The latest bytes this session wrote, which may not be fsynced yet.
    let mut last_write: Option<ReplicatedWrite> = None;
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

    // The highest watermark the leader has told this session. It can be ahead
    // of what this node has applied (the frame travels beside the records, not
    // behind them), so it is held and adopted up to the local tail as bytes
    // arrive. Everything at or below it is committed cluster-wide.
    let mut leader_watermark = 0u64;
    let mut archive_up_to = None;
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
                    || (records.first_position == local_tail
                        && records.next_position == local_tail
                        && engine.holds_whole_segment(
                            records.segment_base,
                            Position(records.first_position),
                        ))
                {
                    // Fully duplicated catch-up/live overlap, or the end of a
                    // segment this node holds in the archive. Nothing is
                    // written, but `local_tail` is where bytes were written,
                    // not where they were fsynced: this session may have
                    // applied some of them a moment ago.
                    last_ack_position = local_tail;
                    pending_tx.send_replace(Some(PendingAck::duplicate(
                        last_write,
                        frame_epoch,
                        local_tail,
                        records.stream_bytes_end,
                    )));
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
                    return Err(Error::Internal {
                        message: format!(
                            "Tail frame declared next position {}, but its records decoded to {}; \
                             the leader's frame is inconsistent, so report it with both nodes' logs",
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
                last_write = Some(write);
                engine.adopt_watermark(
                    apply_epoch(&engine, frame_epoch),
                    Position(leader_watermark.min(engine.local_tail().0)),
                );
                pending_tx.send_replace(Some(PendingAck {
                    write: Some(write),
                    epoch: frame_epoch,
                    position: write.durable_position.0,
                    bytes: records.stream_bytes_end,
                }));
            }
            Frame::TailWatermark(watermark) => {
                let frame_epoch = check_epoch(watermark.epoch)?;
                leader_watermark = leader_watermark.max(watermark.position);
                engine.adopt_watermark(
                    apply_epoch(&engine, frame_epoch),
                    Position(leader_watermark.min(engine.local_tail().0)),
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
                // Checking the cut can read the last event before it from the
                // archive.
                {
                    let engine = Arc::clone(&engine);
                    let position = Position(truncate.position);
                    tokio::task::spawn_blocking(move || {
                        engine.truncate_to_matching(position, expected_event)
                    })
                    .await
                    .map_err(|error| Error::Internal {
                        message: format!("cutting the log back: {error}"),
                    })??;
                }
                tracing::info!(
                    context = %config.context,
                    boundary = truncate.position,
                    tail_before = before,
                    "Tail truncated to the leader's boundary"
                );
            }
            Frame::TailNeedArchive(need) => {
                archive_up_to = Some(need.up_to);
                break;
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

    let Some(up_to) = archive_up_to else {
        return Err(Error::Unavailable {
            message: "leader closed Tail stream".into(),
        });
    };
    // The leader has evicted what comes next (ADR-0011): read it from the
    // archive, then open a new Tail where the leader's segments start.
    ack_task.abort();
    drop(stream);
    let before = engine.local_tail().0;
    let applied = {
        let engine = Arc::clone(&engine);
        let session = engine.archive_catchup_session();
        tokio::task::spawn_blocking(move || engine.apply_archived_in(up_to, session))
            .await
            .map_err(|error| Error::Internal {
                message: format!("catching up from the archive: {error}"),
            })?
            .inspect_err(|error| {
                // The session is retried from scratch; say why, since the
                // retry loop only says that it reconnects.
                tracing::warn!(
                    context = %config.context,
                    up_to,
                    %error,
                    "catching up from the archive failed"
                );
            })?
    };
    tracing::info!(
        context = %config.context,
        segments = applied,
        from = before,
        to = engine.local_tail().0,
        "caught up from the archive"
    );
    Box::pin(run_tail(engine, config, stop_at)).await
}

async fn wait_durable(engine: Arc<EventStoreEngine>, write: ReplicatedWrite) -> Result<(), Error> {
    tokio::task::spawn_blocking(move || engine.wait_replicated_durable(write))
        .await
        .map_err(|error| Error::Io(std::io::Error::other(error.to_string())))?
}

/// Latest-wins ack handed to the ack task. `write` is the wave to wait for
/// before acknowledging; it is None only when every acked byte was fsynced
/// before this session started.
#[derive(Clone, Copy)]
struct PendingAck {
    write: Option<ReplicatedWrite>,
    /// The epoch the session runs in, which the source expects on its acks.
    epoch: u64,
    position: u64,
    bytes: u64,
}

impl PendingAck {
    /// The ack for a frame that needed no writing. It still replaces any
    /// pending ack for this session's own writes (latest wins), so it carries
    /// the session's latest write: waves complete in order, so waiting for it
    /// covers every byte up to `local_tail`. A session that wrote nothing has
    /// only bytes that were fsynced when it opened.
    fn duplicate(
        last_write: Option<ReplicatedWrite>,
        epoch: u64,
        local_tail: u64,
        bytes: u64,
    ) -> Self {
        Self {
            write: last_write,
            epoch,
            position: local_tail,
            bytes,
        }
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::append::AppendRequest;
    use crate::event::AppendEvent;
    use crate::store::StoreOptions;

    fn event(name: &str) -> AppendRequest {
        AppendRequest {
            condition: None,
            events: vec![AppendEvent {
                identifier: format!("id-{name}"),
                name: name.into(),
                version: "1.0".into(),
                timestamp: 1_712_345_678_000,
                payload: b"payload".to_vec(),
                metadata: vec![],
                tags: vec![],
            }],
        }
    }

    fn copy_dir(from: &std::path::Path, to: &std::path::Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_dir() {
                copy_dir(&entry.path(), &to.join(entry.file_name()));
            } else {
                std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
            }
        }
    }

    #[test]
    fn a_duplicate_ack_waits_for_the_write_this_session_applied() {
        let tmp = tempfile::tempdir().unwrap();
        let (leader_dir, follower_dir) = (tmp.path().join("leader"), tmp.path().join("follower"));
        let options = StoreOptions {
            max_segment_size: 64 * 1024,
            voters: vec![0],
            ..Default::default()
        };
        // Both nodes share the first event; the leader then writes one more.
        let leader = EventStoreEngine::create_with_store_options(&leader_dir, &options).unwrap();
        leader.append(event("first")).unwrap();
        leader.shutdown();
        drop(leader);
        copy_dir(&leader_dir, &follower_dir);
        let leader = EventStoreEngine::open_with_store_options(&leader_dir, &options).unwrap();
        leader.append(event("second")).unwrap();
        let follower = EventStoreEngine::open_with_store_options(&follower_dir, &options).unwrap();

        let cursor = follower.replication_cursor().unwrap();
        let slice = leader
            .replication_catchup_from_cursor(cursor)
            .unwrap()
            .1
            .into_iter()
            .next()
            .unwrap();
        let bytes = std::fs::read(&slice.path).unwrap()
            [slice.byte_start as usize..slice.byte_end as usize]
            .to_vec();
        let epoch = leader.replication_epoch();
        if follower.replication_epoch() < epoch {
            follower.begin_replication_epoch(epoch, [0]).unwrap();
        }
        let write = follower
            .apply_replicated_records(
                epoch,
                slice.segment_base,
                Position(slice.first_position),
                &bytes,
            )
            .unwrap();

        // The session applied a frame; a duplicate frame then replaces its
        // pending ack. The new one must still wait for that write.
        let ack = PendingAck::duplicate(Some(write), epoch, follower.local_tail().0, 7);
        let waits_for = ack
            .write
            .expect("a duplicate ack must wait for the session's write");
        assert_eq!(waits_for.wave, write.wave);
        assert_eq!(ack.position, write.durable_position.0);

        // A session that wrote nothing has nothing to wait for.
        assert!(PendingAck::duplicate(None, epoch, 0, 0).write.is_none());
    }
}
