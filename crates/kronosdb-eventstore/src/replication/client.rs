//! Follower-side Tail client for the authoritative native segment log.

use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

use crate::error::Error;
use crate::event::Position;
use crate::store::{EventStoreEngine, ReplicatedWrite};

use super::peer::PeerTransportConfig;
use super::proto;
use super::proto::segment_replication_client::SegmentReplicationClient;
use crate::replication::PEER_MAX_MESSAGE_BYTES;

#[derive(Debug, Clone)]
pub struct TailClientConfig {
    pub context: String,
    pub follower_id: u64,
    pub leader_id: u64,
    pub leader_addr: String,
    pub epoch: u64,
    pub voters: Vec<u64>,
    pub recovery_claim: Option<(u64, u64)>,
    pub transport: PeerTransportConfig,
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
        return Err(Error::Io(std::io::Error::other(format!(
            "Tail epoch {} is older than engine epoch {}",
            config.epoch,
            engine.replication_epoch()
        ))));
    }

    let channel: Channel = config.transport.connect(&config.leader_addr).await?;
    let mut client = SegmentReplicationClient::new(channel)
        .max_decoding_message_size(PEER_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(PEER_MAX_MESSAGE_BYTES);

    let (request_tx, request_rx) = mpsc::channel(16);
    let cursor = engine.replication_cursor()?;
    request_tx
        .send(proto::TailRequest {
            request: Some(proto::tail_request::Request::Open(proto::TailOpen {
                context: config.context.clone(),
                follower_id: config.follower_id,
                from_position: cursor.position.0,
                last_record_crc: cursor.last_record_crc,
                epoch_seen: engine.replication_epoch(),
                segment_base: cursor.segment_base,
                byte_offset: cursor.byte_offset,
                watermark: engine.head().0,
                recovery_source: config.recovery_claim.is_some(),
                claim_epoch: config.recovery_claim.map(|claim| claim.0).unwrap_or(0),
                claim_leader_id: config.recovery_claim.map(|claim| claim.1).unwrap_or(0),
            })),
        })
        .await
        .map_err(|_| Error::Io(std::io::Error::other("Tail request channel closed")))?;

    let request = config.transport.request(ReceiverStream::new(request_rx))?;
    let response = client.tail(request).await.map_err(status_error)?;
    let mut frames = response.into_inner();

    while let Some(frame) = frames.message().await.map_err(status_error)? {
        match frame.frame {
            Some(proto::tail_frame::Frame::EpochChange(change)) => {
                if change.leader_id != config.leader_id || change.epoch != config.epoch {
                    return Err(Error::Io(std::io::Error::other(format!(
                        "Tail leader/epoch changed to {}/{}",
                        change.leader_id, change.epoch
                    ))));
                }
                if engine.replication_epoch() < change.epoch {
                    engine.begin_replication_epoch(change.epoch, config.voters.clone())?;
                }
            }
            Some(proto::tail_frame::Frame::Rotate(rotate)) => {
                if rotate.epoch != config.epoch {
                    return Err(stale_epoch(rotate.epoch, config.epoch));
                }
                engine.rotate_replicated(rotate.epoch, Position(rotate.new_segment_base))?;
            }
            Some(proto::tail_frame::Frame::Records(records)) => {
                if records.epoch != config.epoch {
                    return Err(stale_epoch(records.epoch, config.epoch));
                }
                let local_tail = engine.local_tail().0;
                if records.next_position < local_tail
                    || (records.next_position == local_tail
                        && records.first_position < records.next_position)
                {
                    // Fully duplicated catch-up/live overlap. Bytes are
                    // already durable; acknowledge this session byte cursor.
                    send_ack(&request_tx, &config, local_tail, records.stream_bytes_end).await?;
                    if stop_at
                        .map(|target| local_tail >= target.0)
                        .unwrap_or(false)
                    {
                        return Ok(());
                    }
                    continue;
                }
                if records.first_position != local_tail {
                    return Err(Error::Io(std::io::Error::other(format!(
                        "Tail frame positions {}..{} do not continue local tail {}",
                        records.first_position, records.next_position, local_tail
                    ))));
                }

                let write = engine.apply_replicated_records(
                    records.epoch,
                    records.segment_base,
                    Position(records.first_position),
                    &records.data,
                )?;
                if write.durable_position.0 != records.next_position {
                    return Err(Error::Corrupted {
                        message: format!(
                            "Tail frame declared next position {}, decoded {}",
                            records.next_position, write.durable_position.0
                        ),
                    });
                }
                wait_durable(Arc::clone(&engine), write).await?;
                send_ack(
                    &request_tx,
                    &config,
                    write.durable_position.0,
                    records.stream_bytes_end,
                )
                .await?;
                if stop_at
                    .map(|target| write.durable_position.0 >= target.0)
                    .unwrap_or(false)
                {
                    return Ok(());
                }
            }
            Some(proto::tail_frame::Frame::Watermark(watermark)) => {
                if watermark.epoch == config.epoch {
                    engine.adopt_watermark(watermark.epoch, Position(watermark.position));
                    if stop_at
                        .map(|target| engine.local_tail().0 >= target.0)
                        .unwrap_or(false)
                    {
                        return Ok(());
                    }
                }
            }
            Some(proto::tail_frame::Frame::Truncate(truncate)) => {
                if truncate.epoch != config.epoch {
                    return Err(stale_epoch(truncate.epoch, config.epoch));
                }
                let expected_prev = truncate.has_boundary.then_some(
                    (!truncate.prev_at_segment_start).then_some(truncate.prev_record_crc),
                );
                engine.truncate_to_matching(Position(truncate.position), expected_prev)?;
                if stop_at
                    .map(|target| engine.local_tail().0 >= target.0)
                    .unwrap_or(false)
                {
                    return Ok(());
                }
            }
            Some(proto::tail_frame::Frame::NeedSnapshot(snapshot)) => {
                return Err(Error::Io(std::io::Error::other(format!(
                    "Tail requires snapshot from {} (watermark {}): {}",
                    snapshot.available_from, snapshot.watermark, snapshot.reason
                ))));
            }
            None => {
                return Err(Error::Io(std::io::Error::other("empty Tail frame")));
            }
        }
    }

    Err(Error::Io(std::io::Error::other(
        "leader closed Tail stream",
    )))
}

async fn wait_durable(engine: Arc<EventStoreEngine>, write: ReplicatedWrite) -> Result<(), Error> {
    tokio::task::spawn_blocking(move || engine.wait_replicated_durable(write))
        .await
        .map_err(|error| Error::Io(std::io::Error::other(error.to_string())))?
}

async fn send_ack(
    tx: &mpsc::Sender<proto::TailRequest>,
    config: &TailClientConfig,
    durable_position: u64,
    durable_bytes: u64,
) -> Result<(), Error> {
    tx.send(proto::TailRequest {
        request: Some(proto::tail_request::Request::Ack(proto::TailAck {
            context: config.context.clone(),
            follower_id: config.follower_id,
            epoch: config.epoch,
            durable_position,
            durable_bytes,
        })),
    })
    .await
    .map_err(|_| Error::Io(std::io::Error::other("Tail request channel closed")))
}

fn stale_epoch(actual: u64, expected: u64) -> Error {
    Error::Io(std::io::Error::other(format!(
        "stale Tail epoch {actual}; expected {expected}"
    )))
}

fn status_error(status: tonic::Status) -> Error {
    Error::Io(std::io::Error::other(status.to_string()))
}
