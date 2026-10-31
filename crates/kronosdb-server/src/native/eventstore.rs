//! Event store requests.
//!
//! Reads copy stored records straight into Events frames: the engine writes
//! them after a four-byte count, the marker goes after them, and the body is
//! sent as it is. Nothing is decoded or re-encoded on the way out.

use std::time::Duration;

use bytes::Bytes;
use kronosdb_eventstore::api::EventStore;
use kronosdb_eventstore::append::RecordAppend;
use kronosdb_eventstore::criteria::SourcingCondition;
use kronosdb_eventstore::error::Error;
use kronosdb_eventstore::event::Position;
use kronosdb_eventstore::store::PageBounds;
use kronosdb_protocol as p;
use kronosdb_protocol::net::Failure;
use kronosdb_protocol::net::server::{ServerStream, StreamSender};

use super::Ctx;
use super::convert::{self, invalid};
use crate::auth::policy::Access;

/// Most events per Events frame when the client lets the server choose.
const DEFAULT_BATCH: usize = 1024;
/// Most record bytes per Events frame, whatever the batch size.
const MAX_BATCH_BYTES: usize = 1024 * 1024;

fn batch_size(requested: u32) -> usize {
    match requested {
        0 => DEFAULT_BATCH,
        n => n as usize,
    }
}

/// An Events body under construction: the record count first, then the
/// records the engine copies in, then the marker (schema field order).
fn events_body() -> Vec<u8> {
    let mut body = Vec::with_capacity(16 * 1024);
    body.extend_from_slice(&[0; 4]);
    body
}

fn finish_events_body(mut body: Vec<u8>, count: u32, marker: Option<u64>) -> Bytes {
    body[0..4].copy_from_slice(&count.to_le_bytes());
    match marker {
        Some(m) => {
            body.push(1);
            body.extend_from_slice(&m.to_le_bytes());
        }
        None => body.push(0),
    }
    Bytes::from(body)
}

/// Sends `[from, up_to)` as Events frames; the last carries END and `marker`.
/// The first page is read on the caller; later pages yield first, so a long
/// read never holds the connection's reader.
async fn send_range(
    store: &dyn EventStore,
    from: Position,
    condition: &SourcingCondition,
    up_to: Position,
    batch: usize,
    out: &StreamSender,
) -> Result<(), Failure> {
    let mut cursor = from;
    loop {
        let mut body = events_body();
        let page = store
            .read_records(
                condition,
                &PageBounds {
                    limit: batch,
                    max_bytes: MAX_BATCH_BYTES,
                    ..PageBounds::new(cursor, up_to)
                },
                &mut body,
            )
            .map_err(convert::failure)?;
        let last = !page.full;
        let body = finish_events_body(body, page.count, last.then_some(up_to.0));
        if out.send_shared(p::Events::KIND, last, body).await.is_err() || last {
            return Ok(());
        }
        cursor = Position(page.scanned_to.expect("a full page scanned a record").0 + 1);
        tokio::task::yield_now().await;
    }
}

pub async fn append(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let frame = stream.request;
    let Ok(p::Frame::Append(req)) = frame.decode() else {
        return Err(invalid("malformed Append"));
    };
    let store = ctx.store(req.context, Access::Write)?;
    let request = RecordAppend {
        condition: req.condition.as_ref().map(convert::append_condition),
        events: convert::record_batch(&req.events, frame.bytes())?,
    };
    match store.append_records(request).await {
        Ok(r) => {
            finish(
                &stream.out,
                &p::Frame::Appended(p::Appended {
                    first_position: r.first_position.0,
                    count: r.count,
                    marker: r.consistency_marker.0,
                }),
            )
            .await
        }
        Err(Error::ConsistencyConditionViolated {
            conflicting_position,
        }) => {
            finish(
                &stream.out,
                &p::Frame::AppendConflict(p::AppendConflict {
                    conflicting_position: conflicting_position.0,
                }),
            )
            .await
        }
        Err(e) => Err(convert::failure(e)),
    }
}

pub async fn source(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let frame = stream.request;
    let Ok(p::Frame::Source(req)) = frame.decode() else {
        return Err(invalid("malformed Source"));
    };
    let store = ctx.store(req.context, Access::Read)?;
    let condition = convert::read_condition(&req.criteria);
    // The bound is the consistency marker: frozen first, so the events and
    // the marker the client appends against describe the same log.
    let marker = store.head();
    send_range(
        store.as_ref(),
        Position(req.from),
        &condition,
        marker,
        batch_size(req.batch_size),
        &stream.out,
    )
    .await
}

pub async fn snapshotted_source(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let frame = stream.request;
    let Ok(p::Frame::SnapshottedSource(req)) = frame.decode() else {
        return Err(invalid("malformed SnapshottedSource"));
    };
    let store = ctx.store(req.context, Access::Read)?;
    let condition = convert::read_condition(&req.criteria);
    // Marker first, then the snapshot bounded by it: snapshot, events and
    // marker form one consistent view of the log.
    let marker = store.head();
    let snapshot = store
        .get_snapshot(req.key, Some(marker))
        .map_err(convert::failure)?;
    // A snapshot's position is next-exclusive: the first unfolded event sits
    // at it, so the replay starts there, not after it.
    let from = snapshot.as_ref().map_or(Position(0), |s| s.position);
    send(
        &stream.out,
        &p::Frame::SnapshotResult(p::SnapshotResult {
            snapshot: snapshot.as_ref().map(|s| p::Snapshot {
                state: &s.state,
                position: s.position.0,
            }),
        }),
    )
    .await?;
    send_range(
        store.as_ref(),
        from,
        &condition,
        marker,
        batch_size(req.batch_size),
        &stream.out,
    )
    .await
}

/// A live subscription: catches up from `from`, then follows the log as
/// events become durable. Paced by the stream's window like every stream;
/// never waits to fill a batch.
pub async fn subscribe(ctx: Ctx, mut stream: ServerStream) -> Result<(), Failure> {
    let frame = stream.request.clone();
    let Ok(p::Frame::Subscribe(req)) = frame.decode() else {
        return Err(invalid("malformed Subscribe"));
    };
    let store = ctx.store(req.context, Access::Read)?;
    let condition = convert::read_condition(&req.criteria);
    let exclude: Vec<String> = req.exclude_names.iter().map(|n| n.to_string()).collect();
    let batch = batch_size(req.batch_size);
    // Checked here: a stream is woken only by events that match it, so a
    // condition that can never match would otherwise fail on no read at all.
    kronosdb_eventstore::system::validate_client_condition(&condition).map_err(convert::failure)?;
    let mut events = store.subscribe(Position(req.from), condition.clone(), &exclude);
    let progress_every =
        (req.progress_interval > 0).then(|| Duration::from_millis(req.progress_interval as u64));
    let mut progress = tokio::time::interval(progress_every.unwrap_or(Duration::from_secs(3600)));
    progress.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    progress.tick().await;
    let mut reported = events.cursor;
    let out = stream.out.clone();
    drop(frame);

    loop {
        tokio::select! {
            biased;
            // Cancelled, or the client went away: the runtime answers.
            _ = stream.inbound.cancelled() => return Ok(()),
            bound = events.wait_for_new_events() => {
                let bound = Position(bound);
                if bound <= events.cursor {
                    // Only a closed engine returns without progress.
                    return Err(convert::failure(Error::Unavailable {
                        message: "the context is shutting down; resubscribe".into(),
                    }));
                }
                // A stream that wants every event at the live tail sends the
                // batch the dispatcher encoded for all of them: no read, no
                // copy, no CRC check here.
                if let Some(chunk) = events.live_batch(batch) {
                    if chunk.count > 0 {
                        if out.send_shared(p::Events::KIND, false, chunk.body.clone()).await.is_err() {
                            return Ok(());
                        }
                        reported = Position(chunk.to);
                    }
                    events.advance_cursor(Position(chunk.to));
                    tokio::task::yield_now().await;
                    continue;
                }
                // A filtering stream at the live tail takes what the
                // dispatcher matched for it out of the same chunk: the
                // records are already read, checked and copied.
                if let Some(pick) = events.live_picks(batch) {
                    events.advance_cursor(Position(pick.next));
                    if let Some(body) = pick.body {
                        if out.send_shared(p::Events::KIND, false, body).await.is_err() {
                            return Ok(());
                        }
                        reported = events.cursor;
                    }
                    continue;
                }
                // Most commits hold nothing for a filtered subscriber; the
                // index answers that without a read.
                if !store.has_matches_between(events.cursor, bound, &condition) {
                    events.advance_cursor(bound);
                    continue;
                }
                let mut body = events_body();
                let page = store
                    .read_records(
                        &condition,
                        &PageBounds {
                            limit: batch,
                            max_bytes: MAX_BATCH_BYTES,
                            exclude_names: &exclude,
                            ..PageBounds::new(events.cursor, bound)
                        },
                        &mut body,
                    )
                    .map_err(convert::failure)?;
                if page.count > 0 {
                    let body = finish_events_body(body, page.count, None);
                    if out.send_shared(p::Events::KIND, false, body).await.is_err() {
                        return Ok(());
                    }
                }
                events.advance_cursor(match (page.full, page.scanned_to) {
                    (true, Some(last)) => Position(last.0 + 1),
                    _ => bound,
                });
                // Delivered events tell the client as much as a Progress would, up
                // to the last of them. What lies between it and the cursor
                // matched nothing, and only a Progress can say so: without
                // one, a selective reader whose last match is far behind the
                // head keeps its token there until the log moves on.
                if let Some(last) = page.last_position {
                    reported = Position(last.0 + 1);
                }
                // A subscriber far behind reads page after page; let others run.
                tokio::task::yield_now().await;
            }
            _ = progress.tick(), if progress_every.is_some() => {
                // The frontier, not the cursor: a quiet stream is not woken by
                // commits that hold nothing for it, yet its client still needs
                // to see the position move.
                let frontier = events.frontier();
                if frontier > reported {
                    reported = frontier;
                    if out.send(&p::Frame::Progress(p::Progress { position: reported.0 })).await.is_err() {
                        return Ok(());
                    }
                }
            }
        }
    }
}

pub async fn get_head(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let Ok(p::Frame::GetHead(req)) = stream.request.decode() else {
        return Err(invalid("malformed GetHead"));
    };
    let store = ctx.store(req.context, Access::Read)?;
    // The visible head: positions a client can never read would show as
    // permanent lag in a poll-until-caught-up loop.
    let position = store.visible_head().0;
    finish(&stream.out, &p::Frame::Head(p::Head { position })).await
}

pub async fn get_tail(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let Ok(p::Frame::GetTail(req)) = stream.request.decode() else {
        return Err(invalid("malformed GetTail"));
    };
    let position = ctx.store(req.context, Access::Read)?.tail().0;
    finish(&stream.out, &p::Frame::Tail(p::Tail { position })).await
}

pub async fn get_tags(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let Ok(p::Frame::GetTags(req)) = stream.request.decode() else {
        return Err(invalid("malformed GetTags"));
    };
    let tags = ctx
        .store(req.context, Access::Read)?
        .get_tags(Position(req.position))
        .map_err(convert::failure)?;
    let tags = tags
        .iter()
        .map(|t| p::Tag {
            key: &t.key,
            value: &t.value,
        })
        .collect();
    finish(&stream.out, &p::Frame::Tags(p::Tags { tags })).await
}

pub async fn get_position_at(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let Ok(p::Frame::GetPositionAt(req)) = stream.request.decode() else {
        return Err(invalid("malformed GetPositionAt"));
    };
    let store = ctx.store(req.context, Access::Read)?;
    let timestamp = req.timestamp;
    // A scan over the log: off the async workers.
    let position = tokio::task::spawn_blocking(move || store.get_sequence_at(timestamp))
        .await
        .map_err(|e| {
            convert::failure(Error::Internal {
                message: e.to_string(),
            })
        })?
        .map_err(convert::failure)?;
    finish(
        &stream.out,
        &p::Frame::PositionAt(p::PositionAt {
            position: position.map(|p| p.0),
        }),
    )
    .await
}

pub async fn append_snapshot(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let frame = stream.request;
    let Ok(p::Frame::AppendSnapshot(req)) = frame.decode() else {
        return Err(invalid("malformed AppendSnapshot"));
    };
    if req.key.is_empty() {
        return Err(invalid("snapshot key must not be empty"));
    }
    // Snapshot blobs share the log with events; a runaway state would bloat
    // segments and replication for every consumer.
    if req.state.len() as u64 > ctx.deps.max_snapshot_size {
        return Err(invalid(format!(
            "snapshot state is {} bytes; the configured maximum is {} (max-snapshot-size)",
            req.state.len(),
            ctx.deps.max_snapshot_size
        )));
    }
    let store = ctx.store(req.context, Access::Write)?;
    let position = store
        .append_snapshot(req.key.to_vec(), req.state.to_vec(), Position(req.position))
        .await
        .map_err(convert::failure)?;
    finish(
        &stream.out,
        &p::Frame::SnapshotAppended(p::SnapshotAppended {
            position: position.0,
        }),
    )
    .await
}

pub async fn get_snapshot(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let frame = stream.request;
    let Ok(p::Frame::GetSnapshot(req)) = frame.decode() else {
        return Err(invalid("malformed GetSnapshot"));
    };
    let snapshot = ctx
        .store(req.context, Access::Read)?
        .get_snapshot(req.key, None)
        .map_err(convert::failure)?;
    finish(
        &stream.out,
        &p::Frame::SnapshotResult(p::SnapshotResult {
            snapshot: snapshot.as_ref().map(|s| p::Snapshot {
                state: &s.state,
                position: s.position.0,
            }),
        }),
    )
    .await
}

async fn send(out: &StreamSender, frame: &p::Frame<'_>) -> Result<(), Failure> {
    // A send fails only when the client is gone; nobody is left to tell.
    let _ = out.send(frame).await;
    Ok(())
}

async fn finish(out: &StreamSender, frame: &p::Frame<'_>) -> Result<(), Failure> {
    let _ = out.finish(frame).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn events_bodies_decode_as_events_frames() {
        let mut body = events_body();
        let mut records = p::RecordsBuf::new();
        records
            .push(&p::EventRecord {
                position: 4,
                identifier: "e",
                name: "N",
                version: "1",
                timestamp: 0,
                metadata: vec![],
                tags: vec![],
                payload: b"x",
            })
            .unwrap();
        body.extend_from_slice(records.as_records().bytes());
        for marker in [None, Some(42)] {
            let bytes = finish_events_body(body.clone(), 1, marker);
            let Some(p::Frame::Events(e)) = p::Frame::decode(p::Events::KIND, &bytes).unwrap()
            else {
                panic!("not an Events frame");
            };
            assert_eq!(e.marker, marker);
            assert_eq!(e.events.iter().next().unwrap().event().unwrap().position, 4);
        }
    }
}
