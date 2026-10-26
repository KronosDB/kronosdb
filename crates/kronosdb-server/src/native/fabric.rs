//! The receiving side of the messaging fabric (ADR-0007): commands,
//! queries and subscriptions that another node chose handlers on this node
//! for. Peers only; `auth::policy` demands the `peer` role for these.

use std::time::Duration;

use kronosdb_messaging::types::ClientId;
use kronosdb_protocol as p;
use kronosdb_protocol::net::Failure;
use kronosdb_protocol::net::server::ServerStream;

use super::Ctx;
use super::convert::invalid;
use super::messaging::{send_events, send_results};
use super::wire::{self, Source};

/// The caller's budget as a duration; at least a millisecond, so a zero
/// from a peer is a short wait and not none at all.
fn budget(timeout_ms: u64) -> Duration {
    Duration::from_millis(timeout_ms.max(1))
}

pub async fn forward_command(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let ServerStream {
        request,
        mut inbound,
        out,
    } = stream;
    let Ok(p::Frame::ForwardCommand(req)) = request.decode() else {
        return Err(invalid("malformed ForwardCommand"));
    };
    let bus = ctx.deps.hub.bus(req.bus);
    let command = wire::command(Source(request.bytes()), &req.command);
    let result = tokio::select! {
        result = ctx.deps.hub.dispatch_forwarded_command(
            &bus,
            ClientId(req.target_client_id.to_string()),
            command,
            budget(req.timeout_ms),
        ) => result.map_err(wire::failure)?,
        _ = inbound.cancelled() => return Ok(()),
    };
    let _ = out
        .finish(&p::Frame::CommandResponse(p::CommandResponse {
            result: wire::wire_command_result(&result),
        }))
        .await;
    Ok(())
}

pub async fn forward_query(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let ServerStream {
        request,
        mut inbound,
        out,
    } = stream;
    let Ok(p::Frame::ForwardQuery(req)) = request.decode() else {
        return Err(invalid("malformed ForwardQuery"));
    };
    let bus = ctx.deps.hub.bus(req.bus);
    let query = wire::query(Source(request.bytes()), &req.query);
    let targets = req
        .target_client_ids
        .iter()
        .map(|id| ClientId(id.to_string()))
        .collect();
    let results = tokio::select! {
        results = ctx.deps.hub.dispatch_forwarded_query(&bus, targets, query, budget(req.timeout_ms))
            => results.map_err(wire::failure)?,
        _ = inbound.cancelled() => return Ok(()),
    };
    send_results(&out, &mut inbound, results).await
}

pub async fn relay_subscription(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    let ServerStream {
        request,
        mut inbound,
        out,
    } = stream;
    let Ok(p::Frame::RelaySubscription(req)) = request.decode() else {
        return Err(invalid("malformed RelaySubscription"));
    };
    let bus = ctx.deps.hub.bus(req.bus);
    let subscription = wire::subscription(Source(request.bytes()), &req.subscription);
    let id = subscription.subscription_id.clone();
    let feed = tokio::select! {
        feed = ctx.deps.hub.open_relayed_subscription(
            &bus,
            ClientId(req.target_client_id.to_string()),
            subscription,
        ) => feed.map_err(wire::failure)?,
        _ = inbound.cancelled() => return Ok(()),
    };
    send_events(&out, &mut inbound, feed, &id).await
}
