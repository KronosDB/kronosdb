//! Internode requests: Raft, forwarded appends and segment replication. The
//! work lives in the event store crate, as functions over the request's
//! stream; these only hand the stream over. The caller's `peer` role was
//! checked before routing.

use kronosdb_eventstore::raft::transport;
use kronosdb_protocol::net::Failure;
use kronosdb_protocol::net::server::ServerStream;

use super::Ctx;
use super::convert;

pub async fn raft(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    transport::serve(&ctx.deps.raft, stream)
        .await
        .map_err(convert::failure)
}

pub async fn tail(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    ctx.deps
        .replication
        .tail(stream)
        .await
        .map_err(convert::failure)
}

pub async fn forward_append(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    ctx.deps
        .replication
        .forward_append(stream)
        .await
        .map_err(convert::failure)
}

pub async fn get_cursors(ctx: Ctx, stream: ServerStream) -> Result<(), Failure> {
    ctx.deps
        .replication
        .get_cursors(stream)
        .await
        .map_err(convert::failure)
}
