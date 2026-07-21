pub mod api;
pub mod append;
pub mod criteria;
pub mod error;
pub mod event;

pub mod cache;
pub mod context;
pub mod index;
pub mod metrics;
pub mod raft;
pub mod replication;
pub mod segment;
pub mod snapshot;
pub mod store;
pub mod stream;
pub mod tier;

static ACK_RELAXED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

/// Fixes the acknowledgement mode before the first store opens. The server
/// calls this with its resolved config; embedded/test users fall back to
/// the `KRONOSDB_ACK_MODE` env var. First caller wins.
pub fn configure_ack_mode(relaxed: bool) {
    let _ = ACK_RELAXED.set(relaxed);
}

/// Acknowledgement mode. `written` (the default) releases appends once a
/// quorum has *written* the events to its log — Kafka-class semantics; fsync trails by
/// one group-commit wave and [`ack_lag_limit`] bounds how far acks may run
/// ahead of the durable cursor. `durable` releases only once a quorum has
/// *fsynced*. A correlated hard failure of a quorum inside the sync window
/// can lose acked events in `written` mode; a single node has no quorum
/// beyond itself, so single-node `written` acks survive process crashes
/// (page cache) but not power loss.
pub(crate) fn relaxed_acks() -> bool {
    *ACK_RELAXED.get_or_init(|| {
        !std::env::var("KRONOSDB_ACK_MODE").is_ok_and(|v| v.eq_ignore_ascii_case("durable"))
    })
}

/// In `written` mode, how many events acknowledgements may run ahead of
/// the locally fsynced cursor before append release falls back to waiting
/// for durability. Bounds the loss window under a stalling disk: past the
/// limit the store behaves like `durable` mode until fsync catches up.
pub(crate) fn ack_lag_limit() -> u64 {
    static LIMIT: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *LIMIT.get_or_init(|| {
        std::env::var("KRONOSDB_ACK_LAG_LIMIT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(65_536)
    })
}
