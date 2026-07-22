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

/// Append acknowledgement mode.
///
/// `Written` releases appends once a quorum has *written* the events to its
/// log — Kafka-class semantics; fsync trails by one group-commit wave and
/// [`ack_lag_limit`] bounds how far acks may run ahead of the durable
/// cursor. `Durable` releases only once a quorum has *fsynced*. `Auto` (the
/// default) picks per topology: `Durable` on a single voter (where the ack
/// has no replication behind it and page cache is all a written ack would
/// mean), `Written` at two or more voters (where a correlated hard failure
/// of a quorum inside the sync window is the only loss case). The decision
/// is per-append against the current voter count, so membership changes
/// switch modes without a restart — and written acks always require a live
/// quorum, so losing peers stalls appends rather than weakening the ack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    Auto,
    Written,
    Durable,
}

static ACK_MODE: std::sync::OnceLock<AckMode> = std::sync::OnceLock::new();

/// Fixes the acknowledgement mode before the first store opens. The server
/// calls this with its resolved config; embedded/test users fall back to
/// the `KRONOSDB_ACK_MODE` env var. First caller wins.
pub fn configure_ack_mode(mode: AckMode) {
    let _ = ACK_MODE.set(mode);
}

pub(crate) fn ack_mode() -> AckMode {
    *ACK_MODE.get_or_init(|| {
        match std::env::var("KRONOSDB_ACK_MODE").as_deref() {
            Ok(v) if v.eq_ignore_ascii_case("durable") => AckMode::Durable,
            // "replicated" is a deprecated alias from the prototype era.
            Ok(v) if v.eq_ignore_ascii_case("written") || v.eq_ignore_ascii_case("replicated") => {
                AckMode::Written
            }
            _ => AckMode::Auto,
        }
    })
}

/// Whether acks release at quorum-written for the given voter count.
pub(crate) fn written_acks(voter_count: u64) -> bool {
    match ack_mode() {
        AckMode::Written => true,
        AckMode::Durable => false,
        AckMode::Auto => voter_count >= 2,
    }
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
