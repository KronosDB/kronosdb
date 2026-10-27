use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

pub mod api;
pub mod append;
pub mod criteria;
pub mod error;
pub mod event;
pub mod hardware;

pub mod cache;
pub mod context;
pub(crate) mod inline_breaker;
pub mod metrics;
pub mod raft;
pub mod replication;
pub mod scheduler;
pub mod segment;
pub mod snapshot;
pub mod store;
pub mod stream;
pub(crate) mod subscription;
pub mod system;
pub mod tier;
pub mod wire;

/// Append acknowledgement mode.
///
/// `Written` releases appends once a quorum has *written* the events to its
/// log — Kafka-class semantics; fsync trails by one group-commit wave and
/// [`ack_lag_limit`] bounds how far acks may run ahead of the durable
/// cursor. `Durable` releases only once a quorum has *fsynced*.
///
/// `Auto` (the default) chooses from topology and from the disk. On a single
/// voter it is `Durable`: the ack has no replication behind it, so page cache
/// is all a written ack would mean. At two or more voters it is `Durable` when
/// the startup disk probe (see [`hardware`]) measured a fast disk, and
/// `Written` when the disk is slow or could not be measured. A fast disk
/// makes durable acks nearly free (on NVMe the extra follower fsync costs
/// 0.02–0.2 ms per append); on a slow one they cost a third of the
/// throughput, and the only loss case written acks leave open is a
/// correlated hard failure of a quorum inside the sync window. The probe runs
/// once; the answer is stored in an atomic ([`AckDecision::apply`]), so the
/// per-append decision stays a voter-count compare plus one relaxed load.
/// Without a probe (embedded use, tests) `Auto` keeps the older rule:
/// written at 2+ voters.
///
/// The voter count is read per append, so membership changes switch a
/// single-voter node to the clustered rule without a restart. Written acks
/// always require a live quorum, so losing peers stalls appends rather than
/// weakening the ack. `Written` and `Durable` are explicit and never consult
/// the probe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckMode {
    Auto,
    Written,
    Durable,
}

/// What an ack waits for in one concrete situation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckKind {
    Written,
    Durable,
}

impl AckKind {
    pub fn as_str(self) -> &'static str {
        match self {
            AckKind::Written => "written",
            AckKind::Durable => "durable",
        }
    }
}

/// Under `Auto`, at two or more voters, a disk whose measured fdatasync p99
/// is below this gets durable acks. One millisecond is the knee between the
/// two regimes we measured: NVMe (p50 26 µs, p99 ~190 µs) pays 3–19% for
/// durable acks in a 3-node cluster, while disks at 1–4 ms pay 30–36%. It is
/// also about one cross-AZ round trip (~1 ms), so below it the follower's
/// fsync costs no more than the network hop it sits next to. The p99 rather
/// than the median, so a disk that is usually quick but stalls does not get
/// durable acks.
pub const AUTO_DURABLE_FSYNC_P99: Duration = Duration::from_millis(1);

/// The outcome of resolving [`AckMode`] against the measured disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckDecision {
    pub configured: AckMode,
    /// What acks wait for on a single voter.
    pub single_voter: AckKind,
    /// What acks wait for at two or more voters.
    pub multi_voter: AckKind,
    /// Why, in one sentence (logged at startup).
    pub reason: String,
}

/// Resolves `mode` against the probe result (`None` = not measured). Pure;
/// [`AckDecision::apply`] is what makes it take effect.
pub fn decide_acks(mode: AckMode, p99: Option<Duration>) -> AckDecision {
    match mode {
        AckMode::Written => AckDecision {
            configured: mode,
            single_voter: AckKind::Written,
            multi_voter: AckKind::Written,
            reason: "set explicitly; the disk probe does not change it".to_string(),
        },
        AckMode::Durable => AckDecision {
            configured: mode,
            single_voter: AckKind::Durable,
            multi_voter: AckKind::Durable,
            reason: "set explicitly; the disk probe does not change it".to_string(),
        },
        AckMode::Auto => {
            let (multi_voter, reason) = match p99 {
                Some(p99) if p99 < AUTO_DURABLE_FSYNC_P99 => (
                    AckKind::Durable,
                    format!(
                        "fdatasync p99 {} is under {}: durable acks are cheap here, \
                         so clusters get them too",
                        hardware::fmt_duration(p99),
                        hardware::fmt_duration(AUTO_DURABLE_FSYNC_P99),
                    ),
                ),
                Some(p99) => (
                    AckKind::Written,
                    format!(
                        "fdatasync p99 {} is not under {}: clusters keep written acks \
                         (set ack-mode = \"durable\" to force fsync-before-ack)",
                        hardware::fmt_duration(p99),
                        hardware::fmt_duration(AUTO_DURABLE_FSYNC_P99),
                    ),
                ),
                None => (
                    AckKind::Written,
                    "the disk could not be measured: clusters keep written acks".to_string(),
                ),
            };
            AckDecision {
                configured: mode,
                single_voter: AckKind::Durable,
                multi_voter,
                reason,
            }
        }
    }
}

impl AckDecision {
    /// Stores the decision where the per-append check reads it. Call once at
    /// startup, before the first append.
    pub fn apply(&self) {
        AUTO_MULTI_VOTER_DURABLE.store(self.auto_multi_voter_durable(), Ordering::Relaxed);
    }

    /// The one bit [`written_acks`] needs from an `Auto` decision.
    fn auto_multi_voter_durable(&self) -> bool {
        self.configured == AckMode::Auto && self.multi_voter == AckKind::Durable
    }
}

static ACK_MODE: std::sync::OnceLock<AckMode> = std::sync::OnceLock::new();

/// `Auto` at two or more voters acks durably when set. False until
/// [`AckDecision::apply`] says otherwise, which is the pre-probe behavior.
static AUTO_MULTI_VOTER_DURABLE: AtomicBool = AtomicBool::new(false);

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
    written_acks_in(
        ack_mode(),
        voter_count,
        AUTO_MULTI_VOTER_DURABLE.load(Ordering::Relaxed),
    )
}

/// [`written_acks`] with its inputs passed in.
fn written_acks_in(mode: AckMode, voter_count: u64, auto_multi_voter_durable: bool) -> bool {
    match mode {
        AckMode::Written => true,
        AckMode::Durable => false,
        AckMode::Auto => voter_count >= 2 && !auto_multi_voter_durable,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn us(n: u64) -> Duration {
        Duration::from_micros(n)
    }

    #[test]
    fn auto_goes_durable_in_clusters_only_on_a_fast_disk() {
        // The AWS NVMe numbers: p99 ~190 µs.
        let fast = decide_acks(AckMode::Auto, Some(us(190)));
        assert_eq!(fast.single_voter, AckKind::Durable);
        assert_eq!(fast.multi_voter, AckKind::Durable);
        // The slow disks (1-4 ms) keep written acks in clusters.
        let slow = decide_acks(AckMode::Auto, Some(Duration::from_millis(2)));
        assert_eq!(slow.single_voter, AckKind::Durable);
        assert_eq!(slow.multi_voter, AckKind::Written);
    }

    #[test]
    fn the_threshold_is_exclusive_at_one_millisecond() {
        let multi = |p99| decide_acks(AckMode::Auto, Some(p99)).multi_voter;
        assert_eq!(multi(us(999)), AckKind::Durable);
        assert_eq!(multi(us(1000)), AckKind::Written);
        assert_eq!(multi(us(1001)), AckKind::Written);
    }

    #[test]
    fn auto_without_a_measurement_keeps_the_older_written_rule() {
        let d = decide_acks(AckMode::Auto, None);
        assert_eq!(d.single_voter, AckKind::Durable);
        assert_eq!(d.multi_voter, AckKind::Written);
        assert!(d.reason.contains("could not be measured"));
    }

    #[test]
    fn explicit_modes_ignore_the_probe() {
        for p99 in [None, Some(us(10)), Some(Duration::from_millis(50))] {
            let written = decide_acks(AckMode::Written, p99);
            assert_eq!(
                (written.single_voter, written.multi_voter),
                (AckKind::Written, AckKind::Written)
            );
            let durable = decide_acks(AckMode::Durable, p99);
            assert_eq!(
                (durable.single_voter, durable.multi_voter),
                (AckKind::Durable, AckKind::Durable)
            );
        }
    }

    #[test]
    fn written_acks_follow_mode_voters_and_decision() {
        // Explicit modes: voter count and probe are irrelevant.
        for voters in [1, 2, 3, 5] {
            for flag in [false, true] {
                assert!(written_acks_in(AckMode::Written, voters, flag));
                assert!(!written_acks_in(AckMode::Durable, voters, flag));
            }
        }
        // Auto: a single voter is always durable.
        assert!(!written_acks_in(AckMode::Auto, 1, false));
        assert!(!written_acks_in(AckMode::Auto, 1, true));
        // Auto at 2+ voters: written until the probe says the disk is fast.
        for voters in [2, 3, 5] {
            assert!(written_acks_in(AckMode::Auto, voters, false));
            assert!(!written_acks_in(AckMode::Auto, voters, true));
        }
    }

    #[test]
    fn only_a_durable_auto_decision_sets_the_flag() {
        // `apply` writes a process-global; the bit it stores is checked here
        // without touching it.
        assert!(decide_acks(AckMode::Auto, Some(us(100))).auto_multi_voter_durable());
        assert!(
            !decide_acks(AckMode::Auto, Some(Duration::from_millis(3))).auto_multi_voter_durable()
        );
        assert!(!decide_acks(AckMode::Auto, None).auto_multi_voter_durable());
        assert!(!decide_acks(AckMode::Durable, Some(us(100))).auto_multi_voter_durable());
        assert!(!decide_acks(AckMode::Written, Some(us(100))).auto_multi_voter_durable());
    }
}
