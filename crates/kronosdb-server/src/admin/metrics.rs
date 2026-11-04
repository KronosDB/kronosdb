//! Prometheus text-format `/metrics` endpoint.
//!
//! Hand-rolled exposition (text format 0.0.4) over the engine's lock-free
//! counters and openraft's metrics watch channel — no exporter dependency.
//! The endpoint is unauthenticated by design (scrapers), like /health and
//! /ready; it exposes counts only, never event payloads.

use std::fmt::Write as _;
use std::sync::Arc;

use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;

use super::AdminState;

/// One metric family: TYPE header emitted once, then one sample per context.
fn family(out: &mut String, name: &str, kind: &str, help: &str, samples: &[(String, u64)]) {
    let _ = writeln!(out, "# HELP {name} {help}");
    let _ = writeln!(out, "# TYPE {name} {kind}");
    for (labels, value) in samples {
        let _ = writeln!(out, "{name}{labels} {value}");
    }
}

/// Whether a leader knows where `status`'s archive ends: once a pass over
/// it has succeeded. A pass lists the archive before anything else, so a
/// failed first pass knows nothing.
fn archived_end_known(status: &kronosdb_eventstore::tier::ContextArchiveStatus) -> bool {
    status.last_success_ms > 0
}

fn ctx_label(context: &str) -> String {
    format!("{{context=\"{}\"}}", context.replace('"', ""))
}

pub async fn metrics(State(state): State<AdminState>) -> impl IntoResponse {
    let mut out = String::with_capacity(8192);

    // ── Per-context engine metrics ──
    let mut snaps = Vec::new();
    let mut poisoned = Vec::new();
    let mut sized = Vec::new();
    for name in state.contexts.list_contexts() {
        if let Ok(engine) = state.contexts.get_context(&name) {
            let watermark = engine.head().0;
            let local_tail = engine.local_tail().0;
            let durable_tail = engine.durable_tail().0;
            let tail = engine.tail().0;
            poisoned.push((ctx_label(&name), engine.is_poisoned() as u64));
            sized.push((ctx_label(&name), Arc::clone(&engine)));
            snaps.push((
                name,
                engine.metrics_snapshot(),
                watermark,
                local_tail,
                durable_tail,
                tail,
            ));
        }
    }

    // One directory walk per context per scrape — a blocking recursive
    // stat, so it runs on the blocking pool, not a runtime worker. The same
    // walk is what a cap counts: their sum is reported with a cap, and only
    // when every walk succeeded, so a failed one does not read as room.
    let capped = state.cluster.archive_status().eviction().max_local_bytes > 0;
    let (data_bytes, local_bytes): (Vec<(String, u64)>, Option<u64>) =
        tokio::task::spawn_blocking(move || {
            let mut local_bytes = Some(0u64);
            let data_bytes = sized
                .into_iter()
                .map(|(label, engine)| {
                    let bytes = match engine.local_bytes() {
                        Ok(bytes) => bytes,
                        Err(_) => {
                            local_bytes = None;
                            engine.data_dir_bytes()
                        }
                    };
                    local_bytes = local_bytes.map(|sum| sum + bytes);
                    (label, bytes)
                })
                .collect();
            (data_bytes, local_bytes.filter(|_| capped))
        })
        .await
        .unwrap_or_default();

    macro_rules! engine_family {
        ($metric:literal, $kind:literal, $help:literal, $field:ident) => {
            family(
                &mut out,
                $metric,
                $kind,
                $help,
                &snaps
                    .iter()
                    .map(|(n, s, _, _, _, _)| (ctx_label(n), s.$field))
                    .collect::<Vec<_>>(),
            );
        };
    }

    family(
        &mut out,
        "kronosdb_head_position",
        "gauge",
        "Next-exclusive quorum-committed watermark in the context",
        &snaps
            .iter()
            .map(|(n, _, watermark, _, _, _)| (ctx_label(n), *watermark))
            .collect::<Vec<_>>(),
    );
    family(
        &mut out,
        "kronosdb_local_tail_position",
        "gauge",
        "Next-exclusive locally written event cursor",
        &snaps
            .iter()
            .map(|(n, _, _, local_tail, _, _)| (ctx_label(n), *local_tail))
            .collect::<Vec<_>>(),
    );
    family(
        &mut out,
        "kronosdb_durable_tail_position",
        "gauge",
        "Next-exclusive locally fdatasynced event cursor",
        &snaps
            .iter()
            .map(|(n, _, _, _, durable_tail, _)| (ctx_label(n), *durable_tail))
            .collect::<Vec<_>>(),
    );
    family(
        &mut out,
        "kronosdb_replication_lag_events",
        "gauge",
        "Locally durable events not yet visible below the quorum watermark",
        &snaps
            .iter()
            .map(|(n, _, watermark, _, durable_tail, _)| {
                (ctx_label(n), durable_tail.saturating_sub(*watermark))
            })
            .collect::<Vec<_>>(),
    );
    family(
        &mut out,
        "kronosdb_tail_position",
        "gauge",
        "Oldest retained position in the context",
        &snaps
            .iter()
            .map(|(n, _, _, _, _, tail)| (ctx_label(n), *tail))
            .collect::<Vec<_>>(),
    );
    engine_family!(
        "kronosdb_appends_total",
        "counter",
        "Successful append calls",
        appends
    );
    engine_family!(
        "kronosdb_events_appended_total",
        "counter",
        "Events appended across all append calls",
        events_appended
    );
    engine_family!(
        "kronosdb_dcb_violations_total",
        "counter",
        "Appends rejected by a DCB consistency condition",
        dcb_violations
    );
    engine_family!(
        "kronosdb_ack_degradations_total",
        "counter",
        "Appends that fell back from written-ack to durable pacing (disk behind)",
        ack_degradations
    );
    engine_family!(
        "kronosdb_inline_stage_stalls_total",
        "counter",
        "Appends staged on an async worker that exceeded the stall limit (inline staging then pauses for a cooldown)",
        inline_stalls
    );
    family(
        &mut out,
        "kronosdb_engine_poisoned",
        "gauge",
        "1 after an fsync failure poisoned the engine; all writes fail until restart",
        &poisoned,
    );
    family(
        &mut out,
        "kronosdb_data_dir_bytes",
        "gauge",
        "Bytes on disk under the context's data directory",
        &data_bytes,
    );
    engine_family!(
        "kronosdb_append_duration_us_total",
        "counter",
        "Cumulative append duration in microseconds",
        append_duration_us
    );
    engine_family!(
        "kronosdb_source_queries_total",
        "counter",
        "Source (read) queries",
        source_queries
    );
    engine_family!(
        "kronosdb_events_sourced_total",
        "counter",
        "Events returned by source queries",
        events_sourced
    );
    engine_family!(
        "kronosdb_source_duration_us_total",
        "counter",
        "Cumulative source duration in microseconds",
        source_duration_us
    );
    engine_family!(
        "kronosdb_index_cache_hits_total",
        "counter",
        "Sealed segment index lookups served by an open mapping",
        index_cache_hits
    );
    engine_family!(
        "kronosdb_index_cache_misses_total",
        "counter",
        "Sealed segment indexes mapped from disk",
        index_cache_misses
    );
    engine_family!(
        "kronosdb_key_table_checks_total",
        "counter",
        "Key tables consulted by reads",
        key_table_checks
    );
    engine_family!(
        "kronosdb_key_table_skips_total",
        "counter",
        "Key tables that ruled a read out",
        key_table_skips
    );
    engine_family!(
        "kronosdb_segment_rotations_total",
        "counter",
        "Segment rotations since startup",
        segment_rotations
    );

    // ── Archive tier (ADR-0011) ──
    {
        let status = state.cluster.archive_status();
        let archiving = status.contexts();
        let per_context =
            |value: &dyn Fn(&kronosdb_eventstore::tier::ContextArchiveStatus) -> u64| {
                archiving
                    .iter()
                    .map(|(name, status)| (ctx_label(name), value(status)))
                    .collect::<Vec<_>>()
            };
        // Where the archive ends is known from a context's first successful
        // pass; before it, after a restart or a failover, it reads 0, and
        // the lag would be the whole log.
        let known: Vec<_> = archiving
            .iter()
            .filter(|(_, status)| archived_end_known(status))
            .collect();
        family(
            &mut out,
            "kronosdb_archive_archived_position",
            "gauge",
            "End of the archived prefix; reported by the node that archives (the leader) from its first successful pass",
            &known
                .iter()
                .map(|(name, status)| (ctx_label(name), status.archived_end))
                .collect::<Vec<_>>(),
        );
        // Against the log as it is now: a pass that cannot reach the
        // archive leaves its last archived position, and the lag grows.
        let lag: Vec<(String, u64)> = known
            .iter()
            .filter_map(|(name, status)| {
                let engine = state.contexts.get_context(name).ok()?;
                Some((
                    ctx_label(name),
                    engine.final_end().saturating_sub(status.archived_end),
                ))
            })
            .collect();
        family(
            &mut out,
            "kronosdb_archive_lag_events",
            "gauge",
            "Events in final segments not archived yet; reported by the leader from its first successful pass",
            &lag,
        );
        family(
            &mut out,
            "kronosdb_archive_last_success_timestamp_seconds",
            "gauge",
            "When an archive pass last succeeded (0 for never); reported by the leader",
            &per_context(&|status| status.last_success_ms / 1000),
        );
        family(
            &mut out,
            "kronosdb_archive_pass_failures_total",
            "counter",
            "Archive passes that failed while this node led",
            &per_context(&|status| status.failures),
        );

        let eviction = status.eviction();
        family(
            &mut out,
            "kronosdb_archive_max_local_bytes",
            "gauge",
            "Cap on sealed segment bytes kept on disk; 0 without a cap",
            &[(String::new(), eviction.max_local_bytes)],
        );
        if let Some(local_bytes) = local_bytes {
            family(
                &mut out,
                "kronosdb_archive_local_bytes",
                "gauge",
                "Bytes of the logs' files on disk, which the cap bounds; above the cap, the archive is behind",
                &[(String::new(), local_bytes)],
            );
        }
        family(
            &mut out,
            "kronosdb_archive_evicted_segments_total",
            "counter",
            "Segment bodies evicted to the archive",
            &[(String::new(), eviction.evicted_segments)],
        );
        family(
            &mut out,
            "kronosdb_archive_evicted_bytes_total",
            "counter",
            "Bytes of the segment bodies evicted to the archive",
            &[(String::new(), eviction.evicted_bytes)],
        );
        family(
            &mut out,
            "kronosdb_archive_eviction_failures_total",
            "counter",
            "Eviction passes that failed",
            &[(String::new(), eviction.failures)],
        );
        family(
            &mut out,
            "kronosdb_archive_divergences_total",
            "counter",
            "Contexts an eviction pass found with a segment unlike its archived copy, once per pass",
            &[(String::new(), eviction.diverged)],
        );

        let mut evicted = Vec::new();
        let mut requests = Vec::new();
        let mut fetched = Vec::new();
        let mut failures = Vec::new();
        for name in state.contexts.list_contexts() {
            let Ok(engine) = state.contexts.get_context(&name) else {
                continue;
            };
            let Some(reads) = engine.archive_reads() else {
                continue;
            };
            evicted.push((ctx_label(&name), engine.evicted_end()));
            requests.push((ctx_label(&name), reads.requests));
            fetched.push((ctx_label(&name), reads.fetched_bytes));
            failures.push((ctx_label(&name), reads.failures));
        }
        family(
            &mut out,
            "kronosdb_archive_evicted_position",
            "gauge",
            "End of the evicted prefix: events below it are read from the archive",
            &evicted,
        );
        family(
            &mut out,
            "kronosdb_archive_read_requests_total",
            "counter",
            "Ranged requests to the archive for evicted segments",
            &requests,
        );
        family(
            &mut out,
            "kronosdb_archive_read_bytes_total",
            "counter",
            "Segment bytes fetched from the archive",
            &fetched,
        );
        family(
            &mut out,
            "kronosdb_archive_read_failures_total",
            "counter",
            "Reads from the archive that failed (unreachable, or a short answer)",
            &failures,
        );
    }

    // ── Metadata Raft metrics (node-wide) ──
    if let Some(raft) = state.cluster.raft_node() {
        let m = raft.metrics().borrow().clone();
        let is_leader = (m.current_leader == Some(m.id)) as u64;
        let leader_known = m.current_leader.is_some() as u64;
        let term = m.current_term;
        let last_log = m.last_log_index.unwrap_or(0);
        let last_applied = m.last_applied.map(|l| l.index).unwrap_or(0);
        let voters = m.membership_config.membership().voter_ids().count() as u64;

        family(
            &mut out,
            "kronosdb_raft_is_leader",
            "gauge",
            "1 when this node is the raft leader",
            &[(String::new(), is_leader)],
        );
        family(
            &mut out,
            "kronosdb_raft_leader_known",
            "gauge",
            "1 when a raft leader is known (readiness signal)",
            &[(String::new(), leader_known)],
        );
        family(
            &mut out,
            "kronosdb_raft_current_term",
            "gauge",
            "Current raft term",
            &[(String::new(), term)],
        );
        family(
            &mut out,
            "kronosdb_raft_last_log_index",
            "gauge",
            "Highest raft log index",
            &[(String::new(), last_log)],
        );
        family(
            &mut out,
            "kronosdb_raft_last_applied_index",
            "gauge",
            "Highest applied raft log index",
            &[(String::new(), last_applied)],
        );
        family(
            &mut out,
            "kronosdb_raft_voters",
            "gauge",
            "Number of voters in the raft membership",
            &[(String::new(), voters)],
        );
    }

    let control = state.cluster.replication_control();
    let claim = control.claim();
    family(
        &mut out,
        "kronosdb_native_epoch",
        "gauge",
        "Committed native replication fencing epoch",
        &[(String::new(), claim.map(|claim| claim.epoch).unwrap_or(0))],
    );
    family(
        &mut out,
        "kronosdb_native_write_gate_open",
        "gauge",
        "1 when this node may execute native appends locally",
        &[(
            String::new(),
            claim
                .map(|claim| (claim.leader_id == control.node_id() && claim.writable) as u64)
                .unwrap_or(0),
        )],
    );

    // ── Startup hardware checks (measured once, at boot) ──
    {
        use kronosdb_eventstore::AckKind;
        use kronosdb_eventstore::hardware::Check;
        let hw = &state.hardware;
        family(
            &mut out,
            "kronosdb_dev_mode",
            "gauge",
            "1 when the node runs in dev mode (hardware findings logged at info, not warn)",
            &[(String::new(), hw.dev as u64)],
        );
        family(
            &mut out,
            "kronosdb_disk_probe_ok",
            "gauge",
            "1 when the startup fdatasync probe of the data directory completed",
            &[(String::new(), hw.facts.probe.is_ok() as u64)],
        );
        if let Ok(probe) = &hw.facts.probe {
            let quantile = |q: &str, d: std::time::Duration| {
                (format!("{{quantile=\"{q}\"}}"), d.as_micros() as u64)
            };
            family(
                &mut out,
                "kronosdb_disk_fsync_microseconds",
                "gauge",
                "fdatasync latency of the data directory's disk, measured once at startup",
                &[
                    quantile("0.5", probe.p50),
                    quantile("0.99", probe.p99),
                    quantile("1", probe.max),
                ],
            );
        }
        let durable = |kind: AckKind| (kind == AckKind::Durable) as u64;
        family(
            &mut out,
            "kronosdb_ack_durable",
            "gauge",
            "1 when appends are acknowledged only after fsync in this topology (0 = after quorum write), as resolved from ack-mode and the disk probe",
            &[
                (
                    "{topology=\"single_voter\"}".to_string(),
                    durable(hw.ack.single_voter),
                ),
                (
                    "{topology=\"multi_voter\"}".to_string(),
                    durable(hw.ack.multi_voter),
                ),
            ],
        );
        family(
            &mut out,
            "kronosdb_hardware_finding",
            "gauge",
            "1 when a startup hardware check found something suboptimal; see the logs or /api/v1/node for the message",
            &Check::ALL
                .iter()
                .map(|check| {
                    (
                        format!(
                            "{{check=\"{}\",severity=\"{}\"}}",
                            check.id(),
                            check.severity().as_str()
                        ),
                        hw.has(*check) as u64,
                    )
                })
                .collect::<Vec<_>>(),
        );
    }

    // ── Identity layer (native protocol) ──
    {
        use std::sync::atomic::Ordering::Relaxed;
        let stats = &state.identities.stats;
        let method = |m: &str| format!("{{method=\"{m}\"}}");
        family(
            &mut out,
            "kronosdb_auth_authenticated_total",
            "counter",
            "Native protocol authentications (Hello and Reauthenticate), by credential type",
            &[
                (method("token"), stats.token.load(Relaxed)),
                (method("jwt"), stats.jwt.load(Relaxed)),
                (method("mtls"), stats.mtls.load(Relaxed)),
            ],
        );
        family(
            &mut out,
            "kronosdb_auth_rejected_total",
            "counter",
            "Native protocol requests rejected by the identity layer",
            &[
                (
                    "{reason=\"unauthenticated\"}".to_string(),
                    stats.unauthenticated.load(Relaxed),
                ),
                (
                    "{reason=\"permission_denied\"}".to_string(),
                    stats.denied.load(Relaxed),
                ),
            ],
        );
    }

    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kronosdb_eventstore::tier::ArchiveStatus;

    #[test]
    fn the_archive_end_is_unknown_until_a_pass_succeeds() {
        let status = ArchiveStatus::default();
        status.archive_failed("orders");
        let contexts = status.contexts();
        assert!(!archived_end_known(&contexts[0].1), "a failed first pass");

        status.archived("orders", 0);
        let contexts = status.contexts();
        assert!(
            archived_end_known(&contexts[0].1),
            "nothing archived yet, known"
        );
    }
}
