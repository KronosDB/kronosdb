//! Prometheus text-format `/metrics` endpoint.
//!
//! Hand-rolled exposition (text format 0.0.4) over the engine's lock-free
//! counters and openraft's metrics watch channel — no exporter dependency.
//! The endpoint is unauthenticated by design (scrapers), like /health and
//! /ready; it exposes counts only, never event payloads.

use std::fmt::Write as _;

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

fn ctx_label(context: &str) -> String {
    format!("{{context=\"{}\"}}", context.replace('"', ""))
}

pub async fn metrics(State(state): State<AdminState>) -> impl IntoResponse {
    let mut out = String::with_capacity(8192);

    // ── Per-context engine metrics ──
    let mut snaps = Vec::new();
    let mut poisoned = Vec::new();
    let mut data_bytes = Vec::new();
    for name in state.contexts.list_contexts() {
        if let Ok(engine) = state.contexts.get_context(&name) {
            let watermark = engine.head().0;
            let local_tail = engine.local_tail().0;
            let durable_tail = engine.durable_tail().0;
            let tail = engine.tail().0;
            poisoned.push((ctx_label(&name), engine.is_poisoned() as u64));
            // Directory walk per scrape, off the request path's hot loops.
            data_bytes.push((ctx_label(&name), engine.data_dir_bytes()));
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
        "Segment index cache hits",
        index_cache_hits
    );
    engine_family!(
        "kronosdb_index_cache_misses_total",
        "counter",
        "Segment index cache misses",
        index_cache_misses
    );
    engine_family!(
        "kronosdb_bloom_checks_total",
        "counter",
        "Bloom filter checks",
        bloom_checks
    );
    engine_family!(
        "kronosdb_bloom_rejections_total",
        "counter",
        "Segments skipped by bloom filter",
        bloom_rejections
    );
    engine_family!(
        "kronosdb_segment_rotations_total",
        "counter",
        "Segment rotations since startup",
        segment_rotations
    );

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

    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out)
}
