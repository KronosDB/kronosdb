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
    for name in state.contexts.list_contexts() {
        if let Ok(engine) = state.contexts.get_context(&name) {
            let head = engine.head().0;
            let tail = engine.tail().0;
            snaps.push((name, engine.metrics_snapshot(), head, tail));
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
                    .map(|(n, s, _, _)| (ctx_label(n), s.$field))
                    .collect::<Vec<_>>(),
            );
        };
    }

    family(
        &mut out,
        "kronosdb_head_position",
        "gauge",
        "Next position to be assigned in the context",
        &snaps
            .iter()
            .map(|(n, _, head, _)| (ctx_label(n), *head))
            .collect::<Vec<_>>(),
    );
    family(
        &mut out,
        "kronosdb_tail_position",
        "gauge",
        "Oldest retained position in the context",
        &snaps
            .iter()
            .map(|(n, _, _, tail)| (ctx_label(n), *tail))
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

    // ── Raft metrics (node-wide; empty on fast-path-only startup) ──
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

    family(
        &mut out,
        "kronosdb_write_path_fast",
        "gauge",
        "1 when the single-node fast path (raft bypass) is active",
        &[(String::new(), state.cluster.is_fast_path() as u64)],
    );

    ([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out)
}
