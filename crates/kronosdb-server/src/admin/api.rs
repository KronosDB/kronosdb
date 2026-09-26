//! JSON read API (`/api/v1/*`) — the machine-readable twin of the console
//! pages, for the `kronos` CLI/TUI and scripts. Same data calls as the HTML
//! pages, same auth gate. Counts and topology only, never event payloads
//! (events are read over gRPC, where per-context grants apply).

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::routing::get;
use serde_json::{Value, json};

use super::AdminState;

pub fn routes() -> Router<AdminState> {
    Router::new()
        .route("/api/v1/snapshot", get(snapshot))
        .route(
            "/api/v1/node",
            get(|s: State<AdminState>| async move { Json(node(&s)) }),
        )
        // Who has access (names, roles, scopes — never secrets).
        .route(
            "/api/v1/access",
            get(|s: State<AdminState>| async move { Json(s.identities.access_summary()) }),
        )
        .route(
            "/api/v1/cluster",
            get(|s: State<AdminState>| async move { Json(cluster(&s)) }),
        )
        .route(
            "/api/v1/contexts",
            get(|s: State<AdminState>| async move { Json(contexts(&s)) }),
        )
        .route(
            "/api/v1/clients",
            get(|s: State<AdminState>| async move { Json(clients(&s)) }),
        )
        .route(
            "/api/v1/commands",
            get(|s: State<AdminState>| async move { Json(commands(&s)) }),
        )
        .route(
            "/api/v1/queries",
            get(|s: State<AdminState>| async move { Json(queries(&s)) }),
        )
        .route(
            "/api/v1/subscriptions",
            get(|s: State<AdminState>| async move { Json(subscriptions(&s)) }),
        )
        .route(
            "/api/v1/processors",
            get(|s: State<AdminState>| async move { Json(processors(&s)) }),
        )
}

/// Everything in one response: the TUI polls this once per tick instead of
/// fanning out eight requests.
async fn snapshot(State(state): State<AdminState>) -> Json<Value> {
    Json(json!({
        "node": node(&state),
        "cluster": cluster(&state),
        "contexts": contexts(&state),
        "clients": clients(&state),
        "commands": commands(&state),
        "queries": queries(&state),
        "subscriptions": subscriptions(&state),
        "processors": processors(&state),
    }))
}

fn node(state: &AdminState) -> Value {
    let config = &state.config;
    json!({
        "name": config.node_name,
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_secs": state.started_at.elapsed().as_secs(),
        "grpc_addr": config.listen_addr.to_string(),
        "admin_addr": config.admin_listen_addr.to_string(),
        "tls": config.tls_cert.is_some() && config.tls_key.is_some(),
        "auth": state.identities.describe(),
        "ready": state.cluster.native_ready(),
    })
}

fn cluster(state: &AdminState) -> Value {
    let control = state.cluster.replication_control();
    let claim = control.claim();
    let mut out = json!({
        "node_id": control.node_id(),
        "node_type": state.config.cluster_node_type,
        "multi_node": state.cluster.is_multi_node(),
        "claim": claim.map(|c| json!({
            "epoch": c.epoch,
            "leader_id": c.leader_id,
            "term": c.term,
            "writable": c.writable,
        })),
    });
    if let Some(raft) = state.cluster.raft_node() {
        let m = raft.metrics().borrow().clone();
        let membership = m.membership_config.membership();
        let voters: Vec<u64> = membership.voter_ids().collect();
        let nodes: Vec<Value> = membership
            .nodes()
            .map(|(id, node)| {
                json!({
                    "id": id,
                    "addr": node.addr,
                    "voter": voters.contains(id),
                    "leader": m.current_leader == Some(*id),
                })
            })
            .collect();
        out["raft"] = json!({
            "state": format!("{:?}", m.state),
            "leader_id": m.current_leader,
            "term": m.current_term,
            "last_log_index": m.last_log_index,
            "last_applied_index": m.last_applied.map(|l| l.index),
            "nodes": nodes,
        });
    }
    out
}

fn contexts(state: &AdminState) -> Value {
    let mut names = state.contexts.list_contexts();
    names.sort();
    names
        .iter()
        .filter_map(|name| {
            let engine = state.contexts.get_context(name).ok()?;
            let m = engine.metrics_snapshot();
            Some(json!({
                "name": name,
                "head": engine.head().0,
                "tail": engine.tail().0,
                "local_tail": engine.local_tail().0,
                "durable_tail": engine.durable_tail().0,
                "poisoned": engine.is_poisoned(),
                "data_bytes": engine.data_dir_bytes(),
                "appends": m.appends,
                "events_appended": m.events_appended,
                "dcb_violations": m.dcb_violations,
                "source_queries": m.source_queries,
                "events_sourced": m.events_sourced,
                "segment_rotations": m.segment_rotations,
            }))
        })
        .collect()
}

fn clients(state: &AdminState) -> Value {
    let mut clients = state.client_registry.list_client_details();
    clients.sort_by(|a, b| {
        (a.component_name.0.as_str(), a.client_id.0.as_str())
            .cmp(&(b.component_name.0.as_str(), b.client_id.0.as_str()))
    });
    clients
        .iter()
        .map(|c| {
            json!({
                "client_id": c.client_id.0,
                "component": c.component_name.0,
                "version": c.version,
                "connected_secs": c.connected_since.as_secs(),
                "last_heartbeat_ms": c.since_last_heartbeat.as_millis() as u64,
                "streaming": c.has_active_stream,
            })
        })
        .collect()
}

fn message_types(mut details: Vec<kronosdb_messaging::handler::MessageTypeDetail>) -> Value {
    details
        .sort_by(|a, b| (a.bus.as_str(), a.name.as_str()).cmp(&(b.bus.as_str(), b.name.as_str())));
    details
        .iter()
        .map(|d| {
            json!({
                "bus": d.bus,
                "name": d.name,
                "handlers": d.handlers.iter().map(|h| json!({
                    "client_id": h.client_id,
                    "component": h.component_name,
                    "load_factor": h.load_factor,
                    "available_permits": h.available_permits,
                })).collect::<Vec<_>>(),
                "dispatched": d.metrics.dispatched,
                "succeeded": d.metrics.succeeded,
                "failed": d.metrics.failed,
                "no_handler": d.metrics.no_handler,
                "no_permits": d.metrics.no_permits,
                "avg_duration_us": d.metrics.avg_duration_us,
                "success_rate": d.metrics.success_rate,
            })
        })
        .collect()
}

fn commands(state: &AdminState) -> Value {
    message_types(state.messaging.all_command_details())
}

fn queries(state: &AdminState) -> Value {
    message_types(state.messaging.all_query_details())
}

fn subscriptions(state: &AdminState) -> Value {
    state
        .messaging
        .all_subscription_stats()
        .iter()
        .map(|s| {
            json!({
                "bus": s.bus,
                "subscription_id": s.subscription_id,
                "query": s.query_name,
                "subscriber_client_id": s.subscriber_client_id.0,
                "subscriber_component": s.subscriber_component.0,
                "handler_client_id": s.handler_client_id.0,
                "open_secs": s.since_opened.as_secs(),
            })
        })
        .collect()
}

fn processors(state: &AdminState) -> Value {
    state
        .processor_registry
        .list_aggregated()
        .iter()
        .map(|p| {
            json!({
                "name": p.processor_name,
                "mode": p.mode,
                "streaming": p.is_streaming,
                "instances": p.instances.iter().map(|i| json!({
                    "running": i.running,
                    "error": i.error,
                    "segments": i.segments.iter().map(|s| json!({
                        "segment_id": s.segment_id,
                        "one_part_of": s.one_part_of,
                        "caught_up": s.caught_up,
                        "replaying": s.replaying,
                        "token_position": s.token_position,
                        "error_state": s.error_state,
                    })).collect::<Vec<_>>(),
                })).collect::<Vec<_>>(),
            })
        })
        .collect()
}
