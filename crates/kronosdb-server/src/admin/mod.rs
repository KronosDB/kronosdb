mod console;
pub mod layout;

use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, Uri, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use rust_embed::Embed;

use kronosdb_eventstore::context::ContextManager;
use kronosdb_eventstore::raft::cluster::ClusterManager;
use kronosdb_messaging::client::ClientRegistry;
use kronosdb_messaging::manager::MessagingManager;

use crate::config::ServerConfig;
use crate::platform::service::ClientChannelRegistry;
use crate::processor::ProcessorRegistry;

#[derive(Embed)]
#[folder = "static/"]
struct StaticAssets;

/// Shared state for admin HTTP handlers.
#[derive(Clone)]
pub struct AdminState {
    pub config: ServerConfig,
    pub contexts: Arc<ContextManager>,
    pub client_registry: Arc<ClientRegistry>,
    pub messaging: Arc<MessagingManager>,
    pub cluster: Arc<ClusterManager>,
    pub processor_registry: Arc<ProcessorRegistry>,
    pub channel_registry: Arc<ClientChannelRegistry>,
    pub started_at: std::time::Instant,
}

/// Starts the admin HTTP server on the configured address.
pub async fn start_admin_server(
    state: AdminState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = state.config.admin_listen_addr;

    let app = Router::new()
        // Page routes — each served by its console module
        .route("/", get(|| async { Redirect::to("/overview") }))
        .route("/overview", get(console::overview::page))
        .route("/contexts", get(console::contexts::page))
        .route("/clients", get(console::clients::page))
        .route("/events", get(console::events::page))
        .route("/commands", get(console::commands::page))
        .route("/queries", get(console::queries::page))
        .route("/subscriptions", get(console::subscriptions::page))
        .route("/processors", get(console::processors::page))
        .route("/cluster", get(console::cluster::page))
        .route("/settings", get(console::settings::page))
        .route("/health", get(health))
        // Cluster membership API
        .route(
            "/api/cluster/add-learner",
            post(console::cluster::api_add_learner),
        )
        .route(
            "/api/cluster/add-voter",
            post(console::cluster::api_add_voter),
        )
        .route(
            "/api/cluster/remove-node",
            post(console::cluster::api_remove_node),
        )
        // Context API
        .route("/api/contexts", post(console::contexts::api_create_context))
        // HTMX fragment endpoints
        .route("/fragments/stats", get(console::overview::stats_fragment))
        .route(
            "/fragments/create-context",
            post(console::contexts::create_context_fragment),
        )
        .route("/fragments/events", get(console::events::events_fragment))
        .route(
            "/fragments/contexts",
            get(console::contexts::contexts_fragment),
        )
        .route(
            "/fragments/contexts-mini",
            get(console::contexts::contexts_mini_fragment),
        )
        .route(
            "/fragments/clients",
            get(console::clients::clients_fragment),
        )
        .route(
            "/fragments/clients-mini",
            get(console::clients::clients_mini_fragment),
        )
        .route(
            "/fragments/commands",
            get(console::commands::commands_fragment),
        )
        .route(
            "/fragments/queries",
            get(console::queries::queries_fragment),
        )
        .route(
            "/fragments/subscriptions",
            get(console::subscriptions::subscriptions_fragment),
        )
        .route(
            "/fragments/processors",
            get(console::processors::processors_fragment),
        )
        .route("/fragments/context-chart", get(context_chart_fragment))
        // Processor control API
        .route("/api/processors/{name}/pause", post(processor_action_pause))
        .route("/api/processors/{name}/start", post(processor_action_start))
        .route("/api/processors/{name}/split", post(processor_action_split))
        .route("/api/processors/{name}/merge", post(processor_action_merge))
        .route("/static/{*path}", get(static_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

// Context chart fragment — returns bar chart HTML for the overview
async fn context_chart_fragment(State(state): State<AdminState>) -> axum::response::Html<String> {
    let contexts = state.contexts.list_contexts();
    let mut bars = String::new();
    let mut max_events: u64 = 1;

    let mut data = Vec::new();
    for name in &contexts {
        let events = match state.contexts.get_context(name) {
            Ok(store) => {
                let h = store.head().0;
                let t = store.tail().0;
                if h > t { h - t } else { 0 }
            }
            Err(_) => 0,
        };
        if events > max_events {
            max_events = events;
        }
        data.push((name.clone(), events));
    }

    for (name, events) in &data {
        let pct = (*events as f64 / max_events as f64 * 100.0) as u32;
        bars.push_str(&format!(
            r#"<div class="chart-bar" style="height:{pct}%" data-events="{events}" data-time="{name}" onmouseenter="showTooltip(this,this.dataset.time,this.dataset.events+' events')" onmouseleave="hideTooltip()"></div>"#,
            pct = pct.max(2),
            events = events,
            name = layout::html_escape(name),
        ));
    }

    axum::response::Html(bars)
}

// ── Processor control API ────────────────────────────────────────────

use crate::proto::kronosdb::platform as pb;

async fn processor_action_pause(
    State(state): State<AdminState>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    send_processor_instruction(&state, &name, |n| {
        pb::platform_outbound::Request::PauseEventProcessor(pb::EventProcessorReference {
            processor_name: n.to_string(),
        })
    })
    .await
}

async fn processor_action_start(
    State(state): State<AdminState>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    send_processor_instruction(&state, &name, |n| {
        pb::platform_outbound::Request::StartEventProcessor(pb::EventProcessorReference {
            processor_name: n.to_string(),
        })
    })
    .await
}

async fn processor_action_split(
    State(state): State<AdminState>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    // Split the segment with the most events (largest one_part_of denominator = smallest fraction).
    // Find the segment with the largest one_part_of across all instances.
    let entries = state.processor_registry.list();
    let segment_id = entries
        .iter()
        .filter(|e| e.processor_name == name)
        .flat_map(|e| e.segments.iter())
        .min_by_key(|s| s.one_part_of) // smallest fraction = largest workload
        .map(|s| s.segment_id)
        .unwrap_or(0);

    send_processor_instruction(&state, &name, |n| {
        pb::platform_outbound::Request::SplitEventProcessorSegment(
            pb::EventProcessorSegmentReference {
                processor_name: n.to_string(),
                segment_identifier: segment_id,
            },
        )
    })
    .await
}

async fn processor_action_merge(
    State(state): State<AdminState>,
    axum::extract::Path(name): axum::extract::Path<String>,
) -> axum::Json<serde_json::Value> {
    // Merge the smallest segments (largest one_part_of = finest granularity).
    let entries = state.processor_registry.list();
    let segment_id = entries
        .iter()
        .filter(|e| e.processor_name == name)
        .flat_map(|e| e.segments.iter())
        .max_by_key(|s| s.one_part_of) // largest fraction denominator = smallest segment
        .map(|s| s.segment_id)
        .unwrap_or(0);

    send_processor_instruction(&state, &name, |n| {
        pb::platform_outbound::Request::MergeEventProcessorSegment(
            pb::EventProcessorSegmentReference {
                processor_name: n.to_string(),
                segment_identifier: segment_id,
            },
        )
    })
    .await
}

/// Sends a processor instruction to all client instances running the named processor.
async fn send_processor_instruction(
    state: &AdminState,
    processor_name: &str,
    make_request: impl Fn(&str) -> pb::platform_outbound::Request,
) -> axum::Json<serde_json::Value> {
    let entries = state.processor_registry.list();
    let client_ids: Vec<String> = entries
        .iter()
        .filter(|e| e.processor_name == processor_name)
        .map(|e| e.client_id.clone())
        .collect();

    if client_ids.is_empty() {
        return axum::Json(
            serde_json::json!({ "ok": false, "error": "no clients found for processor" }),
        );
    }

    let instruction_id = uuid::Uuid::new_v4().to_string();
    let request = make_request(processor_name);

    let mut sent = 0;
    for client_id in &client_ids {
        if state
            .channel_registry
            .send_instruction(client_id, instruction_id.clone(), request.clone())
            .await
        {
            sent += 1;
        }
    }

    axum::Json(serde_json::json!({ "ok": sent > 0, "sent": sent, "total": client_ids.len() }))
}

async fn static_handler(uri: Uri) -> Response {
    let path = uri.path().trim_start_matches("/static/");

    match StaticAssets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, mime.as_ref())],
                content.data.into_owned(),
            )
                .into_response()
        }
        None => StatusCode::NOT_FOUND.into_response(),
    }
}
