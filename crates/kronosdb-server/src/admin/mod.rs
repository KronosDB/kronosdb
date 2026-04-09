mod templates;

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{header, StatusCode, Uri};
use axum::response::{Html, IntoResponse, Json, Redirect, Response};
use axum::routing::{get, post};
use axum::Router;
use rust_embed::Embed;
use serde::Deserialize;

use kronosdb_eventstore::context::ContextManager;
use kronosdb_eventstore::criteria::{Criterion, SourcingCondition};
use kronosdb_eventstore::event::{Position, Tag};
use kronosdb_messaging::client::ClientRegistry;
use kronosdb_messaging::manager::MessagingManager;
use kronosdb_raft::cluster::ClusterManager;

use crate::config::ServerConfig;

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
    pub started_at: std::time::Instant,
}

/// Starts the admin HTTP server on the configured address.
pub async fn start_admin_server(state: AdminState) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let addr = state.config.admin_listen_addr;

    let app = Router::new()
        .route("/", get(|| async { Redirect::to("/overview") }))
        .route("/overview", get(overview_page))
        .route("/contexts", get(contexts_page))
        .route("/clients", get(clients_page))
        .route("/commands", get(commands_page))
        .route("/queries", get(queries_page))
        .route("/events", get(events_page))
        .route("/settings", get(settings_page))
        .route("/health", get(health))
        // Cluster membership API
        .route("/api/cluster/add-learner", post(api_add_learner))
        .route("/api/cluster/add-voter", post(api_add_voter))
        .route("/api/cluster/remove-node", post(api_remove_node))
        // Context API
        .route("/api/contexts", post(api_create_context))
        // HTMX fragment endpoints
        .route("/fragments/stats", get(stats_fragment))
        .route("/fragments/create-context", post(create_context_fragment))
        .route("/fragments/events", get(events_fragment))
        .route("/fragments/contexts", get(contexts_fragment))
        .route("/fragments/contexts-mini", get(contexts_mini_fragment))
        .route("/fragments/clients", get(clients_fragment))
        .route("/fragments/clients-mini", get(clients_mini_fragment))
        .route("/fragments/commands", get(commands_fragment))
        .route("/fragments/queries", get(queries_fragment))
        .route("/fragments/context-chart", get(context_chart_fragment))
        .route("/static/{*path}", get(static_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> &'static str {
    "ok"
}

// ── Page handlers ──────────────────────────────────────────────────

async fn overview_page(State(state): State<AdminState>) -> Html<String> {
    let uptime = state.started_at.elapsed();
    let contexts = state.contexts.list_contexts();
    let clients = state.client_registry.list_client_details();
    let commands = state.messaging.all_command_stats();
    let queries = state.messaging.all_query_stats();

    let mut total_events: u64 = 0;
    let mut context_data = Vec::new();
    for name in &contexts {
        let (head, tail) = match state.contexts.get_context(name) {
            Ok(store) => (store.head().0, store.tail().0),
            Err(_) => (0, 0),
        };
        let events = if head > tail { head - tail } else { 0 };
        total_events += events;
        context_data.push((name.clone(), events));
    }

    Html(templates::overview_page(
        &state.config,
        uptime,
        contexts.len(),
        &clients,
        total_events,
        commands.len(),
        queries.len(),
        &context_data,
    ))
}

async fn contexts_page(State(state): State<AdminState>) -> Html<String> {
    Html(templates::contexts_page(&state.config))
}

async fn clients_page(State(state): State<AdminState>) -> Html<String> {
    Html(templates::clients_page(&state.config))
}

async fn commands_page(State(state): State<AdminState>) -> Html<String> {
    Html(templates::commands_page(&state.config))
}

async fn queries_page(State(state): State<AdminState>) -> Html<String> {
    Html(templates::queries_page(&state.config))
}

async fn settings_page(State(state): State<AdminState>) -> Html<String> {
    Html(templates::settings_page(&state.config))
}

// ── HTMX fragment handlers ────────────────────────────────────────

async fn stats_fragment(State(state): State<AdminState>) -> Html<String> {
    let uptime = state.started_at.elapsed();
    let contexts = state.contexts.list_contexts();
    let clients = state.client_registry.client_count();
    let commands = state.messaging.all_command_stats();
    let queries = state.messaging.all_query_stats();

    let mut total_events: u64 = 0;
    for name in &contexts {
        if let Ok(store) = state.contexts.get_context(name) {
            let h = store.head().0;
            let t = store.tail().0;
            if h > t {
                total_events += h - t;
            }
        }
    }

    Html(templates::stats_fragment(
        uptime,
        contexts.len(),
        clients,
        total_events,
        commands.len(),
        queries.len(),
    ))
}

async fn contexts_fragment(State(state): State<AdminState>) -> Html<String> {
    let contexts = state.contexts.list_contexts();

    let mut rows = Vec::new();
    for name in &contexts {
        let (head, tail) = match state.contexts.get_context(name) {
            Ok(store) => (store.head().0, store.tail().0),
            Err(_) => (0, 0),
        };
        let event_count = if head > tail { head - tail } else { 0 };
        rows.push((name.clone(), head, tail, event_count));
    }

    Html(templates::contexts_table(&rows))
}

async fn contexts_mini_fragment(State(state): State<AdminState>) -> Html<String> {
    let contexts = state.contexts.list_contexts();
    let mut data = Vec::new();
    for name in &contexts {
        let (head, tail) = match state.contexts.get_context(name) {
            Ok(store) => (store.head().0, store.tail().0),
            Err(_) => (0, 0),
        };
        let events = if head > tail { head - tail } else { 0 };
        data.push((name.clone(), events));
    }
    Html(templates::contexts_table_mini_pub(&data))
}

async fn clients_fragment(State(state): State<AdminState>) -> Html<String> {
    let clients = state.client_registry.list_client_details();
    Html(templates::clients_table(&clients))
}

async fn clients_mini_fragment(State(state): State<AdminState>) -> Html<String> {
    let clients = state.client_registry.list_client_details();
    Html(templates::clients_table_mini_pub(&clients))
}

async fn commands_fragment(State(state): State<AdminState>) -> Html<String> {
    let mut stats = state.messaging.all_command_stats();
    stats.sort_by(|a, b| a.0.cmp(&b.0));
    Html(templates::commands_table(&stats))
}

async fn queries_fragment(State(state): State<AdminState>) -> Html<String> {
    let mut stats = state.messaging.all_query_stats();
    stats.sort_by(|a, b| a.0.cmp(&b.0));
    Html(templates::queries_table(&stats))
}

async fn context_chart_fragment(State(state): State<AdminState>) -> Html<String> {
    let contexts = state.contexts.list_contexts();
    let mut data = Vec::new();
    for name in &contexts {
        let (head, tail) = match state.contexts.get_context(name) {
            Ok(store) => (store.head().0, store.tail().0),
            Err(_) => (0, 0),
        };
        let events = if head > tail { head - tail } else { 0 };
        data.push((name.clone(), events));
    }
    Html(templates::context_bar_chart(&data))
}

// ── Events page ───────────────────────────────────────────────────

/// Query parameters for the events page / fragment.
#[derive(Deserialize, Default)]
struct EventsQuery {
    /// Context to query. Defaults to first available.
    context: Option<String>,
    /// Start position (inclusive). Defaults to tail.
    from: Option<u64>,
    /// Number of events to return.
    limit: Option<usize>,
    /// Filter by event name (substring match).
    name: Option<String>,
    /// Filter by tag key=value.
    tag: Option<String>,
}

const DEFAULT_EVENT_LIMIT: usize = 50;
const MAX_EVENT_LIMIT: usize = 200;

async fn events_page(
    State(state): State<AdminState>,
    Query(params): Query<EventsQuery>,
) -> Html<String> {
    let contexts = state.contexts.list_contexts();
    let selected = params.context.clone()
        .unwrap_or_else(|| contexts.first().cloned().unwrap_or_else(|| "default".into()));

    let limit = params.limit.unwrap_or(DEFAULT_EVENT_LIMIT).min(MAX_EVENT_LIMIT);

    // Server-render the initial events so there's no "Loading..." flash.
    let initial_html = match state.contexts.get_context(&selected) {
        Ok(store) => {
            let from = Position(params.from.unwrap_or(1));
            let condition = build_condition(&params);
            match store.source_stored(from, &condition, limit) {
                Ok(events) => {
                    let head = store.head().0;
                    let has_more = events.len() == limit;
                    let next_position = events.last().map(|e| e.position.0 + 1);
                    templates::events_table(&events, head, has_more, next_position, &selected, &params)
                }
                Err(e) => templates::events_error(&format!("Query failed: {e}")),
            }
        }
        Err(_) => templates::events_error(&format!("Context '{}' not found", selected)),
    };

    Html(templates::events_page(&state.config, &contexts, &selected, &params, &initial_html))
}

async fn events_fragment(
    State(state): State<AdminState>,
    Query(params): Query<EventsQuery>,
) -> Html<String> {
    let contexts = state.contexts.list_contexts();
    let selected = params.context.clone()
        .unwrap_or_else(|| contexts.first().cloned().unwrap_or_else(|| "default".into()));

    let limit = params.limit.unwrap_or(DEFAULT_EVENT_LIMIT).min(MAX_EVENT_LIMIT);

    let store = match state.contexts.get_context(&selected) {
        Ok(s) => s,
        Err(_) => return Html(templates::events_error(&format!("Context '{}' not found", selected))),
    };

    let from = Position(params.from.unwrap_or(1));

    // Build sourcing condition from filters.
    let condition = build_condition(&params);

    let events = match store.source_stored(from, &condition, limit) {
        Ok(e) => e,
        Err(e) => return Html(templates::events_error(&format!("Query failed: {e}"))),
    };

    let head = store.head().0;
    let has_more = events.len() == limit;
    let next_position = events.last().map(|e| e.position.0 + 1);

    Html(templates::events_table(&events, head, has_more, next_position, &selected, &params))
}

fn build_condition(params: &EventsQuery) -> SourcingCondition {
    let mut criteria = Vec::new();

    let names = match &params.name {
        Some(n) if !n.is_empty() => vec![n.clone()],
        _ => vec![],
    };

    let tags = match &params.tag {
        Some(t) if !t.is_empty() => {
            t.split(',')
                .filter_map(|pair| {
                    let (k, v) = pair.split_once('=')?;
                    Some(Tag::from_str(k.trim(), v.trim()))
                })
                .collect()
        }
        _ => vec![],
    };

    if names.is_empty() && tags.is_empty() {
        // No filter — match everything. Empty criteria list = match all.
        return SourcingCondition { criteria: vec![Criterion { names: vec![], tags: vec![] }] };
    }

    criteria.push(Criterion { names, tags });
    SourcingCondition { criteria }
}

// ── Context API ──────────────────────────────────────────────────

#[derive(Deserialize)]
struct CreateContextRequest {
    name: String,
}

async fn api_create_context(
    State(state): State<AdminState>,
    Json(req): Json<CreateContextRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    state
        .contexts
        .create_context(&req.name)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(
        serde_json::json!({"status": "ok", "context": req.name}),
    ))
}

async fn create_context_fragment(
    State(state): State<AdminState>,
    axum::Form(req): axum::Form<CreateContextRequest>,
) -> Response {
    match state.contexts.create_context(&req.name) {
        Ok(()) => (
            StatusCode::OK,
            [("HX-Trigger", "refreshContexts")],
            Html(format!(
                r##"<span class="tag tag-success">Created '{}'</span>"##,
                templates::html_escape(&req.name),
            )),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Html(format!(
                r##"<span style="color: hsl(0 62% 55%); font-size: 0.8125rem;">{}</span>"##,
                templates::html_escape(&e.to_string()),
            )),
        )
            .into_response(),
    }
}

// ── Cluster membership API ────────────────────────────────────────

#[derive(Deserialize)]
struct AddNodeRequest {
    id: u64,
    addr: String,
    #[serde(default = "default_context")]
    context: String,
}

fn default_context() -> String {
    "default".to_string()
}

#[derive(Deserialize)]
struct RemoveNodeRequest {
    id: u64,
    #[serde(default = "default_context")]
    context: String,
}

async fn api_add_learner(
    State(state): State<AdminState>,
    Json(req): Json<AddNodeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    state
        .cluster
        .add_learner(&req.context, req.id, req.addr.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({
        "status": "ok",
        "action": "add_learner",
        "node_id": req.id,
        "addr": req.addr,
    })))
}

async fn api_add_voter(
    State(state): State<AdminState>,
    Json(req): Json<AddNodeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    // First add as learner, then promote to voter.
    state
        .cluster
        .add_learner(&req.context, req.id, req.addr.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Get current voter IDs and add the new one.
    let raft = state.cluster.get_raft_node(&req.context)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "context not found".to_string()))?;

    let metrics = raft.metrics().borrow().clone();
    let mut voter_ids: Vec<u64> = metrics
        .membership_config
        .membership()
        .voter_ids()
        .collect();
    if !voter_ids.contains(&req.id) {
        voter_ids.push(req.id);
    }

    state
        .cluster
        .change_membership(&req.context, voter_ids)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({
        "status": "ok",
        "action": "add_voter",
        "node_id": req.id,
        "addr": req.addr,
    })))
}

async fn api_remove_node(
    State(state): State<AdminState>,
    Json(req): Json<RemoveNodeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let raft = state.cluster.get_raft_node(&req.context)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "context not found".to_string()))?;

    let metrics = raft.metrics().borrow().clone();
    let voter_ids: Vec<u64> = metrics
        .membership_config
        .membership()
        .voter_ids()
        .filter(|id| *id != req.id)
        .collect();

    state
        .cluster
        .change_membership(&req.context, voter_ids)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({
        "status": "ok",
        "action": "remove_node",
        "node_id": req.id,
    })))
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
