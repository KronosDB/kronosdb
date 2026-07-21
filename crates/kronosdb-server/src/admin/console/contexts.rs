use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Json, Response};
use serde::Deserialize;

use crate::admin::AdminState;
use crate::admin::layout::{self, format_number, html_escape};

// ── Page handler ───────────────────────────────────────────────────

pub async fn page(State(state): State<AdminState>) -> Html<String> {
    let contexts = state.contexts.list_contexts();

    let mut rows = Vec::new();
    for name in &contexts {
        let (head, tail) = match state.contexts.get_context(name) {
            Ok(store) => (store.head().0, store.tail().0),
            Err(_) => (0, 0),
        };
        let event_count = head.saturating_sub(tail);
        rows.push((name.clone(), head, tail, event_count));
    }

    let table_html = contexts_table_html(&rows);

    let content = format!(
        r##"<div class="flex flex-col flex-1 gap-4" id="page-contexts">
  <!-- Create context form -->
  <div class="card" data-size="sm">
    <section class="flex items-end gap-3">
      <form class="flex items-end gap-3" id="create-context-form">
        <div class="flex flex-col gap-1.5">
          <label class="label text-[11px] uppercase tracking-wider text-k-muted" for="new-context-name">New Context</label>
          <input id="new-context-name" type="text" name="name" placeholder="e.g. orders" required pattern="[a-zA-Z0-9_\-]+"
            class="input font-mono text-xs flex-none min-w-[200px]">
        </div>
        <button type="submit"
          class="btn"
          data-variant="primary"
          hx-post="/fragments/create-context"
          hx-include="#create-context-form"
          hx-target="#context-feedback"
          hx-swap="innerHTML">
          Create
        </button>
      </form>
      <span id="context-feedback" class="text-xs self-center"></span>
    </section>
  </div>

  <!-- Contexts table -->
  <div class="card gap-0 py-0 overflow-hidden flex-1">
    <header class="items-center border-b border-k-subtle px-[18px] py-3">
      <h2 class="text-[13px] font-semibold flex items-center gap-2">
        Event Store Contexts
        <span class="badge font-mono text-k-text2" data-variant="secondary">{count}</span>
      </h2>
    </header>
    <div class="flex-1 overflow-auto" hx-get="/fragments/contexts" hx-trigger="every 60s, sse-contexts from:body, refreshContexts from:body" hx-swap="morph:innerHTML">
      {table}
    </div>
  </div>
</div>"##,
        count = rows.len(),
        table = table_html,
    );

    Html(layout::layout(
        "contexts",
        "Contexts",
        &state.config.node_name,
        &contexts,
        &content,
    ))
}

// ── Fragments ───────────────────────────────────────────────────────

pub async fn contexts_fragment(State(state): State<AdminState>) -> Html<String> {
    let contexts = state.contexts.list_contexts();

    let mut rows = Vec::new();
    for name in &contexts {
        let (head, tail) = match state.contexts.get_context(name) {
            Ok(store) => (store.head().0, store.tail().0),
            Err(_) => (0, 0),
        };
        let event_count = head.saturating_sub(tail);
        rows.push((name.clone(), head, tail, event_count));
    }

    Html(contexts_table_html(&rows))
}

pub async fn contexts_mini_fragment(State(state): State<AdminState>) -> Html<String> {
    let contexts = state.contexts.list_contexts();
    let mut data = Vec::new();
    for name in &contexts {
        let (head, tail) = match state.contexts.get_context(name) {
            Ok(store) => (store.head().0, store.tail().0),
            Err(_) => (0, 0),
        };
        let events = head.saturating_sub(tail);
        data.push((name.clone(), events, head));
    }
    Html(contexts_table_mini_html(&data))
}

#[derive(Deserialize)]
pub struct CreateContextRequest {
    name: String,
}

pub async fn create_context_fragment(
    State(state): State<AdminState>,
    axum::Form(req): axum::Form<CreateContextRequest>,
) -> Response {
    // Through Raft consensus: replicated to every node and immediately
    // servable (the event store layer registers the context lazily).
    match state.cluster.create_context_replicated(&req.name).await {
        Ok(()) => (
            StatusCode::OK,
            [("HX-Trigger", "refreshContexts")],
            Html(format!(
                r#"<span class="badge font-mono text-k-teal" data-variant="outline"><span class="w-1.5 h-1.5 rounded-full bg-k-teal"></span>Created '{}'</span>"#,
                html_escape(&req.name),
            )),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Html(format!(
                r#"<span class="badge font-mono text-k-red" data-variant="outline"><span class="w-1.5 h-1.5 rounded-full bg-k-red"></span>{}</span>"#,
                html_escape(&e.to_string()),
            )),
        )
            .into_response(),
    }
}

// ── JSON API ───────────────────────────────────────────────────────

pub async fn api_create_context(
    State(state): State<AdminState>,
    Json(req): Json<CreateContextRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    state
        .cluster
        .create_context_replicated(&req.name)
        .await
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(
        serde_json::json!({"status": "ok", "context": req.name}),
    ))
}

// ── Helpers ─────────────────────────────────────────────────────────

/// Sanitize a natural key into a DOM id fragment so idiomorph can match
/// rows across refreshes.
fn dom_id(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

fn contexts_table_html(rows: &[(String, u64, u64, u64)]) -> String {
    if rows.is_empty() {
        return r#"<div class="text-center text-k-muted py-8 text-xs">No contexts created yet</div>"#.to_string();
    }
    let mut html = String::new();
    for (name, head, tail, event_count) in rows {
        let escaped = html_escape(name);
        html.push_str(&format!(
            r#"<tr id="ctx-{row_id}">
  <td class="font-mono text-xs !text-k-text"><a href="/events?context={escaped}" class="text-k-gold no-underline hover:underline">{escaped}</a></td>
  <td class="font-mono text-xs text-right">{head}</td>
  <td class="font-mono text-xs text-right">{tail}</td>
  <td class="font-mono text-xs text-right">{events}</td>
  <td class="text-right"><span class="badge font-mono text-k-teal" data-variant="outline"><span class="w-1.5 h-1.5 rounded-full bg-k-teal"></span>active</span></td>
</tr>"#,
            row_id = dom_id(name),
            head = format_number(*head),
            tail = format_number(*tail),
            events = format_number(*event_count),
        ));
    }
    format!(
        r#"<table><thead><tr><th>Context</th><th class="text-right">Head</th><th class="text-right">Tail</th><th class="text-right">Events</th><th class="text-right">Status</th></tr></thead><tbody>{html}</tbody></table>"#
    )
}

/// Compact context table shared by the overview page and its refresh
/// fragment — one source of truth so idiomorph sees identical markup.
/// Rows are `(name, events, head)`.
pub(crate) fn contexts_table_mini_html(data: &[(String, u64, u64)]) -> String {
    if data.is_empty() {
        return r#"<div class="text-center text-k-muted py-8 text-xs">No contexts created yet</div>"#.to_string();
    }
    let mut rows = String::new();
    for (name, events, head) in data {
        rows.push_str(&format!(
            r#"<tr id="ctx-mini-{row_id}"><td class="font-mono text-xs !text-k-text">{name}</td><td class="font-mono text-xs text-right">{events}</td><td class="font-mono text-xs text-right">{head}</td></tr>"#,
            row_id = dom_id(name),
            name = html_escape(name),
            events = format_number(*events),
            head = format_number(*head),
        ));
    }
    format!(
        r#"<table><thead><tr><th>Name</th><th class="text-right">Events</th><th class="text-right">Head</th></tr></thead><tbody>{rows}</tbody></table>"#
    )
}
