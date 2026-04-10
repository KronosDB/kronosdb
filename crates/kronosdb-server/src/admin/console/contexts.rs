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
        let event_count = if head > tail { head - tail } else { 0 };
        rows.push((name.clone(), head, tail, event_count));
    }

    let table_html = contexts_table_html(&rows);

    let content = format!(
        r##"<div class="flex flex-col flex-1" id="page-contexts">
  <!-- Create context form -->
  <div class="bg-k-surface border border-k-subtle rounded-lg p-4 mb-4 flex items-end gap-3">
    <form class="flex items-end gap-3" id="create-context-form">
      <div class="flex flex-col gap-1">
        <label class="text-[10px] font-semibold tracking-[0.6px] uppercase text-k-muted">New Context</label>
        <input type="text" name="name" placeholder="e.g. orders" required pattern="[a-zA-Z0-9_\-]+"
          class="font-mono text-xs px-2.5 py-1.5 border border-k-border rounded-[5px] bg-k-base text-k-text outline-none focus:border-k-gold min-w-[200px]">
      </div>
      <button type="submit"
        class="px-3 py-1.5 rounded-[5px] border border-k-gold bg-k-gold-d text-k-gold text-xs font-medium cursor-pointer hover:bg-k-gold hover:text-k-inv transition-colors"
        hx-post="/fragments/create-context"
        hx-include="#create-context-form"
        hx-target="#context-feedback"
        hx-swap="innerHTML">
        Create
      </button>
    </form>
    <span id="context-feedback" class="text-xs"></span>
  </div>

  <!-- Contexts table -->
  <div class="bg-k-surface border border-k-subtle rounded-lg overflow-hidden flex flex-col flex-1">
    <div class="flex items-center justify-between px-[18px] py-3 border-b border-k-subtle">
      <div class="text-[13px] font-semibold flex items-center gap-2">
        Event Store Contexts
        <span class="font-mono text-[11px] bg-k-overlay px-[7px] py-px rounded-full text-k-text2">{count}</span>
      </div>
    </div>
    <div class="flex-1 overflow-auto" hx-get="/fragments/contexts" hx-trigger="every 5s, refreshContexts from:body" hx-swap="innerHTML">
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
        let event_count = if head > tail { head - tail } else { 0 };
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
        let events = if head > tail { head - tail } else { 0 };
        data.push((name.clone(), events));
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
    match state.contexts.create_context(&req.name) {
        Ok(()) => (
            StatusCode::OK,
            [("HX-Trigger", "refreshContexts")],
            Html(format!(
                r#"<span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[11px] font-mono bg-k-teal-d text-k-teal"><span class="w-1.5 h-1.5 rounded-full bg-k-teal"></span>Created '{}'</span>"#,
                html_escape(&req.name),
            )),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Html(format!(
                r#"<span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[11px] font-mono bg-k-red-d text-k-red"><span class="w-1.5 h-1.5 rounded-full bg-k-red"></span>{}</span>"#,
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
        .contexts
        .create_context(&req.name)
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    Ok(Json(
        serde_json::json!({"status": "ok", "context": req.name}),
    ))
}

// ── Helpers ─────────────────────────────────────────────────────────

fn contexts_table_html(rows: &[(String, u64, u64, u64)]) -> String {
    if rows.is_empty() {
        return r#"<div class="text-center text-k-muted py-8 text-xs">No contexts created yet</div>"#.to_string();
    }
    let mut html = String::new();
    for (name, head, tail, event_count) in rows {
        let escaped = html_escape(name);
        html.push_str(&format!(
            r#"<tr>
  <td class="font-mono text-xs !text-k-text"><a href="/events?context={escaped}" class="text-k-gold no-underline hover:underline">{escaped}</a></td>
  <td class="font-mono text-xs text-right">{head}</td>
  <td class="font-mono text-xs text-right">{tail}</td>
  <td class="font-mono text-xs text-right">{events}</td>
  <td class="text-right"><span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[11px] font-mono bg-k-teal-d text-k-teal"><span class="w-1.5 h-1.5 rounded-full bg-k-teal"></span>active</span></td>
</tr>"#,
            head = format_number(*head),
            tail = format_number(*tail),
            events = format_number(*event_count),
        ));
    }
    format!(
        r#"<table><thead><tr><th>Context</th><th class="text-right">Head</th><th class="text-right">Tail</th><th class="text-right">Events</th><th class="text-right">Status</th></tr></thead><tbody>{html}</tbody></table>"#
    )
}

fn contexts_table_mini_html(data: &[(String, u64)]) -> String {
    if data.is_empty() {
        return r#"<div class="text-center text-k-muted py-8 text-xs">No contexts created yet</div>"#.to_string();
    }
    let mut rows = String::new();
    for (name, events) in data {
        rows.push_str(&format!(
            r#"<tr><td class="font-mono text-xs !text-k-text">{name}</td><td class="font-mono text-xs text-right">{events}</td></tr>"#,
            name = html_escape(name),
            events = format_number(*events),
        ));
    }
    format!(
        r#"<table><thead><tr><th>Name</th><th class="text-right">Events</th></tr></thead><tbody>{rows}</tbody></table>"#
    )
}
