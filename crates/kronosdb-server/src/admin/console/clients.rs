use axum::extract::State;
use axum::response::Html;

use crate::admin::AdminState;
use crate::admin::layout::{self, format_duration_connected, format_uptime_short, html_escape};

// ── Page handler ───────────────────────────────────────────────────

pub async fn page(State(state): State<AdminState>) -> Html<String> {
    let clients = state.client_registry.list_client_details();
    let table_html = clients_table_html(&clients);

    let content = format!(
        r##"<div class="flex flex-col flex-1 gap-4" id="page-clients">
  <div class="card gap-0 py-0 overflow-hidden flex-1">
    <header class="items-center border-b border-k-subtle px-[18px] py-3">
      <h2 class="text-[13px] font-semibold flex items-center gap-2">
        Connected Clients
        <span class="badge font-mono text-k-text2" data-variant="secondary">{count}</span>
      </h2>
    </header>
    <div class="flex-1 overflow-auto" hx-get="/fragments/clients" hx-trigger="every 10s, sse-clients from:body" hx-swap="morph:innerHTML">
      {table}
    </div>
  </div>
</div>"##,
        count = clients.len(),
        table = table_html,
    );

    Html(layout::layout(
        "clients",
        "Clients",
        &state.config.node_name,
        &state.contexts.list_contexts(),
        &content,
    ))
}

// ── Fragments ───────────────────────────────────────────────────────

pub async fn clients_fragment(State(state): State<AdminState>) -> Html<String> {
    let clients = state.client_registry.list_client_details();
    Html(clients_table_html(&clients))
}

pub async fn clients_mini_fragment(State(state): State<AdminState>) -> Html<String> {
    let clients = state.client_registry.list_client_details();
    Html(clients_table_mini_html(&clients))
}

// ── Helpers ─────────────────────────────────────────────────────────

use kronosdb_messaging::client::ClientInfo;

/// Sanitize a natural key into a DOM id fragment so idiomorph can match
/// rows across refreshes.
fn dom_id(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}

fn clients_table_html(clients: &[ClientInfo]) -> String {
    if clients.is_empty() {
        return r#"<div class="text-center text-k-muted py-8 text-xs">No clients connected</div>"#
            .to_string();
    }
    let mut rows = String::new();
    for c in clients {
        let hb_secs = c.since_last_heartbeat.as_secs();
        let (badge_tint, badge_text, dot_cls) = if hb_secs > 15 {
            ("text-k-red", "stale", "bg-k-red")
        } else if hb_secs > 5 {
            ("text-k-amber", "slow", "bg-k-amber")
        } else {
            ("text-k-teal", "healthy", "bg-k-teal")
        };
        let stream_badge = if c.has_active_stream {
            r#"<span class="badge font-mono text-k-teal" data-variant="outline"><span class="w-1.5 h-1.5 rounded-full bg-k-teal"></span>active</span>"#
        } else {
            r#"<span class="text-k-muted text-xs">-</span>"#
        };
        rows.push_str(&format!(
            r#"<tr id="client-{row_id}">
  <td class="font-mono text-xs !text-k-text">{client_id}</td>
  <td class="!text-k-text">{component}</td>
  <td class="font-mono text-xs">{version}</td>
  <td class="font-mono text-xs">{connected}</td>
  <td class="font-mono text-xs">{heartbeat}</td>
  <td><span class="badge font-mono {badge_tint}" data-variant="outline"><span class="w-1.5 h-1.5 rounded-full {dot_cls}"></span>{badge_text}</span></td>
  <td class="text-center">{stream_badge}</td>
</tr>"#,
            row_id = dom_id(&c.client_id.0),
            client_id = html_escape(&c.client_id.0),
            component = html_escape(&c.component_name.0),
            version = if c.version.is_empty() { "-" } else { &c.version },
            connected = format_duration_connected(c.connected_since),
            heartbeat = format_uptime_short(c.since_last_heartbeat),
        ));
    }
    format!(
        r#"<table><thead><tr><th>Client ID</th><th>Component</th><th>Version</th><th>Connected</th><th>Heartbeat</th><th>Health</th><th class="text-center">Stream</th></tr></thead><tbody>{rows}</tbody></table>"#
    )
}

/// Compact client table shared by the overview page and its refresh
/// fragment — one source of truth so idiomorph sees identical markup.
pub(crate) fn clients_table_mini_html(clients: &[ClientInfo]) -> String {
    if clients.is_empty() {
        return r#"<div class="text-center text-k-muted py-8 text-xs">No clients connected</div>"#
            .to_string();
    }
    let mut rows = String::new();
    for c in clients.iter().take(4) {
        let hb = format_uptime_short(c.since_last_heartbeat);
        let connected = format_duration_connected(c.connected_since);
        let (badge_tint, badge_text, dot_cls) = if c.since_last_heartbeat.as_secs() > 15 {
            ("text-k-amber", "slow", "bg-k-amber")
        } else {
            ("text-k-gold", "ok", "bg-k-gold")
        };
        rows.push_str(&format!(
            r#"<tr id="client-mini-{row_id}"><td class="!text-k-text">{component}</td><td class="font-mono text-xs">{client_id}</td><td class="font-mono text-xs">{connected}</td><td class="font-mono text-xs">{hb}</td><td><span class="badge font-mono {badge_tint}" data-variant="outline"><span class="w-1.5 h-1.5 rounded-full {dot_cls}"></span>{badge_text}</span></td></tr>"#,
            row_id = dom_id(&c.client_id.0),
            component = html_escape(&c.component_name.0),
            client_id = html_escape(&c.client_id.0),
        ));
    }
    format!(
        r#"<table><thead><tr><th>Component</th><th>Client ID</th><th>Connected</th><th>Heartbeat</th><th>Status</th></tr></thead><tbody>{rows}</tbody></table>"#
    )
}
