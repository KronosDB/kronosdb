use axum::extract::State;
use axum::response::Html;

use crate::admin::AdminState;
use crate::admin::layout::{self, format_duration_connected, format_uptime_short, html_escape};

// ── Page handler ───────────────────────────────────────────────────

pub async fn page(State(state): State<AdminState>) -> Html<String> {
    let clients = state.client_registry.list_client_details();
    let table_html = clients_table_html(&clients);

    let content = format!(
        r##"<div class="flex flex-col flex-1" id="page-clients">
  <div class="bg-k-surface border border-k-subtle rounded-lg overflow-hidden flex flex-col flex-1">
    <div class="flex items-center justify-between px-[18px] py-3 border-b border-k-subtle">
      <div class="text-[13px] font-semibold flex items-center gap-2">
        Connected Clients
        <span class="font-mono text-[11px] bg-k-overlay px-[7px] py-px rounded-full text-k-text2">{count}</span>
      </div>
    </div>
    <div class="flex-1 overflow-auto" hx-get="/fragments/clients" hx-trigger="every 5s" hx-swap="innerHTML">
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

fn clients_table_html(clients: &[ClientInfo]) -> String {
    if clients.is_empty() {
        return r#"<div class="text-center text-k-muted py-8 text-xs">No clients connected</div>"#
            .to_string();
    }
    let mut rows = String::new();
    for c in clients {
        let hb_secs = c.since_last_heartbeat.as_secs();
        let (badge_cls, badge_text, dot_cls) = if hb_secs > 15 {
            ("bg-k-red-d text-k-red", "stale", "bg-k-red")
        } else if hb_secs > 5 {
            ("bg-k-amber-d text-k-amber", "slow", "bg-k-amber")
        } else {
            ("bg-k-teal-d text-k-teal", "healthy", "bg-k-teal")
        };
        let stream_badge = if c.has_active_stream {
            r#"<span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[11px] font-mono bg-k-teal-d text-k-teal"><span class="w-1.5 h-1.5 rounded-full bg-k-teal"></span>active</span>"#
        } else {
            r#"<span class="text-k-muted text-xs">-</span>"#
        };
        rows.push_str(&format!(
            r#"<tr>
  <td class="font-mono text-xs !text-k-text">{client_id}</td>
  <td class="!text-k-text">{component}</td>
  <td class="font-mono text-xs">{version}</td>
  <td class="font-mono text-xs">{connected}</td>
  <td class="font-mono text-xs">{heartbeat}</td>
  <td><span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[11px] font-mono {badge_cls}"><span class="w-1.5 h-1.5 rounded-full {dot_cls}"></span>{badge_text}</span></td>
  <td class="text-center">{stream_badge}</td>
</tr>"#,
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

fn clients_table_mini_html(clients: &[ClientInfo]) -> String {
    if clients.is_empty() {
        return r#"<div class="text-center text-k-muted py-8 text-xs">No clients connected</div>"#
            .to_string();
    }
    let mut rows = String::new();
    for c in clients.iter().take(4) {
        let hb = format_uptime_short(c.since_last_heartbeat);
        let connected = format_duration_connected(c.connected_since);
        let (badge_cls, badge_text) = if c.since_last_heartbeat.as_secs() > 15 {
            ("bg-k-amber-d text-k-amber", "slow")
        } else {
            ("bg-k-gold-d text-k-gold", "ok")
        };
        let dot_cls = if c.since_last_heartbeat.as_secs() > 15 {
            "bg-k-amber"
        } else {
            "bg-k-gold"
        };
        rows.push_str(&format!(
            r#"<tr><td class="!text-k-text">{component}</td><td class="font-mono text-xs">{client_id}</td><td class="font-mono text-xs">{connected}</td><td class="font-mono text-xs">{hb}</td><td><span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[11px] font-mono {badge_cls}"><span class="w-1.5 h-1.5 rounded-full {dot_cls}"></span>{badge_text}</span></td></tr>"#,
            component = html_escape(&c.component_name.0),
            client_id = html_escape(&c.client_id.0),
        ));
    }
    format!(
        r#"<table><thead><tr><th>Component</th><th>Client ID</th><th>Connected</th><th>Heartbeat</th><th>Status</th></tr></thead><tbody>{rows}</tbody></table>"#
    )
}
