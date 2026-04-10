use axum::extract::State;
use axum::response::Html;

use crate::admin::AdminState;
use crate::admin::layout::{self, format_number, format_uptime, html_escape};

// ── Page handler ───────────────────────────────────────────────────

pub async fn page(State(state): State<AdminState>) -> Html<String> {
    let uptime = state.started_at.elapsed();
    let contexts = state.contexts.list_contexts();
    let clients = state.client_registry.list_client_details();

    let mut total_events: u64 = 0;
    let mut context_rows = String::new();
    for name in &contexts {
        let (head, tail) = match state.contexts.get_context(name) {
            Ok(store) => (store.head().0, store.tail().0),
            Err(_) => (0, 0),
        };
        let events = if head > tail { head - tail } else { 0 };
        total_events += events;
        context_rows.push_str(&format!(
            r#"<tr><td class="font-mono text-xs !text-k-text">{name}</td><td class="font-mono text-xs text-right">{events}</td><td class="font-mono text-xs text-right">{head}</td></tr>"#,
            name = html_escape(name),
            events = format_number(events),
            head = format_number(head),
        ));
    }

    let mut client_rows = String::new();
    for c in clients.iter().take(4) {
        let hb = layout::format_uptime_short(c.since_last_heartbeat);
        let connected = layout::format_duration_connected(c.connected_since);
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
        client_rows.push_str(&format!(
            r#"<tr><td class="!text-k-text">{component}</td><td class="font-mono text-xs">{client_id}</td><td class="font-mono text-xs">{connected}</td><td class="font-mono text-xs">{hb}</td><td><span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[11px] font-mono {badge_cls}"><span class="w-1.5 h-1.5 rounded-full {dot_cls}"></span>{badge_text}</span></td></tr>"#,
            component = html_escape(&c.component_name.0),
            client_id = html_escape(&c.client_id.0),
        ));
    }

    let content = format!(
        r##"<div class="flex flex-col flex-1" id="page-overview">
  <!-- Stat cards -->
  <div class="flex flex-wrap gap-3 mb-6" hx-get="/fragments/stats" hx-trigger="every 5s" hx-swap="innerHTML">
    {stats}
  </div>

  <!-- Chart + Contexts -->
  <div class="flex gap-4 flex-1 min-h-0 mb-4">
    <div class="flex-1 bg-k-surface border border-k-subtle rounded-lg overflow-hidden flex flex-col">
      <div class="flex items-center justify-between px-[18px] py-3 border-b border-k-subtle">
        <div class="text-[13px] font-semibold flex items-center gap-2">Event Activity <span class="font-mono text-[11px] bg-k-overlay px-[7px] py-px rounded-full text-k-text2">24h</span></div>
      </div>
      <div class="chart-bars" id="activity-chart" hx-get="/fragments/context-chart" hx-trigger="every 30s" hx-swap="innerHTML"></div>
      <div class="flex justify-between px-[18px] pb-3 text-[11px] text-k-muted font-mono"><span>00:00</span><span>06:00</span><span>12:00</span><span>18:00</span><span>now</span></div>
    </div>
    <div class="flex-1 bg-k-surface border border-k-subtle rounded-lg overflow-hidden flex flex-col">
      <div class="flex items-center justify-between px-[18px] py-3 border-b border-k-subtle">
        <div class="text-[13px] font-semibold flex items-center gap-2">Contexts <span class="font-mono text-[11px] bg-k-overlay px-[7px] py-px rounded-full text-k-text2">{ctx_count}</span></div>
        <a href="/contexts" class="text-xs font-medium px-2.5 py-1 rounded-[5px] border border-k-border bg-k-elevated text-k-text2 no-underline hover:bg-k-hover hover:text-k-text transition-colors">View All</a>
      </div>
      <div class="flex-1 overflow-auto" hx-get="/fragments/contexts-mini" hx-trigger="every 5s" hx-swap="innerHTML">
        <table><thead><tr><th>Name</th><th class="text-right">Events</th><th class="text-right">Head</th></tr></thead><tbody>{context_rows}</tbody></table>
      </div>
    </div>
  </div>

  <!-- Connected Clients -->
  <div class="bg-k-surface border border-k-subtle rounded-lg overflow-hidden flex flex-col shrink-0">
    <div class="flex items-center justify-between px-[18px] py-3 border-b border-k-subtle">
      <div class="text-[13px] font-semibold flex items-center gap-2">Connected Clients <span class="font-mono text-[11px] bg-k-overlay px-[7px] py-px rounded-full text-k-text2">{client_count}</span></div>
      <a href="/clients" class="text-xs font-medium px-2.5 py-1 rounded-[5px] border border-k-border bg-k-elevated text-k-text2 no-underline hover:bg-k-hover hover:text-k-text transition-colors">View All</a>
    </div>
    <div hx-get="/fragments/clients-mini" hx-trigger="every 5s" hx-swap="innerHTML">
      <table><thead><tr><th>Component</th><th>Client ID</th><th>Connected</th><th>Heartbeat</th><th>Status</th></tr></thead><tbody>{client_rows}</tbody></table>
    </div>
  </div>
</div>"##,
        stats = stats_cards_html(uptime, contexts.len(), total_events, clients.len()),
        ctx_count = contexts.len(),
        context_rows = context_rows,
        client_count = clients.len(),
        client_rows = client_rows,
    );

    Html(layout::layout(
        "overview",
        "Overview",
        &state.config.node_name,
        &state.contexts.list_contexts(),
        &content,
    ))
}

// ── Stats fragment (HTMX) ──────────────────────────────────────────

pub async fn stats_fragment(State(state): State<AdminState>) -> Html<String> {
    let uptime = state.started_at.elapsed();
    let contexts = state.contexts.list_contexts();
    let clients = state.client_registry.client_count();

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

    Html(stats_cards_html(
        uptime,
        contexts.len(),
        total_events,
        clients,
    ))
}

fn stats_cards_html(
    uptime: std::time::Duration,
    ctx_count: usize,
    total_events: u64,
    client_count: usize,
) -> String {
    let card = |color: &str, label: &str, value: &str, detail: &str| -> String {
        format!(
            r#"<div class="flex-1 min-w-[170px] bg-k-surface border border-k-subtle rounded-lg p-4 pl-5 relative flex flex-col overflow-hidden before:absolute before:left-0 before:top-0 before:bottom-0 before:w-[3px] before:bg-k-{color} before:rounded-l-lg"><div class="text-[11px] font-medium uppercase tracking-wider text-k-muted mb-2">{label}</div><div class="font-mono text-[26px] font-semibold leading-none">{value}</div><div class="font-mono text-[11px] text-k-muted mt-auto pt-2">{detail}</div></div>"#,
        )
    };

    let mut html = String::new();
    html.push_str(&card("gold", "Uptime", &format_uptime(uptime), ""));
    html.push_str(&card("blue", "Contexts", &ctx_count.to_string(), ""));
    html.push_str(&card(
        "gold",
        "Total Events",
        &format_number(total_events),
        "",
    ));
    html.push_str(&card(
        "blue",
        "Connected Clients",
        &client_count.to_string(),
        "",
    ));
    html
}
