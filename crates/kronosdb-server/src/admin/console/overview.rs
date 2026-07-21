use axum::extract::State;
use axum::response::Html;

use crate::admin::AdminState;
use crate::admin::layout::{self, format_number, format_uptime};

// ── Page handler ───────────────────────────────────────────────────

pub async fn page(State(state): State<AdminState>) -> Html<String> {
    let uptime = state.started_at.elapsed();
    let contexts = state.contexts.list_contexts();
    let clients = state.client_registry.list_client_details();

    let mut total_events: u64 = 0;
    let mut ctx_data = Vec::new();
    for name in &contexts {
        let (head, tail) = match state.contexts.get_context(name) {
            Ok(store) => (store.head().0, store.tail().0),
            Err(_) => (0, 0),
        };
        let events = head.saturating_sub(tail);
        total_events += events;
        ctx_data.push((name.clone(), events, head));
    }

    // Reuse the fragment renderers so the initial page markup is identical
    // to what the SSE-driven refreshes morph in.
    let context_table = super::contexts::contexts_table_mini_html(&ctx_data);
    let client_table = super::clients::clients_table_mini_html(&clients);

    let content = format!(
        r##"<div class="flex flex-col flex-1 gap-4" id="page-overview">
  <!-- Stat cards -->
  <div class="flex flex-wrap gap-3" hx-get="/fragments/stats" hx-trigger="every 10s, sse-stats from:body" hx-swap="morph:innerHTML">
    {stats}
  </div>

  <!-- Chart + Contexts -->
  <div class="flex gap-4 flex-1 min-h-0">
    <div class="card flex-1 gap-0 py-0 overflow-hidden">
      <header class="items-center border-b border-k-subtle px-[18px] py-3">
        <h2 class="text-[13px] font-semibold flex items-center gap-2">Event Activity <span class="badge font-mono text-k-text2" data-variant="secondary">24h</span></h2>
      </header>
      <div class="chart-bars" id="activity-chart" hx-get="/fragments/context-chart" hx-trigger="load, every 30s, sse-events from:body" hx-swap="morph:innerHTML"></div>
      <div class="flex justify-between px-[18px] pb-3 text-[11px] text-k-muted font-mono"><span>24h ago</span><span>18h</span><span>12h</span><span>6h</span><span>now</span></div>
    </div>
    <div class="card flex-1 gap-0 py-0 overflow-hidden">
      <header class="items-center border-b border-k-subtle px-[18px] py-3">
        <h2 class="text-[13px] font-semibold flex items-center gap-2">Contexts <span class="badge font-mono text-k-text2" data-variant="secondary">{ctx_count}</span></h2>
        <a href="/contexts" class="btn card-action" data-variant="outline" data-size="xs">View All</a>
      </header>
      <div class="flex-1 overflow-auto" hx-get="/fragments/contexts-mini" hx-trigger="every 60s, sse-contexts from:body" hx-swap="morph:innerHTML">
        {context_table}
      </div>
    </div>
  </div>

  <!-- Connected Clients -->
  <div class="card gap-0 py-0 overflow-hidden shrink-0">
    <header class="items-center border-b border-k-subtle px-[18px] py-3">
      <h2 class="text-[13px] font-semibold flex items-center gap-2">Connected Clients <span class="badge font-mono text-k-text2" data-variant="secondary">{client_count}</span></h2>
      <a href="/clients" class="btn card-action" data-variant="outline" data-size="xs">View All</a>
    </header>
    <div hx-get="/fragments/clients-mini" hx-trigger="every 10s, sse-clients from:body" hx-swap="morph:innerHTML">
      {client_table}
    </div>
  </div>
</div>"##,
        stats = stats_cards_html(uptime, contexts.len(), total_events, clients.len()),
        ctx_count = contexts.len(),
        context_table = context_table,
        client_count = clients.len(),
        client_table = client_table,
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
    // `accent` must be a literal Tailwind class so the CSS build's source
    // scan picks it up (no interpolated class names).
    let tile = |id: &str, accent: &str, label: &str, value: &str| -> String {
        format!(
            r#"<div id="stat-{id}" class="card relative flex-1 min-w-[170px] gap-0 before:absolute before:left-0 before:top-0 before:bottom-0 before:w-[3px] {accent}" data-size="sm"><section class="flex flex-col gap-2"><div class="text-[11px] font-medium uppercase tracking-wider text-k-muted">{label}</div><div class="font-mono text-[26px] font-semibold leading-none">{value}</div></section></div>"#,
        )
    };

    let mut html = String::new();
    html.push_str(&tile(
        "uptime",
        "before:bg-k-gold",
        "Uptime",
        &format_uptime(uptime),
    ));
    html.push_str(&tile(
        "contexts",
        "before:bg-k-blue",
        "Contexts",
        &ctx_count.to_string(),
    ));
    html.push_str(&tile(
        "events",
        "before:bg-k-gold",
        "Total Events",
        &format_number(total_events),
    ));
    html.push_str(&tile(
        "clients",
        "before:bg-k-blue",
        "Connected Clients",
        &client_count.to_string(),
    ));
    html
}
