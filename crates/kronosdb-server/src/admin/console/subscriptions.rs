use axum::extract::State;
use axum::response::Html;

use kronosdb_messaging::subscription::SubscriptionInfo;

use crate::admin::AdminState;
use crate::admin::layout::{self, format_duration_connected, html_escape};

// ── Page handler ───────────────────────────────────────────────────

pub async fn page(State(state): State<AdminState>) -> Html<String> {
    let mut stats = state.messaging.all_subscription_stats();
    stats.sort_by(|a, b| a.query_name.cmp(&b.query_name));
    let table_html = subscriptions_table_html(&stats);

    let content = format!(
        r##"<div class="flex flex-col flex-1" id="page-subscriptions">
  <div class="bg-k-surface border border-k-subtle rounded-lg overflow-hidden flex flex-col flex-1">
    <div class="flex items-center justify-between px-[18px] py-3 border-b border-k-subtle">
      <div class="text-[13px] font-semibold flex items-center gap-2">
        Subscription Queries
        <span class="font-mono text-[11px] bg-k-overlay px-[7px] py-px rounded-full text-k-text2">{count}</span>
      </div>
    </div>
    <div class="flex-1 overflow-auto" hx-get="/fragments/subscriptions" hx-trigger="every 5s" hx-swap="innerHTML">
      {table}
    </div>
  </div>
</div>"##,
        count = stats.len(),
        table = table_html,
    );

    Html(layout::layout(
        "subscriptions",
        "Subscriptions",
        &state.config.node_name,
        &state.contexts.list_contexts(),
        &content,
    ))
}

// ── Fragment ───────────────────────────────────────────────────────

pub async fn subscriptions_fragment(State(state): State<AdminState>) -> Html<String> {
    let mut stats = state.messaging.all_subscription_stats();
    stats.sort_by(|a, b| a.query_name.cmp(&b.query_name));
    Html(subscriptions_table_html(&stats))
}

// ── Helpers ─────────────────────────────────────────────────────────

fn subscriptions_table_html(subs: &[SubscriptionInfo]) -> String {
    if subs.is_empty() {
        return r#"<div class="text-center text-k-muted py-8 text-xs">No active subscription queries</div>"#.to_string();
    }
    let mut rows = String::new();
    for sub in subs {
        let duration = format_duration_connected(sub.since_opened);
        rows.push_str(&format!(
            r#"<tr>
  <td class="font-mono text-xs !text-k-text">{id}</td>
  <td class="font-mono text-xs">{query}</td>
  <td class="text-xs">{component} <span class="text-k-muted">({client})</span></td>
  <td class="text-xs">{handler}</td>
  <td class="text-right text-xs text-k-muted">{duration}</td>
  <td class="text-right"><span class="inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-mono bg-k-teal-d text-k-teal">active</span></td>
</tr>"#,
            id = html_escape(&sub.subscription_id),
            query = html_escape(&sub.query_name),
            component = html_escape(&sub.subscriber_component.0),
            client = html_escape(&sub.subscriber_client_id.0),
            handler = html_escape(&sub.handler_client_id.0),
        ));
    }
    format!(
        r#"<table><thead><tr><th>Subscription ID</th><th>Query Type</th><th>Subscriber</th><th>Handler</th><th class="text-right">Uptime</th><th class="text-right">Status</th></tr></thead><tbody>{rows}</tbody></table>"#
    )
}
