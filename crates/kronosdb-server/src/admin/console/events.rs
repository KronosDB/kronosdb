use axum::extract::{Query, State};
use axum::response::Html;
use serde::Deserialize;

use kronosdb_eventstore::criteria::{Criterion, SourcingCondition};
use kronosdb_eventstore::event::{Position, StoredEvent, Tag};

use crate::admin::AdminState;
use crate::admin::layout::{self, format_number, format_timestamp, html_escape, try_utf8_preview};

// ── Types ──────────────────────────────────────────────────────────

#[derive(Deserialize, Default)]
pub struct EventsQuery {
    pub context: Option<String>,
    /// Page number (1-based). Used to compute the position window.
    pub page: Option<u64>,
    pub limit: Option<usize>,
    /// Filter by event name (exact match for sourcing).
    pub name: Option<String>,
    /// Filter by tag key=value.
    pub tag: Option<String>,
}

const DEFAULT_EVENT_LIMIT: usize = 50;
const MAX_EVENT_LIMIT: usize = 200;

// ── Page handler ───────────────────────────────────────────────────

pub async fn page(
    State(state): State<AdminState>,
    Query(params): Query<EventsQuery>,
) -> Html<String> {
    let contexts = state.contexts.list_contexts();
    let selected = params.context.clone().unwrap_or_else(|| {
        contexts
            .first()
            .cloned()
            .unwrap_or_else(|| "default".into())
    });

    let initial_html = render_events(&state, &selected, &params);

    let mut context_options = String::new();
    for name in &contexts {
        let sel = if name == &selected { " selected" } else { "" };
        context_options.push_str(&format!(
            r#"<option value="{name}"{sel}>{name}</option>"#,
            name = html_escape(name),
        ));
    }

    let name_val = params.name.as_deref().unwrap_or("");
    let tag_val = params.tag.as_deref().unwrap_or("");

    let content = format!(
        r##"<div class="flex flex-col flex-1" id="page-events">
  <!-- Filter toolbar -->
  <div class="bg-k-surface border border-k-subtle rounded-lg overflow-hidden mb-4">
    <div class="flex items-center gap-2.5 px-[18px] py-2.5 flex-wrap">
      <span class="text-[11px] font-medium text-k-muted uppercase tracking-[0.5px]">Filter</span>
      <input id="ev-name" type="text" value="{name_val}" placeholder="Event name..."
        class="font-mono text-xs px-3 py-[7px] border border-k-border rounded-[5px] bg-k-base text-k-text outline-none focus:border-k-gold w-[180px]">
      <input id="ev-tag" type="text" value="{tag_val}" placeholder="tag=value"
        class="font-mono text-xs px-3 py-[7px] border border-k-border rounded-[5px] bg-k-base text-k-text outline-none focus:border-k-gold w-[180px]">
      <div class="w-px h-5 bg-k-border mx-1"></div>
      <span class="text-[11px] font-medium text-k-muted uppercase tracking-[0.5px]">Context</span>
      <select id="ev-context"
        class="font-mono text-xs px-2.5 py-[7px] border border-k-border rounded-[5px] bg-k-base text-k-text outline-none cursor-pointer">
        {context_options}
      </select>
      <div class="w-px h-5 bg-k-border mx-1"></div>
      <span class="text-[11px] font-medium text-k-muted uppercase tracking-[0.5px]">Limit</span>
      <select id="ev-limit"
        class="font-mono text-xs px-2.5 py-[7px] border border-k-border rounded-[5px] bg-k-base text-k-text outline-none cursor-pointer">
        <option value="25">25</option>
        <option value="50" selected>50</option>
        <option value="100">100</option>
        <option value="200">200</option>
      </select>
      <button onclick="evQuery(1)"
        class="ml-auto flex items-center gap-1.5 text-xs font-medium px-3 py-[7px] rounded-[5px] bg-k-gold text-k-inv border border-k-gold cursor-pointer hover:brightness-110 transition-all">
        <svg viewBox="0 0 16 16" width="12" height="12" fill="none" stroke="currentColor" stroke-width="2"><circle cx="7" cy="7" r="5"/><line x1="11" y1="11" x2="14" y2="14"/></svg>
        Query
      </button>
    </div>
  </div>

  <!-- Events results -->
  <div class="bg-k-surface border border-k-subtle rounded-lg overflow-hidden flex flex-col flex-1">
    <div id="events-results" class="flex-1 overflow-auto">
      {initial_html}
    </div>
  </div>
</div>

<script>
var evCurrentPage = 1;

// Live by default on page 1
var evLiveInterval = setInterval(function() {{ evQuery(1); }}, 3000);

function evBuildUrl(page) {{
  var p = new URLSearchParams();
  p.set('context', document.getElementById('ev-context').value);
  p.set('page', page);
  p.set('limit', document.getElementById('ev-limit').value);
  var name = document.getElementById('ev-name').value.trim();
  var tag = document.getElementById('ev-tag').value.trim();
  if (name) p.set('name', name);
  if (tag) p.set('tag', tag);
  return '/fragments/events?' + p.toString();
}}

function evQuery(page) {{
  evCurrentPage = page || 1;
  fetch(evBuildUrl(evCurrentPage))
    .then(r => r.text())
    .then(html => {{ document.getElementById('events-results').innerHTML = html; }});
}}

function evAddFilter(type, value) {{
  if (type === 'name') {{
    document.getElementById('ev-name').value = value;
  }} else if (type === 'tag') {{
    var el = document.getElementById('ev-tag');
    var current = el.value.trim();
    if (current && !current.includes(value)) {{
      el.value = current + ',' + value;
    }} else if (!current) {{
      el.value = value;
    }}
  }}
  evQuery(1);
}}

function evToggle(row) {{
  var detail = row.nextElementSibling;
  if (detail && detail.classList.contains('ev-detail')) {{
    var hidden = detail.classList.toggle('hidden');
    row.style.background = hidden ? '' : 'var(--c-hover)';
  }}
}}

function evSetLive(on) {{
  var btn = document.getElementById('ev-live-btn');
  if (!btn) return;
  if (on && !evLiveInterval) {{
    evLiveInterval = setInterval(function() {{ evQuery(1); }}, 3000);
  }} else if (!on && evLiveInterval) {{
    clearInterval(evLiveInterval);
    evLiveInterval = null;
  }}
  if (on) {{
    btn.classList.add('bg-k-teal', 'text-k-inv', 'border-k-teal');
    btn.classList.remove('bg-k-overlay', 'text-k-text2', 'border-k-border');
    btn.textContent = 'Live ●';
  }} else {{
    btn.classList.remove('bg-k-teal', 'text-k-inv', 'border-k-teal');
    btn.classList.add('bg-k-overlay', 'text-k-text2', 'border-k-border');
    btn.textContent = 'Live';
  }}
}}

function evToggleLive() {{
  evSetLive(!evLiveInterval);
}}

// Pause live when navigating away from page 1
function evPage(page) {{
  if (page !== 1) evSetLive(false);
  evQuery(page);
}}

// Set live button to active state on load
setTimeout(function() {{ evSetLive(true); }}, 0);

// Enter key triggers query
document.getElementById('ev-name')?.addEventListener('keydown', function(e) {{ if (e.key === 'Enter') evQuery(1); }});
document.getElementById('ev-tag')?.addEventListener('keydown', function(e) {{ if (e.key === 'Enter') evQuery(1); }});
</script>"##,
        name_val = html_escape(name_val),
        tag_val = html_escape(tag_val),
    );

    Html(layout::layout(
        "events",
        "Events",
        &state.config.node_name,
        &contexts,
        &content,
    ))
}

// ── Fragment ───────────────────────────────────────────────────────

pub async fn events_fragment(
    State(state): State<AdminState>,
    Query(params): Query<EventsQuery>,
) -> Html<String> {
    let contexts = state.contexts.list_contexts();
    let selected = params.context.clone().unwrap_or_else(|| {
        contexts
            .first()
            .cloned()
            .unwrap_or_else(|| "default".into())
    });
    Html(render_events(&state, &selected, &params))
}

// ── Core render ────────────────────────────────────────────────────

fn render_events(state: &AdminState, context: &str, params: &EventsQuery) -> String {
    let store = match state.contexts.get_context(context) {
        Ok(s) => s,
        Err(_) => return events_error_html(&format!("Context '{}' not found", context)),
    };

    let head = store.head().0;
    let tail = store.tail().0;
    let total_events = if head > tail { head - tail } else { 0 };

    if total_events == 0 {
        return r#"<div class="text-center text-k-muted py-8 text-xs">No events in this context</div>"#.to_string();
    }

    let limit = params
        .limit
        .unwrap_or(DEFAULT_EVENT_LIMIT)
        .min(MAX_EVENT_LIMIT);
    let page = params.page.unwrap_or(1).max(1);
    let condition = build_condition(params);

    // For newest-first: we need to compute the right window.
    // Fetch from tail, get all matching, then paginate in reverse.
    // For large stores this would be expensive, so instead we compute
    // the position window from the end.
    let has_filter = params.name.as_ref().is_some_and(|n| !n.is_empty())
        || params.tag.as_ref().is_some_and(|t| !t.is_empty());

    if has_filter {
        // With filters: fetch forward from tail, reverse for display, paginate.
        // We fetch more than needed to support pagination.
        let fetch_limit = limit * page as usize;
        let from = Position(tail.max(1));
        match store.source_stored(from, &condition, fetch_limit) {
            Ok(mut events) => {
                events.reverse(); // newest first
                let total_matching = events.len();
                let skip = (page as usize - 1) * limit;
                let page_events: Vec<StoredEvent> =
                    events.into_iter().skip(skip).take(limit).collect();
                let total_pages = (total_matching + limit - 1) / limit;
                events_table_html(
                    &page_events,
                    head,
                    total_matching as u64,
                    page,
                    total_pages as u64,
                    context,
                    params,
                )
            }
            Err(e) => events_error_html(&format!("Query failed: {e}")),
        }
    } else {
        // No filter: compute position window from the end for efficient pagination.
        let skip_from_end = (page - 1) * limit as u64;
        let from_pos = if head > skip_from_end + limit as u64 {
            head - skip_from_end - limit as u64
        } else {
            tail.max(1)
        };

        let fetch_count = if head > skip_from_end {
            let available = head - from_pos;
            (available as usize).min(limit)
        } else {
            0
        };

        if fetch_count == 0 {
            return r#"<div class="text-center text-k-muted py-8 text-xs">No events on this page</div>"#.to_string();
        }

        match store.source_stored(Position(from_pos), &condition, fetch_count) {
            Ok(mut events) => {
                events.reverse(); // newest first
                let total_pages = (total_events as usize + limit - 1) / limit;
                events_table_html(
                    &events,
                    head,
                    total_events,
                    page,
                    total_pages as u64,
                    context,
                    params,
                )
            }
            Err(e) => events_error_html(&format!("Query failed: {e}")),
        }
    }
}

// ── HTML rendering ─────────────────────────────────────────────────

fn events_table_html(
    events: &[StoredEvent],
    _head: u64,
    total_count: u64,
    current_page: u64,
    total_pages: u64,
    _context: &str,
    _params: &EventsQuery,
) -> String {
    if events.is_empty() {
        return r#"<div class="text-center text-k-muted py-8 text-xs">No events found matching your query</div>"#.to_string();
    }

    let mut rows = String::new();
    for event in events {
        // Main row (collapsed view)
        rows.push_str(&format!(
            r#"<tr class="cursor-pointer hover:bg-k-hover transition-colors" onclick="evToggle(this)">
  <td class="font-mono text-xs text-k-gold font-medium">{position}</td>
  <td class="font-mono text-xs !text-k-text">{name}</td>
  <td class="font-mono text-xs text-k-text2">{version}</td>
  <td class="font-mono text-xs text-k-text2">{timestamp}</td>
</tr>"#,
            position = format_number(event.position.0),
            name = html_escape(&event.name),
            version = html_escape(&event.version),
            timestamp = format_timestamp(event.timestamp),
        ));

        // Detail row (expanded, hidden by default)
        let payload_html = if event.payload.is_empty() {
            r#"<span class="text-k-muted italic">empty</span>"#.to_string()
        } else {
            let preview = try_utf8_preview(&event.payload, 4096);
            // Try to pretty-print JSON
            let formatted = try_pretty_json(&preview);
            format!(
                r#"<div class="bg-k-elevated border border-k-subtle rounded-[5px] p-3.5 font-mono text-xs leading-relaxed overflow-auto max-h-[300px] whitespace-pre">{}</div>"#,
                html_escape(&formatted),
            )
        };

        // Tags
        let mut tags_html = String::new();
        for tag in &event.tags {
            let key = String::from_utf8_lossy(&tag.key);
            let val = String::from_utf8_lossy(&tag.value);
            let tag_filter = format!("{}={}", key, val);
            tags_html.push_str(&format!(
                r#"<span class="inline-flex items-center gap-0.5 px-2 py-0.5 bg-k-overlay rounded text-[11px] font-mono text-k-text2 cursor-pointer hover:bg-k-hover transition-colors" onclick="event.stopPropagation();evAddFilter('tag','{filter}')"><span class="text-k-gold">{key}</span><span class="text-k-muted">=</span>{val}</span>"#,
                filter = html_escape(&tag_filter),
                key = html_escape(&key),
                val = html_escape(&val),
            ));
        }
        if tags_html.is_empty() {
            tags_html = r#"<span class="text-k-muted italic text-[11px]">none</span>"#.to_string();
        }

        // Metadata
        let mut meta_html = String::new();
        for (k, v) in &event.metadata {
            meta_html.push_str(&format!(
                r#"<div class="flex justify-between gap-4"><span class="font-mono text-k-muted">{key}</span><span class="font-mono text-k-text2 text-right break-all">{val}</span></div>"#,
                key = html_escape(k),
                val = html_escape(v),
            ));
        }
        if meta_html.is_empty() {
            meta_html = r#"<span class="text-k-muted italic text-[11px]">none</span>"#.to_string();
        }

        // Clickable event name filter
        let name_click = format!(
            r#"onclick="event.stopPropagation();evAddFilter('name','{}')""#,
            html_escape(&event.name),
        );

        rows.push_str(&format!(
            r#"<tr class="ev-detail hidden"><td colspan="4" class="!p-0">
  <div class="flex gap-4 p-4 bg-k-base">
    <div class="flex-1">
      <div class="text-[11px] font-semibold uppercase tracking-wider text-k-muted mb-2.5">Payload</div>
      {payload}
    </div>
    <div class="flex-1">
      <div class="text-[11px] font-semibold uppercase tracking-wider text-k-muted mb-2.5">Event Details</div>
      <div class="flex flex-col gap-1.5 text-xs">
        <div class="flex justify-between"><span class="text-k-muted font-medium">Position</span><span class="font-mono text-k-text2">{position}</span></div>
        <div class="flex justify-between"><span class="text-k-muted font-medium">Identifier</span><span class="font-mono text-k-text2">{identifier}</span></div>
        <div class="flex justify-between"><span class="text-k-muted font-medium">Name</span><span class="font-mono text-k-gold cursor-pointer hover:underline" {name_click}>{name}</span></div>
        <div class="flex justify-between"><span class="text-k-muted font-medium">Version</span><span class="font-mono text-k-text2">{version}</span></div>
        <div class="flex justify-between"><span class="text-k-muted font-medium">Timestamp</span><span class="font-mono text-k-text2">{timestamp}</span></div>
      </div>
      <div class="mt-3.5">
        <div class="text-[11px] font-semibold uppercase tracking-wider text-k-muted mb-2">Metadata</div>
        <div class="flex flex-col gap-1 text-xs">{metadata}</div>
      </div>
      <div class="mt-3.5">
        <div class="text-[11px] font-semibold uppercase tracking-wider text-k-muted mb-2">Tags</div>
        <div class="flex gap-1.5 flex-wrap">{tags}</div>
      </div>
    </div>
  </div>
</td></tr>"#,
            payload = payload_html,
            position = format_number(event.position.0),
            identifier = html_escape(&event.identifier),
            name = html_escape(&event.name),
            version = html_escape(&event.version),
            timestamp = format_timestamp(event.timestamp),
            metadata = meta_html,
            tags = tags_html,
        ));
    }

    // Pagination controls
    let pagination = if total_pages > 1 {
        let mut pages = String::new();

        // Previous
        if current_page > 1 {
            pages.push_str(&format!(
                r#"<button onclick="evPage({})" class="px-2.5 py-1 rounded text-xs font-mono bg-k-overlay text-k-text2 hover:bg-k-hover hover:text-k-text transition-colors cursor-pointer">&larr; Newer</button>"#,
                current_page - 1,
            ));
        }

        // Page numbers (show up to 7 pages around current)
        let start = (current_page as i64 - 3).max(1) as u64;
        let end = (current_page + 3).min(total_pages);
        for p in start..=end {
            if p == current_page {
                pages.push_str(&format!(
                    r#"<span class="px-2.5 py-1 rounded text-xs font-mono bg-k-gold-d text-k-gold">{p}</span>"#
                ));
            } else {
                pages.push_str(&format!(
                    r#"<button onclick="evPage({p})" class="px-2.5 py-1 rounded text-xs font-mono bg-k-overlay text-k-text2 hover:bg-k-hover hover:text-k-text transition-colors cursor-pointer">{p}</button>"#
                ));
            }
        }

        // Next
        if current_page < total_pages {
            pages.push_str(&format!(
                r#"<button onclick="evPage({})" class="px-2.5 py-1 rounded text-xs font-mono bg-k-overlay text-k-text2 hover:bg-k-hover hover:text-k-text transition-colors cursor-pointer">Older &rarr;</button>"#,
                current_page + 1,
            ));
        }

        format!(r#"<div class="flex items-center justify-center gap-1 py-3">{pages}</div>"#)
    } else {
        String::new()
    };

    let count = events.len();
    let first_pos = events
        .first()
        .map(|e| format_number(e.position.0))
        .unwrap_or_default();
    let last_pos = events
        .last()
        .map(|e| format_number(e.position.0))
        .unwrap_or_default();

    format!(
        r#"<div class="flex items-center justify-between px-[18px] py-2 border-b border-k-subtle">
  <div class="text-[11px] text-k-muted font-mono">
    Showing {count} events ({first_pos} – {last_pos}) · {total} total · page {current_page} of {total_pages}
  </div>
  <button id="ev-live-btn" onclick="evToggleLive()"
    class="px-2.5 py-1 rounded text-[11px] font-medium border bg-k-overlay text-k-text2 border-k-border cursor-pointer hover:bg-k-hover transition-colors">
    Live
  </button>
</div>
<table>
  <thead><tr>
    <th style="width:90px">Position</th>
    <th>Event</th>
    <th style="width:60px">Version</th>
    <th style="width:180px">Timestamp</th>
  </tr></thead>
  <tbody>{rows}</tbody>
</table>
{pagination}"#,
        total = format_number(total_count),
    )
}

// ── Helpers ─────────────────────────────────────────────────────────

pub fn build_condition(params: &EventsQuery) -> SourcingCondition {
    let names = match &params.name {
        Some(n) if !n.is_empty() => vec![n.clone()],
        _ => vec![],
    };

    let tags = match &params.tag {
        Some(t) if !t.is_empty() => t
            .split(',')
            .filter_map(|pair| {
                let (k, v) = pair.split_once('=')?;
                Some(Tag::from_str(k.trim(), v.trim()))
            })
            .collect(),
        _ => vec![],
    };

    if names.is_empty() && tags.is_empty() {
        return SourcingCondition {
            criteria: vec![Criterion {
                names: vec![],
                tags: vec![],
            }],
        };
    }

    SourcingCondition {
        criteria: vec![Criterion { names, tags }],
    }
}

fn events_error_html(message: &str) -> String {
    format!(
        r#"<div class="text-center text-k-red py-8 text-xs">{}</div>"#,
        html_escape(message),
    )
}

fn try_pretty_json(s: &str) -> String {
    if let Ok(val) = serde_json::from_str::<serde_json::Value>(s) {
        serde_json::to_string_pretty(&val).unwrap_or_else(|_| s.to_string())
    } else {
        s.to_string()
    }
}
