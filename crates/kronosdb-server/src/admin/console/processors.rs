use axum::extract::State;
use axum::response::Html;

use crate::admin::AdminState;
use crate::admin::layout::{self, format_number, html_escape};
use crate::processor::{ProcessorView, SegmentInfo};

// ── Page handler ───────────────────────────────────────────────────

pub async fn page(State(state): State<AdminState>) -> Html<String> {
    let views = state.processor_registry.list_aggregated();
    let table_html = processors_table_html(&views, &state);

    let content = format!(
        r##"<div class="flex flex-col flex-1" id="page-processors">
  <div class="card flex-1 gap-0 py-0 overflow-hidden">
    <header class="py-3 border-b border-k-subtle">
      <h2 class="text-[13px] font-semibold flex items-center gap-2">
        Event Processors
        <span class="badge font-mono" data-variant="secondary">{count}</span>
      </h2>
    </header>
    <div class="flex-1 overflow-auto" hx-get="/fragments/processors" hx-trigger="every 60s, sse-processors from:body" hx-swap="morph:innerHTML">
      {table}
    </div>
  </div>
</div>"##,
        count = views.len(),
        table = table_html,
    );

    Html(layout::layout(
        "processors",
        "Event Processors",
        &state.config.node_name,
        &state.contexts.list_contexts(),
        &content,
    ))
}

// ── Fragment ───────────────────────────────────────────────────────

pub async fn processors_fragment(State(state): State<AdminState>) -> Html<String> {
    let views = state.processor_registry.list_aggregated();
    Html(processors_table_html(&views, &state))
}

// ── Helpers ─────────────────────────────────────────────────────────

fn processors_table_html(views: &[ProcessorView], _state: &AdminState) -> String {
    if views.is_empty() {
        return r#"<div class="text-center text-k-muted py-8 text-xs">
  <div class="mb-2">No event processors registered</div>
  <div class="text-[11px]">Processor status will appear here when clients report their tracking tokens</div>
</div>"#.to_string();
    }

    let mut rows = String::new();
    for view in views {
        // Aggregate across all instances.
        let any_running = view.instances.iter().any(|i| i.running);
        let any_error = view.instances.iter().any(|i| i.error);

        // Collect all segments across instances.
        let all_segments: Vec<&SegmentInfo> = view
            .instances
            .iter()
            .flat_map(|i| i.segments.iter())
            .collect();

        // Compute overall position (min token_position across segments) and lag if possible.
        let min_position = all_segments
            .iter()
            .map(|s| s.token_position)
            .min()
            .unwrap_or(-1);
        let max_position = all_segments
            .iter()
            .map(|s| s.token_position)
            .max()
            .unwrap_or(-1);
        let all_caught_up = all_segments.iter().all(|s| s.caught_up);
        let any_replaying = all_segments.iter().any(|s| s.replaying);
        let any_seg_error = all_segments.iter().any(|s| !s.error_state.is_empty());

        // Status badge tint (k-* utilities override the badge variant colors).
        let (status_text, status_tint) = if !any_running {
            ("stopped", "text-k-muted bg-k-overlay")
        } else if any_error || any_seg_error {
            ("error", "text-k-red bg-k-red-d")
        } else if any_replaying {
            ("replaying", "text-k-amber bg-k-amber-d")
        } else if all_caught_up {
            ("caught up", "text-k-teal bg-k-teal-d")
        } else {
            ("behind", "text-k-amber bg-k-amber-d")
        };

        // Position display.
        let position_display = if min_position >= 0 {
            if min_position == max_position {
                format_number(min_position as u64)
            } else {
                format!(
                    "{} – {}",
                    format_number(min_position as u64),
                    format_number(max_position as u64)
                )
            }
        } else {
            "–".to_string()
        };

        // Segments display.
        let segments_display = if all_segments.is_empty() {
            "–".to_string()
        } else {
            format!("{}", all_segments.len())
        };

        // Instances count.
        let instances_count = view.instances.len();

        // Mode badge tint.
        let mode_tint = match view.mode.as_str() {
            "Tracking" => "text-k-blue bg-k-blue-d",
            "Subscribing" => "text-k-teal bg-k-teal-d",
            _ => "text-k-muted bg-k-overlay",
        };

        // Action buttons (for streaming processors).
        let actions = if view.is_streaming {
            let name = html_escape(&view.processor_name);
            if any_running {
                format!(
                    r#"<div class="flex gap-1.5 justify-end">
  <button class="btn" data-variant="outline" data-size="sm" onclick="processorAction('{name}','pause')">Pause</button>
  <button class="btn" data-variant="outline" data-size="sm" onclick="processorAction('{name}','split')">Split</button>
  <button class="btn" data-variant="outline" data-size="sm" onclick="processorAction('{name}','merge')">Merge</button>
</div>"#
                )
            } else {
                format!(
                    r#"<div class="flex gap-1.5 justify-end">
  <button class="btn" data-variant="primary" data-size="sm" onclick="processorAction('{name}','start')">Start</button>
</div>"#
                )
            }
        } else {
            String::new()
        };

        rows.push_str(&format!(
            r#"<tr id="proc-{name}">
  <td>
    <div class="font-mono text-xs !text-k-text">{name}</div>
    <div class="text-[11px] text-k-muted mt-0.5">{instances} instance{inst_s} · <span class="badge font-mono {mode_tint}" data-variant="secondary">{mode}</span></div>
  </td>
  <td class="text-xs font-mono">{segments}</td>
  <td class="text-right font-mono text-xs">{position}</td>
  <td class="text-right"><span class="badge font-mono {status_tint}" data-variant="secondary">{status}</span></td>
  <td class="text-right text-xs">{actions}</td>
</tr>"#,
            name = html_escape(&view.processor_name),
            instances = instances_count,
            inst_s = if instances_count == 1 { "" } else { "s" },
            mode = html_escape(&view.mode),
            segments = segments_display,
            position = position_display,
            status = status_text,
        ));
    }

    format!(
        r#"<table><thead><tr><th>Processor</th><th>Segments</th><th class="text-right">Position</th><th class="text-right">Status</th><th class="text-right">Actions</th></tr></thead><tbody>{rows}</tbody></table>
<script>
function processorAction(name, action) {{
  fetch('/api/processors/' + encodeURIComponent(name) + '/' + action, {{ method: 'POST' }})
    .then(r => r.json())
    .then(d => {{ if (!d.ok) alert(d.error || 'Failed'); }})
    .catch(() => alert('Request failed'));
}}
</script>"#
    )
}
