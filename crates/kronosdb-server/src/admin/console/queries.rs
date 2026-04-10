use axum::extract::State;
use axum::response::Html;

use kronosdb_messaging::handler::{HandlerDetail, MessageTypeDetail, MetricsSnapshot};

use crate::admin::AdminState;
use crate::admin::layout::{self, format_number, html_escape};

// ── Page handler ───────────────────────────────────────────────────

pub async fn page(State(state): State<AdminState>) -> Html<String> {
    let mut details = state.messaging.all_query_details();
    details.sort_by(|a, b| a.name.cmp(&b.name));

    let content = queries_page_html(&details);

    Html(layout::layout(
        "queries",
        "Queries",
        &state.config.node_name,
        &state.contexts.list_contexts(),
        &content,
    ))
}

// ── Fragment ───────────────────────────────────────────────────────

pub async fn queries_fragment(State(state): State<AdminState>) -> Html<String> {
    let mut details = state.messaging.all_query_details();
    details.sort_by(|a, b| a.name.cmp(&b.name));
    Html(queries_inner_html(&details))
}

// ── Page HTML ─────────────────────────────────────────────────────

fn queries_page_html(details: &[MessageTypeDetail]) -> String {
    format!(
        r##"<div class="flex flex-col flex-1" id="page-queries">
  <div class="bg-k-surface border border-k-subtle rounded-lg overflow-hidden flex flex-col flex-1">
    <div class="flex items-center justify-between px-[18px] py-3 border-b border-k-subtle">
      <div class="text-[13px] font-semibold flex items-center gap-2">
        Registered Query Handlers
        <span class="font-mono text-[11px] bg-k-overlay px-[7px] py-px rounded-full text-k-text2">{count}</span>
      </div>
    </div>
    <div class="flex-1 overflow-hidden" hx-get="/fragments/queries" hx-trigger="every 5s" hx-swap="innerHTML">
      {inner}
    </div>
  </div>
</div>"##,
        count = details.len(),
        inner = queries_inner_html(details),
    )
}

fn queries_inner_html(details: &[MessageTypeDetail]) -> String {
    if details.is_empty() {
        return r#"<div class="text-center text-k-muted py-8 text-xs">No query handlers registered</div>"#.to_string();
    }

    let master_items = master_list_html(details);
    let detail_pane = detail_pane_html(&details[0]);

    format!(
        r##"<div class="flex flex-1 min-h-0 h-full">
  <div class="w-[280px] min-w-[280px] border-r border-k-subtle overflow-y-auto" id="qry-master-list">
    {master_items}
  </div>
  <div class="flex-1 p-[18px] overflow-y-auto" id="qry-detail-pane">
    {detail_pane}
  </div>
</div>
<script>
(function(){{
  const details = {details_json};
  const masterList = document.getElementById('qry-master-list');
  const detailPane = document.getElementById('qry-detail-pane');
  if (!masterList || !detailPane) return;
  masterList.addEventListener('click', function(e) {{
    const item = e.target.closest('[data-qry]');
    if (!item) return;
    masterList.querySelectorAll('[data-qry]').forEach(function(i) {{
      i.classList.remove('active');
      i.style.background = '';
      i.style.borderLeftColor = 'transparent';
      const n = i.querySelector('.mi-name');
      if (n) {{ n.classList.remove('text-k-blue'); n.classList.add('text-k-text'); }}
    }});
    item.classList.add('active');
    item.style.background = 'var(--c-blue-dim)';
    item.style.borderLeftColor = 'var(--c-blue)';
    const n = item.querySelector('.mi-name');
    if (n) {{ n.classList.add('text-k-blue'); n.classList.remove('text-k-text'); }}
    const name = item.getAttribute('data-qry');
    const d = details.find(function(x) {{ return x.name === name; }});
    if (d) detailPane.innerHTML = renderDetail(d);
  }});
  function renderDetail(d) {{
    var m = d.metrics;
    var avgMs = m.avg_duration_us > 0 ? (m.avg_duration_us / 1000).toFixed(1) + 'ms' : '-';
    var rate = m.dispatched > 0 ? m.success_rate.toFixed(1) + '%' : '-';
    var mode = d.handlers.length > 1 ? 'scatter-gather' : 'point-to-point';
    var modeColor = d.handlers.length > 1 ? 'bg-k-teal-d text-k-teal' : 'bg-k-blue-d text-k-blue';
    var html = '<div class="flex items-baseline gap-3 mb-4">';
    html += '<h3 class="text-base font-semibold">' + esc(d.name) + '</h3>';
    html += '<span class="inline-flex px-2 py-0.5 rounded-full text-[11px] font-mono bg-k-blue-d text-k-blue">' + d.handlers.length + ' handler' + (d.handlers.length !== 1 ? 's' : '') + '</span>';
    html += '<span class="inline-flex px-2 py-0.5 rounded-full text-[11px] font-mono ' + modeColor + '">' + mode + '</span>';
    html += '</div>';
    html += '<div class="flex gap-4 mb-5 flex-wrap">';
    html += metricCard('Dispatched', fmtNum(m.dispatched));
    html += metricCard('Succeeded', fmtNum(m.succeeded));
    html += metricCard('Failed', fmtNum(m.failed), m.failed > 0);
    html += metricCard('Success Rate', rate);
    html += metricCard('Avg Latency', avgMs);
    html += metricCard('No Handler', fmtNum(m.no_handler), m.no_handler > 0);
    html += metricCard('No Permits', fmtNum(m.no_permits), m.no_permits > 0);
    html += '</div>';
    if (d.handlers.length > 0) {{
      html += '<div class="text-[11px] font-semibold uppercase tracking-wider text-k-muted mb-2.5">Registered Handlers</div>';
      html += '<table><thead><tr><th>Component</th><th>Client ID</th><th class="text-right">Permits</th></tr></thead><tbody>';
      d.handlers.forEach(function(h) {{
        html += '<tr>';
        html += '<td class="!text-k-text">' + esc(h.component_name) + '</td>';
        html += '<td class="font-mono text-xs">' + esc(h.client_id) + '</td>';
        html += '<td class="font-mono text-xs text-right">' + h.available_permits + '</td>';
        html += '</tr>';
      }});
      html += '</tbody></table>';
    }}
    return html;
  }}
  function metricCard(label, value, warn) {{
    var cls = warn ? 'text-k-red' : '';
    return '<div class="bg-k-elevated border border-k-subtle rounded-[5px] px-3.5 py-2.5">' +
      '<div class="text-[10px] font-semibold uppercase tracking-[0.5px] text-k-muted mb-1">' + label + '</div>' +
      '<div class="font-mono text-base font-semibold ' + cls + '">' + value + '</div></div>';
  }}
  function fmtNum(n) {{ return n >= 1000000 ? (n/1000000).toFixed(2)+'M' : n >= 10000 ? (n/1000).toFixed(1)+'K' : n.toLocaleString(); }}
  function esc(s) {{ var d = document.createElement('div'); d.textContent = s; return d.innerHTML; }}
}})();
</script>"##,
        master_items = master_items,
        detail_pane = detail_pane,
        details_json = details_to_json(details),
    )
}

fn master_list_html(details: &[MessageTypeDetail]) -> String {
    let mut html = String::new();
    for (i, d) in details.iter().enumerate() {
        let is_active = i == 0;
        let active_cls = if is_active {
            r#" active" style="background:var(--c-blue-dim);border-left-color:var(--c-blue)"#
        } else {
            r#" hover:bg-k-hover transition-colors" style="border-left-color:transparent"#
        };
        let name_cls = if is_active {
            "text-k-blue"
        } else {
            "text-k-text"
        };
        let plural = if d.handlers.len() == 1 { "" } else { "s" };
        let dispatched = if d.metrics.dispatched > 0 {
            format!(
                r#" <span class="font-mono text-[10px] text-k-muted ml-auto">{}</span>"#,
                format_number(d.metrics.dispatched)
            )
        } else {
            String::new()
        };
        html.push_str(&format!(
            r#"<div data-qry="{name}" class="flex items-center justify-between px-4 py-2.5 border-b border-k-subtle cursor-pointer border-l-2{active_cls}"><span class="mi-name font-mono text-xs {name_cls}">{name}</span><span class="font-mono text-[11px] text-k-muted">{count} handler{plural}</span>{dispatched}</div>"#,
            name = html_escape(&d.name),
            count = d.handlers.len(),
        ));
    }
    html
}

fn detail_pane_html(d: &MessageTypeDetail) -> String {
    let m = &d.metrics;
    let avg_ms = if m.avg_duration_us > 0 {
        format!("{:.1}ms", m.avg_duration_us as f64 / 1000.0)
    } else {
        "-".to_string()
    };
    let rate = if m.dispatched > 0 {
        format!("{:.1}%", m.success_rate)
    } else {
        "-".to_string()
    };
    let plural = if d.handlers.len() == 1 { "" } else { "s" };
    let (mode_label, mode_cls) = if d.handlers.len() > 1 {
        ("scatter-gather", "bg-k-teal-d text-k-teal")
    } else {
        ("point-to-point", "bg-k-blue-d text-k-blue")
    };

    let mut html = format!(
        r#"<div class="flex items-baseline gap-3 mb-4">
  <h3 class="text-base font-semibold">{name}</h3>
  <span class="inline-flex px-2 py-0.5 rounded-full text-[11px] font-mono bg-k-blue-d text-k-blue">{count} handler{plural}</span>
  <span class="inline-flex px-2 py-0.5 rounded-full text-[11px] font-mono {mode_cls}">{mode_label}</span>
</div>
<div class="flex gap-4 mb-5 flex-wrap">
  {cards}
</div>"#,
        name = html_escape(&d.name),
        count = d.handlers.len(),
        cards = [
            metric_card("Dispatched", &format_number(m.dispatched), false),
            metric_card("Succeeded", &format_number(m.succeeded), false),
            metric_card("Failed", &format_number(m.failed), m.failed > 0),
            metric_card("Success Rate", &rate, false),
            metric_card("Avg Latency", &avg_ms, false),
            metric_card("No Handler", &format_number(m.no_handler), m.no_handler > 0),
            metric_card("No Permits", &format_number(m.no_permits), m.no_permits > 0),
        ]
        .join("\n  "),
    );

    if !d.handlers.is_empty() {
        html.push_str(r#"<div class="text-[11px] font-semibold uppercase tracking-wider text-k-muted mb-2.5">Registered Handlers</div>"#);
        html.push_str("<table><thead><tr><th>Component</th><th>Client ID</th><th class=\"text-right\">Permits</th></tr></thead><tbody>");
        for h in &d.handlers {
            html.push_str(&handler_row_html(h));
        }
        html.push_str("</tbody></table>");
    }

    html
}

fn metric_card(label: &str, value: &str, warn: bool) -> String {
    let cls = if warn { " text-k-red" } else { "" };
    format!(
        r#"<div class="bg-k-elevated border border-k-subtle rounded-[5px] px-3.5 py-2.5"><div class="text-[10px] font-semibold uppercase tracking-[0.5px] text-k-muted mb-1">{label}</div><div class="font-mono text-base font-semibold{cls}">{value}</div></div>"#,
    )
}

fn handler_row_html(h: &HandlerDetail) -> String {
    format!(
        r#"<tr><td class="!text-k-text">{component}</td><td class="font-mono text-xs">{client}</td><td class="font-mono text-xs text-right">{permits}</td></tr>"#,
        component = html_escape(&h.component_name),
        client = html_escape(&h.client_id),
        permits = h.available_permits,
    )
}

fn details_to_json(details: &[MessageTypeDetail]) -> String {
    let mut json = String::from("[");
    for (i, d) in details.iter().enumerate() {
        if i > 0 {
            json.push(',');
        }
        json.push_str(&format!(
            r#"{{"name":"{}","handlers":[{}],"metrics":{}}}"#,
            json_escape(&d.name),
            d.handlers.iter().enumerate().map(|(j, h)| {
                format!(
                    r#"{}{{"client_id":"{}","component_name":"{}","load_factor":{},"available_permits":{}}}"#,
                    if j > 0 { "," } else { "" },
                    json_escape(&h.client_id),
                    json_escape(&h.component_name),
                    h.load_factor,
                    h.available_permits,
                )
            }).collect::<String>(),
            metrics_to_json(&d.metrics),
        ));
    }
    json.push(']');
    json
}

fn metrics_to_json(m: &MetricsSnapshot) -> String {
    format!(
        r#"{{"dispatched":{},"succeeded":{},"failed":{},"no_handler":{},"no_permits":{},"avg_duration_us":{},"success_rate":{:.1}}}"#,
        m.dispatched,
        m.succeeded,
        m.failed,
        m.no_handler,
        m.no_permits,
        m.avg_duration_us,
        m.success_rate,
    )
}

fn json_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
