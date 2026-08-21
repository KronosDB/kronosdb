pub mod clients;
pub mod cluster;
pub mod commands;
pub mod contexts;
pub mod events;
pub mod overview;
pub mod processors;
pub mod queries;
pub mod settings;
pub mod subscriptions;

use serde::Deserialize;

use kronosdb_messaging::handler::{HandlerDetail, MessageTypeDetail};

use crate::admin::layout::{format_number, html_escape};

// ── Shared master/detail page helpers (commands + queries) ─────────
//
// The commands and queries pages are the same master-detail layout with a
// handful of differences (SSE topic, accent color, table columns). Both
// pages configure a `MessagePage` and share the builders below. The detail
// pane is server-rendered: master items fetch `/fragments/{topic}/detail`
// and the fragment root refreshes itself on the topic's SSE event.

/// Static configuration for a master-detail message page.
pub(crate) struct MessagePage {
    /// SSE topic and fragment path segment: "commands" | "queries".
    pub topic: &'static str,
    /// Short prefix for DOM ids: "cmd" | "qry".
    pub id_prefix: &'static str,
    /// DOM id of the detail pane that master items target.
    pub detail_target: &'static str,
    /// Card header title.
    pub title: &'static str,
    /// Copy shown when no handlers are registered.
    pub empty_copy: &'static str,
    /// k-* accent classes for the handler-count badge.
    pub badge_accent: &'static str,
    /// Whether the handlers table has a Load Factor column.
    pub show_load_factor: bool,
    /// Whether the detail header shows a dispatch-mode badge.
    pub show_mode_badge: bool,
}

/// Query params for the `/fragments/{topic}/detail` routes.
///
/// Handlers are keyed by (bus, name): the same message type can be
/// registered on several buses, so the name alone is ambiguous.
#[derive(Deserialize)]
pub(crate) struct DetailParams {
    pub name: String,
    #[serde(default)]
    pub bus: String,
}

/// Full page content: Basecoat card with master list (left, SSE-refreshed)
/// and detail pane (right, self-refreshing fragment for the first item).
pub(crate) fn message_page_html(cfg: &MessagePage, details: &[MessageTypeDetail]) -> String {
    let master = if details.is_empty() {
        empty_state_html(cfg.empty_copy)
    } else {
        master_items_html(cfg, details, true)
    };
    let detail = details
        .first()
        .map(|d| detail_fragment_html(cfg, Some(d), &d.name, &d.bus))
        .unwrap_or_default();

    // After the master list is re-rendered by SSE, re-apply the active
    // highlight to whichever item matches the currently open detail.
    let reselect = format!(
        "var p=document.getElementById('{target}').firstElementChild;if(p){{var u=p.getAttribute('hx-get');if(u){{var m=Array.prototype.find.call(this.children,function(c){{return c.getAttribute('hx-get')===u}});if(m)selectMaster(m)}}}}",
        target = cfg.detail_target,
    );

    format!(
        r##"<div class="flex flex-col flex-1" id="page-{topic}">
  <div class="card flex-1 min-h-0 gap-0 py-0 border border-k-subtle overflow-hidden" data-size="sm">
    <header class="py-3 border-b border-k-subtle">
      <h2 class="text-[13px] font-semibold flex items-center gap-2">{title} <span class="badge font-mono text-[11px]" data-variant="secondary">{count}</span></h2>
    </header>
    <section class="flex-1 min-h-0 overflow-hidden px-0">
      <div class="flex h-full min-h-0">
        <div id="{prefix}-master" class="w-[280px] min-w-[280px] border-r border-k-subtle overflow-y-auto" hx-get="/fragments/{topic}" hx-trigger="every 60s, sse-{topic} from:body" hx-swap="morph:innerHTML" hx-on::after-settle="{reselect}">
          {master}
        </div>
        <div id="{target}" class="flex-1 p-[18px] overflow-y-auto">
          {detail}
        </div>
      </div>
    </section>
  </div>
</div>"##,
        topic = cfg.topic,
        title = cfg.title,
        count = details.len(),
        prefix = cfg.id_prefix,
        target = cfg.detail_target,
    )
}

/// Master-list fragment: the items only, no active marker (the client
/// re-applies the highlight after the swap).
pub(crate) fn master_list_fragment_html(
    cfg: &MessagePage,
    details: &[MessageTypeDetail],
) -> String {
    if details.is_empty() {
        empty_state_html(cfg.empty_copy)
    } else {
        master_items_html(cfg, details, false)
    }
}

/// Detail-pane fragment: a self-refreshing root that re-fetches itself on
/// the topic's SSE event, so an open detail stays fresh.
pub(crate) fn detail_fragment_html(
    cfg: &MessagePage,
    detail: Option<&MessageTypeDetail>,
    name: &str,
    bus: &str,
) -> String {
    let body = match detail {
        Some(d) => detail_body_html(cfg, d),
        None => format!(
            r#"<div class="text-center text-k-muted py-8 text-xs">No handlers registered for <span class="font-mono">{}</span></div>"#,
            html_escape(name),
        ),
    };
    format!(
        r#"<div id="{prefix}-detail-root" hx-get="/fragments/{topic}/detail?name={enc}&bus={bus_enc}" hx-trigger="sse-{topic} from:body" hx-swap="morph:outerHTML">{body}</div>"#,
        prefix = cfg.id_prefix,
        topic = cfg.topic,
        enc = urlencode(name),
        bus_enc = urlencode(bus),
    )
}

fn empty_state_html(copy: &str) -> String {
    format!(
        r#"<div class="flex flex-col items-center justify-center h-full min-h-[240px] gap-1.5 py-12 text-center">
  <div class="text-k-muted text-xs">{copy}</div>
  <div class="text-[11px] text-k-muted opacity-60">Handlers appear here when clients register</div>
</div>"#
    )
}

fn master_items_html(cfg: &MessagePage, details: &[MessageTypeDetail], mark_first: bool) -> String {
    let mut html = String::new();
    for (i, d) in details.iter().enumerate() {
        let active = mark_first && i == 0;
        let active_cls = if active { " active" } else { "" };
        let name_cls = if active { "text-k-gold" } else { "text-k-text" };
        let plural = if d.handlers.len() == 1 { "" } else { "s" };
        let dispatched = if d.metrics.dispatched > 0 {
            format!(
                r#"<span class="font-mono text-[10px] text-k-muted ml-auto">{}</span>"#,
                format_number(d.metrics.dispatched)
            )
        } else {
            String::new()
        };
        // Only label rows with their bus when it isn't the default —
        // single-bus deployments shouldn't pay a noise tax.
        let bus_tag = if d.bus.is_empty() || d.bus == "default" {
            String::new()
        } else {
            format!(
                r#"<span class="font-mono text-[10px] text-k-muted">@{}</span>"#,
                html_escape(&d.bus)
            )
        };
        html.push_str(&format!(
            r##"<div id="{prefix}-row-{dom}" class="master-item flex items-center justify-between gap-2 px-4 py-2.5 border-b border-k-subtle cursor-pointer border-l-2 border-l-transparent hover:bg-k-hover transition-colors{active_cls}" onclick="selectMaster(this)" hx-get="/fragments/{topic}/detail?name={enc}&bus={bus_enc}" hx-target="#{target}" hx-swap="morph:innerHTML"><span class="mi-name font-mono text-xs {name_cls}">{name}</span>{bus_tag}<span class="font-mono text-[11px] text-k-muted ml-auto">{count} handler{plural}</span>{dispatched}</div>"##,
            prefix = cfg.id_prefix,
            dom = dom_id(&format!("{}-{}", d.bus, d.name)),
            topic = cfg.topic,
            enc = urlencode(&d.name),
            bus_enc = urlencode(&d.bus),
            target = cfg.detail_target,
            name = html_escape(&d.name),
            count = d.handlers.len(),
        ));
    }
    html
}

fn detail_body_html(cfg: &MessagePage, d: &MessageTypeDetail) -> String {
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

    let bus_badge = if d.bus.is_empty() || d.bus == "default" {
        String::new()
    } else {
        format!(
            r#" <span class="badge font-mono text-[11px]" data-variant="secondary">@{}</span>"#,
            html_escape(&d.bus)
        )
    };

    let mode_badge = if cfg.show_mode_badge {
        let (label, cls) = if d.handlers.len() > 1 {
            ("scatter-gather", "bg-k-teal-d text-k-teal")
        } else {
            ("point-to-point", "bg-k-blue-d text-k-blue")
        };
        format!(
            r#" <span class="badge font-mono text-[11px] {cls}" data-variant="secondary">{label}</span>"#
        )
    } else {
        String::new()
    };

    let mut html = format!(
        r#"<div class="flex items-baseline gap-3 mb-4">
  <h3 class="text-base font-semibold">{name}</h3>
  <span class="badge font-mono text-[11px] {accent}" data-variant="secondary">{count} handler{plural}</span>{bus_badge}{mode_badge}
</div>
<div class="flex gap-4 mb-5 flex-wrap">
  {cards}
</div>"#,
        name = html_escape(&d.name),
        accent = cfg.badge_accent,
        count = d.handlers.len(),
        cards = [
            metric_card_html("Dispatched", &format_number(m.dispatched), false),
            metric_card_html("Succeeded", &format_number(m.succeeded), false),
            metric_card_html("Failed", &format_number(m.failed), m.failed > 0),
            metric_card_html("Success Rate", &rate, false),
            metric_card_html("Avg Latency", &avg_ms, false),
            metric_card_html("No Handler", &format_number(m.no_handler), m.no_handler > 0),
            metric_card_html("No Permits", &format_number(m.no_permits), m.no_permits > 0),
        ]
        .join("\n  "),
    );

    if !d.handlers.is_empty() {
        html.push_str(r#"<div class="text-[11px] font-semibold uppercase tracking-wider text-k-muted mb-2.5">Registered Handlers</div>"#);
        if cfg.show_load_factor {
            html.push_str("<table><thead><tr><th>Component</th><th>Client ID</th><th>Load Factor</th><th class=\"text-right\">Permits</th></tr></thead><tbody>");
        } else {
            html.push_str("<table><thead><tr><th>Component</th><th>Client ID</th><th class=\"text-right\">Permits</th></tr></thead><tbody>");
        }
        for h in &d.handlers {
            html.push_str(&handler_row_html(cfg, h));
        }
        html.push_str("</tbody></table>");
    }

    html
}

fn metric_card_html(label: &str, value: &str, warn: bool) -> String {
    let cls = if warn { " text-k-red" } else { "" };
    format!(
        r#"<div class="card bg-k-elevated border border-k-subtle px-3.5 py-2.5 gap-1" data-size="sm"><div class="text-[10px] font-semibold uppercase tracking-[0.5px] text-k-muted">{label}</div><div class="font-mono text-base font-semibold{cls}">{value}</div></div>"#,
    )
}

fn handler_row_html(cfg: &MessagePage, h: &HandlerDetail) -> String {
    let load_factor = if cfg.show_load_factor {
        format!(r#"<td class="font-mono text-xs">{}</td>"#, h.load_factor)
    } else {
        String::new()
    };
    format!(
        r#"<tr><td class="!text-k-text">{component}</td><td class="font-mono text-xs">{client}</td>{load_factor}<td class="font-mono text-xs text-right">{permits}</td></tr>"#,
        component = html_escape(&h.component_name),
        client = html_escape(&h.client_id),
        permits = h.available_permits,
    )
}

/// Percent-encodes a string for use in a URL query value.
pub(crate) fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Sanitizes a message-type name into a stable DOM id fragment.
pub(crate) fn dom_id(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect()
}
