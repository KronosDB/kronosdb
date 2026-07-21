use std::time::Duration;

// ── Helpers ────────────────────────────────────────────────────────

pub fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

pub fn format_uptime(d: Duration) -> String {
    let secs = d.as_secs();
    let days = secs / 86400;
    let hours = (secs % 86400) / 3600;
    let mins = (secs % 3600) / 60;
    let s = secs % 60;
    if days > 0 {
        format!("{days}d {hours}h {mins}m {s}s")
    } else if hours > 0 {
        format!("{hours}h {mins}m {s}s")
    } else if mins > 0 {
        format!("{mins}m {s}s")
    } else {
        format!("{s}s")
    }
}

pub fn format_uptime_short(d: Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        format!("{secs}s ago")
    } else if secs < 3600 {
        format!("{}m {}s ago", secs / 60, secs % 60)
    } else if secs < 86400 {
        format!("{}h {}m ago", secs / 3600, (secs % 3600) / 60)
    } else {
        format!("{}d {}h ago", secs / 86400, (secs % 86400) / 3600)
    }
}

pub fn format_duration_connected(d: Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else if secs < 86400 {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    } else {
        format!(
            "{}d {}h {}m",
            secs / 86400,
            (secs % 86400) / 3600,
            (secs % 3600) / 60
        )
    }
}

pub fn format_number(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.2}M", n as f64 / 1_000_000.0)
    } else if n >= 10_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        // Insert commas
        let s = n.to_string();
        let mut result = String::new();
        for (i, ch) in s.chars().rev().enumerate() {
            if i > 0 && i % 3 == 0 {
                result.push(',');
            }
            result.push(ch);
        }
        result.chars().rev().collect()
    }
}

pub fn format_bytes(bytes: u64) -> String {
    if bytes >= 1024 * 1024 * 1024 {
        format!("{:.1} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    } else if bytes >= 1024 * 1024 {
        format!("{:.0} MB", bytes as f64 / (1024.0 * 1024.0))
    } else if bytes >= 1024 {
        format!("{:.0} KB", bytes as f64 / 1024.0)
    } else {
        format!("{bytes} B")
    }
}

pub fn format_timestamp(epoch_ms: i64) -> String {
    let secs = epoch_ms / 1000;
    let millis = (epoch_ms % 1000).unsigned_abs();
    let (y, m, d, h, min, s) = epoch_to_datetime(secs);
    format!("{y:04}-{m:02}-{d:02} {h:02}:{min:02}:{s:02}.{millis:03}")
}

fn epoch_to_datetime(epoch: i64) -> (i64, u32, u32, u32, u32, u32) {
    let s = epoch.rem_euclid(86400) as u32;
    let h = s / 3600;
    let min = (s % 3600) / 60;
    let sec = s % 60;
    let days = epoch.div_euclid(86400);
    let (y, m, d) = days_to_ymd(days + 719_468);
    (y, m, d, h, min, sec)
}

fn days_to_ymd(g: i64) -> (i64, u32, u32) {
    let era = g.div_euclid(146_097);
    let doe = g.rem_euclid(146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = (yoe as i64) + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

// ── Shared layout ──────────────────────────────────────────────────

/// Wraps page content in the full HTML shell (sidebar, topbar, scripts).
/// `active_page` should match the data-page attribute (e.g. "overview", "events").
/// `title` is shown in the topbar.
/// `content` is the inner HTML for the page area.
pub fn layout(
    active_page: &str,
    title: &str,
    node_name: &str,
    contexts: &[String],
    content: &str,
) -> String {
    let nav_items = nav_items(active_page);
    let context_options = context_dropdown_items(contexts);

    format!(
        r##"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>KronosDB — {title}</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 16 16'><rect width='16' height='16' rx='3' fill='%2308080b'/><path d='M4 3v10M4 8l5-5M4.5 8.5 10 13' stroke='%23c8a44e' stroke-width='1.8' fill='none' stroke-linecap='round'/></svg>">
<link rel="stylesheet" href="/static/css/app.min.css">
<script src="/static/js/htmx.min.js"></script>
<script src="/static/js/idiomorph-ext.min.js"></script>
<script src="/static/js/basecoat.min.js" defer></script>
</head>
<body class="bg-k-base text-k-text font-sans text-sm antialiased overflow-hidden h-screen" hx-ext="morph">
<div class="flex h-screen">

  <!-- Sidebar -->
  <aside class="w-[210px] min-w-[210px] bg-k-surface border-r border-k-subtle flex flex-col">
    <div class="px-4 pt-[18px] pb-3.5 border-b border-k-subtle">
      <a href="/overview" class="font-semibold text-xl tracking-[3px] uppercase text-k-text no-underline">Kronos<span class="text-k-gold">DB</span></a>
    </div>
    <div class="brand-pulse"></div>
    <nav class="flex-1 px-2 py-2.5 flex flex-col gap-px overflow-y-auto">
      {nav_items}
    </nav>
    <div class="px-4 py-3 border-t border-k-subtle font-mono text-[11px] text-k-muted flex items-center gap-1.5">
      <div class="w-1.5 h-1.5 rounded-full bg-k-gold opacity-70"></div>
      v{version}
    </div>
  </aside>

  <!-- Main -->
  <div class="flex-1 flex flex-col overflow-hidden">
    <div class="h-[50px] min-h-[50px] bg-k-surface border-b border-k-subtle flex items-center justify-between px-6">
      <div class="text-[15px] font-medium">{title}</div>
      <div class="flex items-center gap-4">
        <div class="flex items-center gap-2">
          <span class="text-[11px] font-medium uppercase tracking-[0.5px] text-k-muted">Context</span>
          <div class="ctx-dropdown relative inline-flex" id="context-dropdown">
            <div class="dd-arrow font-mono text-xs px-2.5 py-1.5 pr-7 border border-k-border rounded-[5px] bg-k-base text-k-text cursor-pointer whitespace-nowrap relative select-none" onclick="toggleDropdown('context-dropdown')">All Contexts</div>
            <div class="ctx-dropdown-menu hidden absolute top-[calc(100%+4px)] right-0 min-w-full bg-k-surface border border-k-border rounded-[5px] shadow-lg z-50 p-1 max-h-60 overflow-y-auto">
              <button class="ctx-dropdown-item active block w-full px-2.5 py-1.5 font-mono text-xs text-k-gold bg-k-gold-d border-none rounded-[3px] cursor-pointer text-left whitespace-nowrap" onclick="selectContext('all','All Contexts')">All Contexts</button>
              {context_options}
            </div>
          </div>
        </div>
        <button class="theme-toggle p-1.5 rounded-[5px] bg-transparent border-none text-k-text2 cursor-pointer hover:bg-k-hover transition-colors" onclick="toggleTheme()">
          <svg class="icon-sun" viewBox="0 0 18 18" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="9" cy="9" r="4"/><path d="M9 1v2M9 15v2M1 9h2M15 9h2M3.3 3.3l1.4 1.4M13.3 13.3l1.4 1.4M14.7 3.3l-1.4 1.4M4.7 13.3l-1.4 1.4"/></svg>
          <svg class="icon-moon" viewBox="0 0 18 18" width="16" height="16" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M15.1 10.4A7 7 0 0 1 7.6 2.9 7 7 0 1 0 15.1 10.4z"/></svg>
        </button>
        <div class="flex items-center gap-2 font-mono text-xs text-k-text2 bg-k-elevated px-3 py-1.5 rounded-[5px] border border-k-subtle">
          <div class="w-[7px] h-[7px] rounded-full bg-k-gold opacity-80"></div>
          {node_name}
        </div>
      </div>
    </div>
    <div class="h-0.5 bg-k-base relative overflow-hidden"><div class="pulse-bar absolute inset-0"></div></div>

    <div class="flex-1 overflow-y-auto p-6 flex flex-col">
      {content}
    </div>
  </div>
</div>

<!-- Chart tooltip -->
<div class="chart-tooltip bg-k-surface border border-k-border rounded-[5px] px-2.5 py-1.5 font-mono text-[11px] shadow-lg" id="chart-tooltip"></div>

{SHARED_JS}
</body>
</html>"##,
        title = html_escape(title),
        nav_items = nav_items,
        context_options = context_options,
        node_name = html_escape(node_name),
        version = env!("CARGO_PKG_VERSION"),
        content = content,
        SHARED_JS = SHARED_JS,
    )
}

fn nav_items(active: &str) -> String {
    let sections = [
        (
            "Monitor",
            vec![
                ("overview", "Overview", ICON_OVERVIEW),
                ("contexts", "Contexts", ICON_CONTEXTS),
                ("clients", "Clients", ICON_CLIENTS),
                ("events", "Events", ICON_EVENTS),
            ],
        ),
        (
            "Messaging",
            vec![
                ("commands", "Commands", ICON_COMMANDS),
                ("queries", "Queries", ICON_QUERIES),
                ("subscriptions", "Subscriptions", ICON_SUBSCRIPTIONS),
            ],
        ),
        (
            "Processing",
            vec![("processors", "Event Processors", ICON_PROCESSORS)],
        ),
        (
            "Infrastructure",
            vec![
                ("cluster", "Cluster", ICON_CLUSTER),
                ("settings", "Settings", ICON_SETTINGS),
            ],
        ),
    ];

    let mut html = String::new();
    for (label, items) in &sections {
        html.push_str(&format!(
            r#"<div class="text-[10px] font-semibold tracking-[1.1px] uppercase text-k-muted px-3 pt-3.5 pb-1.5">{label}</div>"#
        ));
        for (page, name, icon) in items {
            let is_active = *page == active;
            let cls = if is_active {
                "bg-k-gold-d text-k-gold"
            } else {
                "text-k-text2 hover:bg-k-hover hover:text-k-text"
            };
            let icon_cls = if is_active {
                "text-k-gold"
            } else {
                "text-k-muted"
            };
            html.push_str(&format!(
                r#"<a href="/{page}" class="flex items-center gap-[9px] px-3 py-2 rounded-[5px] text-[13px] font-[450] w-full no-underline transition-colors {cls}"><svg class="w-[17px] h-[17px] shrink-0 {icon_cls}" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.5">{icon}</svg>{name}</a>"#
            ));
        }
    }
    html
}

fn context_dropdown_items(contexts: &[String]) -> String {
    contexts
        .iter()
        .map(|c| {
            format!(
                r#"<button class="ctx-dropdown-item block w-full px-2.5 py-1.5 font-mono text-xs text-k-text2 bg-transparent border-none rounded-[3px] cursor-pointer text-left whitespace-nowrap hover:bg-k-hover hover:text-k-text transition-colors" onclick="selectContext('{c}','{c}')">{c}</button>"#,
                c = html_escape(c),
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

// ── Nav icons (SVG paths) ──────────────────────────────────────────

const ICON_OVERVIEW: &str = r#"<rect x="2" y="2" width="7" height="7" rx="1.5"/><rect x="11" y="2" width="7" height="7" rx="1.5"/><rect x="2" y="11" width="7" height="7" rx="1.5"/><rect x="11" y="11" width="7" height="7" rx="1.5"/>"#;
const ICON_CONTEXTS: &str = r#"<rect x="3" y="3" width="14" height="4" rx="1"/><rect x="3" y="9" width="14" height="4" rx="1"/><rect x="3" y="15" width="14" height="4" rx="1"/>"#;
const ICON_CLIENTS: &str =
    r#"<circle cx="10" cy="6" r="3"/><path d="M3 18c0-3.9 3.1-7 7-7s7 3.1 7 7"/>"#;
const ICON_EVENTS: &str = r#"<path d="M4 4h12M4 8h12M4 12h9M4 16h6"/>"#;
const ICON_COMMANDS: &str =
    r#"<path d="M5 10l4 4 6-8"/><rect x="2" y="2" width="16" height="16" rx="2"/>"#;
const ICON_QUERIES: &str =
    r#"<circle cx="8" cy="8" r="5"/><line x1="12" y1="12" x2="17" y2="17"/>"#;
const ICON_SUBSCRIPTIONS: &str = r#"<path d="M4 10h4l2-4 3 8 2-4h4"/>"#;
const ICON_PROCESSORS: &str =
    r#"<path d="M3 4h14M3 8h10M3 12h12M3 16h8"/><circle cx="16" cy="12" r="2.5"/>"#;
const ICON_CLUSTER: &str = r#"<circle cx="10" cy="4" r="2.5"/><circle cx="4" cy="16" r="2.5"/><circle cx="16" cy="16" r="2.5"/><line x1="10" y1="6.5" x2="4" y2="13.5"/><line x1="10" y1="6.5" x2="16" y2="13.5"/><line x1="6.5" y1="16" x2="13.5" y2="16"/>"#;
const ICON_SETTINGS: &str = r#"<circle cx="10" cy="10" r="2.5"/><path d="M10 2v2.5M10 15.5V18M2 10h2.5M15.5 10H18M4.2 4.2l1.8 1.8M14 14l1.8 1.8M15.8 4.2L14 6M6 14l-1.8 1.8"/>"#;

// ── Shared JS ──────────────────────────────────────────────────────

const SHARED_JS: &str = r##"<script>
// Theme
function toggleTheme(){var t=document.documentElement.getAttribute('data-theme')==='light'?null:'light';t?document.documentElement.setAttribute('data-theme','light'):document.documentElement.removeAttribute('data-theme');localStorage.setItem('kronosdb-theme',t||'dark')}
(function(){if(localStorage.getItem('kronosdb-theme')==='light')document.documentElement.setAttribute('data-theme','light')})();

// Dropdowns
function toggleDropdown(id){var dd=document.getElementById(id),w=dd.classList.contains('open');document.querySelectorAll('.ctx-dropdown.open').forEach(function(d){d.classList.remove('open');d.querySelector('.ctx-dropdown-menu').classList.add('hidden')});if(!w){dd.classList.add('open');dd.querySelector('.ctx-dropdown-menu').classList.remove('hidden')}}
function selectContext(val,label){var dd=document.getElementById('context-dropdown');dd.querySelector('.dd-arrow').textContent=label;dd.querySelectorAll('.ctx-dropdown-item').forEach(function(i){var a=i.textContent.trim()===label;i.className='ctx-dropdown-item block w-full px-2.5 py-1.5 font-mono text-xs border-none rounded-[3px] cursor-pointer text-left whitespace-nowrap transition-colors '+(a?'active text-k-gold bg-k-gold-d':'text-k-text2 bg-transparent hover:bg-k-hover hover:text-k-text')});dd.classList.remove('open');dd.querySelector('.ctx-dropdown-menu').classList.add('hidden')}
document.addEventListener('click',function(e){if(!e.target.closest('.ctx-dropdown')){document.querySelectorAll('.ctx-dropdown.open').forEach(function(d){d.classList.remove('open');d.querySelector('.ctx-dropdown-menu').classList.add('hidden')})}});

// Chart tooltip
var tooltip=document.getElementById('chart-tooltip');
function showTooltip(bar,label,value){var r=bar.getBoundingClientRect();tooltip.innerHTML='<div class="text-k-gold font-semibold">'+value+'</div><div class="text-k-muted text-[10px]">'+label+'</div>';tooltip.style.display='block';tooltip.style.left=(r.left+r.width/2)+'px';tooltip.style.top=(r.top-tooltip.offsetHeight-8)+'px'}
function hideTooltip(){tooltip.style.display='none'}

// Master-detail
function selectMaster(el){el.parentElement.querySelectorAll('.master-item').forEach(function(i){i.classList.remove('active');var n=i.querySelector('.mi-name');if(n){n.classList.remove('text-k-gold');n.classList.add('text-k-text')}});el.classList.add('active');var nm=el.querySelector('.mi-name');if(nm){nm.classList.add('text-k-gold');nm.classList.remove('text-k-text')}}

// SSE change ticks -> htmx triggers. Fragments listen with
// hx-trigger="sse-<topic> from:body"; EventSource reconnects on its own.
(function(){
  var es=new EventSource('/sse');
  es.addEventListener('tick',function(e){htmx.trigger(document.body,'sse-'+e.data)});
})();
</script>"##;
