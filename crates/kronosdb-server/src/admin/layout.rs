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
    let days = (epoch.div_euclid(86400)) as i64;
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

/// Try to decode payload bytes as UTF-8 for display, falling back to hex preview.
pub fn try_utf8_preview(data: &[u8], max_len: usize) -> String {
    match std::str::from_utf8(data) {
        Ok(s) => {
            if s.len() <= max_len {
                html_escape(s)
            } else {
                format!("{}...", html_escape(&s[..max_len]))
            }
        }
        Err(_) => {
            let hex: String = data
                .iter()
                .take(max_len / 2)
                .map(|b| format!("{b:02x}"))
                .collect();
            format!("{hex}...")
        }
    }
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
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">
<script src="/static/js/htmx.min.js"></script>
<script src="/static/js/tailwind.js"></script>
<script>
tailwind.config = {{
  theme: {{
    extend: {{
      colors: {{
        k: {{
          base: 'var(--c-base)', surface: 'var(--c-surface)', elevated: 'var(--c-elevated)',
          overlay: 'var(--c-overlay)', hover: 'var(--c-hover)',
          border: 'var(--c-border)', subtle: 'var(--c-subtle)',
          text: 'var(--c-text)', text2: 'var(--c-text2)', muted: 'var(--c-muted)', inv: 'var(--c-inv)',
          gold: 'var(--c-gold)', 'gold-d': 'var(--c-gold-dim)',
          blue: 'var(--c-blue)', 'blue-d': 'var(--c-blue-dim)',
          amber: 'var(--c-amber)', 'amber-d': 'var(--c-amber-dim)',
          red: 'var(--c-red)', 'red-d': 'var(--c-red-dim)',
          teal: 'var(--c-teal)', 'teal-d': 'var(--c-teal-dim)',
        }}
      }},
      fontFamily: {{
        sans: ['Outfit', 'system-ui', 'sans-serif'],
        mono: ['JetBrains Mono', 'SF Mono', 'monospace'],
      }},
    }}
  }}
}}
</script>
{THEME_CSS}
</head>
<body class="bg-k-base text-k-text font-sans text-sm antialiased overflow-hidden h-screen">
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
          <div class="dropdown relative inline-flex" id="context-dropdown">
            <div class="dd-arrow font-mono text-xs px-2.5 py-1.5 pr-7 border border-k-border rounded-[5px] bg-k-base text-k-text cursor-pointer whitespace-nowrap relative select-none" onclick="toggleDropdown('context-dropdown')">All Contexts</div>
            <div class="dropdown-menu hidden absolute top-[calc(100%+4px)] right-0 min-w-full bg-k-surface border border-k-border rounded-[5px] shadow-lg z-50 p-1 max-h-60 overflow-y-auto">
              <button class="dropdown-item active block w-full px-2.5 py-1.5 font-mono text-xs text-k-gold bg-k-gold-d border-none rounded-[3px] cursor-pointer text-left whitespace-nowrap" onclick="selectContext('all','All Contexts')">All Contexts</button>
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
        THEME_CSS = THEME_CSS,
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
                r#"<button class="dropdown-item block w-full px-2.5 py-1.5 font-mono text-xs text-k-text2 bg-transparent border-none rounded-[3px] cursor-pointer text-left whitespace-nowrap hover:bg-k-hover hover:text-k-text transition-colors" onclick="selectContext('{c}','{c}')">{c}</button>"#,
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

// ── Inline CSS (theme variables + minimal custom styles) ───────────

const THEME_CSS: &str = r##"<style>
:root {
  --c-base:#08080b;--c-surface:#0d0d11;--c-elevated:#131318;
  --c-overlay:#1a1a21;--c-hover:#1f1f28;
  --c-border:#232330;--c-subtle:#191922;
  --c-text:#e9e9ec;--c-text2:#86868f;--c-muted:#4e4e58;--c-inv:#08080b;
  --c-gold:#c8a44e;--c-gold-dim:rgba(200,164,78,0.12);
  --c-blue:#6b8fd4;--c-blue-dim:rgba(107,143,212,0.10);
  --c-amber:#fbbf24;--c-amber-dim:rgba(251,191,36,0.10);
  --c-red:#f87171;--c-red-dim:rgba(248,113,113,0.10);
  --c-teal:#5eead4;--c-teal-dim:rgba(94,234,212,0.08);
  --chart-op:0.4;--chart-hop:0.8;
}
[data-theme="light"] {
  --c-base:#f5f4f1;--c-surface:#ffffff;--c-elevated:#f9f8f6;
  --c-overlay:#eeece8;--c-hover:#f0eeea;
  --c-border:#d8d5ce;--c-subtle:#e5e2db;
  --c-text:#1a1917;--c-text2:#5c5a54;--c-muted:#94918a;--c-inv:#ffffff;
  --c-gold:#9a7b2e;--c-gold-dim:rgba(154,123,46,0.10);
  --c-blue:#4a6ba8;--c-blue-dim:rgba(74,107,168,0.08);
  --c-amber:#b88a0a;--c-amber-dim:rgba(184,138,10,0.10);
  --c-red:#d94444;--c-red-dim:rgba(217,68,68,0.08);
  --c-teal:#2a9d8f;--c-teal-dim:rgba(42,157,143,0.08);
  --chart-op:0.35;--chart-hop:0.7;
}
::selection{background:rgba(200,164,78,0.3);color:#fff}
::-webkit-scrollbar{width:5px;height:5px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--c-border);border-radius:3px}
table{width:100%;border-spacing:0}
thead{background:var(--c-elevated);box-shadow:inset 0 -1px 0 var(--c-border)}
thead th{text-align:left;padding:9px 18px;font-size:11px;font-weight:600;text-transform:uppercase;letter-spacing:0.6px;color:var(--c-muted);white-space:nowrap}
tbody td{padding:9px 18px;border-bottom:1px solid var(--c-subtle);color:var(--c-text2);vertical-align:middle}
tbody tr{transition:background 0.08s}
tbody tr:hover{background:var(--c-hover)}
tbody tr:last-child td{border-bottom:none}
tbody td:first-child{border-left:2px solid transparent}
tbody tr:hover td:first-child{border-left-color:var(--c-gold)}
.chart-bars{display:flex;align-items:flex-end;gap:3px;flex:1;min-height:80px;padding:14px 18px;background-image:linear-gradient(var(--c-subtle) 1px,transparent 1px);background-size:100% 25%}
.chart-bar{flex:1;border-radius:2px 2px 0 0;background:var(--c-gold);opacity:var(--chart-op);min-width:3px;transition:opacity 0.15s}
.chart-bar:hover{opacity:var(--chart-hop)}
.chart-bar.blue{background:var(--c-blue)}
@keyframes pulse-flow{0%{background-position:200% 0}100%{background-position:-200% 0}}
.pulse-bar{background:linear-gradient(90deg,transparent 0%,var(--c-gold) 30%,rgba(200,164,78,0.2) 50%,var(--c-gold) 70%,transparent 100%);background-size:200% 100%;animation:pulse-flow 6s linear infinite;opacity:0.3}
.brand-pulse{height:1px;background:linear-gradient(90deg,transparent,var(--c-gold),transparent);opacity:0.3}
.dd-arrow::after{content:'';position:absolute;right:10px;top:50%;transform:translateY(-50%);width:0;height:0;border-left:4px solid transparent;border-right:4px solid transparent;border-top:4px solid var(--c-muted);transition:transform 0.15s}
.dropdown.open .dd-arrow::after{transform:translateY(-50%) rotate(180deg)}
.chart-tooltip{position:fixed;pointer-events:none;z-index:60;display:none;transform:translateX(-50%)}
.chart-tooltip::after{content:'';position:absolute;top:100%;left:50%;transform:translateX(-50%);border:5px solid transparent;border-top-color:var(--c-border)}
.icon-sun{display:block}.icon-moon{display:none}
[data-theme="light"] .icon-sun{display:none}
[data-theme="light"] .icon-moon{display:block}
[data-theme="light"] .panel{box-shadow:0 1px 3px rgba(0,0,0,0.06)}
.progress-fill{height:100%;border-radius:2px}
.master-item.active{background:var(--c-gold-dim);border-left:2px solid var(--c-gold)}
.master-item.active .mi-name{color:var(--c-gold)}
body,.panel,thead{transition:background-color 0.2s,border-color 0.2s,color 0.2s}
.htmx-settling{opacity:0.6}
</style>"##;

// ── Shared JS ──────────────────────────────────────────────────────

const SHARED_JS: &str = r##"<script>
// Theme
function toggleTheme(){var t=document.documentElement.getAttribute('data-theme')==='light'?null:'light';t?document.documentElement.setAttribute('data-theme','light'):document.documentElement.removeAttribute('data-theme');localStorage.setItem('kronosdb-theme',t||'dark')}
(function(){if(localStorage.getItem('kronosdb-theme')==='light')document.documentElement.setAttribute('data-theme','light')})();

// Dropdowns
function toggleDropdown(id){var dd=document.getElementById(id),w=dd.classList.contains('open');document.querySelectorAll('.dropdown.open').forEach(function(d){d.classList.remove('open');d.querySelector('.dropdown-menu').classList.add('hidden')});if(!w){dd.classList.add('open');dd.querySelector('.dropdown-menu').classList.remove('hidden')}}
function selectContext(val,label){var dd=document.getElementById('context-dropdown');dd.querySelector('.dd-arrow').textContent=label;dd.querySelectorAll('.dropdown-item').forEach(function(i){var a=i.textContent.trim()===label;i.className='dropdown-item block w-full px-2.5 py-1.5 font-mono text-xs border-none rounded-[3px] cursor-pointer text-left whitespace-nowrap transition-colors '+(a?'active text-k-gold bg-k-gold-d':'text-k-text2 bg-transparent hover:bg-k-hover hover:text-k-text')});dd.classList.remove('open');dd.querySelector('.dropdown-menu').classList.add('hidden')}
document.addEventListener('click',function(e){if(!e.target.closest('.dropdown')){document.querySelectorAll('.dropdown.open').forEach(function(d){d.classList.remove('open');d.querySelector('.dropdown-menu').classList.add('hidden')})}});

// Chart tooltip
var tooltip=document.getElementById('chart-tooltip');
function showTooltip(bar,label,value){var r=bar.getBoundingClientRect();tooltip.innerHTML='<div class="text-k-gold font-semibold">'+value+'</div><div class="text-k-muted text-[10px]">'+label+'</div>';tooltip.style.display='block';tooltip.style.left=(r.left+r.width/2)+'px';tooltip.style.top=(r.top-tooltip.offsetHeight-8)+'px'}
function hideTooltip(){tooltip.style.display='none'}

// Master-detail
function selectMaster(el){el.parentElement.querySelectorAll('.master-item').forEach(function(i){i.classList.remove('active');var n=i.querySelector('.mi-name');if(n){n.classList.remove('text-k-gold');n.classList.add('text-k-text')}});el.classList.add('active');var nm=el.querySelector('.mi-name');if(nm){nm.classList.add('text-k-gold');nm.classList.remove('text-k-text')}}
</script>"##;
