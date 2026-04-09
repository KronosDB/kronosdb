use crate::admin::EventsQuery;
use crate::config::ServerConfig;
use kronosdb_eventstore::event::StoredEvent;
use kronosdb_messaging::client::ClientInfo;
use std::time::Duration;

// ── Helpers ────────────────────────────────────────────────────────

fn format_uptime(d: Duration) -> String {
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

fn format_duration_short(d: Duration) -> String {
    let secs = d.as_secs();
    if secs < 60 {
        format!("{secs}s")
    } else if secs < 3600 {
        format!("{}m {}s", secs / 60, secs % 60)
    } else if secs < 86400 {
        format!("{}h {}m", secs / 3600, (secs % 3600) / 60)
    } else {
        format!("{}d {}h", secs / 86400, (secs % 86400) / 3600)
    }
}

fn format_bytes(bytes: u64) -> String {
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

fn format_number(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

pub fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

// ── SVG Icons (Lucide-style) ──────────────────────────────────────

const ICON_OVERVIEW: &str = r#"<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="7" height="9" x="3" y="3" rx="1"/><rect width="7" height="5" x="14" y="3" rx="1"/><rect width="7" height="9" x="14" y="12" rx="1"/><rect width="7" height="5" x="3" y="16" rx="1"/></svg>"#;

const ICON_CONTEXTS: &str = r#"<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><ellipse cx="12" cy="5" rx="9" ry="3"/><path d="M3 5V19A9 3 0 0 0 21 19V5"/><path d="M3 12A9 3 0 0 0 21 12"/></svg>"#;

const ICON_CLIENTS: &str = r#"<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect width="20" height="14" x="2" y="3" rx="2"/><line x1="8" x2="16" y1="21" y2="21"/><line x1="12" x2="12" y1="17" y2="21"/></svg>"#;

const ICON_COMMANDS: &str = r#"<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="m5 12 7-7 7 7"/><path d="M12 19V5"/></svg>"#;

const ICON_QUERIES: &str = r#"<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/></svg>"#;

const ICON_EVENTS: &str = r#"<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3v17.25"/><path d="m6 15 6 5 6-5"/><circle cx="12" cy="6" r="2.5" fill="currentColor" stroke="none"/></svg>"#;

const ICON_SETTINGS: &str = r#"<svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12.22 2h-.44a2 2 0 0 0-2 2v.18a2 2 0 0 1-1 1.73l-.43.25a2 2 0 0 1-2 0l-.15-.08a2 2 0 0 0-2.73.73l-.22.38a2 2 0 0 0 .73 2.73l.15.1a2 2 0 0 1 1 1.72v.51a2 2 0 0 1-1 1.74l-.15.09a2 2 0 0 0-.73 2.73l.22.38a2 2 0 0 0 2.73.73l.15-.08a2 2 0 0 1 2 0l.43.25a2 2 0 0 1 1 1.73V20a2 2 0 0 0 2 2h.44a2 2 0 0 0 2-2v-.18a2 2 0 0 1 1-1.73l.43-.25a2 2 0 0 1 2 0l.15.08a2 2 0 0 0 2.73-.73l.22-.39a2 2 0 0 0-.73-2.73l-.15-.08a2 2 0 0 1-1-1.74v-.5a2 2 0 0 1 1-1.74l.15-.09a2 2 0 0 0 .73-2.73l-.22-.38a2 2 0 0 0-2.73-.73l-.15.08a2 2 0 0 1-2 0l-.43-.25a2 2 0 0 1-1-1.73V4a2 2 0 0 0-2-2z"/><circle cx="12" cy="12" r="3"/></svg>"#;

const ICON_LOGO: &str = r#"<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>"#;

// ── Layout ─────────────────────────────────────────────────────────

fn layout(config: &ServerConfig, active_page: &str, title: &str, subtitle: &str, content: &str) -> String {
    let nav_items = [
        ("overview", "Overview", ICON_OVERVIEW),
        ("events", "Events", ICON_EVENTS),
        ("contexts", "Contexts", ICON_CONTEXTS),
        ("clients", "Clients", ICON_CLIENTS),
        ("commands", "Commands", ICON_COMMANDS),
        ("queries", "Queries", ICON_QUERIES),
        ("settings", "Settings", ICON_SETTINGS),
    ];

    let mut nav_html = String::new();
    for (id, label, icon) in &nav_items {
        let active_class = if *id == active_page { " active" } else { "" };
        nav_html.push_str(&format!(
            r#"<a href="/{id}" class="nav-item{active_class}">{icon}<span>{label}</span></a>"#,
        ));
    }

    format!(
        r##"<!DOCTYPE html>
<html lang="en" class="dark">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title} - KronosDB</title>
  <link rel="stylesheet" href="/static/css/basecoat.min.css">
  <script src="/static/js/basecoat.min.js"></script>
  <script src="/static/js/htmx.min.js"></script>
  <style>
    :root {{
      --sidebar-width: 240px;
      --sidebar-bg: hsl(240 6% 7%);
      --sidebar-border: hsl(240 4% 14%);
      --nav-hover: hsl(240 4% 12%);
      --nav-active: hsl(240 4% 16%);
      --accent: hsl(262 83% 58%);
      --accent-soft: hsl(262 83% 58% / 0.12);
      --success: hsl(142 71% 45%);
      --warning: hsl(38 92% 50%);
      --chart-bar: hsl(262 83% 58%);
      --chart-bar-hover: hsl(262 83% 68%);
    }}
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
      font-family: system-ui, -apple-system, 'Segoe UI', sans-serif;
      background: hsl(240 5% 6%);
      color: hsl(0 0% 93%);
      display: flex;
      min-height: 100vh;
    }}

    /* ── Sidebar ── */
    .sidebar {{
      width: var(--sidebar-width);
      background: var(--sidebar-bg);
      border-right: 1px solid var(--sidebar-border);
      display: flex;
      flex-direction: column;
      position: fixed;
      top: 0;
      left: 0;
      bottom: 0;
      z-index: 10;
    }}
    .sidebar-header {{
      padding: 1.25rem 1rem;
      display: flex;
      align-items: center;
      gap: 0.625rem;
      border-bottom: 1px solid var(--sidebar-border);
    }}
    .sidebar-header svg {{ color: var(--accent); flex-shrink: 0; }}
    .sidebar-header .logo-text {{
      font-weight: 700;
      font-size: 1rem;
      letter-spacing: -0.02em;
    }}
    .sidebar-header .logo-version {{
      font-size: 0.6875rem;
      color: hsl(0 0% 50%);
      margin-left: auto;
      font-family: ui-monospace, 'SF Mono', 'Cascadia Code', monospace;
    }}
    .sidebar-nav {{
      flex: 1;
      padding: 0.75rem 0.5rem;
      display: flex;
      flex-direction: column;
      gap: 2px;
    }}
    .nav-item {{
      display: flex;
      align-items: center;
      gap: 0.625rem;
      padding: 0.5rem 0.75rem;
      border-radius: 0.375rem;
      color: hsl(0 0% 60%);
      text-decoration: none;
      font-size: 0.875rem;
      font-weight: 450;
      transition: all 0.15s ease;
    }}
    .nav-item:hover {{
      color: hsl(0 0% 85%);
      background: var(--nav-hover);
    }}
    .nav-item.active {{
      color: hsl(0 0% 95%);
      background: var(--nav-active);
    }}
    .nav-item.active svg {{ color: var(--accent); }}
    .sidebar-footer {{
      padding: 0.75rem 1rem;
      border-top: 1px solid var(--sidebar-border);
    }}
    .server-info {{
      display: flex;
      align-items: center;
      gap: 0.5rem;
      font-size: 0.75rem;
      color: hsl(0 0% 45%);
    }}
    .status-dot {{
      width: 7px;
      height: 7px;
      border-radius: 50%;
      background: var(--success);
      flex-shrink: 0;
      box-shadow: 0 0 6px hsl(142 71% 45% / 0.5);
    }}

    /* ── Main content ── */
    .main {{
      margin-left: var(--sidebar-width);
      flex: 1;
      min-height: 100vh;
    }}
    .page-header {{
      padding: 1.5rem 2rem 0;
    }}
    .page-header h1 {{
      font-size: 1.5rem;
      font-weight: 700;
      letter-spacing: -0.02em;
      margin-bottom: 0.25rem;
    }}
    .page-header .subtitle {{
      color: hsl(0 0% 50%);
      font-size: 0.875rem;
    }}
    .page-body {{
      padding: 1.5rem 2rem 2rem;
    }}

    /* ── Stat cards ── */
    .stat-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 0.75rem;
      margin-bottom: 1.5rem;
    }}
    .stat-card {{
      padding: 1rem 1.125rem;
      border-radius: 0.5rem;
      border: 1px solid hsl(240 4% 14%);
      background: hsl(240 5% 8.5%);
    }}
    .stat-label {{
      font-size: 0.75rem;
      font-weight: 500;
      color: hsl(0 0% 50%);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-bottom: 0.375rem;
    }}
    .stat-value {{
      font-size: 1.625rem;
      font-weight: 700;
      letter-spacing: -0.02em;
      font-variant-numeric: tabular-nums;
    }}
    .stat-sub {{
      font-size: 0.75rem;
      color: hsl(0 0% 42%);
      margin-top: 0.125rem;
    }}
    .stat-card-link {{
      text-decoration: none;
      color: inherit;
      transition: border-color 0.15s, background 0.15s;
      cursor: pointer;
    }}
    .stat-card-link:hover {{
      border-color: var(--accent);
      background: hsl(262 83% 58% / 0.04);
    }}

    /* ── Section headers ── */
    .section-header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 0.75rem;
    }}
    .section-header h2 {{
      font-size: 0.9375rem;
      font-weight: 600;
    }}
    .section-header .badge {{
      font-size: 0.6875rem;
      padding: 0.125rem 0.5rem;
      border-radius: 9999px;
      background: var(--accent-soft);
      color: var(--accent);
      font-weight: 600;
    }}

    /* ── Tables ── */
    .table-card {{
      border-radius: 0.5rem;
      border: 1px solid hsl(240 4% 14%);
      background: hsl(240 5% 8.5%);
      overflow: hidden;
      margin-bottom: 1.5rem;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
    }}
    th {{
      padding: 0.625rem 1rem;
      text-align: left;
      font-size: 0.75rem;
      font-weight: 500;
      color: hsl(0 0% 45%);
      text-transform: uppercase;
      letter-spacing: 0.04em;
      border-bottom: 1px solid hsl(240 4% 14%);
      background: hsl(240 5% 7%);
    }}
    td {{
      padding: 0.625rem 1rem;
      font-size: 0.8125rem;
      border-bottom: 1px solid hsl(240 4% 11%);
    }}
    tr:last-child td {{ border-bottom: none; }}
    tr:hover td {{ background: hsl(240 4% 10%); }}
    .mono {{
      font-family: ui-monospace, 'SF Mono', 'Cascadia Code', monospace;
      font-size: 0.8125rem;
    }}
    .text-muted {{ color: hsl(0 0% 45%); }}
    .text-right {{ text-align: right; }}
    .text-center {{ text-align: center; }}

    /* ── Section links ── */
    .section-link {{
      color: inherit;
      text-decoration: none;
      transition: color 0.15s;
    }}
    .section-link:hover {{
      color: var(--accent);
    }}

    /* ── Badges ── */
    .tag-link {{
      text-decoration: none;
      transition: background 0.15s;
    }}
    .tag-link:hover {{
      background: hsl(262 83% 58% / 0.2);
    }}
    .tag {{
      display: inline-block;
      padding: 0.125rem 0.5rem;
      border-radius: 0.25rem;
      font-size: 0.75rem;
      font-weight: 500;
      background: hsl(240 4% 16%);
      color: hsl(0 0% 75%);
    }}
    .tag-accent {{
      background: var(--accent-soft);
      color: hsl(262 83% 72%);
    }}
    .tag-success {{
      background: hsl(142 71% 45% / 0.12);
      color: hsl(142 71% 55%);
    }}

    /* ── Chart ── */
    .chart-card {{
      border-radius: 0.5rem;
      border: 1px solid hsl(240 4% 14%);
      background: hsl(240 5% 8.5%);
      padding: 1rem 1.125rem;
      margin-bottom: 1.5rem;
    }}
    .chart-card h3 {{
      font-size: 0.8125rem;
      font-weight: 500;
      color: hsl(0 0% 50%);
      margin-bottom: 0.75rem;
    }}
    .chart-bar-group {{
      display: flex;
      align-items: end;
      gap: 6px;
      height: 120px;
      padding-bottom: 1.5rem;
      position: relative;
    }}
    .chart-col {{
      flex: 1;
      display: flex;
      flex-direction: column;
      align-items: center;
      gap: 4px;
      height: 100%;
      justify-content: flex-end;
    }}
    .chart-bar {{
      width: 100%;
      max-width: 48px;
      border-radius: 3px 3px 0 0;
      background: var(--chart-bar);
      min-height: 2px;
      transition: background 0.15s;
    }}
    .chart-col:hover .chart-bar {{ background: var(--chart-bar-hover); }}
    .chart-label {{
      font-size: 0.625rem;
      color: hsl(0 0% 42%);
      text-align: center;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      max-width: 64px;
      position: absolute;
      bottom: 0;
    }}
    .chart-value {{
      font-size: 0.625rem;
      color: hsl(0 0% 55%);
      font-family: ui-monospace, 'SF Mono', monospace;
    }}

    /* ── Settings grid ── */
    .settings-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
      gap: 0.75rem;
    }}
    .settings-card {{
      border-radius: 0.5rem;
      border: 1px solid hsl(240 4% 14%);
      background: hsl(240 5% 8.5%);
      padding: 1rem 1.125rem;
    }}
    .settings-card h3 {{
      font-size: 0.8125rem;
      font-weight: 600;
      margin-bottom: 0.75rem;
      color: hsl(0 0% 75%);
    }}
    .setting-row {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 0.375rem 0;
      font-size: 0.8125rem;
    }}
    .setting-row + .setting-row {{
      border-top: 1px solid hsl(240 4% 11%);
    }}
    .setting-key {{ color: hsl(0 0% 50%); }}
    .setting-val {{
      font-family: ui-monospace, 'SF Mono', monospace;
      font-size: 0.8125rem;
      color: hsl(0 0% 80%);
    }}

    /* ── Empty state ── */
    .empty-state {{
      padding: 2rem;
      text-align: center;
      color: hsl(0 0% 40%);
      font-size: 0.875rem;
    }}

    /* ── Two-column grid ── */
    .grid-2 {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 1rem;
    }}
    @media (max-width: 900px) {{
      .grid-2 {{ grid-template-columns: 1fr; }}
    }}

    /* ── Loading pulse ── */
    .loading {{
      color: hsl(0 0% 35%);
      font-size: 0.8125rem;
      padding: 1rem;
    }}

    /* ── Events page ── */
    .events-toolbar {{
      display: flex;
      gap: 0.5rem;
      align-items: end;
      flex-wrap: wrap;
      margin-bottom: 1rem;
      padding: 1rem;
      border-radius: 0.5rem;
      border: 1px solid hsl(240 4% 14%);
      background: hsl(240 5% 8.5%);
    }}
    .field {{
      display: flex;
      flex-direction: column;
      gap: 0.25rem;
    }}
    .field label {{
      font-size: 0.6875rem;
      font-weight: 500;
      color: hsl(0 0% 45%);
      text-transform: uppercase;
      letter-spacing: 0.04em;
    }}
    .field select, .field input {{
      padding: 0.375rem 0.625rem;
      border-radius: 0.375rem;
      border: 1px solid hsl(240 4% 18%);
      background: hsl(240 5% 10%);
      color: hsl(0 0% 85%);
      font-size: 0.8125rem;
      font-family: inherit;
      outline: none;
      min-width: 120px;
    }}
    .field select:focus, .field input:focus {{
      border-color: var(--accent);
      box-shadow: 0 0 0 2px hsl(262 83% 58% / 0.15);
    }}
    .field input::placeholder {{ color: hsl(0 0% 35%); }}
    .btn {{
      padding: 0.375rem 0.875rem;
      border-radius: 0.375rem;
      border: 1px solid hsl(240 4% 18%);
      background: var(--accent);
      color: white;
      font-size: 0.8125rem;
      font-weight: 500;
      cursor: pointer;
      transition: background 0.15s;
    }}
    .btn:hover {{ background: hsl(262 83% 52%); }}
    .btn-outline {{
      background: transparent;
      color: hsl(0 0% 70%);
      border-color: hsl(240 4% 18%);
    }}
    .btn-outline:hover {{ background: hsl(240 4% 12%); }}
    .event-detail {{
      margin-top: 0.25rem;
    }}
    .event-tags {{
      display: flex;
      flex-wrap: wrap;
      gap: 0.25rem;
      margin-top: 0.375rem;
    }}
    .event-tag {{
      display: inline-flex;
      align-items: center;
      gap: 0.25rem;
      padding: 0.125rem 0.375rem;
      border-radius: 0.25rem;
      font-size: 0.6875rem;
      font-family: ui-monospace, 'SF Mono', monospace;
      background: hsl(240 4% 14%);
      color: hsl(0 0% 65%);
    }}
    .event-tag .tag-key {{ color: hsl(262 60% 70%); }}
    .event-tag .tag-sep {{ color: hsl(0 0% 35%); }}
    .event-payload {{
      margin-top: 0.375rem;
      padding: 0.5rem;
      border-radius: 0.25rem;
      background: hsl(240 5% 7%);
      border: 1px solid hsl(240 4% 12%);
      font-family: ui-monospace, 'SF Mono', monospace;
      font-size: 0.75rem;
      color: hsl(0 0% 60%);
      max-height: 120px;
      overflow: auto;
      white-space: pre-wrap;
      word-break: break-all;
    }}
    .event-meta {{
      display: flex;
      gap: 0.75rem;
      font-size: 0.6875rem;
      color: hsl(0 0% 40%);
      margin-top: 0.25rem;
    }}
    .pagination {{
      display: flex;
      justify-content: center;
      gap: 0.5rem;
      padding: 1rem;
    }}
    .event-row {{ cursor: default; }}
    .event-row td {{ vertical-align: top; }}
    .event-name {{
      font-weight: 500;
      color: hsl(0 0% 88%);
    }}
    .event-pos {{
      color: hsl(262 60% 70%);
      font-weight: 500;
    }}
  </style>
</head>
<body>
  <aside class="sidebar">
    <div class="sidebar-header">
      {logo}
      <span class="logo-text">KronosDB</span>
      <span class="logo-version">v{version}</span>
    </div>
    <nav class="sidebar-nav">
      {nav}
    </nav>
    <div class="sidebar-footer">
      <div class="server-info">
        <span class="status-dot"></span>
        <span>{node_name} &middot; {listen_addr}</span>
      </div>
    </div>
  </aside>
  <main class="main">
    <div class="page-header">
      <h1>{title}</h1>
      <p class="subtitle">{subtitle}</p>
    </div>
    <div class="page-body">
      {content}
    </div>
  </main>
</body>
</html>"##,
        logo = ICON_LOGO,
        version = env!("CARGO_PKG_VERSION"),
        node_name = html_escape(&config.node_name),
        listen_addr = config.listen_addr,
        nav = nav_html,
        title = title,
        subtitle = subtitle,
        content = content,
    )
}

// ── Page: Overview ─────────────────────────────────────────────────

pub fn overview_page(
    config: &ServerConfig,
    uptime: Duration,
    context_count: usize,
    clients: &[ClientInfo],
    total_events: u64,
    command_count: usize,
    query_count: usize,
    context_data: &[(String, u64)],
) -> String {
    let stats = stats_fragment(uptime, context_count, clients.len(), total_events, command_count, query_count);
    let chart = context_bar_chart(context_data);
    let contexts_html = contexts_table_mini(context_data);
    let clients_html = clients_table_mini(clients);

    let content = format!(
        r#"<div hx-get="/fragments/stats" hx-trigger="every 5s" hx-swap="innerHTML" id="stats-grid">
  {stats}
</div>

<div class="section-header">
  <h2>Events by Context</h2>
</div>
<div class="chart-card" hx-get="/fragments/context-chart" hx-trigger="every 5s" hx-swap="innerHTML">
  {chart}
</div>

<div class="grid-2">
  <div>
    <div class="section-header">
      <h2><a href="/contexts" class="section-link">Contexts</a></h2>
      <span class="badge">{context_count}</span>
    </div>
    <div class="table-card" hx-get="/fragments/contexts-mini" hx-trigger="every 5s" hx-swap="innerHTML">
      {contexts_html}
    </div>
  </div>
  <div>
    <div class="section-header">
      <h2><a href="/clients" class="section-link">Connected Clients</a></h2>
      <span class="badge">{client_count}</span>
    </div>
    <div class="table-card" hx-get="/fragments/clients-mini" hx-trigger="every 5s" hx-swap="innerHTML">
      {clients_html}
    </div>
  </div>
</div>"#,
        client_count = clients.len(),
    );

    layout(
        config,
        "overview",
        "Overview",
        &format!("Server uptime: {}", format_uptime(uptime)),
        &content,
    )
}

pub fn stats_fragment(
    uptime: Duration,
    context_count: usize,
    client_count: usize,
    total_events: u64,
    command_count: usize,
    query_count: usize,
) -> String {
    format!(
        r#"<div class="stat-grid">
  <div class="stat-card">
    <div class="stat-label">Uptime</div>
    <div class="stat-value">{uptime}</div>
  </div>
  <a href="/contexts" class="stat-card stat-card-link">
    <div class="stat-label">Contexts</div>
    <div class="stat-value">{context_count}</div>
  </a>
  <a href="/events" class="stat-card stat-card-link">
    <div class="stat-label">Total Events</div>
    <div class="stat-value">{events}</div>
    <div class="stat-sub">{events_raw} across all contexts</div>
  </a>
  <a href="/clients" class="stat-card stat-card-link">
    <div class="stat-label">Clients</div>
    <div class="stat-value">{client_count}</div>
  </a>
  <a href="/commands" class="stat-card stat-card-link">
    <div class="stat-label">Command Types</div>
    <div class="stat-value">{command_count}</div>
  </a>
  <a href="/queries" class="stat-card stat-card-link">
    <div class="stat-label">Query Types</div>
    <div class="stat-value">{query_count}</div>
  </a>
</div>"#,
        uptime = format_uptime(uptime),
        events = format_number(total_events),
        events_raw = total_events,
    )
}

pub fn contexts_table_mini_pub(context_data: &[(String, u64)]) -> String {
    contexts_table_mini(context_data)
}

fn contexts_table_mini(context_data: &[(String, u64)]) -> String {
    if context_data.is_empty() {
        return r#"<div class="empty-state">No contexts created yet</div>"#.to_string();
    }
    let mut rows = String::new();
    for (name, events) in context_data {
        let escaped = html_escape(name);
        rows.push_str(&format!(
            r#"<tr><td><a href="/events?context={escaped}" class="tag tag-accent tag-link">{escaped}</a></td><td class="text-right mono">{events}</td></tr>"#,
        ));
    }
    format!(
        r#"<table>
  <thead><tr><th>Context</th><th class="text-right">Events</th></tr></thead>
  <tbody>{rows}</tbody>
</table>"#
    )
}

pub fn clients_table_mini_pub(clients: &[ClientInfo]) -> String {
    clients_table_mini(clients)
}

fn clients_table_mini(clients: &[ClientInfo]) -> String {
    if clients.is_empty() {
        return r#"<div class="empty-state">No clients connected</div>"#.to_string();
    }
    let mut rows = String::new();
    for c in clients {
        let heartbeat_class = if c.since_last_heartbeat.as_secs() < 10 {
            "tag-success"
        } else {
            ""
        };
        rows.push_str(&format!(
            r#"<tr><td><span class="tag">{component}</span></td><td class="mono" style="font-size: 0.75rem;">{client_id}</td><td class="text-right"><span class="tag {heartbeat_class}">{heartbeat}s</span></td></tr>"#,
            component = html_escape(&c.component_name.0),
            client_id = html_escape(&c.client_id.0),
            heartbeat = c.since_last_heartbeat.as_secs(),
        ));
    }
    format!(
        r#"<table>
  <thead><tr><th>Component</th><th>Client ID</th><th class="text-right">Heartbeat</th></tr></thead>
  <tbody>{rows}</tbody>
</table>"#
    )
}

// ── Page: Contexts ─────────────────────────────────────────────────

pub fn contexts_page(config: &ServerConfig) -> String {
    let content = r##"<form class="events-toolbar" style="margin-bottom: 1rem;" id="create-context-form">
  <div class="field">
    <label>New Context</label>
    <input type="text" name="name" placeholder="e.g. orders" style="width: 220px;" required pattern="[a-zA-Z0-9_\-]+">
  </div>
  <button type="submit" class="btn"
    hx-post="/fragments/create-context"
    hx-include="#create-context-form"
    hx-target="#context-feedback"
    hx-swap="innerHTML">
    Create
  </button>
  <span id="context-feedback" style="font-size: 0.8125rem; align-self: center;"></span>
</form>
<div class="table-card" hx-get="/fragments/contexts" hx-trigger="load, every 5s, refreshContexts from:body" hx-swap="innerHTML">
  <p class="loading">Loading contexts...</p>
</div>"##;

    layout(
        config,
        "contexts",
        "Contexts",
        "Event store contexts and their current state",
        content,
    )
}

pub fn contexts_table(rows: &[(String, u64, u64, u64)]) -> String {
    if rows.is_empty() {
        return r#"<div class="empty-state">No contexts created yet</div>"#.to_string();
    }
    let mut html = String::new();
    for (name, head, tail, event_count) in rows {
        let escaped = html_escape(name);
        html.push_str(&format!(
            r#"<tr>
  <td><a href="/events?context={escaped}" class="tag tag-accent tag-link">{escaped}</a></td>
  <td class="mono text-right">{head}</td>
  <td class="mono text-right">{tail}</td>
  <td class="mono text-right">{event_count}</td>
  <td class="text-right"><span class="tag tag-success">active</span></td>
</tr>"#,
        ));
    }
    format!(
        r#"<table>
  <thead>
    <tr><th>Context</th><th class="text-right">Head</th><th class="text-right">Tail</th><th class="text-right">Events</th><th class="text-right">Status</th></tr>
  </thead>
  <tbody>{html}</tbody>
</table>"#
    )
}

// ── Page: Clients ──────────────────────────────────────────────────

pub fn clients_page(config: &ServerConfig) -> String {
    let content = r#"<div class="table-card" hx-get="/fragments/clients" hx-trigger="load, every 5s" hx-swap="innerHTML">
  <p class="loading">Loading clients...</p>
</div>"#;

    layout(
        config,
        "clients",
        "Clients",
        "Connected application instances and their health",
        content,
    )
}

pub fn clients_table(clients: &[ClientInfo]) -> String {
    if clients.is_empty() {
        return r#"<div class="empty-state">No clients connected</div>"#.to_string();
    }
    let mut rows = String::new();
    for c in clients {
        let heartbeat_class = if c.since_last_heartbeat.as_secs() < 10 {
            "tag-success"
        } else {
            ""
        };
        rows.push_str(&format!(
            r#"<tr>
  <td class="mono">{client_id}</td>
  <td><span class="tag">{component}</span></td>
  <td class="mono">{version}</td>
  <td class="text-right">{connected}</td>
  <td class="text-right"><span class="tag {heartbeat_class}">{heartbeat}s ago</span></td>
  <td class="text-center">{stream}</td>
</tr>"#,
            client_id = html_escape(&c.client_id.0),
            component = html_escape(&c.component_name.0),
            version = if c.version.is_empty() { "-" } else { &c.version },
            connected = format_duration_short(c.connected_since),
            heartbeat = c.since_last_heartbeat.as_secs(),
            stream = if c.has_active_stream {
                r#"<span class="tag tag-success">active</span>"#
            } else {
                r#"<span class="tag text-muted">-</span>"#
            },
        ));
    }
    format!(
        r#"<table>
  <thead>
    <tr><th>Client ID</th><th>Component</th><th>Version</th><th class="text-right">Connected</th><th class="text-right">Heartbeat</th><th class="text-center">Stream</th></tr>
  </thead>
  <tbody>{rows}</tbody>
</table>"#
    )
}

// ── Page: Commands ─────────────────────────────────────────────────

pub fn commands_page(config: &ServerConfig) -> String {
    let content = r#"<div class="table-card" hx-get="/fragments/commands" hx-trigger="load, every 5s" hx-swap="innerHTML">
  <p class="loading">Loading command handlers...</p>
</div>"#;

    layout(
        config,
        "commands",
        "Commands",
        "Registered command types and their handlers",
        content,
    )
}

pub fn commands_table(stats: &[(String, usize)]) -> String {
    if stats.is_empty() {
        return r#"<div class="empty-state">No command handlers registered</div>"#.to_string();
    }
    let mut rows = String::new();
    for (name, count) in stats {
        rows.push_str(&format!(
            r#"<tr>
  <td class="mono">{name}</td>
  <td class="text-right"><span class="tag">{count} handler{s}</span></td>
</tr>"#,
            name = html_escape(name),
            s = if *count == 1 { "" } else { "s" },
        ));
    }
    format!(
        r#"<table>
  <thead>
    <tr><th>Command Type</th><th class="text-right">Handlers</th></tr>
  </thead>
  <tbody>{rows}</tbody>
</table>"#
    )
}

// ── Page: Queries ──────────────────────────────────────────────────

pub fn queries_page(config: &ServerConfig) -> String {
    let content = r#"<div class="table-card" hx-get="/fragments/queries" hx-trigger="load, every 5s" hx-swap="innerHTML">
  <p class="loading">Loading query handlers...</p>
</div>"#;

    layout(
        config,
        "queries",
        "Queries",
        "Registered query types and their handlers",
        content,
    )
}

pub fn queries_table(stats: &[(String, usize)]) -> String {
    if stats.is_empty() {
        return r#"<div class="empty-state">No query handlers registered</div>"#.to_string();
    }
    let mut rows = String::new();
    for (name, count) in stats {
        rows.push_str(&format!(
            r#"<tr>
  <td class="mono">{name}</td>
  <td class="text-right"><span class="tag">{count} handler{s}</span></td>
</tr>"#,
            name = html_escape(name),
            s = if *count == 1 { "" } else { "s" },
        ));
    }
    format!(
        r#"<table>
  <thead>
    <tr><th>Query Type</th><th class="text-right">Handlers</th></tr>
  </thead>
  <tbody>{rows}</tbody>
</table>"#
    )
}

// ── Page: Settings ─────────────────────────────────────────────────

pub fn settings_page(config: &ServerConfig) -> String {
    let content = format!(
        r#"<div class="settings-grid">
  <div class="settings-card">
    <h3>Server</h3>
    <div class="setting-row">
      <span class="setting-key">Node Name</span>
      <span class="setting-val">{node_name}</span>
    </div>
    <div class="setting-row">
      <span class="setting-key">gRPC Address</span>
      <span class="setting-val">{listen}</span>
    </div>
    <div class="setting-row">
      <span class="setting-key">Admin Address</span>
      <span class="setting-val">{admin_listen}</span>
    </div>
    <div class="setting-row">
      <span class="setting-key">Data Directory</span>
      <span class="setting-val">{data_dir}</span>
    </div>
    <div class="setting-row">
      <span class="setting-key">Version</span>
      <span class="setting-val">{version}</span>
    </div>
  </div>
  <div class="settings-card">
    <h3>Storage</h3>
    <div class="setting-row">
      <span class="setting-key">Segment Size</span>
      <span class="setting-val">{segment_size}</span>
    </div>
    <div class="setting-row">
      <span class="setting-key">Index Cache</span>
      <span class="setting-val">{index_cache} entries</span>
    </div>
    <div class="setting-row">
      <span class="setting-key">Bloom Cache</span>
      <span class="setting-val">{bloom_cache} entries</span>
    </div>
  </div>
  <div class="settings-card">
    <h3>Timeouts</h3>
    <div class="setting-row">
      <span class="setting-key">Command Timeout</span>
      <span class="setting-val">{cmd_timeout}s</span>
    </div>
    <div class="setting-row">
      <span class="setting-key">Query Timeout</span>
      <span class="setting-val">{query_timeout}s</span>
    </div>
    <div class="setting-row">
      <span class="setting-key">Heartbeat Interval</span>
      <span class="setting-val">{hb_interval}s</span>
    </div>
    <div class="setting-row">
      <span class="setting-key">Heartbeat Timeout</span>
      <span class="setting-val">{hb_timeout}s</span>
    </div>
  </div>
</div>"#,
        node_name = html_escape(&config.node_name),
        listen = config.listen_addr,
        admin_listen = config.admin_listen_addr,
        data_dir = html_escape(&config.data_dir.display().to_string()),
        version = env!("CARGO_PKG_VERSION"),
        segment_size = format_bytes(config.segment_size),
        index_cache = config.index_cache_size,
        bloom_cache = config.bloom_cache_size,
        cmd_timeout = config.command_timeout_secs,
        query_timeout = config.query_timeout_secs,
        hb_interval = config.heartbeat_interval_secs,
        hb_timeout = config.heartbeat_timeout_secs,
    );

    layout(
        config,
        "settings",
        "Settings",
        "Server configuration (read-only)",
        &content,
    )
}

// ── Chart: Context bar chart ───────────────────────────────────────

pub fn context_bar_chart(data: &[(String, u64)]) -> String {
    if data.is_empty() {
        return r#"<div class="empty-state">No contexts to chart</div>"#.to_string();
    }

    let max_events = data.iter().map(|(_, e)| *e).max().unwrap_or(1).max(1);

    let mut bars = String::new();
    for (name, events) in data {
        let height_pct = (*events as f64 / max_events as f64 * 100.0).max(2.0);
        bars.push_str(&format!(
            r#"<div class="chart-col">
  <span class="chart-value">{events}</span>
  <div class="chart-bar" style="height: {height_pct:.0}%"></div>
  <span class="chart-label">{name}</span>
</div>"#,
            name = html_escape(name),
            events = format_number(*events),
        ));
    }

    format!(r#"<div class="chart-bar-group">{bars}</div>"#)
}

// ── Page: Events ───────────────────────────────────────────────────

fn format_timestamp(millis: i64) -> String {
    if millis == 0 {
        return "-".to_string();
    }
    // Simple UTC formatting without pulling in chrono for the admin UI.
    let secs = millis / 1000;
    let ms = millis % 1000;
    // Use chrono if available, otherwise just show epoch.
    // For now, show a compact ISO-ish format from epoch math.
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let mins = (time_of_day % 3600) / 60;
    let s = time_of_day % 60;

    // Rough year/month/day from days since epoch (1970-01-01).
    // Good enough for display purposes.
    let (year, month, day) = days_to_ymd(days_since_epoch);
    format!("{year:04}-{month:02}-{day:02} {hours:02}:{mins:02}:{s:02}.{ms:03}")
}

fn days_to_ymd(days: i64) -> (i64, i64, i64) {
    // Simplified Gregorian calendar conversion.
    let mut y = 1970;
    let mut remaining = days;

    loop {
        let days_in_year = if is_leap(y) { 366 } else { 365 };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        y += 1;
    }

    let months = if is_leap(y) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut m = 1;
    for &days_in_month in &months {
        if remaining < days_in_month {
            break;
        }
        remaining -= days_in_month;
        m += 1;
    }

    (y, m, remaining + 1)
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
}

fn try_utf8_preview(data: &[u8], max_len: usize) -> String {
    match std::str::from_utf8(data) {
        Ok(s) => {
            let truncated = if s.len() > max_len { &s[..max_len] } else { s };
            html_escape(truncated)
        }
        Err(_) => {
            // Show hex preview for binary data.
            let bytes: Vec<String> = data.iter().take(max_len / 3).map(|b| format!("{b:02x}")).collect();
            format!("<span class=\"text-muted\">[binary] {}</span>", bytes.join(" "))
        }
    }
}

pub fn events_page(
    config: &ServerConfig,
    contexts: &[String],
    selected: &str,
    params: &EventsQuery,
    initial_results: &str,
) -> String {
    let mut context_options = String::new();
    for name in contexts {
        let sel = if name == selected { " selected" } else { "" };
        context_options.push_str(&format!(
            r#"<option value="{name}"{sel}>{name}</option>"#,
            name = html_escape(name),
        ));
    }

    let from_val = params.from.map(|f| f.to_string()).unwrap_or_default();
    let name_val = params.name.as_deref().unwrap_or("");
    let tag_val = params.tag.as_deref().unwrap_or("");

    let content = format!(
        r##"<form class="events-toolbar" id="events-form">
  <div class="field">
    <label>Context</label>
    <select name="context" hx-get="/fragments/events" hx-target="#events-results" hx-include="#events-form" hx-trigger="change">
      {context_options}
    </select>
  </div>
  <div class="field">
    <label>From Position</label>
    <input type="number" name="from" value="{from_val}" placeholder="1" min="1" style="width: 100px;">
  </div>
  <div class="field">
    <label>Event Name</label>
    <input type="text" name="name" value="{name_val}" placeholder="e.g. OrderPlaced" style="width: 180px;">
  </div>
  <div class="field">
    <label>Tags (key=value, comma-separated)</label>
    <input type="text" name="tag" value="{tag_val}" placeholder="e.g. orderId=abc-123" style="width: 240px;">
  </div>
  <button type="button" class="btn" hx-get="/fragments/events" hx-target="#events-results" hx-include="#events-form">
    Search
  </button>
</form>

<div id="events-results" class="table-card">
  {initial_results}
</div>"##,
        name_val = html_escape(name_val),
        tag_val = html_escape(tag_val),
    );

    layout(
        config,
        "events",
        "Events",
        "Browse and search events across contexts",
        &content,
    )
}

pub fn events_table(
    events: &[StoredEvent],
    head: u64,
    has_more: bool,
    next_position: Option<u64>,
    context: &str,
    params: &EventsQuery,
) -> String {
    if events.is_empty() {
        return r#"<div class="empty-state">No events found matching your query</div>"#.to_string();
    }

    let mut rows = String::new();
    for event in events {
        // Tags
        let mut tags_html = String::new();
        for tag in &event.tags {
            let key = String::from_utf8_lossy(&tag.key);
            let val = String::from_utf8_lossy(&tag.value);
            tags_html.push_str(&format!(
                r#"<span class="event-tag"><span class="tag-key">{key}</span><span class="tag-sep">=</span>{val}</span>"#,
                key = html_escape(&key),
                val = html_escape(&val),
            ));
        }

        // Payload preview
        let payload_preview = if event.payload.is_empty() {
            String::new()
        } else {
            format!(
                r#"<div class="event-payload">{}</div>"#,
                try_utf8_preview(&event.payload, 512),
            )
        };

        // Metadata
        let mut meta_html = String::new();
        if !event.metadata.is_empty() {
            for (k, v) in &event.metadata {
                meta_html.push_str(&format!(
                    r#"<span>{k}: {v}</span>"#,
                    k = html_escape(k),
                    v = html_escape(v),
                ));
            }
        }

        rows.push_str(&format!(
            r#"<tr class="event-row">
  <td class="mono event-pos">{position}</td>
  <td>
    <div class="event-name">{name}</div>
    <div class="event-meta">
      <span>v{version}</span>
      <span>{timestamp}</span>
      <span>{identifier}</span>
    </div>
    <div class="event-tags">{tags_html}</div>
    {payload_preview}
  </td>
</tr>"#,
            position = event.position.0,
            name = html_escape(&event.name),
            version = html_escape(&event.version),
            timestamp = format_timestamp(event.timestamp),
            identifier = html_escape(&event.identifier),
        ));
    }

    // Pagination
    let mut pagination = String::new();
    if has_more {
        if let Some(next) = next_position {
            let name_param = params.name.as_deref().unwrap_or("");
            let tag_param = params.tag.as_deref().unwrap_or("");
            pagination = format!(
                r##"<div class="pagination">
  <button class="btn btn-outline"
    hx-get="/fragments/events?context={context}&from={next}&name={name}&tag={tag}"
    hx-target="#events-results"
    hx-swap="innerHTML">
    Load more events &rarr;
  </button>
</div>"##,
                context = html_escape(context),
                name = html_escape(name_param),
                tag = html_escape(tag_param),
            );
        }
    }

    let count = events.len();
    let first_pos = events.first().map(|e| e.position.0).unwrap_or(0);
    let last_pos = events.last().map(|e| e.position.0).unwrap_or(0);

    format!(
        r#"<div style="padding: 0.5rem 1rem; font-size: 0.75rem; color: hsl(0 0% 42%); border-bottom: 1px solid hsl(240 4% 14%);">
  Showing {count} events (position {first_pos}–{last_pos} of {head} total)
</div>
<table>
  <thead>
    <tr><th style="width: 80px;">Position</th><th>Event</th></tr>
  </thead>
  <tbody>{rows}</tbody>
</table>
{pagination}"#,
    )
}

pub fn events_error(message: &str) -> String {
    format!(
        r#"<div class="empty-state" style="color: hsl(0 62% 55%);">{}</div>"#,
        html_escape(message),
    )
}
