use axum::extract::State;
use axum::response::Html;

use crate::admin::AdminState;
use crate::admin::layout::{self, format_bytes, html_escape};

// ── Page handler ───────────────────────────────────────────────────

pub async fn page(State(state): State<AdminState>) -> Html<String> {
    let config = &state.config;

    let setting = |key: &str, val: &str| -> String {
        format!(
            r#"<div class="flex justify-between items-center py-2 border-b border-k-subtle last:border-0">
  <span class="text-xs text-k-muted">{key}</span>
  <span class="font-mono text-xs !text-k-text">{val}</span>
</div>"#,
        )
    };

    let card = |title: &str, rows: &str| -> String {
        format!(
            r#"<div class="bg-k-surface border border-k-subtle rounded-lg overflow-hidden">
  <div class="px-[18px] py-3 border-b border-k-subtle">
    <div class="text-[13px] font-semibold">{title}</div>
  </div>
  <div class="px-[18px] py-1">
    {rows}
  </div>
</div>"#,
        )
    };

    let server_rows = [
        setting("Node Name", &html_escape(&config.node_name)),
        setting("gRPC Address", &config.listen_addr.to_string()),
        setting("Admin Address", &config.admin_listen_addr.to_string()),
        setting(
            "Data Directory",
            &html_escape(&config.data_dir.display().to_string()),
        ),
        setting("Version", env!("CARGO_PKG_VERSION")),
    ]
    .join("");

    let storage_rows = [
        setting("Segment Size", &format_bytes(config.segment_size)),
        setting(
            "Index Cache",
            &format!("{} entries", config.index_cache_size),
        ),
        setting(
            "Bloom Cache",
            &format!("{} entries", config.bloom_cache_size),
        ),
        setting("Group Commit", &format!("{}ms", config.group_commit_ms)),
    ]
    .join("");

    let timeout_rows = [
        setting(
            "Command Timeout",
            &format!("{}s", config.command_timeout_secs),
        ),
        setting("Query Timeout", &format!("{}s", config.query_timeout_secs)),
        setting(
            "Heartbeat Interval",
            &format!("{}s", config.heartbeat_interval_secs),
        ),
        setting(
            "Heartbeat Timeout",
            &format!("{}s", config.heartbeat_timeout_secs),
        ),
    ]
    .join("");

    let cluster_rows = [
        setting(
            "Node ID",
            &config
                .cluster_node_id
                .map(|id| id.to_string())
                .unwrap_or_else(|| "-".to_string()),
        ),
        setting("Node Type", &html_escape(&config.cluster_node_type)),
        setting("Configured Peers", &config.cluster_peers.len().to_string()),
        setting(
            "Configured Learners",
            &config.cluster_learners.len().to_string(),
        ),
    ]
    .join("");

    let security_rows = [
        setting(
            "Access Token",
            if config.access_token.is_some() {
                "configured"
            } else {
                "none (open access)"
            },
        ),
        setting(
            "TLS Certificate",
            if config.tls_cert.is_some() {
                "configured"
            } else {
                "none"
            },
        ),
        setting(
            "TLS Key",
            if config.tls_key.is_some() {
                "configured"
            } else {
                "none"
            },
        ),
        setting(
            "mTLS CA",
            if config.tls_ca.is_some() {
                "configured"
            } else {
                "none"
            },
        ),
    ]
    .join("");

    let content = format!(
        r##"<div class="flex flex-col flex-1" id="page-settings">
  <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
    {server_card}
    {storage_card}
    {timeout_card}
    {cluster_card}
    {security_card}
  </div>
</div>"##,
        server_card = card("Server", &server_rows),
        storage_card = card("Storage", &storage_rows),
        timeout_card = card("Timeouts", &timeout_rows),
        cluster_card = card("Cluster", &cluster_rows),
        security_card = card("Security", &security_rows),
    );

    Html(layout::layout(
        "settings",
        "Settings",
        &state.config.node_name,
        &state.contexts.list_contexts(),
        &content,
    ))
}
