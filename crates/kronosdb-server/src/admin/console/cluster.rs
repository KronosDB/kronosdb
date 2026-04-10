use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{Html, Json};
use serde::Deserialize;

use crate::admin::AdminState;
use crate::admin::layout::{self, html_escape};

// ── Page handler ───────────────────────────────────────────────────

pub async fn page(State(state): State<AdminState>) -> Html<String> {
    let config = &state.config;
    let node_id = config
        .cluster_node_id
        .map(|id| id.to_string())
        .unwrap_or_else(|| "-".to_string());
    let node_type = &config.cluster_node_type;
    let clustering_enabled = config.cluster_node_id.is_some();

    let status_badge = if clustering_enabled {
        r#"<span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[11px] font-mono bg-k-teal-d text-k-teal"><span class="w-1.5 h-1.5 rounded-full bg-k-teal"></span>enabled</span>"#
    } else {
        r#"<span class="inline-flex items-center gap-1.5 px-2 py-0.5 rounded-full text-[11px] font-mono bg-k-amber-d text-k-amber"><span class="w-1.5 h-1.5 rounded-full bg-k-amber"></span>standalone</span>"#
    };

    // Peer info from config
    let mut peer_rows = String::new();
    for peer in &config.cluster_peers {
        peer_rows.push_str(&format!(
            r#"<tr><td class="font-mono text-xs !text-k-text">{id}</td><td class="font-mono text-xs">{addr}</td><td><span class="inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-mono bg-k-blue-d text-k-blue">voter</span></td></tr>"#,
            id = peer.id,
            addr = html_escape(&peer.addr),
        ));
    }
    for peer in &config.cluster_learners {
        peer_rows.push_str(&format!(
            r#"<tr><td class="font-mono text-xs !text-k-text">{id}</td><td class="font-mono text-xs">{addr}</td><td><span class="inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-mono bg-k-overlay text-k-text2">learner</span></td></tr>"#,
            id = peer.id,
            addr = html_escape(&peer.addr),
        ));
    }

    let peers_table = if peer_rows.is_empty() {
        r#"<div class="text-center text-k-muted py-6 text-xs">No peers configured</div>"#
            .to_string()
    } else {
        format!(
            r#"<table><thead><tr><th>Node ID</th><th>Address</th><th>Role</th></tr></thead><tbody>{peer_rows}</tbody></table>"#
        )
    };

    let management_section = if clustering_enabled {
        r##"<!-- Cluster management -->
  <div class="bg-k-surface border border-k-subtle rounded-lg overflow-hidden mt-4">
    <div class="flex items-center justify-between px-[18px] py-3 border-b border-k-subtle">
      <div class="text-[13px] font-semibold">Membership Management</div>
    </div>
    <div class="p-4">
      <div class="flex flex-wrap gap-4">
        <!-- Add Learner -->
        <div class="flex-1 min-w-[280px]">
          <div class="text-[11px] font-semibold uppercase tracking-[0.6px] text-k-muted mb-2">Add Learner</div>
          <form id="add-learner-form" class="flex flex-col gap-2"
            onsubmit="event.preventDefault(); clusterAction('/api/cluster/add-learner', 'add-learner-form', 'add-learner-result');">
            <input type="number" name="id" placeholder="Node ID" required min="1"
              class="font-mono text-xs px-2.5 py-1.5 border border-k-border rounded-[5px] bg-k-base text-k-text outline-none">
            <input type="text" name="addr" placeholder="host:port" required
              class="font-mono text-xs px-2.5 py-1.5 border border-k-border rounded-[5px] bg-k-base text-k-text outline-none">
            <input type="text" name="context" value="default" placeholder="context"
              class="font-mono text-xs px-2.5 py-1.5 border border-k-border rounded-[5px] bg-k-base text-k-text outline-none">
            <button type="submit"
              class="px-3 py-1.5 rounded-[5px] border border-k-blue bg-k-blue-d text-k-blue text-xs font-medium cursor-pointer hover:bg-k-blue hover:text-k-inv transition-colors w-fit">
              Add Learner
            </button>
            <span id="add-learner-result" class="text-xs"></span>
          </form>
        </div>
        <!-- Add Voter -->
        <div class="flex-1 min-w-[280px]">
          <div class="text-[11px] font-semibold uppercase tracking-[0.6px] text-k-muted mb-2">Promote to Voter</div>
          <form id="add-voter-form" class="flex flex-col gap-2"
            onsubmit="event.preventDefault(); clusterAction('/api/cluster/add-voter', 'add-voter-form', 'add-voter-result');">
            <input type="number" name="id" placeholder="Node ID" required min="1"
              class="font-mono text-xs px-2.5 py-1.5 border border-k-border rounded-[5px] bg-k-base text-k-text outline-none">
            <input type="text" name="addr" placeholder="host:port" required
              class="font-mono text-xs px-2.5 py-1.5 border border-k-border rounded-[5px] bg-k-base text-k-text outline-none">
            <input type="text" name="context" value="default" placeholder="context"
              class="font-mono text-xs px-2.5 py-1.5 border border-k-border rounded-[5px] bg-k-base text-k-text outline-none">
            <button type="submit"
              class="px-3 py-1.5 rounded-[5px] border border-k-blue bg-k-blue-d text-k-blue text-xs font-medium cursor-pointer hover:bg-k-blue hover:text-k-inv transition-colors w-fit">
              Promote to Voter
            </button>
            <span id="add-voter-result" class="text-xs"></span>
          </form>
        </div>
        <!-- Remove Node -->
        <div class="flex-1 min-w-[280px]">
          <div class="text-[11px] font-semibold uppercase tracking-[0.6px] text-k-muted mb-2">Remove Node</div>
          <form id="remove-node-form" class="flex flex-col gap-2"
            onsubmit="event.preventDefault(); clusterAction('/api/cluster/remove-node', 'remove-node-form', 'remove-node-result');">
            <input type="number" name="id" placeholder="Node ID" required min="1"
              class="font-mono text-xs px-2.5 py-1.5 border border-k-border rounded-[5px] bg-k-base text-k-text outline-none">
            <input type="text" name="context" value="default" placeholder="context"
              class="font-mono text-xs px-2.5 py-1.5 border border-k-border rounded-[5px] bg-k-base text-k-text outline-none">
            <button type="submit"
              class="px-3 py-1.5 rounded-[5px] border border-k-red bg-k-red-d text-k-red text-xs font-medium cursor-pointer hover:bg-k-red hover:text-k-inv transition-colors w-fit">
              Remove Node
            </button>
            <span id="remove-node-result" class="text-xs"></span>
          </form>
        </div>
      </div>
    </div>
  </div>

  <script>
  async function clusterAction(url, formId, resultId) {
    var form = document.getElementById(formId);
    var result = document.getElementById(resultId);
    var data = {};
    new FormData(form).forEach(function(v, k) { data[k] = k === 'id' ? parseInt(v) : v; });
    try {
      var resp = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) });
      var json = await resp.json();
      if (resp.ok) {
        result.innerHTML = '<span class="text-k-teal">Success: ' + json.action + '</span>';
      } else {
        result.innerHTML = '<span class="text-k-red">Error: ' + (json.message || resp.statusText) + '</span>';
      }
    } catch (e) {
      result.innerHTML = '<span class="text-k-red">Error: ' + e.message + '</span>';
    }
  }
  </script>"##
    } else {
        ""
    };

    let content = format!(
        r##"<div class="flex flex-col flex-1" id="page-cluster">
  <!-- Node info -->
  <div class="flex flex-wrap gap-3 mb-4">
    <div class="flex-1 min-w-[170px] bg-k-surface border border-k-subtle rounded-lg p-4 pl-5 relative flex flex-col overflow-hidden before:absolute before:left-0 before:top-0 before:bottom-0 before:w-[3px] before:bg-k-gold before:rounded-l-lg">
      <div class="text-[11px] font-medium uppercase tracking-wider text-k-muted mb-2">Node ID</div>
      <div class="font-mono text-[26px] font-semibold leading-none">{node_id}</div>
    </div>
    <div class="flex-1 min-w-[170px] bg-k-surface border border-k-subtle rounded-lg p-4 pl-5 relative flex flex-col overflow-hidden before:absolute before:left-0 before:top-0 before:bottom-0 before:w-[3px] before:bg-k-blue before:rounded-l-lg">
      <div class="text-[11px] font-medium uppercase tracking-wider text-k-muted mb-2">Node Type</div>
      <div class="font-mono text-[26px] font-semibold leading-none">{node_type}</div>
    </div>
    <div class="flex-1 min-w-[170px] bg-k-surface border border-k-subtle rounded-lg p-4 pl-5 relative flex flex-col overflow-hidden before:absolute before:left-0 before:top-0 before:bottom-0 before:w-[3px] before:bg-k-teal before:rounded-l-lg">
      <div class="text-[11px] font-medium uppercase tracking-wider text-k-muted mb-2">Clustering</div>
      <div class="mt-1">{status_badge}</div>
    </div>
  </div>

  <!-- Configured peers -->
  <div class="bg-k-surface border border-k-subtle rounded-lg overflow-hidden flex flex-col">
    <div class="flex items-center justify-between px-[18px] py-3 border-b border-k-subtle">
      <div class="text-[13px] font-semibold flex items-center gap-2">
        Configured Peers
        <span class="font-mono text-[11px] bg-k-overlay px-[7px] py-px rounded-full text-k-text2">{peer_count}</span>
      </div>
    </div>
    <div class="overflow-auto">
      {peers_table}
    </div>
  </div>

  {management_section}
</div>"##,
        node_id = html_escape(&node_id),
        node_type = html_escape(node_type),
        peer_count = config.cluster_peers.len() + config.cluster_learners.len(),
    );

    Html(layout::layout(
        "cluster",
        "Cluster",
        &state.config.node_name,
        &state.contexts.list_contexts(),
        &content,
    ))
}

// ── Cluster API handlers ──────────────────────────────────────────

#[derive(Deserialize)]
pub struct AddNodeRequest {
    id: u64,
    addr: String,
    #[serde(default = "default_context")]
    context: String,
}

fn default_context() -> String {
    "default".to_string()
}

#[derive(Deserialize)]
pub struct RemoveNodeRequest {
    id: u64,
    #[serde(default = "default_context")]
    context: String,
}

pub async fn api_add_learner(
    State(state): State<AdminState>,
    Json(req): Json<AddNodeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    state
        .cluster
        .add_learner(&req.context, req.id, req.addr.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({
        "status": "ok",
        "action": "add_learner",
        "node_id": req.id,
        "addr": req.addr,
    })))
}

pub async fn api_add_voter(
    State(state): State<AdminState>,
    Json(req): Json<AddNodeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    // First add as learner, then promote to voter.
    state
        .cluster
        .add_learner(&req.context, req.id, req.addr.clone())
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    // Get current voter IDs and add the new one.
    let raft = state
        .cluster
        .get_raft_node(&req.context)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "context not found".to_string()))?;

    let metrics = raft.metrics().borrow().clone();
    let mut voter_ids: Vec<u64> = metrics.membership_config.membership().voter_ids().collect();
    if !voter_ids.contains(&req.id) {
        voter_ids.push(req.id);
    }

    state
        .cluster
        .change_membership(&req.context, voter_ids)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({
        "status": "ok",
        "action": "add_voter",
        "node_id": req.id,
        "addr": req.addr,
    })))
}

pub async fn api_remove_node(
    State(state): State<AdminState>,
    Json(req): Json<RemoveNodeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    let raft = state
        .cluster
        .get_raft_node(&req.context)
        .ok_or_else(|| (StatusCode::NOT_FOUND, "context not found".to_string()))?;

    let metrics = raft.metrics().borrow().clone();
    let voter_ids: Vec<u64> = metrics
        .membership_config
        .membership()
        .voter_ids()
        .filter(|id| *id != req.id)
        .collect();

    state
        .cluster
        .change_membership(&req.context, voter_ids)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({
        "status": "ok",
        "action": "remove_node",
        "node_id": req.id,
    })))
}
