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
        r#"<span class="badge font-mono bg-k-teal-d text-k-teal" data-variant="secondary"><span class="w-1.5 h-1.5 rounded-full bg-k-teal"></span>enabled</span>"#
    } else {
        r#"<span class="badge font-mono bg-k-amber-d text-k-amber" data-variant="secondary"><span class="w-1.5 h-1.5 rounded-full bg-k-amber"></span>standalone</span>"#
    };

    // Peer info from config
    let mut peer_rows = String::new();
    for peer in &config.cluster_peers {
        peer_rows.push_str(&format!(
            r#"<tr id="peer-{id}"><td class="font-mono text-xs !text-k-text">{id}</td><td class="font-mono text-xs">{addr}</td><td><span class="badge font-mono bg-k-blue-d text-k-blue" data-variant="secondary">voter</span></td></tr>"#,
            id = peer.id,
            addr = html_escape(&peer.addr),
        ));
    }
    for peer in &config.cluster_learners {
        peer_rows.push_str(&format!(
            r#"<tr id="peer-{id}"><td class="font-mono text-xs !text-k-text">{id}</td><td class="font-mono text-xs">{addr}</td><td><span class="badge font-mono bg-k-overlay text-k-text2" data-variant="secondary">learner</span></td></tr>"#,
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
  <div class="card gap-0 py-0 mt-4">
    <header class="py-3 border-b border-k-subtle">
      <h2 class="text-[13px] font-semibold">Membership Management</h2>
    </header>
    <section class="py-4">
      <div class="flex flex-wrap gap-4">
        <!-- Add Learner -->
        <div class="flex-1 min-w-[280px]">
          <div class="text-[11px] font-semibold uppercase tracking-[0.6px] text-k-muted mb-2">Add Learner</div>
          <form id="add-learner-form" class="flex flex-col gap-3"
            onsubmit="event.preventDefault(); clusterAction('/api/cluster/add-learner', 'add-learner-form', 'add-learner-result');">
            <div class="flex flex-col gap-1.5">
              <label class="label" for="add-learner-id">Node ID</label>
              <input id="add-learner-id" class="input font-mono" type="number" name="id" placeholder="Node ID" required min="1">
            </div>
            <div class="flex flex-col gap-1.5">
              <label class="label" for="add-learner-addr">Address</label>
              <input id="add-learner-addr" class="input font-mono" type="text" name="addr" placeholder="host:port" required>
            </div>
            <button type="submit" class="btn w-fit" data-variant="outline" data-size="sm">
              Add Learner
            </button>
            <span id="add-learner-result" class="text-xs"></span>
          </form>
        </div>
        <!-- Add Voter -->
        <div class="flex-1 min-w-[280px]">
          <div class="text-[11px] font-semibold uppercase tracking-[0.6px] text-k-muted mb-2">Promote to Voter</div>
          <form id="add-voter-form" class="flex flex-col gap-3"
            onsubmit="event.preventDefault(); clusterAction('/api/cluster/add-voter', 'add-voter-form', 'add-voter-result');">
            <div class="flex flex-col gap-1.5">
              <label class="label" for="add-voter-id">Node ID</label>
              <input id="add-voter-id" class="input font-mono" type="number" name="id" placeholder="Node ID" required min="1">
            </div>
            <div class="flex flex-col gap-1.5">
              <label class="label" for="add-voter-addr">Address</label>
              <input id="add-voter-addr" class="input font-mono" type="text" name="addr" placeholder="host:port" required>
            </div>
            <button type="submit" class="btn w-fit" data-variant="outline" data-size="sm">
              Promote to Voter
            </button>
            <span id="add-voter-result" class="text-xs"></span>
          </form>
        </div>
        <!-- Remove Node -->
        <div class="flex-1 min-w-[280px]">
          <div class="text-[11px] font-semibold uppercase tracking-[0.6px] text-k-muted mb-2">Remove Node</div>
          <form id="remove-node-form" class="flex flex-col gap-3"
            onsubmit="event.preventDefault(); clusterAction('/api/cluster/remove-node', 'remove-node-form', 'remove-node-result');">
            <div class="flex flex-col gap-1.5">
              <label class="label" for="remove-node-id">Node ID</label>
              <input id="remove-node-id" class="input font-mono" type="number" name="id" placeholder="Node ID" required min="1">
            </div>
            <button type="submit" class="btn w-fit" data-variant="destructive" data-size="sm">
              Remove Node
            </button>
            <span id="remove-node-result" class="text-xs"></span>
          </form>
        </div>
      </div>
    </section>
  </div>

  <script>
  async function clusterAction(url, formId, resultId) {
    var form = document.getElementById(formId);
    var result = document.getElementById(resultId);
    var data = {};
    new FormData(form).forEach(function(v, k) { data[k] = k === 'id' ? parseInt(v) : v; });
    try {
      var resp = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(data) });
      var text = await resp.text();
      var json = null;
      try { json = JSON.parse(text); } catch (ignored) {}
      if (resp.ok) {
        result.innerHTML = '<span class="text-k-teal">Success: ' + (json && json.action || 'ok') + '</span>';
      } else {
        result.innerHTML = '<span class="text-k-red"></span>';
        result.firstChild.textContent = 'Error: ' + ((json && json.message) || text || resp.statusText);
      }
    } catch (e) {
      result.innerHTML = '<span class="text-k-red"></span>';
      result.firstChild.textContent = 'Error: ' + e.message;
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
    <div class="card flex-1 min-w-[170px] gap-2 py-4 pl-5 pr-4 relative overflow-hidden before:absolute before:left-0 before:top-0 before:bottom-0 before:w-[3px] before:bg-k-gold">
      <div class="text-[11px] font-medium uppercase tracking-wider text-k-muted">Node ID</div>
      <div class="font-mono text-[26px] font-semibold leading-none">{node_id}</div>
    </div>
    <div class="card flex-1 min-w-[170px] gap-2 py-4 pl-5 pr-4 relative overflow-hidden before:absolute before:left-0 before:top-0 before:bottom-0 before:w-[3px] before:bg-k-blue">
      <div class="text-[11px] font-medium uppercase tracking-wider text-k-muted">Node Type</div>
      <div class="font-mono text-[26px] font-semibold leading-none">{node_type}</div>
    </div>
    <div class="card flex-1 min-w-[170px] gap-2 py-4 pl-5 pr-4 relative overflow-hidden before:absolute before:left-0 before:top-0 before:bottom-0 before:w-[3px] before:bg-k-teal">
      <div class="text-[11px] font-medium uppercase tracking-wider text-k-muted">Clustering</div>
      <div>{status_badge}</div>
    </div>
  </div>

  <!-- Configured peers -->
  <div class="card gap-0 py-0 overflow-hidden">
    <header class="py-3 border-b border-k-subtle">
      <h2 class="text-[13px] font-semibold flex items-center gap-2">
        Configured Peers
        <span class="badge font-mono" data-variant="secondary">{peer_count}</span>
      </h2>
    </header>
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
    /// Promote even when the node's replication lag exceeds the safe
    /// threshold. A cold voter joins the watermark quorum immediately, so
    /// commit acknowledgements stall until it catches up.
    #[serde(default)]
    force: bool,
}

/// Maximum per-context position lag at which promotion is considered safe.
/// The residual gap closes within one replication round-trip, so quorum
/// commit latency is unaffected by the new voter.
const PROMOTE_MAX_LAG: u64 = 1024;

#[derive(Deserialize)]
pub struct RemoveNodeRequest {
    id: u64,
}

pub async fn api_add_learner(
    State(state): State<AdminState>,
    Json(req): Json<AddNodeRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    state
        .cluster
        .add_learner(req.id, req.addr.clone())
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
    let raft = state
        .cluster
        .raft_node()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "raft not initialized".to_string()))?;

    let metrics = raft.metrics().borrow().clone();
    let mut voter_ids: Vec<u64> = metrics.membership_config.membership().voter_ids().collect();
    let already_member = metrics
        .membership_config
        .membership()
        .nodes()
        .any(|(id, _)| *id == req.id);

    if !voter_ids.contains(&req.id) {
        // Gate BEFORE any membership mutation. A voter joins the watermark
        // quorum the moment membership changes, so promoting a node that is
        // still catching up stalls every commit acknowledgement until its
        // cursor reaches the tail — and change_membership itself would block
        // on a down or frozen node (joint consensus needs the new quorum).
        // Refuse unless the node is within one replication round of caught-up;
        // a node with no recorded progress (never added as learner, or never
        // connected) is refused without touching membership at all.
        if !req.force {
            if !state.cluster.is_claimed_leader() {
                return Err((
                    StatusCode::CONFLICT,
                    "this node does not hold the data-plane leader claim, so \
                     replication progress is unknown here; call the leader's \
                     admin API, or pass force=true to promote anyway"
                        .to_string(),
                ));
            }
            let status = state
                .cluster
                .replication_catchup_status(req.id)
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            let lagging: Vec<String> = status
                .iter()
                .filter_map(|ctx| {
                    if ctx.leader_tail == 0 {
                        return None;
                    }
                    match ctx.follower_position {
                        Some(pos) if ctx.leader_tail.saturating_sub(pos) <= PROMOTE_MAX_LAG => None,
                        Some(pos) => Some(format!(
                            "{}: {}/{} ({} behind)",
                            ctx.context,
                            pos,
                            ctx.leader_tail,
                            ctx.leader_tail - pos
                        )),
                        None => Some(format!(
                            "{}: no replication acknowledgement observed (leader tail {})",
                            ctx.context, ctx.leader_tail
                        )),
                    }
                })
                .collect();
            if !lagging.is_empty() {
                return Err((
                    StatusCode::CONFLICT,
                    format!(
                        "node {} is not caught up — promoting now would stall \
                         commit acknowledgements until it is. Lagging contexts: \
                         [{}]. Wait for catch-up and retry, or pass force=true.",
                        req.id,
                        lagging.join(", ")
                    ),
                ));
            }
        }
        // Gate passed (or forced): register as learner first if needed, then
        // promote. add_learner is non-blocking, so a forced promotion of a
        // down node fails in change_membership rather than hanging here.
        if !already_member {
            state
                .cluster
                .add_learner(req.id, req.addr.clone())
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }
        voter_ids.push(req.id);
    }

    state
        .cluster
        .change_membership(voter_ids)
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
        .raft_node()
        .ok_or_else(|| (StatusCode::NOT_FOUND, "raft not initialized".to_string()))?;

    let metrics = raft.metrics().borrow().clone();
    let voter_ids: Vec<u64> = metrics
        .membership_config
        .membership()
        .voter_ids()
        .filter(|id| *id != req.id)
        .collect();

    state
        .cluster
        .change_membership(voter_ids)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(serde_json::json!({
        "status": "ok",
        "action": "remove_node",
        "node_id": req.id,
    })))
}
