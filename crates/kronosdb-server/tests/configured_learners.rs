//! Learners named in `KRONOSDB_CLUSTER_LEARNERS` end up in the cluster's
//! membership, the way the Helm chart configures them: every pod, voters and
//! learners alike, gets the same learner list.
//!
//! Adding a learner only succeeds on the leader, and at startup nobody is
//! leader yet, so the nodes keep trying in the background until the
//! membership holds the learner.

use std::process::{Child, Command as ProcCommand, Stdio};
use std::time::{Duration, Instant};

mod support;
use support::free_port;

struct Node {
    child: Child,
    admin: String,
    /// Removed after `drop` has stopped the server.
    _dir: support::WorkDir,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn spawn_node(
    node_id: u64,
    node_type: &str,
    port: u16,
    admin_port: u16,
    voters: &str,
    learners: &str,
) -> Node {
    let dir = support::work_dir(&format!("learner-node-{node_id}"));
    let mut cmd = ProcCommand::new(env!("CARGO_BIN_EXE_kronosdb-server"));
    support::inherit_only_stdio(&mut cmd);
    cmd.env("KRONOSDB_LISTEN", format!("127.0.0.1:{port}"))
        .env("KRONOSDB_ADMIN_LISTEN", format!("127.0.0.1:{admin_port}"))
        .env("KRONOSDB_DATA_DIR", &*dir)
        .env("KRONOSDB_NODE_NAME", format!("learner-node-{node_id}"))
        .env("KRONOSDB_CLUSTER_NODE_ID", node_id.to_string())
        .env("KRONOSDB_CLUSTER_NODE_TYPE", node_type)
        .env("KRONOSDB_CLUSTER_PEERS", voters)
        .env("KRONOSDB_CLUSTER_LEARNERS", learners)
        .env("RUST_LOG", "warn");
    if std::env::var_os("KRONOSDB_TEST_LOGS").is_some() {
        cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    } else {
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
    }
    Node {
        child: cmd.spawn().expect("spawn kronosdb-server"),
        admin: format!("127.0.0.1:{admin_port}"),
        _dir: dir,
    }
}

/// The node ids in the membership `node` sees (voters and learners).
async fn members(node: &Node) -> Vec<u64> {
    let Ok(response) = reqwest::get(format!("http://{}/api/v1/cluster", node.admin)).await else {
        return Vec::new();
    };
    let Ok(body) = response.json::<serde_json::Value>().await else {
        return Vec::new();
    };
    body["raft"]["nodes"]
        .as_array()
        .map(|nodes| nodes.iter().filter_map(|n| n["id"].as_u64()).collect())
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_configured_learner_joins_the_membership() {
    let ports: Vec<u16> = (0..4).map(|_| free_port()).collect();
    let admins: Vec<u16> = (0..4).map(|_| free_port()).collect();
    let voters = (1..=3)
        .map(|id| format!("{id}=127.0.0.1:{}", ports[id - 1]))
        .collect::<Vec<_>>()
        .join(",");
    let learners = format!("4=127.0.0.1:{}", ports[3]);

    let mut nodes: Vec<Node> = (1..=3u64)
        .map(|id| {
            let i = id as usize - 1;
            spawn_node(id, "standard", ports[i], admins[i], &voters, &learners)
        })
        .collect();
    nodes.push(spawn_node(
        4,
        "passive-backup",
        ports[3],
        admins[3],
        &voters,
        &learners,
    ));

    // Every node's membership view ends up with all four.
    let deadline = Instant::now() + support::STARTUP;
    loop {
        let mut views = Vec::new();
        for node in &nodes {
            let mut ids = members(node).await;
            ids.sort_unstable();
            views.push(ids);
        }
        if views.iter().all(|ids| *ids == [1, 2, 3, 4]) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "learner 4 never joined everywhere; membership seen per node: {views:?}"
        );
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}
