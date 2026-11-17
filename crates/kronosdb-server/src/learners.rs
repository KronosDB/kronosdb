//! Keeps the configured learners (`KRONOSDB_CLUSTER_LEARNERS`) in the
//! cluster's membership.
//!
//! Adding a learner goes through openraft, which only accepts it on the
//! leader, and right after startup there may be no leader yet. Every node
//! runs one background task per configured learner; each tries until the
//! membership it sees contains the learner. Only the leader's attempt can
//! succeed, and membership replicates to the others, which then stop.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{debug, info, warn};

use kronosdb_eventstore::raft::cluster::{ClusterManager, PeerConfig};

const FIRST_DELAY: Duration = Duration::from_millis(250);
const MAX_DELAY: Duration = Duration::from_secs(5);
/// A learner that is still not a member after this long is worth a warning.
const WARN_AFTER: Duration = Duration::from_secs(60);

/// What one look at the cluster says about a learner.
pub(crate) enum Attempt {
    /// The learner is in the membership this node sees.
    Member,
    /// This node is not the leader, or there is none yet.
    NotLeader,
    /// The add was accepted.
    Added,
    /// The add was refused or failed.
    Failed(String),
}

/// Starts one background task per learner.
pub(crate) fn spawn(
    cluster: Arc<ClusterManager>,
    learners: Vec<PeerConfig>,
    stop: watch::Receiver<bool>,
) {
    for learner in learners {
        let cluster = Arc::clone(&cluster);
        let stop = stop.clone();
        tokio::spawn(async move {
            let id = learner.id;
            keep_adding(id, stop, || attempt(&cluster, &learner)).await;
        });
    }
}

async fn attempt(cluster: &ClusterManager, learner: &PeerConfig) -> Attempt {
    let Some(raft) = cluster.raft_node() else {
        return Attempt::NotLeader;
    };
    let metrics = raft.metrics().borrow().clone();
    if metrics
        .membership_config
        .membership()
        .nodes()
        .any(|(id, _)| *id == learner.id)
    {
        return Attempt::Member;
    }
    if metrics.current_leader != Some(cluster.local_node_id()) {
        return Attempt::NotLeader;
    }
    match cluster.add_learner(learner.id, learner.addr.clone()).await {
        Ok(()) => Attempt::Added,
        Err(e) => Attempt::Failed(e.to_string()),
    }
}

/// Calls `attempt` with backoff until the learner is a member or `stop`
/// fires.
pub(crate) async fn keep_adding<F, Fut>(id: u64, mut stop: watch::Receiver<bool>, mut attempt: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Attempt>,
{
    let started = tokio::time::Instant::now();
    let mut delay = FIRST_DELAY;
    let mut last_warn: Option<tokio::time::Instant> = None;
    loop {
        if *stop.borrow() {
            return;
        }
        match attempt().await {
            Attempt::Member => return,
            Attempt::Added => {
                info!(learner = id, "added configured learner to the cluster");
                return;
            }
            Attempt::NotLeader => {
                debug!(learner = id, "no leader here yet; not adding learner");
            }
            Attempt::Failed(reason) => {
                debug!(learner = id, %reason, "adding learner failed; retrying");
                let now = tokio::time::Instant::now();
                let quiet = last_warn.is_none_or(|t| now - t >= WARN_AFTER);
                if now - started >= WARN_AFTER && quiet {
                    last_warn = Some(now);
                    warn!(learner = id, %reason, "learner still not added to the cluster; retrying");
                }
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = stop.changed() => {}
        }
        delay = (delay * 2).min(MAX_DELAY);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test(start_paused = true)]
    async fn retries_until_added_then_stops() {
        let (_tx, rx) = watch::channel(false);
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        keep_adding(7, rx, move || {
            let n = seen.fetch_add(1, Ordering::SeqCst);
            async move {
                match n {
                    0 | 1 => Attempt::NotLeader,
                    2 => Attempt::Failed("boom".into()),
                    _ => Attempt::Added,
                }
            }
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn stops_once_membership_has_it() {
        let (_tx, rx) = watch::channel(false);
        let calls = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&calls);
        keep_adding(7, rx, move || {
            let n = seen.fetch_add(1, Ordering::SeqCst);
            async move {
                if n < 1 {
                    Attempt::NotLeader
                } else {
                    Attempt::Member
                }
            }
        })
        .await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn stops_on_shutdown() {
        let (tx, rx) = watch::channel(false);
        let task = tokio::spawn(keep_adding(7, rx, || async { Attempt::NotLeader }));
        tokio::time::sleep(Duration::from_secs(1)).await;
        tx.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("loop stops on shutdown")
            .unwrap();
    }
}
