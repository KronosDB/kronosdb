//! A voter that starts empty abstains until it is whole.
//!
//! Raft and the native claim both assume that a voter remembers: the vote it
//! gave, and every event it acknowledged. A voter that comes back with an
//! empty data directory (a replaced disk under the same node id) remembers
//! neither, and nothing in its configuration says so. Treated as a full voter
//! it can vote a second time in a term, and it can stand in a leader claim's
//! quorum for events it no longer holds.
//!
//! Such a node is *amnesiac*. While it is, it
//!
//! - does not initialize a Raft group,
//! - refuses every vote request,
//! - reports claim epoch 0 in its cursor answers, so no leader claim counts it
//!   in the quorum it reconciles against.
//!
//! It still follows a leader, runs its Tail sessions and acknowledges what it
//! really holds: those describe its present state, which is true. It stops
//! being amnesiac once every context has caught up with a leader.
//!
//! The state is a marker file, so a node that restarts before it is whole is
//! still amnesiac. The one thing that looks the same as a wipe is a cluster's
//! first start, where every voter is empty; [`rule_on_vote`] and
//! `ClusterManager::bootstrap` tell the two apart.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

use openraft::raft::VoteRequest;
use parking_lot::Mutex;

use super::types::NodeId;
use crate::replication::control::ReplicationControl;

/// The marker's name in the data directory. A file, so the context scan
/// (directories only) never takes it for a context.
const MARKER: &str = "amnesiac";

/// What an amnesiac node does with a vote request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoteRuling {
    /// Not amnesiac: Raft decides.
    Normal,
    /// Amnesiac: not granted.
    Refuse,
    /// Amnesiac, and the request can only come from a cluster that has never
    /// had a leader: stop being amnesiac, then let Raft decide.
    FirstStart,
}

/// Whether a node is amnesiac as it starts.
///
/// `marker` is whether the marker file exists, `has_raft_state` whether the
/// Raft stores recovered anything, `other_voters` whether the configuration
/// names a voter besides this node. A lone voter has nobody to catch up with
/// and nobody to mislead. A node with Raft state and no marker is one that was
/// never wiped, or one that finished catching up; that includes every node of
/// a cluster that predates the marker.
pub fn starts_amnesiac(marker: bool, has_raft_state: bool, other_voters: bool) -> bool {
    other_voters && (marker || !has_raft_state)
}

/// Whether `<data_dir>/raft` holds anything a Raft node keeps: its vote, its
/// log, its purge and commit marks, a snapshot, the applied membership or
/// control state. Looked at before the stores open, because they create the
/// directories (and nothing else) as they do.
///
/// A `.tmp` file is a write that was never renamed into place, so nothing was
/// answered on the strength of it: it is not state.
pub fn raft_state_exists(raft_dir: &Path) -> io::Result<bool> {
    Ok(holds_state_file(raft_dir)? || holds_state_file(&raft_dir.join("snapshots"))?)
}

fn holds_state_file(dir: &Path) -> io::Result<bool> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error),
    };
    for entry in entries {
        let entry = entry?;
        let is_tmp = entry.path().extension().is_some_and(|ext| ext == "tmp");
        if entry.file_type()?.is_file() && !is_tmp {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Whether a voter's cursor answer shows no trace of a cluster that has run:
/// it vouches for no claim (`control_epoch`), no context has adopted an epoch
/// (`epoch`, the lowest among them), and each context, given as
/// `(next position, log epoch, watermark)`, holds no event, no `EpochChange`
/// and no committed position.
///
/// An event is acknowledged only after a majority of voters hold it together
/// with the `EpochChange` it was written under. So if one voter lost its disk
/// and every other voter looks like this, no event was ever acknowledged.
pub fn looks_empty(
    control_epoch: u64,
    epoch: u64,
    contexts: impl IntoIterator<Item = (u64, u64, u64)>,
) -> bool {
    control_epoch == 0
        && epoch == 0
        && contexts
            .into_iter()
            .all(|(position, log_epoch, watermark)| {
                position == 0 && log_epoch == 0 && watermark == 0
            })
}

/// What the lowest-id voter does when it has no Raft state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FirstStart {
    /// Every other voter answered and is empty: the cluster is new.
    Initialize,
    /// These voters have not answered: ask again. A voter that does not
    /// answer may be the one that holds the log.
    Wait { unanswered: Vec<NodeId> },
    /// This voter holds data: the cluster has run, and this node lost its
    /// data directory. It never initializes; it catches up.
    NotNew { holder: NodeId },
}

/// Decides whether the lowest-id voter may initialize a cluster. `others` is
/// every other configured voter with its answer: `None` when it did not
/// answer, otherwise whether it [`looks_empty`].
///
/// Initializing is only right when the cluster is new. One voter that holds
/// data settles that it is not, whoever else is silent; otherwise every
/// voter has to have answered, because the silent one may be the holder.
pub fn decide_first_start(others: &[(NodeId, Option<bool>)]) -> FirstStart {
    if let Some((holder, _)) = others.iter().find(|(_, empty)| *empty == Some(false)) {
        return FirstStart::NotNew { holder: *holder };
    }
    let unanswered: Vec<NodeId> = others
        .iter()
        .filter(|(_, empty)| empty.is_none())
        .map(|(node_id, _)| *node_id)
        .collect();
    if unanswered.is_empty() {
        FirstStart::Initialize
    } else {
        FirstStart::Wait { unanswered }
    }
}

/// What a node does with a vote request.
///
/// An amnesiac node grants nothing, with one exception that lets a new
/// cluster form: the candidate is the configured lowest-id voter (the only
/// node that initializes a cluster) and its log holds nothing beyond the
/// initial membership entry. `Raft::initialize` writes that entry at index 0
/// and campaigns; the first leader appends at index 1 as it takes over, and
/// no other voter can lead before it has. So such a request comes from a
/// cluster in which nothing was ever committed, and there is nothing this
/// node could have forgotten. The bootstrapper never qualifies for its own
/// exception.
pub fn rule_on_vote(
    amnesiac: bool,
    node_id: NodeId,
    bootstrapper: NodeId,
    request: &VoteRequest<NodeId>,
) -> VoteRuling {
    if !amnesiac {
        return VoteRuling::Normal;
    }
    let from_bootstrapper = request.vote.leader_id().voted_for() == Some(bootstrapper);
    let nothing_beyond_membership = request.last_log_id.is_none_or(|log_id| log_id.index == 0);
    if node_id != bootstrapper && from_bootstrapper && nothing_beyond_membership {
        VoteRuling::FirstStart
    } else {
        VoteRuling::Refuse
    }
}

/// This node's amnesia: the marker file and the flag the request paths read.
pub struct Amnesia {
    marker: PathBuf,
    node_id: NodeId,
    /// The configured lowest-id voter, which initializes a new cluster.
    bootstrapper: NodeId,
    other_voters: bool,
    amnesiac: AtomicBool,
    /// Entering and leaving touch the file and the flag together.
    transition: Mutex<()>,
}

impl Amnesia {
    /// Not amnesiac until [`Amnesia::begin`] has looked at the disk.
    pub fn new(data_dir: &Path, node_id: NodeId, voters: &[NodeId]) -> Self {
        Self {
            marker: data_dir.join(MARKER),
            node_id,
            bootstrapper: voters.iter().copied().min().unwrap_or(node_id),
            other_voters: voters.iter().any(|voter| *voter != node_id),
            amnesiac: AtomicBool::new(false),
            transition: Mutex::new(()),
        }
    }

    /// Decides at startup whether this node is amnesiac, and records it
    /// durably before Raft runs. `has_raft_state` is what the Raft stores
    /// recovered from `<data_dir>/raft`.
    pub fn begin(&self, has_raft_state: bool) -> io::Result<bool> {
        let _guard = self.transition.lock();
        let marker = self.marker.exists();
        if !starts_amnesiac(marker, has_raft_state, self.other_voters) {
            if marker {
                // A lone voter: the marker is from a configuration that had
                // other voters, and there is nobody left to catch up with.
                remove_marker(&self.marker)?;
                tracing::warn!(
                    marker = %self.marker.display(),
                    "removed an amnesia marker: this node is configured as the only voter, so it \
                     has nobody to catch up with and serves what it holds"
                );
            }
            return Ok(false);
        }
        if !marker {
            write_marker(&self.marker).map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!(
                        "could not write {}: {error}. This node started with no Raft state and \
                         has to record that before it joins the cluster; make the data directory \
                         writable and start it again",
                        self.marker.display()
                    ),
                )
            })?;
        }
        self.amnesiac.store(true, Ordering::Release);
        if marker {
            tracing::warn!(
                marker = %self.marker.display(),
                "this node restarted before it had caught up after starting empty: it still \
                 does not vote, does not count towards a leader's claim and does not initialize \
                 a cluster. That ends by itself once every context has caught up with a leader"
            );
        } else {
            tracing::warn!(
                marker = %self.marker.display(),
                "this node started with no Raft state while its configuration names other \
                 voters: either the cluster is new, or this node lost its data directory. Until \
                 it is known to hold the whole log it does not vote, does not count towards a \
                 leader's claim and does not initialize a cluster; it follows a leader and \
                 catches up. That ends by itself once every context has caught up with a leader, \
                 or, in a new cluster, once the lowest-id voter has found every voter empty"
            );
        }
        Ok(true)
    }

    pub fn is_amnesiac(&self) -> bool {
        self.amnesiac.load(Ordering::Acquire)
    }

    /// Whether this node is the one that initializes a new cluster.
    pub fn is_bootstrapper(&self) -> bool {
        self.node_id == self.bootstrapper
    }

    /// Stops being amnesiac: removes the marker durably, then clears the
    /// flag. `because` finishes the sentence "this node is whole again: ".
    /// The removal is one fsync of the data directory, once in a node's life,
    /// so callers on the async runtime do not hand it to a blocking thread.
    /// On an error the node stays amnesiac, which is the safe side; the
    /// caller tries again.
    pub fn leave(&self, because: &str) -> io::Result<()> {
        let _guard = self.transition.lock();
        if !self.is_amnesiac() {
            return Ok(());
        }
        remove_marker(&self.marker)?;
        self.amnesiac.store(false, Ordering::Release);
        tracing::info!(
            "this node is whole again: {because}. It votes and counts towards a leader's claim \
             from now on"
        );
        Ok(())
    }

    /// What to do with a vote request ([`rule_on_vote`]).
    pub fn rule_on_vote(&self, request: &VoteRequest<NodeId>) -> VoteRuling {
        rule_on_vote(self.is_amnesiac(), self.node_id, self.bootstrapper, request)
    }

    /// The claim epoch this node vouches for in a cursor answer. A claimant
    /// counts only voters that report its own epoch, so 0 keeps an amnesiac
    /// node out of the quorum the claim is reconciled against: its cursors
    /// are true, but they say nothing about what it acknowledged before it
    /// lost its disk.
    pub fn vouched_epoch(&self, control: &ReplicationControl) -> u64 {
        if self.is_amnesiac() {
            return 0;
        }
        control.claim().map(|claim| claim.epoch).unwrap_or(0)
    }
}

fn write_marker(path: &Path) -> io::Result<()> {
    let mut file = std::fs::File::create(path)?;
    file.write_all(
        b"This node started with no Raft state while other voters were configured.\n\
          Until every context has caught up with a leader it does not vote and does\n\
          not count towards a leader's claim. KronosDB removes this file itself.\n",
    )?;
    file.sync_all()?;
    sync_parent(path)
}

fn remove_marker(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    sync_parent(path)
}

/// Makes a file's creation or removal durable.
fn sync_parent(path: &Path) -> io::Result<()> {
    match path.parent() {
        Some(parent) => std::fs::File::open(parent)?.sync_all(),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, LogId, Vote};

    fn request(term: u64, candidate: NodeId, last_index: Option<u64>) -> VoteRequest<NodeId> {
        VoteRequest::new(
            Vote::new(term, candidate),
            last_index.map(|index| LogId::new(CommittedLeaderId::new(0, 0), index)),
        )
    }

    #[test]
    fn a_node_is_amnesiac_when_it_has_no_raft_state_and_other_voters() {
        assert!(starts_amnesiac(false, false, true));
        // A cluster that predates the marker, or a node that caught up.
        assert!(!starts_amnesiac(false, true, true));
        // Restarted before it was whole: the state it has is partial.
        assert!(starts_amnesiac(true, true, true));
        assert!(starts_amnesiac(true, false, true));
    }

    #[test]
    fn a_lone_voter_is_never_amnesiac() {
        for marker in [false, true] {
            for has_raft_state in [false, true] {
                assert!(!starts_amnesiac(marker, has_raft_state, false));
            }
        }
    }

    #[test]
    fn a_fresh_raft_directory_holds_no_state() {
        let dir = tempfile::tempdir().unwrap();
        let raft = dir.path().join("raft");
        // Not there at all.
        assert!(!raft_state_exists(&raft).unwrap());
        // As the stores leave it before anything is written.
        std::fs::create_dir_all(raft.join("snapshots")).unwrap();
        assert!(!raft_state_exists(&raft).unwrap());
        // A write that never completed.
        std::fs::write(raft.join("vote.tmp"), b"x").unwrap();
        assert!(!raft_state_exists(&raft).unwrap());
    }

    #[test]
    fn any_file_a_raft_node_keeps_is_state() {
        for file in [
            "vote.bin",
            "committed.bin",
            "purged.bin",
            "log-0000000000000000.bin",
            "snapshots/membership.bin",
            "snapshots/control-state.bin",
        ] {
            let dir = tempfile::tempdir().unwrap();
            let raft = dir.path().join("raft");
            std::fs::create_dir_all(raft.join("snapshots")).unwrap();
            std::fs::write(raft.join(file), b"x").unwrap();
            assert!(raft_state_exists(&raft).unwrap(), "{file}");
        }
    }

    /// What the real stores leave behind: nothing when they open on an empty
    /// directory, a vote once one is saved.
    #[tokio::test]
    async fn the_stores_create_no_state_until_raft_writes_some() {
        use crate::raft::log_store::{LogStore, LogStoreConfig};
        use crate::raft::snapshot_store::SnapshotStore;
        use openraft::storage::RaftLogStorage;

        let dir = tempfile::tempdir().unwrap();
        let raft = dir.path().join("raft");
        let mut log = LogStore::new(&raft, LogStoreConfig::default()).unwrap();
        let _snapshots = SnapshotStore::new(raft.join("snapshots")).unwrap();
        assert!(!raft_state_exists(&raft).unwrap());

        log.save_vote(&Vote::new(1, 1)).await.unwrap();
        assert!(raft_state_exists(&raft).unwrap());
    }

    #[test]
    fn a_voter_is_empty_only_without_any_trace_of_a_cluster() {
        assert!(looks_empty(0, 0, []));
        assert!(looks_empty(0, 0, [(0, 0, 0), (0, 0, 0)]));
        // It has seen a leader's claim.
        assert!(!looks_empty(3, 0, [(0, 0, 0)]));
        // A context adopted an epoch.
        assert!(!looks_empty(0, 3, [(0, 0, 0)]));
        // One context holds an event, an EpochChange, a committed position.
        assert!(!looks_empty(0, 0, [(0, 0, 0), (1, 0, 0)]));
        assert!(!looks_empty(0, 0, [(0, 3, 0)]));
        assert!(!looks_empty(0, 0, [(0, 0, 1)]));
    }

    #[test]
    fn the_bootstrapper_initializes_only_when_every_voter_is_empty() {
        assert_eq!(
            decide_first_start(&[(2, Some(true)), (3, Some(true))]),
            FirstStart::Initialize
        );
    }

    #[test]
    fn the_bootstrapper_waits_for_a_voter_that_has_not_answered() {
        assert_eq!(
            decide_first_start(&[(2, Some(true)), (3, None)]),
            FirstStart::Wait {
                unanswered: vec![3]
            }
        );
        assert_eq!(
            decide_first_start(&[(2, None), (3, None)]),
            FirstStart::Wait {
                unanswered: vec![2, 3]
            }
        );
    }

    #[test]
    fn one_voter_with_data_means_the_cluster_is_not_new() {
        assert_eq!(
            decide_first_start(&[(2, Some(true)), (3, Some(false))]),
            FirstStart::NotNew { holder: 3 }
        );
        // Whoever else is silent.
        assert_eq!(
            decide_first_start(&[(2, None), (3, Some(false))]),
            FirstStart::NotNew { holder: 3 }
        );
    }

    #[test]
    fn a_node_that_is_not_amnesiac_leaves_votes_to_raft() {
        assert_eq!(
            rule_on_vote(false, 2, 1, &request(7, 3, Some(40))),
            VoteRuling::Normal
        );
    }

    #[test]
    fn an_amnesiac_node_refuses_a_candidate_with_a_log() {
        // The other voter of a cluster that has lived.
        assert_eq!(
            rule_on_vote(true, 2, 1, &request(7, 3, Some(40))),
            VoteRuling::Refuse
        );
        // The bootstrapper of a cluster that has lived: its log is past the
        // membership entry.
        assert_eq!(
            rule_on_vote(true, 2, 1, &request(7, 1, Some(1))),
            VoteRuling::Refuse
        );
    }

    #[test]
    fn an_amnesiac_node_grants_only_the_bootstrapper_of_a_new_cluster() {
        // `initialize` wrote the membership at index 0 and campaigns.
        assert_eq!(
            rule_on_vote(true, 2, 1, &request(1, 1, Some(0))),
            VoteRuling::FirstStart
        );
        assert_eq!(
            rule_on_vote(true, 3, 1, &request(4, 1, None)),
            VoteRuling::FirstStart
        );
        // Somebody else with an empty log is not starting a cluster.
        assert_eq!(
            rule_on_vote(true, 2, 1, &request(1, 3, Some(0))),
            VoteRuling::Refuse
        );
        assert_eq!(
            rule_on_vote(true, 2, 1, &request(1, 3, None)),
            VoteRuling::Refuse
        );
    }

    #[test]
    fn an_amnesiac_bootstrapper_grants_nobody() {
        for candidate in [1, 2, 3] {
            for last_index in [None, Some(0), Some(9)] {
                assert_eq!(
                    rule_on_vote(true, 1, 1, &request(3, candidate, last_index)),
                    VoteRuling::Refuse,
                    "candidate {candidate}, last index {last_index:?}"
                );
            }
        }
    }

    #[test]
    fn an_empty_voter_writes_the_marker_and_a_restart_finds_it() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join(MARKER);

        let first = Amnesia::new(dir.path(), 2, &[1, 2, 3]);
        assert!(!first.is_amnesiac());
        assert!(first.begin(false).unwrap());
        assert!(first.is_amnesiac());
        assert!(marker.is_file());

        // It restarts having received some Raft state, but not the log.
        let restarted = Amnesia::new(dir.path(), 2, &[1, 2, 3]);
        assert!(restarted.begin(true).unwrap());
        assert!(restarted.is_amnesiac());

        restarted.leave("test").unwrap();
        assert!(!restarted.is_amnesiac());
        assert!(!marker.exists());
        // Leaving twice is nothing.
        restarted.leave("test").unwrap();

        // With its Raft state and no marker it is an ordinary voter.
        let whole = Amnesia::new(dir.path(), 2, &[1, 2, 3]);
        assert!(!whole.begin(true).unwrap());
        assert!(!whole.is_amnesiac());
        assert!(!marker.exists());
    }

    #[test]
    fn a_node_with_raft_state_and_no_marker_writes_none() {
        let dir = tempfile::tempdir().unwrap();
        let amnesia = Amnesia::new(dir.path(), 1, &[1, 2, 3]);
        assert!(!amnesia.begin(true).unwrap());
        assert!(!dir.path().join(MARKER).exists());
    }

    #[test]
    fn a_lone_voter_writes_no_marker_and_drops_a_stale_one() {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join(MARKER);
        let amnesia = Amnesia::new(dir.path(), 1, &[1]);
        assert!(!amnesia.begin(false).unwrap());
        assert!(!marker.exists());

        std::fs::write(&marker, b"left over").unwrap();
        let amnesia = Amnesia::new(dir.path(), 1, &[1]);
        assert!(!amnesia.begin(false).unwrap());
        assert!(!amnesia.is_amnesiac());
        assert!(!marker.exists());
    }

    #[test]
    fn a_learner_beside_voters_starts_amnesiac() {
        // Not in the voter list itself: every voter is another node.
        let dir = tempfile::tempdir().unwrap();
        let amnesia = Amnesia::new(dir.path(), 9, &[1, 2, 3]);
        assert!(amnesia.begin(false).unwrap());
        assert!(!amnesia.is_bootstrapper());
    }

    #[test]
    fn an_amnesiac_node_vouches_for_no_claim() {
        let dir = tempfile::tempdir().unwrap();
        let control = ReplicationControl::new(2, vec![1, 2, 3]);
        control.observe_claim(7, 1, 3);
        let amnesia = Amnesia::new(dir.path(), 2, &[1, 2, 3]);
        assert_eq!(amnesia.vouched_epoch(&control), 7);

        amnesia.begin(false).unwrap();
        assert_eq!(amnesia.vouched_epoch(&control), 0);

        amnesia.leave("test").unwrap();
        assert_eq!(amnesia.vouched_epoch(&control), 7);
    }

    #[test]
    fn the_marker_cannot_be_written_into_a_missing_directory() {
        let dir = tempfile::tempdir().unwrap();
        let amnesia = Amnesia::new(&dir.path().join("gone"), 2, &[1, 2, 3]);
        let error = amnesia.begin(false).unwrap_err();
        assert!(error.to_string().contains("could not write"), "{error}");
        assert!(!amnesia.is_amnesiac());
    }
}
