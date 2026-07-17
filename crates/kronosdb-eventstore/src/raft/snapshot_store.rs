//! On-disk persistence for Raft state-machine snapshots.
//!
//! # Why this exists
//!
//! openraft purges Raft log entries past a snapshot's `last_log_id`. The
//! cluster-init `Membership` entry sits at index 1, so as soon as the first
//! auto-snapshot fires and purge runs, that entry leaves the log forever.
//! From then on, the only durable carrier of `last_membership` is the
//! snapshot's `meta.last_membership` — and that has to live on disk so it
//! survives restart. Without on-disk snapshots, `applied_state()` would
//! return `StoredMembership::default()` after restart, openraft's startup
//! membership lookup (`helper.rs::get_membership`) would fall back to that
//! empty membership, and the node would default to Learner state. Learners
//! never campaign, so the node refuses to elect a leader and writes hang.
//!
//! See: regression test `restart_after_snapshot_single_node.rs`.
//!
//! # On-disk layout
//!
//! A snapshot remains a PAIR of files so openraft can stream its data handle
//! independently from `SnapshotMeta`:
//!
//! - `snap-<index:020>-<term:020>.meta` — `KRSN` header + bincoded
//!   `PersistedMeta` + trailing CRC32C.
//! - `snap-<index:020>-<term:020>.data` — the small `KSM4` metadata payload
//!   from `snapshot_format.rs`. It never contains events.
//!
//! `control-state.bin` durably records the same application metadata together
//! with `last_applied` and membership after every apply batch. This closes the
//! restart window before the next snapshot without coupling Raft progress to
//! event-segment markers.
//!
//! ```text
//! .meta: | magic "KRSN" (4) | ver(1)=2 | rsv(3) | meta_len u32 LE | meta | crc32c |
//! ```
//!
//! # Write protocol + crash points
//!
//! Data is written first (to `*.data.tmp`, fsync, rename), then meta
//! (`*.meta.tmp`, fsync, rename), then the dir is fsynced. A snapshot
//! "exists" iff its `.meta` file is valid AND its `.data` sibling is
//! present — so a crash mid-protocol leaves either nothing visible (data
//! without meta) or a complete pair. After a successful commit, older
//! pairs and stray tmp files are removed; `load_latest_meta` picks the
//! highest `(index, term)` pair, so stragglers from a crashed cleanup are
//! harmless.
//!
//! # Concurrency
//!
//! `SnapshotStore` is `Send + Sync`. openraft serializes snapshot building
//! through its single state-machine worker, so concurrent `commit_snapshot`
//! calls do not occur in practice. Readers racing a commit see either the
//! old or the new pair thanks to the rename-based protocol; a reader that
//! opened the old `.data` file keeps a valid fd even after cleanup unlinks
//! it (POSIX semantics).

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use openraft::SnapshotMeta;
use serde::{Deserialize, Serialize};

use super::snapshot_format::MetadataSnapshot;
use super::types::NodeId;

const MAGIC: &[u8; 4] = b"KRSN";
const VERSION: u8 = 2;
const CONTROL_STATE_MAGIC: &[u8; 4] = b"KRCS";
const CONTROL_STATE_VERSION: u8 = 2;

/// Header layout: magic(4) + version(1) + reserved(3) + meta_len(4) = 12 bytes.
const HEADER_LEN: usize = 12;

/// Encoded form of `SnapshotMeta` so we own the serde shape — `SnapshotMeta`
/// itself derives `Serialize`/`Deserialize` from openraft, but pinning the
/// wire shape here means a future openraft upgrade can't silently change
/// our on-disk format.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedMeta {
    last_log_id: Option<openraft::LogId<NodeId>>,
    last_membership: openraft::StoredMembership<NodeId, openraft::BasicNode>,
    snapshot_id: String,
}

impl From<&SnapshotMeta<NodeId, openraft::BasicNode>> for PersistedMeta {
    fn from(m: &SnapshotMeta<NodeId, openraft::BasicNode>) -> Self {
        Self {
            last_log_id: m.last_log_id,
            last_membership: m.last_membership.clone(),
            snapshot_id: m.snapshot_id.clone(),
        }
    }
}

impl From<PersistedMeta> for SnapshotMeta<NodeId, openraft::BasicNode> {
    fn from(p: PersistedMeta) -> Self {
        SnapshotMeta {
            last_log_id: p.last_log_id,
            last_membership: p.last_membership,
            snapshot_id: p.snapshot_id,
        }
    }
}

/// Durable state-machine progress between snapshots.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedControlState {
    pub last_applied: Option<openraft::LogId<NodeId>>,
    pub last_membership: openraft::StoredMembership<NodeId, openraft::BasicNode>,
    pub metadata: MetadataSnapshot,
}

/// On-disk store for the latest snapshot in a single Raft node's data dir.
pub struct SnapshotStore {
    dir: PathBuf,
}

impl SnapshotStore {
    pub fn new(dir: impl Into<PathBuf>) -> io::Result<Self> {
        let dir = dir.into();
        fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Creates (truncating) the staging file that snapshot data is streamed
    /// into — by the local builder, or by openraft chunk reception
    /// (`begin_receiving_snapshot`). Only one staging file exists at a time;
    /// openraft's state-machine worker serializes both producers.
    pub fn create_staging_data_file(&self) -> io::Result<(PathBuf, fs::File)> {
        let path = self.staging_data_path();
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok((path, file))
    }

    fn staging_data_path(&self) -> PathBuf {
        self.dir.join("staging.data.tmp")
    }

    /// Commits a fully-written staging data file as the new latest snapshot:
    /// fsync + rename data into place, then write the meta file, fsync the
    /// dir, and clean up older snapshots. See the module docs for the crash
    /// analysis.
    pub fn commit_snapshot(
        &self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        staging_data: &Path,
    ) -> io::Result<PathBuf> {
        let (idx, term) = index_term(meta);
        let data_path = self.dir.join(snapshot_filename(idx, term, "data"));
        let meta_path = self.dir.join(snapshot_filename(idx, term, "meta"));

        // 1. Data durable + in place.
        fs::File::open(staging_data)?.sync_all()?;
        fs::rename(staging_data, &data_path)?;

        // 2. Meta durable + in place (its presence is what makes the pair
        //    "exist", so it goes second).
        let meta_bytes = bincode::serialize(&PersistedMeta::from(meta))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let meta_len: u32 = meta_bytes.len().try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot meta exceeds u32::MAX",
            )
        })?;
        let mut buf = Vec::with_capacity(HEADER_LEN + meta_bytes.len() + 4);
        buf.extend_from_slice(MAGIC);
        buf.push(VERSION);
        buf.extend_from_slice(&[0u8; 3]); // reserved
        buf.extend_from_slice(&meta_len.to_le_bytes());
        buf.extend_from_slice(&meta_bytes);
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        let meta_tmp = meta_path.with_extension("meta.tmp");
        {
            let mut f = fs::File::create(&meta_tmp)?;
            f.write_all(&buf)?;
            f.sync_all()?;
        }
        fs::rename(&meta_tmp, &meta_path)?;

        // 3. Make both renames durable.
        if let Ok(d) = fs::File::open(&self.dir) {
            let _ = d.sync_all();
        }

        // 4. Retention: drop older pairs and stray tmp files. Errors are not
        //    fatal — stragglers are harmless and the next commit retries.
        let _ = self.cleanup_except(idx, term);
        Ok(data_path)
    }

    /// Loads the meta of the highest-`(index, term)` snapshot pair, or
    /// `None`. Corrupt meta files and pairs missing their `.data` sibling
    /// are skipped with a warning. Never touches the data file's contents —
    /// this is the boot-time hydration path and must stay cheap.
    pub fn load_latest_meta(
        &self,
    ) -> io::Result<Option<SnapshotMeta<NodeId, openraft::BasicNode>>> {
        Ok(self.latest_valid_pair()?.map(|(meta, _)| meta))
    }

    /// Returns the latest snapshot's meta together with the path of its
    /// data file, for `get_current_snapshot` to open and stream.
    pub fn open_latest(
        &self,
    ) -> io::Result<Option<(SnapshotMeta<NodeId, openraft::BasicNode>, PathBuf)>> {
        self.latest_valid_pair()
    }

    fn latest_valid_pair(
        &self,
    ) -> io::Result<Option<(SnapshotMeta<NodeId, openraft::BasicNode>, PathBuf)>> {
        let mut candidates: Vec<(u64, u64)> = Vec::new();
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if let Some(pair) = parse_snapshot_filename(name, "meta") {
                candidates.push(pair);
            }
        }
        // Highest (index, term) first.
        candidates.sort_unstable_by(|a, b| b.cmp(a));

        for (idx, term) in candidates {
            let meta_path = self.dir.join(snapshot_filename(idx, term, "meta"));
            let data_path = self.dir.join(snapshot_filename(idx, term, "data"));
            if !data_path.exists() {
                tracing::warn!(
                    target: "raft.snapshot",
                    path = %meta_path.display(),
                    "snapshot meta has no data sibling; skipping"
                );
                continue;
            }
            match parse_meta_file(&meta_path) {
                Ok(meta) => return Ok(Some((meta, data_path))),
                Err(e) => {
                    tracing::warn!(
                        target: "raft.snapshot",
                        path = %meta_path.display(),
                        error = %e,
                        "skipping unreadable snapshot meta"
                    );
                }
            }
        }
        Ok(None)
    }

    /// Atomically persists the last applied membership to `membership.bin`.
    ///
    /// The snapshot is not the only durable carrier of the voter set:
    /// a node that restarts cleanly BEFORE its first snapshot (policy:
    /// every 10k log entries) would otherwise recover an empty
    /// `last_membership` — the cluster-init Membership entry sits in the
    /// applied region of the log, which openraft does not rescan — and
    /// come back as a Learner that can never elect a leader. Persisting
    /// membership on every membership apply closes that window.
    pub fn save_membership(
        &self,
        membership: &openraft::StoredMembership<NodeId, openraft::BasicNode>,
    ) -> io::Result<()> {
        let final_path = self.dir.join("membership.bin");
        let tmp_path = self.dir.join("membership.bin.tmp");

        let body = bincode::serialize(membership)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let mut buf = Vec::with_capacity(body.len() + 4);
        buf.extend_from_slice(&body);
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        {
            let mut f = fs::File::create(&tmp_path)?;
            f.write_all(&buf)?;
            f.sync_all()?;
        }
        fs::rename(&tmp_path, &final_path)?;
        if let Ok(d) = fs::File::open(&self.dir) {
            let _ = d.sync_all();
        }
        Ok(())
    }

    /// Loads the persisted membership, or `None` if the file is missing or
    /// unreadable (corrupt files are skipped with a warning — the caller
    /// falls back to snapshot meta / log scan / rescue).
    pub fn load_membership(
        &self,
    ) -> io::Result<Option<openraft::StoredMembership<NodeId, openraft::BasicNode>>> {
        let path = self.dir.join("membership.bin");
        let bytes = match fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        if bytes.len() < 4 {
            tracing::warn!(target: "raft.snapshot", "membership.bin too short; ignoring");
            return Ok(None);
        }
        let (body, crc_bytes) = bytes.split_at(bytes.len() - 4);
        let stored_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        if crc32c::crc32c(body) != stored_crc {
            tracing::warn!(target: "raft.snapshot", "membership.bin CRC mismatch; ignoring");
            return Ok(None);
        }
        match bincode::deserialize(body) {
            Ok(m) => Ok(Some(m)),
            Err(e) => {
                tracing::warn!(target: "raft.snapshot", error = %e, "membership.bin undecodable; ignoring");
                Ok(None)
            }
        }
    }

    /// Atomically persists state-machine progress independently from event
    /// segments. Control-plane traffic is low-volume, so one fsync per apply
    /// batch is preferable to reconstructing `last_applied` from data-plane
    /// markers that no longer exist.
    pub fn save_control_state(&self, state: &PersistedControlState) -> io::Result<()> {
        let final_path = self.dir.join("control-state.bin");
        let tmp_path = self.dir.join("control-state.bin.tmp");
        let body =
            bincode::serialize(state).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let mut buf = Vec::with_capacity(9 + body.len() + 4);
        buf.extend_from_slice(CONTROL_STATE_MAGIC);
        buf.push(CONTROL_STATE_VERSION);
        let body_len: u32 = body.len().try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "control state exceeds u32::MAX",
            )
        })?;
        buf.extend_from_slice(&body_len.to_le_bytes());
        buf.extend_from_slice(&body);
        buf.extend_from_slice(&crc32c::crc32c(&buf).to_le_bytes());

        {
            let mut file = fs::File::create(&tmp_path)?;
            file.write_all(&buf)?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, &final_path)?;
        if let Ok(dir) = fs::File::open(&self.dir) {
            let _ = dir.sync_all();
        }
        Ok(())
    }

    /// Loads durable state-machine progress. A missing file is expected on a
    /// fresh node; corruption is an error because silently replaying from an
    /// older point can resurrect superseded leadership metadata.
    pub fn load_control_state(&self) -> io::Result<Option<PersistedControlState>> {
        let path = self.dir.join("control-state.bin");
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        if bytes.len() < 13 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "control-state.bin too short",
            ));
        }
        if &bytes[0..4] != CONTROL_STATE_MAGIC {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bad control-state.bin magic",
            ));
        }
        let version = bytes[4];
        if version != CONTROL_STATE_VERSION {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unsupported control state version: {version}"),
            ));
        }
        let (body_and_header, crc_bytes) = bytes.split_at(bytes.len() - 4);
        let expected_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
        if crc32c::crc32c(body_and_header) != expected_crc {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "control-state.bin CRC mismatch",
            ));
        }
        let body_len = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize;
        let body_end = 9usize
            .checked_add(body_len)
            .filter(|end| *end == body_and_header.len())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "invalid control state length")
            })?;
        let state = bincode::deserialize(&bytes[9..body_end])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        Ok(Some(state))
    }

    fn cleanup_except(&self, keep_idx: u64, keep_term: u64) -> io::Result<()> {
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let is_current = parse_snapshot_filename(name, "meta") == Some((keep_idx, keep_term))
                || parse_snapshot_filename(name, "data") == Some((keep_idx, keep_term));
            if is_current
                || name == "membership.bin"
                || name == "membership.bin.tmp"
                || name == "control-state.bin"
                || name == "control-state.bin.tmp"
            {
                continue;
            }
            // Older snapshot pairs and orphaned temporary files.
            let droppable =
                name.ends_with(".meta") || name.ends_with(".data") || name.ends_with(".tmp");
            if droppable {
                let _ = fs::remove_file(&path);
            }
        }
        Ok(())
    }
}

fn index_term(meta: &SnapshotMeta<NodeId, openraft::BasicNode>) -> (u64, u64) {
    match meta.last_log_id {
        Some(id) => (id.index, id.leader_id.term),
        None => (0, 0),
    }
}

fn snapshot_filename(index: u64, term: u64, ext: &str) -> String {
    format!("snap-{index:020}-{term:020}.{ext}")
}

fn parse_snapshot_filename(name: &str, ext: &str) -> Option<(u64, u64)> {
    let rest = name
        .strip_prefix("snap-")?
        .strip_suffix(&format!(".{ext}"))?;
    let mut parts = rest.split('-');
    let idx: u64 = parts.next()?.parse().ok()?;
    let term: u64 = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((idx, term))
}

fn parse_meta_file(path: &Path) -> io::Result<SnapshotMeta<NodeId, openraft::BasicNode>> {
    let bytes = fs::read(path)?;
    if bytes.len() < HEADER_LEN + 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot meta file too short",
        ));
    }
    if &bytes[0..4] != MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
    }
    if bytes[4] != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported snapshot meta version: {}", bytes[4]),
        ));
    }
    let (body, crc_bytes) = bytes.split_at(bytes.len() - 4);
    let stored_crc = u32::from_le_bytes(crc_bytes.try_into().unwrap());
    if crc32c::crc32c(body) != stored_crc {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot meta CRC mismatch",
        ));
    }
    let meta_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let meta_end = HEADER_LEN
        .checked_add(meta_len)
        .filter(|&end| end <= body.len())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "truncated meta"))?;
    let meta: PersistedMeta = bincode::deserialize(&bytes[HEADER_LEN..meta_end])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(meta.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{BasicNode, CommittedLeaderId, LogId, Membership, StoredMembership};
    use std::collections::{BTreeMap, BTreeSet};

    fn log_id(term: u64, node_id: u64, index: u64) -> LogId<NodeId> {
        LogId {
            leader_id: CommittedLeaderId::new(term, node_id),
            index,
        }
    }

    fn membership_one_voter(node_id: NodeId) -> StoredMembership<NodeId, BasicNode> {
        let mut nodes = BTreeMap::new();
        nodes.insert(
            node_id,
            BasicNode {
                addr: "127.0.0.1:50051".into(),
            },
        );
        let mut voters = BTreeSet::new();
        voters.insert(node_id);
        let m = Membership::new(vec![voters], nodes);
        StoredMembership::new(Some(log_id(1, node_id, 1)), m)
    }

    fn meta(idx: u64, term: u64) -> SnapshotMeta<NodeId, BasicNode> {
        SnapshotMeta {
            last_log_id: Some(log_id(term, 1, idx)),
            last_membership: membership_one_voter(1),
            snapshot_id: format!("snap-{idx}-{term}"),
        }
    }

    fn commit(store: &SnapshotStore, idx: u64, term: u64, data: &[u8]) -> PathBuf {
        let (path, mut f) = store.create_staging_data_file().unwrap();
        f.write_all(data).unwrap();
        drop(f);
        store.commit_snapshot(&meta(idx, term), &path).unwrap()
    }

    #[test]
    fn roundtrip_preserves_meta_and_data() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path()).unwrap();
        let payload = b"streamed snapshot data".to_vec();
        commit(&store, 42, 7, &payload);

        let (loaded_meta, data_path) = store.open_latest().unwrap().expect("snapshot present");
        assert_eq!(loaded_meta.last_log_id, meta(42, 7).last_log_id);
        assert_eq!(loaded_meta.snapshot_id, "snap-42-7");
        assert_eq!(fs::read(&data_path).unwrap(), payload);
        let members: Vec<_> = loaded_meta
            .last_membership
            .membership()
            .voter_ids()
            .collect();
        assert_eq!(members, vec![1]);
    }

    #[test]
    fn meta_only_load_never_opens_data() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path()).unwrap();
        let data_path = commit(&store, 99, 3, &[0u8; 16 * 1024]);

        // Replace the data file's CONTENT with garbage — meta hydration must
        // still succeed because it never reads the data segment.
        fs::write(&data_path, b"garbage").unwrap();
        let m = store.load_latest_meta().unwrap().expect("meta present");
        assert_eq!(m.last_log_id, meta(99, 3).last_log_id);
    }

    #[test]
    fn latest_picks_highest_index_with_multiple_pairs() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path()).unwrap();
        // Plant pairs directly (bypassing commit's cleanup) to simulate a
        // crash between rename and cleanup.
        for &(idx, term) in &[(10u64, 1u64), (50, 1), (30, 1)] {
            let (staging, mut f) = store.create_staging_data_file().unwrap();
            f.write_all(b"d").unwrap();
            drop(f);
            let data_path = dir.path().join(snapshot_filename(idx, term, "data"));
            fs::rename(&staging, &data_path).unwrap();
            // Write meta by committing to a scratch store, then copying the
            // meta file over — simpler: hand-encode via commit on a temp dir.
            let scratch = tempfile::tempdir().unwrap();
            let sstore = SnapshotStore::new(scratch.path()).unwrap();
            let (spath, mut sf) = sstore.create_staging_data_file().unwrap();
            sf.write_all(b"d").unwrap();
            drop(sf);
            sstore.commit_snapshot(&meta(idx, term), &spath).unwrap();
            fs::copy(
                scratch.path().join(snapshot_filename(idx, term, "meta")),
                dir.path().join(snapshot_filename(idx, term, "meta")),
            )
            .unwrap();
        }

        let (m, _) = store.open_latest().unwrap().unwrap();
        assert_eq!(m.last_log_id.unwrap().index, 50);
    }

    #[test]
    fn commit_cleans_up_older_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path()).unwrap();
        commit(&store, 10, 1, b"a");
        commit(&store, 20, 1, b"b");

        let names: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().into_string().unwrap())
            .filter(|n| n != "membership.bin")
            .collect();
        let mut names = names;
        names.sort();
        assert_eq!(
            names,
            vec![
                snapshot_filename(20, 1, "data"),
                snapshot_filename(20, 1, "meta"),
            ],
            "only the newest snapshot pair should remain"
        );
    }

    #[test]
    fn meta_without_data_sibling_is_skipped() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path()).unwrap();
        let data_path = commit(&store, 7, 1, b"d");
        fs::remove_file(&data_path).unwrap();
        assert!(store.load_latest_meta().unwrap().is_none());
        assert!(store.open_latest().unwrap().is_none());
    }

    #[test]
    fn corrupted_meta_returns_none_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path()).unwrap();
        fs::write(dir.path().join(snapshot_filename(7, 1, "meta")), b"junk").unwrap();
        fs::write(dir.path().join(snapshot_filename(7, 1, "data")), b"junk").unwrap();
        assert!(store.load_latest_meta().unwrap().is_none());
        assert!(store.open_latest().unwrap().is_none());
    }

    #[test]
    fn empty_dir_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path()).unwrap();
        assert!(store.load_latest_meta().unwrap().is_none());
        assert!(store.open_latest().unwrap().is_none());
    }

    #[test]
    fn membership_survives_snapshot_cleanup() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path()).unwrap();
        store.save_membership(&membership_one_voter(1)).unwrap();
        commit(&store, 5, 1, b"d");
        let m = store.load_membership().unwrap().expect("membership kept");
        let voters: Vec<_> = m.membership().voter_ids().collect();
        assert_eq!(voters, vec![1]);
    }

    #[test]
    fn parse_filename_rejects_bad_inputs() {
        assert!(parse_snapshot_filename("snap-1-2.meta", "meta").is_some());
        assert!(parse_snapshot_filename("snap-1.meta", "meta").is_none());
        assert!(parse_snapshot_filename("snap-1-2-3.meta", "meta").is_none());
        assert!(parse_snapshot_filename("not-a-snap.meta", "meta").is_none());
        assert!(parse_snapshot_filename("snap-1-2.meta.tmp", "meta").is_none());
        assert!(parse_snapshot_filename("snap-1-2.data", "meta").is_none());
    }
}
