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
//! # On-disk format ("KRSN" v1)
//!
//! ```text
//! +----------+--------+----------+----------+----------+----------+----------+----------+
//! | magic(4) | ver(1) | rsv(3)   | meta_len | meta     | data_len | data     | crc32c   |
//! | "KRSN"   |  0x01  | zeros    | u32 LE   | bincode  | u64 LE   | raw      | u32 LE   |
//! +----------+--------+----------+----------+----------+----------+----------+----------+
//! ```
//!
//! The CRC covers every byte of the file except itself.
//!
//! # File naming
//!
//! `<dir>/snap-<index:020>-<term:020>.snap` — sortable by filename so
//! "latest" is just the lexicographically-largest entry. The index is the
//! snapshot's `last_log_id.index`; the term is its `leader_id.term`. Both
//! are zero-padded to 20 digits (max u64).
//!
//! # Atomic write + retention
//!
//! Each snapshot is written to `<name>.tmp`, fsynced, renamed into place,
//! and the parent dir fsynced. After a successful write, all older `.snap`
//! files are removed. A crash between rename and cleanup leaves multiple
//! valid snapshot files; `load_latest` picks the highest, and the next
//! successful `write` will clean up the stragglers.
//!
//! # Concurrency
//!
//! `SnapshotStore` is `Send + Sync`. openraft serializes snapshot building
//! through its single state-machine worker, so concurrent `write` calls do
//! not occur in practice. `load_latest_meta` and `load_latest` may be
//! called concurrently with `write` on different threads; the rename-based
//! atomic write guarantees readers see either the old or the new file, not
//! a half-written one.

use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use openraft::SnapshotMeta;
use serde::{Deserialize, Serialize};

use super::types::NodeId;

const MAGIC: &[u8; 4] = b"KRSN";
const VERSION: u8 = 1;

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

/// A snapshot ready to write to disk, or one just read back.
pub struct PersistedSnapshot {
    pub meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    pub data: Vec<u8>,
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

    /// Atomically write `snap` to disk, then delete older `.snap` files.
    ///
    /// Crash points:
    /// - before rename: `.tmp` orphan; ignored by `load_latest`, cleaned on
    ///   the next successful `write`.
    /// - after rename, before cleanup: multiple `.snap` files coexist;
    ///   `load_latest` picks the highest by `(index, term)`; the next
    ///   successful `write` removes the stragglers.
    pub fn write(&self, snap: &PersistedSnapshot) -> io::Result<()> {
        let (idx, term) = match snap.meta.last_log_id {
            Some(id) => (id.index, id.leader_id.term),
            None => (0, 0),
        };
        let final_path = self.dir.join(snapshot_filename(idx, term));
        let tmp_path = final_path.with_extension("snap.tmp");

        let meta_bytes = bincode::serialize(&PersistedMeta::from(&snap.meta))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let meta_len: u32 = meta_bytes.len().try_into().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "snapshot meta exceeds u32::MAX",
            )
        })?;
        let data_len: u64 = snap.data.len() as u64;

        // Build the full payload in memory so the CRC is computed against
        // exactly the bytes we write, then write them in one shot.
        let mut buf = Vec::with_capacity(HEADER_LEN + meta_bytes.len() + 8 + snap.data.len() + 4);
        buf.extend_from_slice(MAGIC);
        buf.push(VERSION);
        buf.extend_from_slice(&[0u8; 3]); // reserved
        buf.extend_from_slice(&meta_len.to_le_bytes());
        buf.extend_from_slice(&meta_bytes);
        buf.extend_from_slice(&data_len.to_le_bytes());
        buf.extend_from_slice(&snap.data);
        let crc = crc32c::crc32c(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());

        {
            let mut f = fs::File::create(&tmp_path)?;
            f.write_all(&buf)?;
            f.sync_all()?;
        }
        fs::rename(&tmp_path, &final_path)?;
        // Fsync the directory to make the rename durable.
        if let Ok(d) = fs::File::open(&self.dir) {
            let _ = d.sync_all();
        }

        // Retention: drop older `.snap` files (keep only the one we just
        // wrote). Errors here are not fatal — stragglers are harmless and
        // the next `write` retries cleanup.
        let _ = self.cleanup_older_than(&final_path);
        Ok(())
    }

    /// Load the highest-`(index, term)` snapshot from disk, or `None` if
    /// no valid snapshot file exists. Stale `.tmp` files and corrupt
    /// `.snap` files (bad magic / version / CRC) are skipped.
    pub fn load_latest(&self) -> io::Result<Option<PersistedSnapshot>> {
        let Some(path) = self.latest_snapshot_path()? else {
            return Ok(None);
        };
        let bytes = fs::read(&path)?;
        match parse(&bytes) {
            Ok(snap) => Ok(Some(snap)),
            Err(e) => {
                tracing::warn!(
                    target: "raft.snapshot",
                    path = %path.display(),
                    error = %e,
                    "skipping unreadable snapshot file"
                );
                Ok(None)
            }
        }
    }

    /// Load only the meta of the highest-`(index, term)` snapshot. Avoids
    /// reading the (potentially large) data segment when only meta is
    /// needed — startup uses this for fast hydration of `last_applied` /
    /// `last_membership`.
    pub fn load_latest_meta(
        &self,
    ) -> io::Result<Option<SnapshotMeta<NodeId, openraft::BasicNode>>> {
        let Some(path) = self.latest_snapshot_path()? else {
            return Ok(None);
        };
        match parse_meta_only(&path) {
            Ok(meta) => Ok(Some(meta)),
            Err(e) => {
                tracing::warn!(
                    target: "raft.snapshot",
                    path = %path.display(),
                    error = %e,
                    "skipping unreadable snapshot file (meta scan)"
                );
                Ok(None)
            }
        }
    }

    fn latest_snapshot_path(&self) -> io::Result<Option<PathBuf>> {
        let mut best: Option<(u64, u64, PathBuf)> = None;
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some((idx, term)) = parse_snapshot_filename(name) else {
                continue;
            };
            let candidate = (idx, term, entry.path());
            best = match best {
                Some(prev) if (prev.0, prev.1) >= (candidate.0, candidate.1) => Some(prev),
                _ => Some(candidate),
            };
        }
        Ok(best.map(|(_, _, p)| p))
    }

    fn cleanup_older_than(&self, keep: &Path) -> io::Result<()> {
        for entry in fs::read_dir(&self.dir)? {
            let entry = entry?;
            let path = entry.path();
            if path == keep {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            // Drop both `.snap` (older snapshots) and `.snap.tmp` (orphans
            // from a crashed write).
            if name.ends_with(".snap") || name.ends_with(".snap.tmp") {
                let _ = fs::remove_file(&path);
            }
        }
        Ok(())
    }
}

fn snapshot_filename(index: u64, term: u64) -> String {
    format!("snap-{index:020}-{term:020}.snap")
}

fn parse_snapshot_filename(name: &str) -> Option<(u64, u64)> {
    let rest = name.strip_prefix("snap-")?.strip_suffix(".snap")?;
    let mut parts = rest.split('-');
    let idx: u64 = parts.next()?.parse().ok()?;
    let term: u64 = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((idx, term))
}

fn parse(bytes: &[u8]) -> io::Result<PersistedSnapshot> {
    if bytes.len() < HEADER_LEN + 8 + 4 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "snapshot file too short",
        ));
    }
    if &bytes[0..4] != MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
    }
    if bytes[4] != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported snapshot file version: {}", bytes[4]),
        ));
    }
    let meta_len = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
    let meta_start = HEADER_LEN;
    let meta_end = meta_start
        .checked_add(meta_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "meta_len overflow"))?;
    if meta_end + 8 > bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "truncated meta or missing data_len",
        ));
    }
    let meta: PersistedMeta = bincode::deserialize(&bytes[meta_start..meta_end])
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let data_len_start = meta_end;
    let data_start = data_len_start + 8;
    let data_len =
        u64::from_le_bytes(bytes[data_len_start..data_start].try_into().unwrap()) as usize;
    let data_end = data_start
        .checked_add(data_len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "data_len overflow"))?;
    if data_end + 4 > bytes.len() {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated data"));
    }
    let data = bytes[data_start..data_end].to_vec();

    let crc_stored = u32::from_le_bytes(bytes[data_end..data_end + 4].try_into().unwrap());
    let crc_computed = crc32c::crc32c(&bytes[..data_end]);
    if crc_stored != crc_computed {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("crc mismatch: stored={crc_stored:08x} computed={crc_computed:08x}"),
        ));
    }

    Ok(PersistedSnapshot {
        meta: meta.into(),
        data,
    })
}

fn parse_meta_only(path: &Path) -> io::Result<SnapshotMeta<NodeId, openraft::BasicNode>> {
    let mut f = fs::File::open(path)?;
    let mut header = [0u8; HEADER_LEN];
    f.read_exact(&mut header)?;
    if &header[0..4] != MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
    }
    if header[4] != VERSION {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("unsupported snapshot file version: {}", header[4]),
        ));
    }
    let meta_len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
    let mut meta_buf = vec![0u8; meta_len];
    f.read_exact(&mut meta_buf)?;
    let meta: PersistedMeta = bincode::deserialize(&meta_buf)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    // Verify CRC against stored value at end-of-file. Without this, a
    // corrupted snapshot could feed garbage membership into startup.
    let total_len = f.metadata()?.len();
    if total_len < (HEADER_LEN as u64) + (meta_len as u64) + 8 + 4 {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "file too short"));
    }
    let crc_offset = total_len - 4;
    f.seek(SeekFrom::Start(crc_offset))?;
    let mut crc_buf = [0u8; 4];
    f.read_exact(&mut crc_buf)?;
    let crc_stored = u32::from_le_bytes(crc_buf);

    f.seek(SeekFrom::Start(0))?;
    let mut all = vec![0u8; crc_offset as usize];
    f.read_exact(&mut all)?;
    let crc_computed = crc32c::crc32c(&all);
    if crc_stored != crc_computed {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("crc mismatch: stored={crc_stored:08x} computed={crc_computed:08x}"),
        ));
    }

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

    fn snap(idx: u64, term: u64, data_len: usize) -> PersistedSnapshot {
        let mut data = vec![0u8; data_len];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        PersistedSnapshot {
            meta: SnapshotMeta {
                last_log_id: Some(log_id(term, 1, idx)),
                last_membership: membership_one_voter(1),
                snapshot_id: format!("snap-{idx}-{term}"),
            },
            data,
        }
    }

    #[test]
    fn roundtrip_preserves_meta_and_data() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path()).unwrap();
        let s = snap(42, 7, 1024);
        store.write(&s).unwrap();

        let loaded = store.load_latest().unwrap().expect("snapshot present");
        assert_eq!(loaded.meta.last_log_id, s.meta.last_log_id);
        assert_eq!(loaded.meta.snapshot_id, s.meta.snapshot_id);
        assert_eq!(loaded.data, s.data);
        let members: Vec<_> = loaded
            .meta
            .last_membership
            .membership()
            .voter_ids()
            .collect();
        assert_eq!(members, vec![1]);
    }

    #[test]
    fn meta_only_load_skips_data_segment() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path()).unwrap();
        let s = snap(99, 3, 16 * 1024);
        store.write(&s).unwrap();

        let meta = store.load_latest_meta().unwrap().expect("meta present");
        assert_eq!(meta.last_log_id, s.meta.last_log_id);
        assert_eq!(meta.snapshot_id, s.meta.snapshot_id);
    }

    /// Simulates a partial-cleanup state where multiple `.snap` files
    /// coexist (e.g. crash between rename and the older-file delete on
    /// the previous `write`). `load_latest` must pick the highest
    /// `(index, term)` pair without depending on cleanup having run.
    #[test]
    fn load_latest_picks_highest_index_with_multiple_snap_files() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path()).unwrap();

        // Plant three valid snapshot files directly so cleanup doesn't run.
        for &(idx, term) in &[(10u64, 1u64), (50, 1), (30, 1)] {
            let s = snap(idx, term, 16);
            let path = dir.path().join(snapshot_filename(idx, term));
            let mut buf = Vec::new();
            buf.extend_from_slice(MAGIC);
            buf.push(VERSION);
            buf.extend_from_slice(&[0u8; 3]);
            let meta_bytes = bincode::serialize(&PersistedMeta::from(&s.meta)).unwrap();
            buf.extend_from_slice(&(meta_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(&meta_bytes);
            buf.extend_from_slice(&(s.data.len() as u64).to_le_bytes());
            buf.extend_from_slice(&s.data);
            let crc = crc32c::crc32c(&buf);
            buf.extend_from_slice(&crc.to_le_bytes());
            fs::write(&path, &buf).unwrap();
        }

        let latest = store.load_latest().unwrap().unwrap();
        assert_eq!(latest.meta.last_log_id.unwrap().index, 50);
    }

    #[test]
    fn write_cleans_up_older_snapshots() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path()).unwrap();
        store.write(&snap(10, 1, 8)).unwrap();
        store.write(&snap(20, 1, 8)).unwrap();

        let snaps: Vec<_> = fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().into_string().unwrap())
            .filter(|n| n.ends_with(".snap"))
            .collect();
        assert_eq!(snaps.len(), 1, "only newest snapshot should remain");
    }

    #[test]
    fn corrupted_file_returns_none_not_panic() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path()).unwrap();
        // Plant a malformed file with the right naming convention so it's
        // discovered as the "latest" candidate.
        let path = dir.path().join(snapshot_filename(7, 1));
        fs::write(&path, b"not a snapshot file").unwrap();

        assert!(store.load_latest().unwrap().is_none());
        assert!(store.load_latest_meta().unwrap().is_none());
    }

    #[test]
    fn empty_dir_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::new(dir.path()).unwrap();
        assert!(store.load_latest().unwrap().is_none());
        assert!(store.load_latest_meta().unwrap().is_none());
    }

    #[test]
    fn parse_filename_rejects_bad_inputs() {
        assert!(parse_snapshot_filename("snap-1-2.snap").is_some());
        assert!(parse_snapshot_filename("snap-1.snap").is_none());
        assert!(parse_snapshot_filename("snap-1-2-3.snap").is_none());
        assert!(parse_snapshot_filename("not-a-snap.snap").is_none());
        assert!(parse_snapshot_filename("snap-1-2.snap.tmp").is_none());
    }
}
