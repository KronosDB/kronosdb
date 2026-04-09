use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use openraft::storage::LogFlushed;
use openraft::storage::RaftLogStorage;
use openraft::{
    Entry, ErrorSubject, ErrorVerb, LogId, LogState, OptionalSend, RaftLogId, RaftLogReader,
    StorageError, Vote,
};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::types::{NodeId, TypeConfig};

/// Vote persisted to disk.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PersistedVote {
    vote: Option<Vote<NodeId>>,
    committed: Option<LogId<NodeId>>,
}

/// In-memory log store with file-backed persistence.
///
/// Log entries are kept in a BTreeMap and flushed to a bincode file on each write.
/// The Raft log is transient — entries are purged after being applied to the event
/// store. In steady state it holds only the small uncommitted tail.
pub struct LogStore {
    inner: Arc<Mutex<LogStoreInner>>,
}

struct LogStoreInner {
    dir: PathBuf,
    log: BTreeMap<u64, Entry<TypeConfig>>,
    vote: PersistedVote,
    last_purged: Option<LogId<NodeId>>,
}

impl LogStore {
    pub fn new(dir: &Path) -> Result<Self, io::Error> {
        std::fs::create_dir_all(dir)?;

        let vote = read_vote(dir).unwrap_or_default();
        let log = read_log(dir).unwrap_or_default();
        let last_purged = read_purged(dir);

        Ok(Self {
            inner: Arc::new(Mutex::new(LogStoreInner {
                dir: dir.to_path_buf(),
                log,
                vote,
                last_purged,
            })),
        })
    }
}

/// A cloneable log reader sharing the same inner state.
pub struct LogReader {
    inner: Arc<Mutex<LogStoreInner>>,
}

fn io_err(e: io::Error) -> StorageError<NodeId> {
    StorageError::from_io_error(ErrorSubject::Logs, ErrorVerb::Write, e)
}

impl RaftLogReader<TypeConfig> for LogReader {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        Ok(inner.log.range(range).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = LogReader;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock();

        let last_log_id = inner
            .log
            .iter()
            .next_back()
            .map(|(_, e)| *e.get_log_id())
            .or(inner.last_purged);

        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        LogReader {
            inner: Arc::clone(&self.inner),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock();
        inner.vote.vote = Some(*vote);
        write_vote(&inner.dir, &inner.vote).map_err(|e| {
            StorageError::from_io_error(ErrorSubject::Vote, ErrorVerb::Write, e)
        })?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        Ok(inner.vote.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock();
        inner.vote.committed = committed;
        write_vote(&inner.dir, &inner.vote).map_err(|e| io_err(e))?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        let inner = self.inner.lock();
        Ok(inner.vote.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = self.inner.lock();

        for entry in entries {
            inner.log.insert(entry.get_log_id().index, entry);
        }

        write_log(&inner.dir, &inner.log).map_err(|e| io_err(e))?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock();
        let to_remove: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in to_remove {
            inner.log.remove(&key);
        }
        write_log(&inner.dir, &inner.log).map_err(|e| io_err(e))?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock();
        inner.last_purged = Some(log_id);

        let to_remove: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in to_remove {
            inner.log.remove(&key);
        }

        write_log(&inner.dir, &inner.log).map_err(|e| io_err(e))?;
        write_purged(&inner.dir, &log_id).map_err(|e| io_err(e))?;
        Ok(())
    }
}

// --- File I/O helpers ---

fn vote_path(dir: &Path) -> PathBuf {
    dir.join("vote.bin")
}

fn log_path(dir: &Path) -> PathBuf {
    dir.join("log.bin")
}

fn purged_path(dir: &Path) -> PathBuf {
    dir.join("purged.bin")
}

fn write_vote(dir: &Path, vote: &PersistedVote) -> Result<(), io::Error> {
    let data =
        bincode::serialize(vote).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    atomic_write(&vote_path(dir), &data)
}

fn read_vote(dir: &Path) -> Option<PersistedVote> {
    let data = std::fs::read(vote_path(dir)).ok()?;
    bincode::deserialize(&data).ok()
}

fn write_log(dir: &Path, log: &BTreeMap<u64, Entry<TypeConfig>>) -> Result<(), io::Error> {
    let entries: Vec<_> = log.values().cloned().collect();
    let data =
        bincode::serialize(&entries).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    atomic_write(&log_path(dir), &data)
}

fn read_log(dir: &Path) -> Option<BTreeMap<u64, Entry<TypeConfig>>> {
    let data = std::fs::read(log_path(dir)).ok()?;
    let entries: Vec<Entry<TypeConfig>> = bincode::deserialize(&data).ok()?;
    let mut map = BTreeMap::new();
    for entry in entries {
        map.insert(entry.get_log_id().index, entry);
    }
    Some(map)
}

fn write_purged(dir: &Path, log_id: &LogId<NodeId>) -> Result<(), io::Error> {
    let data =
        bincode::serialize(log_id).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    atomic_write(&purged_path(dir), &data)
}

fn read_purged(dir: &Path) -> Option<LogId<NodeId>> {
    let data = std::fs::read(purged_path(dir)).ok()?;
    bincode::deserialize(&data).ok()
}

/// Directly inserts entries into the log and persists. For testing only.
#[cfg(test)]
impl LogStore {
    pub fn test_insert_entries(&self, entries: Vec<Entry<TypeConfig>>) {
        let mut inner = self.inner.lock();
        for entry in &entries {
            inner.log.insert(entry.get_log_id().index, entry.clone());
        }
        write_log(&inner.dir, &inner.log).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::{CommittedLeaderId, Entry, LogId, Vote};

    fn log_id(term: u64, index: u64) -> LogId<NodeId> {
        LogId {
            leader_id: CommittedLeaderId::new(term, 0),
            index,
        }
    }

    fn blank_entry(term: u64, index: u64) -> Entry<TypeConfig> {
        let mut e = Entry::<TypeConfig>::default();
        e.set_log_id(&log_id(term, index));
        e
    }

    #[tokio::test]
    async fn fresh_log_state_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::new(dir.path()).unwrap();

        let state = store.get_log_state().await.unwrap();
        assert!(state.last_log_id.is_none());
        assert!(state.last_purged_log_id.is_none());
    }

    #[tokio::test]
    async fn vote_persist_and_recover() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut store = LogStore::new(dir.path()).unwrap();
            assert!(store.read_vote().await.unwrap().is_none());

            let vote = Vote::new(3, 1);
            store.save_vote(&vote).await.unwrap();

            let read = store.read_vote().await.unwrap().unwrap();
            assert_eq!(read, vote);
        }

        // Reopen and verify persistence.
        {
            let mut store = LogStore::new(dir.path()).unwrap();
            let read = store.read_vote().await.unwrap().unwrap();
            assert_eq!(read, Vote::new(3, 1));
        }
    }

    #[tokio::test]
    async fn committed_persist_and_recover() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut store = LogStore::new(dir.path()).unwrap();
            assert!(store.read_committed().await.unwrap().is_none());

            let committed = log_id(2, 10);
            store.save_committed(Some(committed)).await.unwrap();

            let read = store.read_committed().await.unwrap().unwrap();
            assert_eq!(read, committed);
        }

        // Reopen.
        {
            let mut store = LogStore::new(dir.path()).unwrap();
            let read = store.read_committed().await.unwrap().unwrap();
            assert_eq!(read, log_id(2, 10));
        }
    }

    #[tokio::test]
    async fn entries_persist_and_recover() {
        let dir = tempfile::tempdir().unwrap();

        {
            let store = LogStore::new(dir.path()).unwrap();
            store.test_insert_entries(vec![
                blank_entry(1, 1),
                blank_entry(1, 2),
                blank_entry(1, 3),
            ]);
        }

        // Reopen and verify.
        {
            let mut store = LogStore::new(dir.path()).unwrap();
            let state = store.get_log_state().await.unwrap();
            assert_eq!(state.last_log_id.unwrap().index, 3);

            let entries = store.try_get_log_entries(1..4).await.unwrap();
            assert_eq!(entries.len(), 3);
        }
    }

    #[tokio::test]
    async fn truncate_removes_from_index() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::new(dir.path()).unwrap();

        store.test_insert_entries(vec![
            blank_entry(1, 1),
            blank_entry(1, 2),
            blank_entry(1, 3),
            blank_entry(2, 4),
        ]);

        // Truncate from index 3 inclusive.
        store.truncate(log_id(1, 3)).await.unwrap();

        let entries = store.try_get_log_entries(1..10).await.unwrap();
        assert_eq!(entries.len(), 2); // Only 1, 2 remain.
        assert_eq!(entries[0].get_log_id().index, 1);
        assert_eq!(entries[1].get_log_id().index, 2);
    }

    #[tokio::test]
    async fn purge_removes_up_to_and_tracks_last_purged() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::new(dir.path()).unwrap();

        store.test_insert_entries(vec![
            blank_entry(1, 1),
            blank_entry(1, 2),
            blank_entry(1, 3),
        ]);

        // Purge up to index 2 inclusive.
        store.purge(log_id(1, 2)).await.unwrap();

        let entries = store.try_get_log_entries(1..10).await.unwrap();
        assert_eq!(entries.len(), 1); // Only 3 remains.
        assert_eq!(entries[0].get_log_id().index, 3);

        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id.unwrap().index, 2);
        assert_eq!(state.last_log_id.unwrap().index, 3);
    }

    #[tokio::test]
    async fn purge_persists_across_restart() {
        let dir = tempfile::tempdir().unwrap();

        {
            let mut store = LogStore::new(dir.path()).unwrap();
            store.test_insert_entries(vec![
                blank_entry(1, 1),
                blank_entry(1, 2),
                blank_entry(1, 3),
            ]);
            store.purge(log_id(1, 2)).await.unwrap();
        }

        {
            let mut store = LogStore::new(dir.path()).unwrap();
            let state = store.get_log_state().await.unwrap();
            assert_eq!(state.last_purged_log_id.unwrap().index, 2);
            assert_eq!(state.last_log_id.unwrap().index, 3);
        }
    }

    #[tokio::test]
    async fn log_reader_sees_same_entries() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = LogStore::new(dir.path()).unwrap();

        store.test_insert_entries(vec![
            blank_entry(1, 1),
            blank_entry(1, 2),
        ]);

        let mut reader = store.get_log_reader().await;
        let entries = reader.try_get_log_entries(1..3).await.unwrap();
        assert_eq!(entries.len(), 2);
    }
}

fn atomic_write(path: &Path, data: &[u8]) -> Result<(), io::Error> {
    let tmp = path.with_extension("tmp");

    // Write + fsync the file contents.
    let file = std::fs::File::create(&tmp)?;
    let mut writer = io::BufWriter::new(file);
    io::Write::write_all(&mut writer, data)?;
    let file = writer.into_inner().map_err(|e| e.into_error())?;
    file.sync_all()?;

    // Atomic rename.
    std::fs::rename(&tmp, path)?;

    // Fsync the directory to ensure the rename is durable.
    if let Some(parent) = path.parent() {
        if let Ok(dir) = std::fs::File::open(parent) {
            let _ = dir.sync_all();
        }
    }

    Ok(())
}
