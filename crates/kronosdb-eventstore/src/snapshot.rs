use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};

use crate::error::Error;

/// A stored snapshot: projection/read model state at a point in the event stream.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// The name/type of the snapshot.
    pub name: String,
    /// The version of the serialized form.
    pub version: String,
    /// The serialized payload (opaque bytes).
    pub payload: Vec<u8>,
    /// Timestamp of snapshot creation (millis since epoch).
    pub timestamp: i64,
    /// Arbitrary key-value metadata.
    pub metadata: HashMap<String, String>,
}

/// A snapshot entry with its key and sequence.
#[derive(Debug, Clone)]
pub struct SnapshotEntry {
    pub key: Vec<u8>,
    pub sequence: i64,
    pub snapshot: Snapshot,
}

/// Persistent snapshot store.
///
/// Layout on disk:
/// ```text
/// {dir}/
///   {hex_key}/
///     {sequence:020}.snap      # one file per snapshot
/// ```
///
/// No in-memory cache — snapshot access patterns have poor temporal locality
/// (each entity is loaded once per command, then not again for a long time).
/// The OS page cache handles recently-read files naturally.
pub struct SnapshotStore {
    dir: PathBuf,
}

/// Magic bytes for snapshot files.
const SNAP_MAGIC: [u8; 4] = *b"KSNP";
const SNAP_VERSION: u8 = 1;

impl SnapshotStore {
    /// Creates or opens a snapshot store at the given directory.
    pub fn open(dir: &Path) -> Result<Self, Error> {
        fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
        })
    }

    /// Stores a snapshot. If `prune` is true, older snapshots for the same key are deleted.
    pub fn add(
        &self,
        key: &[u8],
        sequence: i64,
        snapshot: &Snapshot,
        prune: bool,
    ) -> Result<(), Error> {
        let key_dir = self.key_dir(key);
        fs::create_dir_all(&key_dir)?;

        let snap_path = key_dir.join(format!("{:020}.snap", sequence));
        let tmp_path = snap_path.with_extension("snap.tmp");

        let mut file = File::create(&tmp_path)?;
        write_snapshot(&mut file, snapshot)?;
        file.sync_all()?;
        fs::rename(&tmp_path, &snap_path)?;

        if prune {
            // Delete all snapshots for this key with sequence < the new one.
            self.delete_range(key, 0, sequence)?;
        }

        Ok(())
    }

    /// Deletes all snapshots for a key with sequence in [0, to_sequence).
    pub fn delete(&self, key: &[u8], to_sequence: i64) -> Result<(), Error> {
        self.delete_range(key, 0, to_sequence)
    }

    /// Lists all snapshots for a key with sequence in [from_sequence, to_sequence).
    pub fn list(
        &self,
        key: &[u8],
        from_sequence: i64,
        to_sequence: i64,
    ) -> Result<Vec<SnapshotEntry>, Error> {
        let key_dir = self.key_dir(key);
        if !key_dir.exists() {
            return Ok(Vec::new());
        }

        let mut entries = Vec::new();
        for seq in self.list_sequences(&key_dir)? {
            if seq >= from_sequence && seq < to_sequence {
                let snap_path = key_dir.join(format!("{:020}.snap", seq));
                let snapshot = read_snapshot(&snap_path)?;
                entries.push(SnapshotEntry {
                    key: key.to_vec(),
                    sequence: seq,
                    snapshot,
                });
            }
        }

        Ok(entries)
    }

    /// Gets the snapshot with the highest sequence for a key, if any.
    pub fn get_last(&self, key: &[u8]) -> Result<Option<SnapshotEntry>, Error> {
        let key_dir = self.key_dir(key);
        if !key_dir.exists() {
            return Ok(None);
        }

        let sequences = self.list_sequences(&key_dir)?;
        match sequences.last() {
            Some(&seq) => {
                let snap_path = key_dir.join(format!("{:020}.snap", seq));
                let snapshot = read_snapshot(&snap_path)?;
                Ok(Some(SnapshotEntry {
                    key: key.to_vec(),
                    sequence: seq,
                    snapshot,
                }))
            }
            None => Ok(None),
        }
    }

    fn key_dir(&self, key: &[u8]) -> PathBuf {
        self.dir.join(hex_encode(key))
    }

    fn list_sequences(&self, key_dir: &Path) -> Result<Vec<i64>, Error> {
        let mut sequences = Vec::new();
        for entry in fs::read_dir(key_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().is_some_and(|ext| ext == "snap") {
                if let Some(seq) = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<i64>().ok())
                {
                    sequences.push(seq);
                }
            }
        }
        sequences.sort();
        Ok(sequences)
    }

    fn delete_range(&self, key: &[u8], from_sequence: i64, to_sequence: i64) -> Result<(), Error> {
        let key_dir = self.key_dir(key);
        if !key_dir.exists() {
            return Ok(());
        }

        for seq in self.list_sequences(&key_dir)? {
            if seq >= from_sequence && seq < to_sequence {
                let snap_path = key_dir.join(format!("{:020}.snap", seq));
                let _ = fs::remove_file(snap_path);
            }
        }

        Ok(())
    }
}

// --- Binary format ---

fn write_snapshot(file: &mut File, snap: &Snapshot) -> Result<(), Error> {
    file.write_all(&SNAP_MAGIC)?;
    file.write_all(&[SNAP_VERSION])?;

    let name_bytes = snap.name.as_bytes();
    file.write_all(&(name_bytes.len() as u16).to_le_bytes())?;
    file.write_all(name_bytes)?;

    let version_bytes = snap.version.as_bytes();
    file.write_all(&(version_bytes.len() as u16).to_le_bytes())?;
    file.write_all(version_bytes)?;

    file.write_all(&snap.timestamp.to_le_bytes())?;

    file.write_all(&(snap.metadata.len() as u16).to_le_bytes())?;
    for (k, v) in &snap.metadata {
        let k_bytes = k.as_bytes();
        let v_bytes = v.as_bytes();
        file.write_all(&(k_bytes.len() as u16).to_le_bytes())?;
        file.write_all(k_bytes)?;
        file.write_all(&(v_bytes.len() as u16).to_le_bytes())?;
        file.write_all(v_bytes)?;
    }

    file.write_all(&(snap.payload.len() as u32).to_le_bytes())?;
    file.write_all(&snap.payload)?;

    Ok(())
}

fn read_snapshot(path: &Path) -> Result<Snapshot, Error> {
    let data = fs::read(path)?;
    if data.len() < 5 || data[0..4] != SNAP_MAGIC {
        return Err(Error::Corrupted {
            message: "invalid snapshot magic bytes".into(),
        });
    }
    if data[4] != SNAP_VERSION {
        return Err(Error::Corrupted {
            message: format!("unsupported snapshot version: {}", data[4]),
        });
    }
    let mut cursor = 5;

    let name_len = read_u16(&data, &mut cursor) as usize;
    let name = read_string(&data, &mut cursor, name_len)?;

    let version_len = read_u16(&data, &mut cursor) as usize;
    let version = read_string(&data, &mut cursor, version_len)?;

    let timestamp = read_i64(&data, &mut cursor);

    let meta_count = read_u16(&data, &mut cursor) as usize;
    let mut metadata = HashMap::with_capacity(meta_count);
    for _ in 0..meta_count {
        let k_len = read_u16(&data, &mut cursor) as usize;
        let k = read_string(&data, &mut cursor, k_len)?;
        let v_len = read_u16(&data, &mut cursor) as usize;
        let v = read_string(&data, &mut cursor, v_len)?;
        metadata.insert(k, v);
    }

    let payload_len = read_u32(&data, &mut cursor) as usize;
    let payload = data[cursor..cursor + payload_len].to_vec();

    Ok(Snapshot {
        name,
        version,
        payload,
        timestamp,
        metadata,
    })
}

fn read_u16(data: &[u8], cursor: &mut usize) -> u16 {
    let val = u16::from_le_bytes([data[*cursor], data[*cursor + 1]]);
    *cursor += 2;
    val
}

fn read_u32(data: &[u8], cursor: &mut usize) -> u32 {
    let val = u32::from_le_bytes(data[*cursor..*cursor + 4].try_into().unwrap());
    *cursor += 4;
    val
}

fn read_i64(data: &[u8], cursor: &mut usize) -> i64 {
    let val = i64::from_le_bytes(data[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    val
}

fn read_string(data: &[u8], cursor: &mut usize, len: usize) -> Result<String, Error> {
    let s = std::str::from_utf8(&data[*cursor..*cursor + len]).map_err(|e| Error::Corrupted {
        message: format!("invalid UTF-8 in snapshot: {e}"),
    })?;
    *cursor += len;
    Ok(s.to_string())
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_snapshot(name: &str, payload: &[u8]) -> Snapshot {
        Snapshot {
            name: name.to_string(),
            version: "1.0".to_string(),
            payload: payload.to_vec(),
            timestamp: 1712345678000,
            metadata: HashMap::from([("source".to_string(), "test".to_string())]),
        }
    }

    #[test]
    fn add_and_get_last() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(&dir.path().join("snapshots")).unwrap();

        let key = b"OrderSummary\x00order-123";
        let snap = make_snapshot("OrderSummary", b"serialized state v1");

        store.add(key, 100, &snap, false).unwrap();

        let entry = store.get_last(key).unwrap().unwrap();
        assert_eq!(entry.sequence, 100);
        assert_eq!(entry.snapshot.name, "OrderSummary");
        assert_eq!(entry.snapshot.payload, b"serialized state v1");
        assert_eq!(entry.snapshot.metadata.get("source").unwrap(), "test");
    }

    #[test]
    fn get_last_returns_highest_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(&dir.path().join("snapshots")).unwrap();

        let key = b"projection-1";
        store
            .add(key, 10, &make_snapshot("P", b"v1"), false)
            .unwrap();
        store
            .add(key, 50, &make_snapshot("P", b"v2"), false)
            .unwrap();
        store
            .add(key, 30, &make_snapshot("P", b"v3"), false)
            .unwrap();

        let entry = store.get_last(key).unwrap().unwrap();
        assert_eq!(entry.sequence, 50);
        assert_eq!(entry.snapshot.payload, b"v2");
    }

    #[test]
    fn get_last_nonexistent_returns_none() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(&dir.path().join("snapshots")).unwrap();

        assert!(store.get_last(b"nonexistent").unwrap().is_none());
    }

    #[test]
    fn list_range() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(&dir.path().join("snapshots")).unwrap();

        let key = b"proj";
        store
            .add(key, 10, &make_snapshot("P", b"a"), false)
            .unwrap();
        store
            .add(key, 20, &make_snapshot("P", b"b"), false)
            .unwrap();
        store
            .add(key, 30, &make_snapshot("P", b"c"), false)
            .unwrap();
        store
            .add(key, 40, &make_snapshot("P", b"d"), false)
            .unwrap();

        let entries = store.list(key, 20, 40).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].sequence, 20);
        assert_eq!(entries[1].sequence, 30);
    }

    #[test]
    fn delete_removes_range() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(&dir.path().join("snapshots")).unwrap();

        let key = b"proj";
        store
            .add(key, 10, &make_snapshot("P", b"a"), false)
            .unwrap();
        store
            .add(key, 20, &make_snapshot("P", b"b"), false)
            .unwrap();
        store
            .add(key, 30, &make_snapshot("P", b"c"), false)
            .unwrap();

        store.delete(key, 25).unwrap();

        let entries = store.list(key, 0, i64::MAX).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, 30);
    }

    #[test]
    fn prune_on_add() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(&dir.path().join("snapshots")).unwrap();

        let key = b"proj";
        store
            .add(key, 10, &make_snapshot("P", b"a"), false)
            .unwrap();
        store
            .add(key, 20, &make_snapshot("P", b"b"), false)
            .unwrap();
        store.add(key, 30, &make_snapshot("P", b"c"), true).unwrap();

        let entries = store.list(key, 0, i64::MAX).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].sequence, 30);
    }

    #[test]
    fn different_keys_isolated() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(&dir.path().join("snapshots")).unwrap();

        store
            .add(b"key-a", 10, &make_snapshot("A", b"a"), false)
            .unwrap();
        store
            .add(b"key-b", 10, &make_snapshot("B", b"b"), false)
            .unwrap();

        let a = store.get_last(b"key-a").unwrap().unwrap();
        let b = store.get_last(b"key-b").unwrap().unwrap();
        assert_eq!(a.snapshot.name, "A");
        assert_eq!(b.snapshot.name, "B");
    }

    #[test]
    fn roundtrip_snapshot_format() {
        let dir = tempfile::tempdir().unwrap();
        let store = SnapshotStore::open(&dir.path().join("snapshots")).unwrap();

        let mut metadata = HashMap::new();
        metadata.insert(
            "position-type".to_string(),
            "GlobalIndexPosition".to_string(),
        );
        metadata.insert("source".to_string(), "order-service".to_string());

        let snap = Snapshot {
            name: "OrderProjection".to_string(),
            version: "2.1".to_string(),
            payload: vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02],
            timestamp: 1712345678999,
            metadata,
        };

        store.add(b"test-key", 42, &snap, false).unwrap();

        let entry = store.get_last(b"test-key").unwrap().unwrap();
        assert_eq!(entry.snapshot.name, "OrderProjection");
        assert_eq!(entry.snapshot.version, "2.1");
        assert_eq!(
            entry.snapshot.payload,
            vec![0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x01, 0x02]
        );
        assert_eq!(entry.snapshot.timestamp, 1712345678999);
        assert_eq!(entry.snapshot.metadata.len(), 2);
    }
}
