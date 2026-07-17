//! Streaming on-disk/wire format for Raft state-machine snapshot DATA.
//!
//! Replaces the v1 whole-DB bincode blob (`StateMachineSnapshot`) with a
//! chunked stream so neither building nor installing a snapshot ever holds
//! more than one chunk of events in memory. `last_applied` and
//! `last_membership` are NOT in the data stream — openraft's `SnapshotMeta`
//! is their carrier (it always was; v1 stored them redundantly).
//!
//! # Layout ("KSD2")
//!
//! ```text
//! magic "KSD2" (4) | version u8 = 2
//! repeat per context:
//!   tag 0x01 | name_len u32 LE | name bytes (UTF-8)
//!   repeat per chunk:
//!     tag 0x02 | payload_len u32 LE | crc32c u32 LE | bincode(Vec<SnapshotEvent>)
//! tag 0x00 (end of stream)
//! ```
//!
//! Every chunk carries its own CRC over the bincode payload; readers verify
//! before deserializing. Format changes require bumping the version byte —
//! there is no migration from v1 (pre-1.0, no deployments; a node with a v1
//! snapshot on disk simply rebuilds at its next snapshot trigger).

use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};

use crate::event::StoredEvent;

pub const MAGIC: &[u8; 4] = b"KSD2";
pub const VERSION: u8 = 2;

const TAG_END: u8 = 0x00;
const TAG_CONTEXT: u8 = 0x01;
const TAG_CHUNK: u8 = 0x02;

/// Events per chunk while building. Bounds peak memory on both sides:
/// build holds one engine page, install holds one decoded chunk.
pub const CHUNK_EVENTS: usize = 4096;

/// One event frozen for transport in a snapshot. Mirrors `StoredEvent`
/// field-for-field but uses `(Vec<u8>, Vec<u8>)` tag tuples to stay in
/// lock-step with `RaftAppendEvent`'s wire shape so install can re-append
/// via `AppendEvent` directly.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotEvent {
    pub position: u64,
    pub identifier: String,
    pub name: String,
    pub version: String,
    pub timestamp: i64,
    pub payload: Vec<u8>,
    pub metadata: Vec<(String, String)>,
    pub tags: Vec<(Vec<u8>, Vec<u8>)>,
}

impl From<StoredEvent> for SnapshotEvent {
    fn from(s: StoredEvent) -> Self {
        Self {
            position: s.position.0,
            identifier: s.identifier,
            name: s.name,
            version: s.version,
            timestamp: s.timestamp,
            payload: s.payload,
            metadata: s.metadata,
            tags: s.tags.into_iter().map(|t| (t.key, t.value)).collect(),
        }
    }
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Incremental writer for the KSD2 stream.
pub struct SnapshotDataWriter<W: Write> {
    w: W,
}

impl<W: Write> SnapshotDataWriter<W> {
    pub fn new(mut w: W) -> io::Result<Self> {
        w.write_all(MAGIC)?;
        w.write_all(&[VERSION])?;
        Ok(Self { w })
    }

    pub fn begin_context(&mut self, name: &str) -> io::Result<()> {
        self.w.write_all(&[TAG_CONTEXT])?;
        self.w.write_all(&(name.len() as u32).to_le_bytes())?;
        self.w.write_all(name.as_bytes())?;
        Ok(())
    }

    pub fn write_chunk(&mut self, events: &[SnapshotEvent]) -> io::Result<()> {
        if events.is_empty() {
            return Ok(());
        }
        let payload = bincode::serialize(events).map_err(|e| invalid(e.to_string()))?;
        self.w.write_all(&[TAG_CHUNK])?;
        self.w.write_all(&(payload.len() as u32).to_le_bytes())?;
        self.w.write_all(&crc32c::crc32c(&payload).to_le_bytes())?;
        self.w.write_all(&payload)?;
        Ok(())
    }

    /// Writes the end marker and returns the inner writer (unflushed —
    /// callers flush/fsync as part of their own durability protocol).
    pub fn finish(mut self) -> io::Result<W> {
        self.w.write_all(&[TAG_END])?;
        Ok(self.w)
    }
}

/// One item produced while reading a KSD2 stream.
pub enum SnapshotItem {
    /// A new context begins; subsequent chunks belong to it.
    Context(String),
    /// One chunk of events for the current context.
    Chunk(Vec<SnapshotEvent>),
}

/// Incremental reader for the KSD2 stream.
pub struct SnapshotDataReader<R: Read> {
    r: R,
    done: bool,
}

impl<R: Read> SnapshotDataReader<R> {
    /// Validates magic + version WITHOUT consuming anything else. Callers
    /// use this before any destructive action (a payload we cannot
    /// interpret must never wipe follower state).
    pub fn new(mut r: R) -> io::Result<Self> {
        let mut header = [0u8; 5];
        r.read_exact(&mut header)
            .map_err(|_| invalid("snapshot data too short for header"))?;
        if &header[0..4] != MAGIC {
            return Err(invalid("bad snapshot data magic (expected KSD2)"));
        }
        if header[4] != VERSION {
            return Err(invalid(format!(
                "unsupported snapshot data version {}",
                header[4]
            )));
        }
        Ok(Self { r, done: false })
    }

    /// Returns the next item, or `None` at the end marker.
    pub fn next_item(&mut self) -> io::Result<Option<SnapshotItem>> {
        if self.done {
            return Ok(None);
        }
        let mut tag = [0u8; 1];
        self.r
            .read_exact(&mut tag)
            .map_err(|_| invalid("snapshot data truncated (missing tag)"))?;
        match tag[0] {
            TAG_END => {
                self.done = true;
                Ok(None)
            }
            TAG_CONTEXT => {
                let mut len = [0u8; 4];
                self.r.read_exact(&mut len)?;
                let mut name = vec![0u8; u32::from_le_bytes(len) as usize];
                self.r.read_exact(&mut name)?;
                let name =
                    String::from_utf8(name).map_err(|_| invalid("context name not UTF-8"))?;
                Ok(Some(SnapshotItem::Context(name)))
            }
            TAG_CHUNK => {
                let mut len = [0u8; 4];
                self.r.read_exact(&mut len)?;
                let mut crc = [0u8; 4];
                self.r.read_exact(&mut crc)?;
                let mut payload = vec![0u8; u32::from_le_bytes(len) as usize];
                self.r.read_exact(&mut payload)?;
                if crc32c::crc32c(&payload) != u32::from_le_bytes(crc) {
                    return Err(invalid("snapshot chunk CRC mismatch"));
                }
                let events: Vec<SnapshotEvent> =
                    bincode::deserialize(&payload).map_err(|e| invalid(e.to_string()))?;
                Ok(Some(SnapshotItem::Chunk(events)))
            }
            other => Err(invalid(format!("unknown snapshot data tag 0x{other:02x}"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(position: u64, name: &str) -> SnapshotEvent {
        SnapshotEvent {
            position,
            identifier: format!("id-{position}"),
            name: name.into(),
            version: "1".into(),
            timestamp: 1712345678000,
            payload: vec![position as u8; 16],
            metadata: vec![("k".into(), "v".into())],
            tags: vec![(b"order".to_vec(), format!("{position}").into_bytes())],
        }
    }

    #[test]
    fn roundtrip_multiple_contexts_and_chunks() {
        let mut buf = Vec::new();
        let mut w = SnapshotDataWriter::new(&mut buf).unwrap();
        w.begin_context("default").unwrap();
        w.write_chunk(&[ev(0, "A"), ev(1, "B")]).unwrap();
        w.write_chunk(&[ev(2, "C")]).unwrap();
        w.begin_context("empty-ctx").unwrap();
        w.begin_context("other").unwrap();
        w.write_chunk(&[ev(0, "D")]).unwrap();
        w.finish().unwrap();

        let mut r = SnapshotDataReader::new(&buf[..]).unwrap();
        let mut log = Vec::new();
        while let Some(item) = r.next_item().unwrap() {
            match item {
                SnapshotItem::Context(name) => log.push(format!("ctx:{name}")),
                SnapshotItem::Chunk(events) => log.push(format!(
                    "chunk:{}",
                    events
                        .iter()
                        .map(|e| e.name.clone())
                        .collect::<Vec<_>>()
                        .join(",")
                )),
            }
        }
        assert_eq!(
            log,
            vec![
                "ctx:default",
                "chunk:A,B",
                "chunk:C",
                "ctx:empty-ctx",
                "ctx:other",
                "chunk:D"
            ]
        );
    }

    #[test]
    fn bad_magic_rejected_before_any_item() {
        assert!(SnapshotDataReader::new(&b"NOPE\x02rest"[..]).is_err());
    }

    #[test]
    fn wrong_version_rejected() {
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC);
        buf.push(99);
        assert!(SnapshotDataReader::new(&buf[..]).is_err());
    }

    #[test]
    fn chunk_crc_mismatch_detected() {
        let mut buf = Vec::new();
        let mut w = SnapshotDataWriter::new(&mut buf).unwrap();
        w.begin_context("c").unwrap();
        w.write_chunk(&[ev(0, "A")]).unwrap();
        w.finish().unwrap();
        // Flip a byte inside the chunk payload (past header+ctx+chunk framing).
        let n = buf.len();
        buf[n - 2] ^= 0xFF;
        let mut r = SnapshotDataReader::new(&buf[..]).unwrap();
        assert!(matches!(r.next_item(), Ok(Some(SnapshotItem::Context(_)))));
        assert!(r.next_item().is_err());
    }

    #[test]
    fn truncated_stream_is_error_not_eof() {
        let mut buf = Vec::new();
        let mut w = SnapshotDataWriter::new(&mut buf).unwrap();
        w.begin_context("c").unwrap();
        w.write_chunk(&[ev(0, "A")]).unwrap();
        w.finish().unwrap();
        buf.truncate(buf.len() - 4); // drop the end marker + payload tail
        let mut r = SnapshotDataReader::new(&buf[..]).unwrap();
        let _ = r.next_item(); // context
        assert!(r.next_item().is_err());
    }
}
