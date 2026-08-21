//! Compact on-disk/wire format for metadata-only Raft snapshots.
//!
//! Event bytes are replicated and recovered by the native segment data plane;
//! they must never enter a Raft request or state-machine snapshot. Openraft's
//! `SnapshotMeta` remains the carrier for `last_log_id` and membership. This
//! payload carries only context names and the current leader claim.
//!
//! Layout ("KSM5"):
//!
//! ```text
//! magic "KSM5" (4) | version u8 = 5 | payload_len u32 LE
//! bincode(MetadataSnapshot) | crc32c u32 LE
//! ```
//!
//! "KSM4" (no handler registry) is still readable: its payload decodes
//! into the v4 shape and upgrades with an empty handler table.

use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};

use super::handler_registry::HandlerRegistration;
use super::types::LeaderClaim;

pub const MAGIC: &[u8; 4] = b"KSM5";
pub const VERSION: u8 = 5;

const LEGACY_MAGIC: &[u8; 4] = b"KSM4";
const LEGACY_VERSION: u8 = 4;

const HEADER_LEN: usize = 9;
const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

/// All application metadata represented by a state-machine snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataSnapshot {
    pub contexts: Vec<String>,
    pub leader_claim: Option<LeaderClaim>,
    /// Replicated messaging-handler registrations (ADR-0007).
    pub handlers: Vec<HandlerRegistration>,
}

/// The v4 payload shape, for upgrading pre-fabric snapshots.
#[derive(Deserialize)]
struct MetadataSnapshotV4 {
    contexts: Vec<String>,
    leader_claim: Option<LeaderClaim>,
}

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Writes one complete metadata snapshot and returns the inner writer.
pub fn write_snapshot<W: Write>(mut writer: W, snapshot: &MetadataSnapshot) -> io::Result<W> {
    let payload = bincode::serialize(snapshot).map_err(|e| invalid(e.to_string()))?;
    if payload.len() > MAX_PAYLOAD_BYTES {
        return Err(invalid(format!(
            "metadata snapshot exceeds {MAX_PAYLOAD_BYTES} bytes"
        )));
    }
    let payload_len: u32 = payload
        .len()
        .try_into()
        .map_err(|_| invalid("metadata snapshot length exceeds u32::MAX"))?;

    writer.write_all(MAGIC)?;
    writer.write_all(&[VERSION])?;
    writer.write_all(&payload_len.to_le_bytes())?;
    writer.write_all(&payload)?;
    writer.write_all(&crc32c::crc32c(&payload).to_le_bytes())?;
    Ok(writer)
}

/// Reads and validates one complete metadata snapshot.
pub fn read_snapshot<R: Read>(mut reader: R) -> io::Result<MetadataSnapshot> {
    let mut header = [0u8; HEADER_LEN];
    reader
        .read_exact(&mut header)
        .map_err(|_| invalid("snapshot data too short for header"))?;
    let legacy = &header[0..4] == LEGACY_MAGIC && header[4] == LEGACY_VERSION;
    if !legacy && (&header[0..4] != MAGIC || header[4] != VERSION) {
        return Err(invalid(format!(
            "unsupported snapshot data format {:?}/{}",
            &header[0..4],
            header[4]
        )));
    }

    let payload_len = u32::from_le_bytes(header[5..9].try_into().unwrap()) as usize;
    if payload_len > MAX_PAYLOAD_BYTES {
        return Err(invalid(format!(
            "metadata snapshot payload exceeds {MAX_PAYLOAD_BYTES} bytes"
        )));
    }
    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload)?;
    let mut crc = [0u8; 4];
    reader.read_exact(&mut crc)?;
    if crc32c::crc32c(&payload) != u32::from_le_bytes(crc) {
        return Err(invalid("metadata snapshot CRC mismatch"));
    }

    let mut trailing = [0u8; 1];
    if reader.read(&mut trailing)? != 0 {
        return Err(invalid("trailing bytes after metadata snapshot"));
    }

    if legacy {
        let v4: MetadataSnapshotV4 =
            bincode::deserialize(&payload).map_err(|e| invalid(e.to_string()))?;
        return Ok(MetadataSnapshot {
            contexts: v4.contexts,
            leader_claim: v4.leader_claim,
            handlers: Vec::new(),
        });
    }
    bincode::deserialize(&payload).map_err(|e| invalid(e.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::raft::types::LeaderClaim;

    #[test]
    fn metadata_roundtrip() {
        let snapshot = MetadataSnapshot {
            contexts: vec!["default".into(), "orders".into()],
            leader_claim: Some(LeaderClaim {
                epoch: 17,
                node_id: 2,
                term: 4,
                prior_epoch: 12,
                voters: vec![1, 2, 3],
                per_context_tails: BTreeMap::from([("orders".into(), 99)]),
            }),
            handlers: vec![HandlerRegistration {
                bus: "main".into(),
                kind: crate::raft::handler_registry::HandlerKind::Command,
                message_type: "CreateOrder".into(),
                client_id: "c1".into(),
                node_id: 2,
                load_factor: 100,
            }],
        };

        let bytes = write_snapshot(Vec::new(), &snapshot).unwrap();
        assert_eq!(read_snapshot(&bytes[..]).unwrap(), snapshot);
    }

    #[test]
    fn legacy_v4_upgrades_with_empty_handlers() {
        #[derive(Serialize)]
        struct V4 {
            contexts: Vec<String>,
            leader_claim: Option<LeaderClaim>,
        }
        let payload = bincode::serialize(&V4 {
            contexts: vec!["default".into()],
            leader_claim: None,
        })
        .unwrap();
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"KSM4");
        bytes.push(4);
        bytes.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&payload);
        bytes.extend_from_slice(&crc32c::crc32c(&payload).to_le_bytes());

        let snapshot = read_snapshot(&bytes[..]).unwrap();
        assert_eq!(snapshot.contexts, vec!["default".to_string()]);
        assert!(snapshot.handlers.is_empty());
    }

    #[test]
    fn bad_crc_is_rejected() {
        let mut bytes = write_snapshot(Vec::new(), &MetadataSnapshot::default()).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        assert!(read_snapshot(&bytes[..]).is_err());
    }

    #[test]
    fn bad_magic_is_rejected() {
        assert!(read_snapshot(&b"NOPE\x04rest"[..]).is_err());
    }
}
