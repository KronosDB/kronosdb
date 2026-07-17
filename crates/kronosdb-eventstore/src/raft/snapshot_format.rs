//! Compact on-disk/wire format for metadata-only Raft snapshots.
//!
//! Event bytes are replicated and recovered by the native segment data plane;
//! they must never enter a Raft request or state-machine snapshot. Openraft's
//! `SnapshotMeta` remains the carrier for `last_log_id` and membership. This
//! payload carries only context names and the current leader claim.
//!
//! Layout ("KSM4"):
//!
//! ```text
//! magic "KSM4" (4) | version u8 = 4 | payload_len u32 LE
//! bincode(MetadataSnapshot) | crc32c u32 LE
//! ```

use std::io::{self, Read, Write};

use serde::{Deserialize, Serialize};

use super::types::LeaderClaim;

pub const MAGIC: &[u8; 4] = b"KSM4";
pub const VERSION: u8 = 4;

const HEADER_LEN: usize = 9;
const MAX_PAYLOAD_BYTES: usize = 16 * 1024 * 1024;

/// All application metadata represented by a state-machine snapshot.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataSnapshot {
    pub contexts: Vec<String>,
    pub leader_claim: Option<LeaderClaim>,
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
    if &header[0..4] != MAGIC || header[4] != VERSION {
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
        };

        let bytes = write_snapshot(Vec::new(), &snapshot).unwrap();
        assert_eq!(read_snapshot(&bytes[..]).unwrap(), snapshot);
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
