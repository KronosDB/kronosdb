use crate::error::Error;
use crate::event::Tag;
use crate::event::{Position, StoredEvent};

/// Type of Raft log entry encoded by a `RaftMarker` record.
///
/// Mirrors openraft's `EntryPayload` shape — Normal carries a client-proposed
/// request (which for KronosDB is an Append of N events), Membership carries
/// a cluster configuration change, Blank is a no-op emitted at term boundaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RaftEntryType {
    Normal = 0,
    Membership = 1,
    Blank = 2,
}

impl RaftEntryType {
    pub fn from_u8(v: u8) -> Result<Self, Error> {
        match v {
            0 => Ok(Self::Normal),
            1 => Ok(Self::Membership),
            2 => Ok(Self::Blank),
            other => Err(Error::Corrupted {
                message: format!("unknown raft entry type: {other}"),
            }),
        }
    }
}

/// On-disk marker that wraps a Raft log entry.
///
/// For `Normal` entries, `event_count` event records immediately follow the
/// marker in the same segment (in write order). For `Membership`, `extra`
/// carries the serialized membership config. For `Blank`, both are empty.
///
/// Readers that only care about events skip markers entirely. Readers that
/// act as the Raft log iterate markers and ignore the intervening event
/// records (they're read alongside the marker they belong to).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftMarker {
    pub term: u64,
    pub index: u64,
    pub entry_type: RaftEntryType,
    /// Number of event records following this marker. Only meaningful for `Normal`.
    pub event_count: u16,
    /// Extra payload (bincode-encoded) for entry types that carry one.
    /// `Membership` uses this; `Normal` and `Blank` leave it empty.
    pub extra: Vec<u8>,
}

impl RaftMarker {
    pub fn normal(term: u64, index: u64, event_count: u16) -> Self {
        Self {
            term,
            index,
            entry_type: RaftEntryType::Normal,
            event_count,
            extra: Vec::new(),
        }
    }

    pub fn membership(term: u64, index: u64, extra: Vec<u8>) -> Self {
        Self {
            term,
            index,
            entry_type: RaftEntryType::Membership,
            event_count: 0,
            extra,
        }
    }

    pub fn blank(term: u64, index: u64) -> Self {
        Self {
            term,
            index,
            entry_type: RaftEntryType::Blank,
            event_count: 0,
            extra: Vec::new(),
        }
    }
}

/// Serializes a Raft marker into the binary on-disk format.
///
/// Layout:
///   term:        u64  (8 bytes)
///   index:       u64  (8 bytes)
///   entry_type:  u8   (1 byte)
///   event_count: u16  (2 bytes)
///   extra_len:   u32  (4 bytes)
///   extra:       [u8] (variable)
///
/// Total fixed overhead: 23 bytes + extra. With the enclosing record header
/// (9 bytes including the flags byte), a Blank/Normal marker is 32 bytes.
pub fn serialize_raft_marker(marker: &RaftMarker, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&marker.term.to_le_bytes());
    buf.extend_from_slice(&marker.index.to_le_bytes());
    buf.push(marker.entry_type as u8);
    buf.extend_from_slice(&marker.event_count.to_le_bytes());
    buf.extend_from_slice(&(marker.extra.len() as u32).to_le_bytes());
    buf.extend_from_slice(&marker.extra);
}

/// Deserializes a Raft marker. Returns the marker and the bytes consumed.
pub fn deserialize_raft_marker(data: &[u8]) -> Result<(RaftMarker, usize), Error> {
    let mut cursor = 0;
    let term = read_u64(data, &mut cursor)?;
    let index = read_u64(data, &mut cursor)?;
    let type_byte = read_u8(data, &mut cursor)?;
    let entry_type = RaftEntryType::from_u8(type_byte)?;
    let event_count = read_u16(data, &mut cursor)?;
    let extra_len = read_u32(data, &mut cursor)? as usize;
    if cursor + extra_len > data.len() {
        return Err(Error::Corrupted {
            message: "raft marker extra bytes extend beyond record boundary".into(),
        });
    }
    let extra = data[cursor..cursor + extra_len].to_vec();
    cursor += extra_len;
    Ok((
        RaftMarker {
            term,
            index,
            entry_type,
            event_count,
            extra,
        },
        cursor,
    ))
}

/// Serializes a stored event (with tags) into the binary on-disk format.
///
/// Layout:
///   position:       u64  (8 bytes)
///   identifier_len: u16  (2 bytes)
///   identifier:     [u8] (variable)
///   name_len:       u16  (2 bytes)
///   name:           [u8] (variable)
///   version_len:    u16  (2 bytes)
///   version:        [u8] (variable)
///   timestamp:      i64  (8 bytes)
///   metadata_count: u16  (2 bytes)
///   for each metadata entry:
///     key_len:      u16  (2 bytes)
///     key:          [u8] (variable)
///     value_len:    u16  (2 bytes)
///     value:        [u8] (variable)
///   tag_count:      u16  (2 bytes)
///   for each tag:
///     key_len:      u16  (2 bytes)
///     key:          [u8] (variable)
///     value_len:    u16  (2 bytes)
///     value:        [u8] (variable)
///   payload_len:    u32  (4 bytes)
///   payload:        [u8] (variable)
///
/// Payload is last so we can skip over fixed-size fields quickly
/// without reading the (potentially large) payload.
pub fn serialize_event(event: &StoredEvent, buf: &mut Vec<u8>) {
    buf.extend_from_slice(&event.position.0.to_le_bytes());

    let id_bytes = event.identifier.as_bytes();
    buf.extend_from_slice(&(id_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(id_bytes);

    let name_bytes = event.name.as_bytes();
    buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(name_bytes);

    let version_bytes = event.version.as_bytes();
    buf.extend_from_slice(&(version_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(version_bytes);

    buf.extend_from_slice(&event.timestamp.to_le_bytes());

    buf.extend_from_slice(&(event.metadata.len() as u16).to_le_bytes());
    for (key, value) in &event.metadata {
        let key_bytes = key.as_bytes();
        buf.extend_from_slice(&(key_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(key_bytes);
        let value_bytes = value.as_bytes();
        buf.extend_from_slice(&(value_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(value_bytes);
    }

    buf.extend_from_slice(&(event.tags.len() as u16).to_le_bytes());
    for tag in &event.tags {
        buf.extend_from_slice(&(tag.key.len() as u16).to_le_bytes());
        buf.extend_from_slice(&tag.key);
        buf.extend_from_slice(&(tag.value.len() as u16).to_le_bytes());
        buf.extend_from_slice(&tag.value);
    }

    buf.extend_from_slice(&(event.payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(&event.payload);
}

/// Deserializes a stored event from the binary on-disk format.
/// Returns the event and the number of bytes consumed.
pub fn deserialize_event(data: &[u8]) -> Result<(StoredEvent, usize), Error> {
    let mut cursor = 0;

    let position = Position(read_u64(data, &mut cursor)?);
    let identifier = read_string(data, &mut cursor)?;
    let name = read_string(data, &mut cursor)?;
    let version = read_string(data, &mut cursor)?;
    let timestamp = read_i64(data, &mut cursor)?;

    let metadata_count = read_u16(data, &mut cursor)? as usize;
    let mut metadata = Vec::with_capacity(metadata_count);
    for _ in 0..metadata_count {
        let key = read_string(data, &mut cursor)?;
        let value = read_string(data, &mut cursor)?;
        metadata.push((key, value));
    }

    let tag_count = read_u16(data, &mut cursor)? as usize;
    let mut tags = Vec::with_capacity(tag_count);
    for _ in 0..tag_count {
        let key = read_bytes(data, &mut cursor)?;
        let value = read_bytes(data, &mut cursor)?;
        tags.push(Tag { key, value });
    }

    let payload_len = read_u32(data, &mut cursor)? as usize;
    if cursor + payload_len > data.len() {
        return Err(Error::Corrupted {
            message: "payload extends beyond record boundary".into(),
        });
    }
    let payload = data[cursor..cursor + payload_len].to_vec();
    cursor += payload_len;

    let event = StoredEvent {
        position,
        identifier,
        name,
        version,
        timestamp,
        payload,
        metadata,
        tags,
    };
    Ok((event, cursor))
}

fn read_u8(data: &[u8], cursor: &mut usize) -> Result<u8, Error> {
    if *cursor + 1 > data.len() {
        return Err(Error::Corrupted {
            message: "unexpected end of data reading u8".into(),
        });
    }
    let value = data[*cursor];
    *cursor += 1;
    Ok(value)
}

fn read_u16(data: &[u8], cursor: &mut usize) -> Result<u16, Error> {
    if *cursor + 2 > data.len() {
        return Err(Error::Corrupted {
            message: "unexpected end of data reading u16".into(),
        });
    }
    let value = u16::from_le_bytes([data[*cursor], data[*cursor + 1]]);
    *cursor += 2;
    Ok(value)
}

fn read_u32(data: &[u8], cursor: &mut usize) -> Result<u32, Error> {
    if *cursor + 4 > data.len() {
        return Err(Error::Corrupted {
            message: "unexpected end of data reading u32".into(),
        });
    }
    let value = u32::from_le_bytes(data[*cursor..*cursor + 4].try_into().unwrap());
    *cursor += 4;
    Ok(value)
}

fn read_u64(data: &[u8], cursor: &mut usize) -> Result<u64, Error> {
    if *cursor + 8 > data.len() {
        return Err(Error::Corrupted {
            message: "unexpected end of data reading u64".into(),
        });
    }
    let value = u64::from_le_bytes(data[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

fn read_i64(data: &[u8], cursor: &mut usize) -> Result<i64, Error> {
    if *cursor + 8 > data.len() {
        return Err(Error::Corrupted {
            message: "unexpected end of data reading i64".into(),
        });
    }
    let value = i64::from_le_bytes(data[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

fn read_string(data: &[u8], cursor: &mut usize) -> Result<String, Error> {
    let len = read_u16(data, cursor)? as usize;
    if *cursor + len > data.len() {
        return Err(Error::Corrupted {
            message: "string extends beyond record boundary".into(),
        });
    }
    let s =
        String::from_utf8(data[*cursor..*cursor + len].to_vec()).map_err(|_| Error::Corrupted {
            message: "invalid UTF-8 in string field".into(),
        })?;
    *cursor += len;
    Ok(s)
}

fn read_bytes(data: &[u8], cursor: &mut usize) -> Result<Vec<u8>, Error> {
    let len = read_u16(data, cursor)? as usize;
    if *cursor + len > data.len() {
        return Err(Error::Corrupted {
            message: "bytes extend beyond record boundary".into(),
        });
    }
    let b = data[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(b)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_event_with_tags() {
        let event = StoredEvent {
            position: Position(42),
            identifier: "evt-001".into(),
            name: "OrderPlaced".into(),
            version: "1.0".into(),
            timestamp: 1712345678000,
            payload: b"hello world".to_vec(),
            metadata: vec![
                ("correlationId".into(), "abc-123".into()),
                ("source".into(), "order-service".into()),
            ],
            tags: vec![
                Tag::from_str("orderId", "order-1"),
                Tag::from_str("region", "EU"),
            ],
        };

        let mut buf = Vec::new();
        serialize_event(&event, &mut buf);

        let (decoded, bytes_read) = deserialize_event(&buf).unwrap();
        assert_eq!(bytes_read, buf.len());
        assert_eq!(decoded.position, event.position);
        assert_eq!(decoded.identifier, event.identifier);
        assert_eq!(decoded.name, event.name);
        assert_eq!(decoded.version, event.version);
        assert_eq!(decoded.timestamp, event.timestamp);
        assert_eq!(decoded.payload, event.payload);
        assert_eq!(decoded.metadata, event.metadata);
        assert_eq!(decoded.tags, event.tags);
    }

    #[test]
    fn roundtrip_normal_marker() {
        let marker = RaftMarker::normal(7, 1234, 5);
        let mut buf = Vec::new();
        serialize_raft_marker(&marker, &mut buf);
        let (decoded, consumed) = deserialize_raft_marker(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded, marker);
    }

    #[test]
    fn roundtrip_membership_marker() {
        let marker = RaftMarker::membership(3, 99, vec![1, 2, 3, 4, 5]);
        let mut buf = Vec::new();
        serialize_raft_marker(&marker, &mut buf);
        let (decoded, consumed) = deserialize_raft_marker(&buf).unwrap();
        assert_eq!(consumed, buf.len());
        assert_eq!(decoded, marker);
        assert_eq!(decoded.extra, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn roundtrip_blank_marker() {
        let marker = RaftMarker::blank(5, 42);
        let mut buf = Vec::new();
        serialize_raft_marker(&marker, &mut buf);
        let (decoded, _) = deserialize_raft_marker(&buf).unwrap();
        assert_eq!(decoded, marker);
        assert_eq!(decoded.entry_type, RaftEntryType::Blank);
    }

    #[test]
    fn unknown_entry_type_is_corrupted() {
        // term(8) + index(8) + invalid type byte + event_count(2) + extra_len(4)
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u64.to_le_bytes());
        buf.extend_from_slice(&2u64.to_le_bytes());
        buf.push(99); // bogus
        buf.extend_from_slice(&0u16.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        assert!(deserialize_raft_marker(&buf).is_err());
    }

    #[test]
    fn roundtrip_empty_event() {
        let event = StoredEvent {
            position: Position(1),
            identifier: String::new(),
            name: "Ping".into(),
            version: String::new(),
            timestamp: 0,
            payload: vec![],
            metadata: vec![],
            tags: vec![],
        };

        let mut buf = Vec::new();
        serialize_event(&event, &mut buf);

        let (decoded, _) = deserialize_event(&buf).unwrap();
        assert_eq!(decoded.name, "Ping");
        assert_eq!(decoded.payload.len(), 0);
        assert_eq!(decoded.metadata.len(), 0);
        assert_eq!(decoded.tags.len(), 0);
    }
}
