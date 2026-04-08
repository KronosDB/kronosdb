use crate::error::Error;
use crate::event::{Position, StoredEvent};
use crate::event::Tag;

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
    let s = String::from_utf8(data[*cursor..*cursor + len].to_vec()).map_err(|_| {
        Error::Corrupted {
            message: "invalid UTF-8 in string field".into(),
        }
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
