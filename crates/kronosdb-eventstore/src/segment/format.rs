use crate::error::Error;
use crate::event::Tag;
use crate::event::{AppendEvent, Position, StoredEvent};

/// Native data-plane control records interleaved with events. They consume
/// segment bytes but no event position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlRecord {
    EpochChange {
        epoch: u64,
        leader_id: u64,
        start_position: u64,
    },
    WatermarkCheckpoint {
        epoch: u64,
        position: u64,
    },
}

/// Serializes a native control record and returns the full flags byte (type +
/// subtype) to use for the enclosing segment record.
pub fn serialize_control(record: &ControlRecord, buf: &mut Vec<u8>) -> u8 {
    match *record {
        ControlRecord::EpochChange {
            epoch,
            leader_id,
            start_position,
        } => {
            buf.extend_from_slice(&epoch.to_le_bytes());
            buf.extend_from_slice(&leader_id.to_le_bytes());
            buf.extend_from_slice(&start_position.to_le_bytes());
            crate::segment::flags::CONTROL_EPOCH_CHANGE
        }
        ControlRecord::WatermarkCheckpoint { epoch, position } => {
            buf.extend_from_slice(&epoch.to_le_bytes());
            buf.extend_from_slice(&position.to_le_bytes());
            crate::segment::flags::CONTROL_WATERMARK_CHECKPOINT
        }
    }
}

pub fn deserialize_control(flags: u8, data: &[u8]) -> Result<ControlRecord, Error> {
    let mut cursor = 0;
    let record = match flags {
        crate::segment::flags::CONTROL_EPOCH_CHANGE => ControlRecord::EpochChange {
            epoch: read_u64(data, &mut cursor)?,
            leader_id: read_u64(data, &mut cursor)?,
            start_position: read_u64(data, &mut cursor)?,
        },
        crate::segment::flags::CONTROL_WATERMARK_CHECKPOINT => ControlRecord::WatermarkCheckpoint {
            epoch: read_u64(data, &mut cursor)?,
            position: read_u64(data, &mut cursor)?,
        },
        other => {
            return Err(Error::Corrupted {
                message: format!("unknown control record flags: {other:#04x}"),
            });
        }
    };
    if cursor != data.len() {
        return Err(Error::Corrupted {
            message: "control record has trailing bytes".into(),
        });
    }
    Ok(record)
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
    serialize_event_fields(
        event.position,
        &event.identifier,
        &event.name,
        &event.version,
        event.timestamp,
        &event.metadata,
        &event.tags,
        &event.payload,
        buf,
    )
}

/// Serializes an event being appended at `position` straight from the
/// request's fields. The writer's hot path: no `StoredEvent` is built, so
/// the payload, tags and metadata are read once and never copied.
pub fn serialize_append_event(position: Position, event: &AppendEvent, buf: &mut Vec<u8>) {
    serialize_event_fields(
        position,
        &event.identifier,
        &event.name,
        &event.version,
        event.timestamp,
        &event.metadata,
        &event.tags,
        &event.payload,
        buf,
    )
}

/// The exact number of bytes `serialize_append_event` writes for `event`
/// (the record's payload, without the 9-byte record header).
pub fn serialized_append_event_len(event: &AppendEvent) -> usize {
    8 + 2
        + event.identifier.len()
        + 2
        + event.name.len()
        + 2
        + event.version.len()
        + 8
        + 2
        + event
            .metadata
            .iter()
            .map(|(key, value)| 4 + key.len() + value.len())
            .sum::<usize>()
        + 2
        + event
            .tags
            .iter()
            .map(|tag| 4 + tag.key.len() + tag.value.len())
            .sum::<usize>()
        + 4
        + event.payload.len()
}

#[allow(clippy::too_many_arguments)]
fn serialize_event_fields(
    position: Position,
    identifier: &str,
    name: &str,
    version: &str,
    timestamp: i64,
    metadata: &[(String, String)],
    tags: &[Tag],
    payload: &[u8],
    buf: &mut Vec<u8>,
) {
    buf.extend_from_slice(&position.0.to_le_bytes());

    let id_bytes = identifier.as_bytes();
    buf.extend_from_slice(&(id_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(id_bytes);

    let name_bytes = name.as_bytes();
    buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(name_bytes);

    let version_bytes = version.as_bytes();
    buf.extend_from_slice(&(version_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(version_bytes);

    buf.extend_from_slice(&timestamp.to_le_bytes());

    buf.extend_from_slice(&(metadata.len() as u16).to_le_bytes());
    for (key, value) in metadata {
        let key_bytes = key.as_bytes();
        buf.extend_from_slice(&(key_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(key_bytes);
        let value_bytes = value.as_bytes();
        buf.extend_from_slice(&(value_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(value_bytes);
    }

    buf.extend_from_slice(&(tags.len() as u16).to_le_bytes());
    for tag in tags {
        buf.extend_from_slice(&(tag.key.len() as u16).to_le_bytes());
        buf.extend_from_slice(&tag.key);
        buf.extend_from_slice(&(tag.value.len() as u16).to_le_bytes());
        buf.extend_from_slice(&tag.value);
    }

    buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buf.extend_from_slice(payload);
}

/// The fields a follower needs to maintain its active indexes while applying
/// raw replicated records. The event payload is deliberately not decoded or
/// copied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventIndexFields {
    pub position: Position,
    pub name: String,
    pub tags: Vec<Tag>,
}

/// Partially decodes an event for follower-side indexing. Validates and skips
/// identifier, version, timestamp, metadata, and payload, materializing only
/// position, name, and tags. Returns the fields and bytes consumed.
pub fn deserialize_event_index_fields(data: &[u8]) -> Result<(EventIndexFields, usize), Error> {
    let mut cursor = 0;

    let position = Position(read_u64(data, &mut cursor)?);
    skip_bytes_u16(data, &mut cursor)?; // identifier
    let name = read_string(data, &mut cursor)?;
    skip_bytes_u16(data, &mut cursor)?; // version
    let _timestamp = read_i64(data, &mut cursor)?;

    let metadata_count = read_u16(data, &mut cursor)? as usize;
    for _ in 0..metadata_count {
        skip_bytes_u16(data, &mut cursor)?; // key
        skip_bytes_u16(data, &mut cursor)?; // value
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
    cursor += payload_len;

    Ok((
        EventIndexFields {
            position,
            name,
            tags,
        },
        cursor,
    ))
}

/// What a read needs to know about a stored event before deciding to copy
/// it, read in place without decoding it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Peek<'a> {
    /// The event type name, as stored.
    pub name: &'a [u8],
    /// A tag key is in the reserved `$` namespace: the event is visible but
    /// the tag must be stripped.
    pub system_tags: bool,
}

impl Peek<'_> {
    /// The event type is reserved: the event is invisible to clients.
    pub fn is_system_event(&self) -> bool {
        self.name.first() == Some(&crate::system::SYSTEM_PREFIX)
    }
}

/// Reads an event's name and tag keys in place.
pub fn peek(data: &[u8]) -> Result<Peek<'_>, Error> {
    let mut cursor = 8; // position
    skip_bytes_u16(data, &mut cursor)?; // identifier
    let name_len = read_u16(data, &mut cursor)? as usize;
    let name = data
        .get(cursor..cursor + name_len)
        .ok_or_else(|| Error::Corrupted {
            message: "event name extends beyond record boundary".into(),
        })?;
    cursor += name_len;
    skip_bytes_u16(data, &mut cursor)?; // version
    cursor += 8; // timestamp
    let metadata_count = read_u16(data, &mut cursor)? as usize;
    for _ in 0..metadata_count {
        skip_bytes_u16(data, &mut cursor)?;
        skip_bytes_u16(data, &mut cursor)?;
    }
    let tag_count = read_u16(data, &mut cursor)? as usize;
    let mut system_tags = false;
    for _ in 0..tag_count {
        let key_len = read_u16(data, &mut cursor)? as usize;
        system_tags |= key_len > 0 && data.get(cursor) == Some(&crate::system::SYSTEM_PREFIX);
        cursor += key_len;
        skip_bytes_u16(data, &mut cursor)?; // value
    }
    Ok(Peek { name, system_tags })
}

/// An event's name and tags, read in place: what matching a live
/// subscription needs, with nothing copied or allocated.
#[derive(Debug, Clone, Copy)]
pub struct Scan<'a> {
    /// The event type name, as stored.
    pub name: &'a [u8],
    tags: &'a [u8],
    count: usize,
}

impl<'a> Scan<'a> {
    /// The event's tags as `(key, value)` byte pairs, in stored order.
    pub fn tags(&self) -> Tags<'a> {
        Tags {
            data: self.tags,
            cursor: 0,
            left: self.count,
        }
    }
}

/// The tags of a scanned event. `scan` validated the region, so iterating
/// cannot fail.
#[derive(Debug, Clone)]
pub struct Tags<'a> {
    data: &'a [u8],
    cursor: usize,
    left: usize,
}

impl<'a> Iterator for Tags<'a> {
    type Item = (&'a [u8], &'a [u8]);

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        let mut field = || {
            let len =
                u16::from_le_bytes(self.data[self.cursor..self.cursor + 2].try_into().unwrap())
                    as usize;
            let bytes = &self.data[self.cursor + 2..self.cursor + 2 + len];
            self.cursor += 2 + len;
            bytes
        };
        let key = field();
        let value = field();
        Some((key, value))
    }
}

/// Checks an event body a client sent, in place, and reads its name and tags.
///
/// Accepts exactly what the protocol's `EventRecord` decoder accepts, no
/// more: every field in bounds, the identifier, name, version and metadata
/// strings valid UTF-8 (tag keys and values are bytes), and no byte left
/// over after the payload. `None` says only that the body is malformed;
/// the caller asks the protocol decoder why. The position field is not
/// looked at: the server sets it.
pub fn check_event(data: &[u8]) -> Option<Scan<'_>> {
    fn field<'a>(data: &'a [u8], cursor: &mut usize) -> Option<&'a [u8]> {
        let len_end = cursor.checked_add(2)?;
        let len = u16::from_le_bytes(data.get(*cursor..len_end)?.try_into().ok()?) as usize;
        let end = len_end.checked_add(len)?;
        let bytes = data.get(len_end..end)?;
        *cursor = end;
        Some(bytes)
    }
    fn text<'a>(data: &'a [u8], cursor: &mut usize) -> Option<&'a [u8]> {
        let bytes = field(data, cursor)?;
        std::str::from_utf8(bytes).ok()?;
        Some(bytes)
    }
    fn count(data: &[u8], cursor: &mut usize) -> Option<usize> {
        let end = cursor.checked_add(2)?;
        let n = u16::from_le_bytes(data.get(*cursor..end)?.try_into().ok()?) as usize;
        *cursor = end;
        Some(n)
    }

    let mut cursor = 8; // position
    if data.len() < cursor {
        return None;
    }
    text(data, &mut cursor)?; // identifier
    let name = text(data, &mut cursor)?;
    text(data, &mut cursor)?; // version
    cursor = cursor.checked_add(8).filter(|&end| end <= data.len())?; // timestamp
    for _ in 0..count(data, &mut cursor)? {
        text(data, &mut cursor)?; // metadata key
        text(data, &mut cursor)?; // metadata value
    }
    let tag_count = count(data, &mut cursor)?;
    let tags_start = cursor;
    for _ in 0..tag_count {
        field(data, &mut cursor)?; // key
        field(data, &mut cursor)?; // value
    }
    let tags = &data[tags_start..cursor];
    let payload_end = cursor.checked_add(4)?;
    let payload_len = u32::from_le_bytes(data.get(cursor..payload_end)?.try_into().ok()?) as usize;
    if payload_end.checked_add(payload_len)? != data.len() {
        return None;
    }
    Some(Scan {
        name,
        tags,
        count: tag_count,
    })
}

/// Reads an event's name and tags in place, validating the bounds of every
/// field it passes.
pub fn scan(data: &[u8]) -> Result<Scan<'_>, Error> {
    let mut cursor = 8; // position
    skip_bytes_u16(data, &mut cursor)?; // identifier
    let name_len = read_u16(data, &mut cursor)? as usize;
    let name = data
        .get(cursor..cursor + name_len)
        .ok_or_else(|| Error::Corrupted {
            message: "event name extends beyond record boundary".into(),
        })?;
    cursor += name_len;
    skip_bytes_u16(data, &mut cursor)?; // version
    cursor += 8; // timestamp
    let metadata_count = read_u16(data, &mut cursor)? as usize;
    for _ in 0..metadata_count {
        skip_bytes_u16(data, &mut cursor)?;
        skip_bytes_u16(data, &mut cursor)?;
    }
    let count = read_u16(data, &mut cursor)? as usize;
    let start = cursor;
    for _ in 0..count {
        skip_bytes_u16(data, &mut cursor)?; // key
        skip_bytes_u16(data, &mut cursor)?; // value
    }
    Ok(Scan {
        name,
        tags: &data[start..cursor],
        count,
    })
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

fn skip_bytes_u16(data: &[u8], cursor: &mut usize) -> Result<(), Error> {
    let len = read_u16(data, cursor)? as usize;
    if *cursor + len > data.len() {
        return Err(Error::Corrupted {
            message: "bytes extend beyond record boundary".into(),
        });
    }
    *cursor += len;
    Ok(())
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
    fn serialized_length_is_exact() {
        for (metadata, tags, payload) in [
            (vec![], vec![], 0usize),
            (
                vec![("k".to_string(), "value".to_string())],
                vec![Tag::from_str("orderId", "o-1"), Tag::from_str("a", "")],
                300,
            ),
        ] {
            let event = AppendEvent {
                identifier: "id-1".into(),
                name: "OrderPlaced".into(),
                version: "2".into(),
                timestamp: 7,
                payload: vec![1; payload],
                metadata,
                tags,
            };
            let mut buf = Vec::new();
            serialize_append_event(Position(5), &event, &mut buf);
            assert_eq!(serialized_append_event_len(&event), buf.len());
        }
    }

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

    /// `check_event` is the protocol's `EventRecord` decoder in place: on
    /// well-formed bodies, and on every kind of damage a client could send,
    /// it accepts exactly when the decoder does and reads the same name and
    /// tags.
    #[test]
    fn check_event_agrees_with_the_protocol_decoder() {
        use kronosdb_protocol as p;

        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        let texts = [
            "",
            "a",
            "OrderPlaced",
            "héllo",
            "日本語",
            "$sys",
            "x".repeat(300).leak(),
        ];
        let mut accepted = 0;
        let mut rejected = 0;
        for round in 0..4000 {
            let text = |n: u64| texts[(n % texts.len() as u64) as usize];
            let metadata: Vec<_> = (0..next() % 3)
                .map(|_| p::MetadataEntry {
                    key: text(next()),
                    value: text(next()),
                })
                .collect();
            let tag_data: Vec<(Vec<u8>, Vec<u8>)> = (0..next() % 4)
                .map(|_| {
                    (
                        text(next()).as_bytes().to_vec(),
                        // Tag values are bytes: not necessarily UTF-8.
                        (0..next() % 5).map(|_| next() as u8).collect(),
                    )
                })
                .collect();
            let payload: Vec<u8> = (0..next() % 40).map(|_| next() as u8).collect();
            let record = p::EventRecord {
                position: next(),
                identifier: text(next()),
                name: text(next()),
                version: text(next()),
                timestamp: next() as i64,
                metadata,
                tags: tag_data
                    .iter()
                    .map(|(key, value)| p::Tag { key, value })
                    .collect(),
                payload: &payload,
            };
            let mut body = Vec::new();
            record.encode(&mut body).unwrap();

            // Intact, then damaged in a few ways.
            let mut variants = vec![body.clone()];
            for _ in 0..6 {
                let mut damaged = body.clone();
                match next() % 5 {
                    0 if !damaged.is_empty() => {
                        let at = (next() % damaged.len() as u64) as usize;
                        damaged[at] ^= 1 << (next() % 8);
                    }
                    1 if !damaged.is_empty() => {
                        let at = (next() % damaged.len() as u64) as usize;
                        damaged.truncate(at);
                    }
                    2 => damaged.push(next() as u8),
                    3 if !damaged.is_empty() => {
                        let at = (next() % damaged.len() as u64) as usize;
                        damaged[at] = next() as u8;
                    }
                    _ => {
                        let at = (next() % (damaged.len() as u64 + 1)) as usize;
                        damaged.insert(at, next() as u8);
                    }
                }
                variants.push(damaged);
            }

            for (n, variant) in variants.iter().enumerate() {
                let decoded = p::RecordRef {
                    flags: 0,
                    body: variant,
                }
                .event();
                let checked = check_event(variant);
                assert_eq!(
                    checked.is_some(),
                    decoded.is_ok(),
                    "round {round} variant {n}: check_event and the decoder disagree on {variant:?}"
                );
                if let (Some(scan), Ok(event)) = (checked, decoded) {
                    accepted += 1;
                    assert_eq!(scan.name, event.name.as_bytes());
                    let tags: Vec<_> = scan.tags().collect();
                    let want: Vec<_> = event.tags.iter().map(|tag| (tag.key, tag.value)).collect();
                    assert_eq!(tags, want);
                } else {
                    rejected += 1;
                }
            }
        }
        assert!(
            accepted > 3000 && rejected > 3000,
            "{accepted} / {rejected}"
        );
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
