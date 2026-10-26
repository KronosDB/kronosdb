use std::path::{Path, PathBuf};

use kronosdb_eventstore::event::{Position, StoredEvent, Tag as StoreTag};
use kronosdb_eventstore::segment::format;

use super::*;

/// A golden frame: `*.bin`, ignoring dotfiles such as macOS `._` sidecars.
fn is_fixture(path: &Path) -> bool {
    path.extension().is_some_and(|x| x == "bin")
        && !path.file_name().unwrap().to_string_lossy().starts_with('.')
}

fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../protocol/fixtures")
}

/// Every golden frame decodes to its canonical JSON and re-encodes to the
/// identical bytes. The fixtures come from the schema-driven reference codec,
/// which shares no code with this one.
#[test]
fn golden_fixtures_round_trip_byte_for_byte() {
    let mut checked = 0;
    for entry in std::fs::read_dir(fixtures_dir()).unwrap() {
        let path = entry.unwrap().path();
        if !is_fixture(&path) {
            continue;
        }
        let name = path.file_stem().unwrap().to_string_lossy().to_string();
        let bin = std::fs::read(&path).unwrap();
        let mut expected: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path.with_extension("json")).unwrap()).unwrap();
        expected.as_object_mut().unwrap().remove("description");

        let SplitFrame { header, body, len } = split_frame(&bin).unwrap().expect("a whole frame");
        assert_eq!(len, bin.len(), "{name}: one frame per fixture");
        let frame = Frame::decode(header.kind, body)
            .unwrap_or_else(|e| panic!("{name}: {e}"))
            .unwrap_or_else(|| panic!("{name}: unknown kind {:#06x}", header.kind));
        let actual = serde_json::json!({
            "frame": frame.name(),
            "stream": header.stream,
            "flags": header.flags,
            "body": frame.body_json(),
        });
        assert_eq!(actual, expected, "{name}: decoded value");

        let mut again = Vec::new();
        encode_frame(&mut again, header.stream, header.flags, &frame).unwrap();
        assert_eq!(again, bin, "{name}: re-encoded bytes");
        checked += 1;
    }
    assert!(checked >= 30, "only {checked} fixtures found");
}

fn stored(position: u64, tags: Vec<StoreTag>, payload: &[u8]) -> StoredEvent {
    StoredEvent {
        position: Position(position),
        identifier: format!("0198a2f4-7c3e-7a11-9b2d-{position:012}"),
        name: "OrderPlaced".into(),
        version: "2".into(),
        timestamp: 1_760_000_000_123,
        payload: payload.to_vec(),
        metadata: vec![
            ("correlationId".into(), "c-1".into()),
            ("causé".into(), "ünïcode".into()),
        ],
        tags,
    }
}

/// `EventRecord` is the on-disk layout: the engine's serializer and the
/// protocol codec must agree on every byte, or splicing records from disk
/// onto the wire would send garbage.
#[test]
fn event_record_is_the_engine_storage_layout() {
    let cases = [
        stored(0, vec![], b""),
        stored(
            42,
            vec![
                StoreTag::from_str("orderId", "o-1"),
                StoreTag::new(vec![0u8, 255], vec![1, 2, 3]),
            ],
            br#"{"sku":"A-1","qty":3}"#,
        ),
        stored(
            u64::MAX - 1,
            vec![StoreTag::from_str("k", "v")],
            &[7u8; 70_000],
        ),
    ];
    for event in cases {
        let mut disk = Vec::new();
        format::serialize_event(&event, &mut disk);

        let decoded = EventRecord::decode(&mut Reader::new(&disk)).unwrap();
        assert_eq!(decoded.position, event.position.0);
        assert_eq!(decoded.identifier, event.identifier);
        assert_eq!(decoded.name, event.name);
        assert_eq!(decoded.version, event.version);
        assert_eq!(decoded.timestamp, event.timestamp);
        assert_eq!(decoded.payload, &event.payload[..]);
        let meta: Vec<_> = decoded.metadata.iter().map(|m| (m.key, m.value)).collect();
        let want: Vec<_> = event
            .metadata
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        assert_eq!(meta, want);
        let tags: Vec<_> = decoded.tags.iter().map(|t| (t.key, t.value)).collect();
        let want: Vec<_> = event
            .tags
            .iter()
            .map(|t| (&t.key[..], &t.value[..]))
            .collect();
        assert_eq!(tags, want);

        let mut wire = Vec::new();
        decoded.encode(&mut wire).unwrap();
        assert_eq!(
            wire, disk,
            "protocol encoding differs from the stored layout"
        );

        // A wire record is the stored record minus its leading CRC, and the
        // CRC it implies is the one the engine stores.
        let mut buf = RecordsBuf::new();
        buf.push(&decoded).unwrap();
        let record = buf.as_records().iter().next().unwrap();
        assert_eq!(record.body, &disk[..]);
        assert_eq!(record.event().unwrap(), decoded);
        assert_eq!(
            record.crc(),
            crc32c::crc32c_append(crc32c::crc32c(&[0]), &disk)
        );
    }
}

fn frame_bytes(frame: &Frame<'_>) -> Vec<u8> {
    let mut out = Vec::new();
    encode_frame(&mut out, 7, FLAG_END, frame).unwrap();
    out
}

#[test]
fn frames_ignore_appended_fields_but_records_do_not() {
    let mut bin = frame_bytes(&Frame::Head(Head { position: 9 }));
    bin.extend_from_slice(&[1, 2, 3]);
    let len = (bin.len() - 4) as u32;
    bin[..4].copy_from_slice(&len.to_le_bytes());
    let SplitFrame { header, body, .. } = split_frame(&bin).unwrap().unwrap();
    assert_eq!(
        Frame::decode(header.kind, body).unwrap(),
        Some(Frame::Head(Head { position: 9 })),
        "a later minor version may append fields to a frame"
    );

    let record = RecordRef {
        flags: 0,
        body: &[0u8; 40],
    };
    // 30 bytes hold an all-empty event; the other 10 are left over.
    assert_eq!(
        record.event(),
        Err(DecodeError::TrailingRecordBytes { extra: 10 })
    );
}

#[test]
fn truncated_and_malformed_input_is_rejected_not_misread() {
    let bin = frame_bytes(&Frame::Tags(Tags {
        tags: vec![Tag {
            key: b"orderId",
            value: b"o-1",
        }],
    }));
    let SplitFrame { header, body, .. } = split_frame(&bin).unwrap().unwrap();
    for cut in 0..body.len() {
        assert!(
            Frame::decode(header.kind, &body[..cut]).is_err(),
            "decoding {cut} of {} bytes succeeded",
            body.len()
        );
    }
    // Incomplete frames wait for more bytes; a nonsense length is an error.
    assert_eq!(split_frame(&bin[..bin.len() - 1]).unwrap(), None);
    assert!(split_frame(&[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]).is_err());

    // Option and bool bytes must be 0 or 1.
    let mut bad = frame_bytes(&Frame::PositionAt(PositionAt { position: None }));
    *bad.last_mut().unwrap() = 2;
    let SplitFrame { header, body, .. } = split_frame(&bad).unwrap().unwrap();
    assert!(matches!(
        Frame::decode(header.kind, body),
        Err(DecodeError::InvalidFlagByte { value: 2, .. })
    ));

    // Strings must be UTF-8.
    let mut w = Vec::new();
    put_bytes16(&mut w, &[0xff, 0xfe], "test").unwrap();
    assert!(matches!(
        Reader::new(&w).str16(),
        Err(DecodeError::InvalidUtf8 { .. })
    ));
}

#[test]
fn unknown_kinds_and_enum_values_are_kept_open() {
    assert_eq!(Frame::decode(0x7ffe, &[]).unwrap(), None);
    let bin = frame_bytes(&Frame::RequestFailed(RequestFailed {
        code: ErrorCode(999),
        message: "from the future",
    }));
    let SplitFrame { header, body, .. } = split_frame(&bin).unwrap().unwrap();
    let Some(Frame::RequestFailed(e)) = Frame::decode(header.kind, body).unwrap() else {
        panic!("expected a RequestFailed frame");
    };
    assert_eq!(e.code, ErrorCode(999));
    assert_eq!(e.code.name(), None);
    assert_eq!(ErrorCode::UNAVAILABLE.name(), Some("unavailable"));
}

#[test]
fn oversized_fields_fail_to_encode_instead_of_truncating() {
    let long = "x".repeat(70_000);
    let mut w = Vec::new();
    let err = Frame::BindContext(BindContext { id: 1, name: &long })
        .encode_body(&mut w)
        .unwrap_err();
    assert_eq!(err.field, "BindContext.name");
    assert_eq!(err.max, u16::MAX as usize);
}

/// The server decodes untrusted bytes: every mutation of every golden frame
/// must decode to an error or a frame, never panic, and anything that decodes
/// must re-encode without error.
#[test]
fn mutated_frames_never_panic() {
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = move || {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        seed
    };
    let mut decoded = 0;
    for entry in std::fs::read_dir(fixtures_dir()).unwrap() {
        let path = entry.unwrap().path();
        if !is_fixture(&path) {
            continue;
        }
        let original = std::fs::read(&path).unwrap();
        let SplitFrame { header, body, .. } = split_frame(&original).unwrap().unwrap();
        for _ in 0..2_000 {
            let mut body = body.to_vec();
            match next() % 4 {
                // Flip bytes, the way a bit error or a hostile client would.
                0 | 1 => {
                    for _ in 0..1 + next() % 4 {
                        if !body.is_empty() {
                            let i = (next() as usize) % body.len();
                            body[i] = next() as u8;
                        }
                    }
                }
                // Inflate a length field to its maximum.
                2 => {
                    if body.len() >= 2 {
                        let i = (next() as usize) % (body.len() - 1);
                        body[i] = 0xff;
                        body[i + 1] = 0xff;
                    }
                }
                // Truncate.
                _ => body.truncate((next() as usize) % (body.len() + 1)),
            }
            if let Ok(Some(frame)) = Frame::decode(header.kind, &body) {
                decoded += 1;
                let mut out = Vec::new();
                frame.encode_body(&mut out).unwrap();
            }
        }
    }
    assert!(
        decoded > 0,
        "no mutation decoded; the test is not exercising the success path"
    );
}

#[test]
fn records_from_raw_checks_framing() {
    let event = EventRecord {
        position: 0,
        identifier: "id",
        name: "N",
        version: "1",
        timestamp: 0,
        metadata: vec![],
        tags: vec![],
        payload: b"p",
    };
    let mut buf = RecordsBuf::new();
    buf.push(&event).unwrap();
    buf.push(&event).unwrap();
    let raw = buf.as_records().bytes().to_vec();
    assert_eq!(Records::from_raw(2, &raw).unwrap().iter().count(), 2);
    assert!(
        Records::from_raw(3, &raw).is_err(),
        "claims a record that is not there"
    );
    assert!(
        Records::from_raw(1, &raw).is_err(),
        "leaves a record unaccounted for"
    );
    let mut copied = RecordsBuf::new();
    for record in buf.as_records().iter() {
        copied.push_raw(record).unwrap();
    }
    assert_eq!(copied.as_records(), buf.as_records());
}
