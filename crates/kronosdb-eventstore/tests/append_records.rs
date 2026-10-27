//! The raw-record append route against the path it replaces.
//!
//! The reference for the old path is the segment format written out by hand
//! below (`serialize_append_event` framed behind a CRC, the way the writer
//! did it when it was handed `AppendEvent`s), so these tests pin the bytes on
//! disk, not just that the two routes agree with each other.

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use kronosdb_eventstore::append::{AppendCondition, AppendRequest, RecordAppend, RecordBatch};
use kronosdb_eventstore::criteria::{Criterion, SourcingCondition};
use kronosdb_eventstore::error::Error;
use kronosdb_eventstore::event::{AppendEvent, Position, Tag};
use kronosdb_eventstore::segment::format::serialize_append_event;
use kronosdb_eventstore::segment::writer::SegmentWriter;
use kronosdb_eventstore::segment::{
    SEGMENT_HEADER_SIZE, SEGMENT_MAGIC, SEGMENT_VERSION, segment_path,
};
use kronosdb_eventstore::store::{EventStoreEngine, StoreOptions};
use kronosdb_eventstore::wire;
use kronosdb_protocol as p;

fn tag(key: &str, value: &str) -> Tag {
    Tag::from_str(key, value)
}

/// Events of every shape the format has: metadata or not, tags or not,
/// empty strings, multi-byte text, payloads from nothing to a few hundred
/// bytes.
fn events(n: usize) -> Vec<AppendEvent> {
    (0..n)
        .map(|i| {
            let mut payload = vec![0u8; (i * 37) % 300];
            for (j, byte) in payload.iter_mut().enumerate() {
                *byte = (i + j) as u8;
            }
            AppendEvent {
                identifier: if i % 7 == 0 {
                    String::new()
                } else {
                    format!("id-{i}")
                },
                name: ["OrderPlaced", "ItemAdded", "Päid", "日本語", "X"][i % 5].into(),
                version: if i % 3 == 0 { "1".into() } else { "2.0".into() },
                timestamp: 1_700_000_000_000 + i as i64,
                payload,
                metadata: (0..i % 3)
                    .map(|m| (format!("k{m}"), format!("välue-{i}-{m}")))
                    .collect(),
                tags: (0..i % 4)
                    .map(|t| {
                        if t == 3 {
                            Tag::new(b"bin".to_vec(), vec![0xff, 0xfe, 0, 1])
                        } else {
                            tag(&format!("tag{t}"), &format!("v-{}", i % 6))
                        }
                    })
                    .collect(),
            }
        })
        .collect()
}

/// The records a client sends for `events`, with a position in them that the
/// server must overwrite.
fn client_records(events: &[AppendEvent], junk_position: u64) -> p::RecordsBuf {
    let mut records = p::RecordsBuf::new();
    for event in events {
        records
            .push(&p::EventRecord {
                position: junk_position,
                identifier: &event.identifier,
                name: &event.name,
                version: &event.version,
                timestamp: event.timestamp,
                metadata: event
                    .metadata
                    .iter()
                    .map(|(key, value)| p::MetadataEntry { key, value })
                    .collect(),
                tags: event
                    .tags
                    .iter()
                    .map(|t| p::Tag {
                        key: &t.key,
                        value: &t.value,
                    })
                    .collect(),
                payload: &event.payload,
            })
            .unwrap();
    }
    records
}

/// A batch of `events` as the server builds it from a received frame: the
/// records share the frame's buffer.
fn batch_of(events: &[AppendEvent], junk_position: u64) -> RecordBatch {
    let sent = client_records(events, junk_position);
    let frame = Bytes::copy_from_slice(sent.as_records().bytes());
    let received = p::Records::from_raw(sent.len(), &frame).unwrap();
    wire::record_batch(&received, &frame).unwrap()
}

/// The segment files the old writer would have produced for `events`, one
/// record at a time: `(base, bytes)`, the last being the active segment
/// without its preallocated tail.
fn model_segments(events: &[AppendEvent], max_segment_size: u64) -> Vec<(u64, Vec<u8>)> {
    fn header(base: u64) -> Vec<u8> {
        let mut bytes = SEGMENT_MAGIC.to_vec();
        bytes.push(SEGMENT_VERSION);
        bytes.extend_from_slice(&base.to_le_bytes());
        assert_eq!(bytes.len(), SEGMENT_HEADER_SIZE);
        bytes
    }
    let mut segments = vec![(0u64, header(0))];
    for (position, event) in events.iter().enumerate() {
        let mut payload = Vec::new();
        serialize_append_event(Position(position as u64), event, &mut payload);
        let mut record = Vec::new();
        let crc = crc32c::crc32c_append(crc32c::crc32c(&[0]), &payload);
        record.extend_from_slice(&crc.to_le_bytes());
        record.extend_from_slice(&((1 + payload.len()) as u32).to_le_bytes());
        record.push(0);
        record.extend_from_slice(&payload);
        if segments.last().unwrap().1.len() as u64 + record.len() as u64 > max_segment_size {
            segments.push((position as u64, header(position as u64)));
        }
        segments.last_mut().unwrap().1.extend_from_slice(&record);
    }
    segments
}

fn assert_segments_match(dir: &Path, model: &[(u64, Vec<u8>)], max_segment_size: u64) {
    for (i, (base, bytes)) in model.iter().enumerate() {
        let on_disk = std::fs::read(segment_path(dir, *base)).unwrap();
        if i + 1 < model.len() {
            // Sealed: trimmed to its data.
            assert!(on_disk == *bytes, "sealed segment {base} differs");
        } else {
            // Active: the data, then the preallocated zeros.
            assert_eq!(on_disk.len() as u64, max_segment_size);
            assert!(
                on_disk[..bytes.len()] == bytes[..],
                "active segment {base} differs"
            );
            assert!(on_disk[bytes.len()..].iter().all(|&b| b == 0));
        }
    }
}

#[test]
fn the_writer_stores_a_clients_records_byte_for_byte_as_the_old_path_did() {
    // Small segments so the run rotates a good many times, mid-batch too.
    let max = 3000u64;
    let all = events(60);
    let expected = model_segments(&all, max);
    assert!(expected.len() > 4, "the run should rotate");

    // Every way of cutting the events into appends gives the same files.
    for sizes in [vec![1usize], vec![7], vec![60], vec![1, 2, 3, 5, 8, 13, 21]] {
        let dir = tempfile::tempdir().unwrap();
        let mut writer = SegmentWriter::new(dir.path(), Position(0), max).unwrap();
        let mut at = 0;
        let mut cut = sizes.iter().cycle();
        while at < all.len() {
            let n = (*cut.next().unwrap()).min(all.len() - at);
            let (first, count) = writer
                .write_batch(&batch_of(&all[at..at + n], 0xDEAD_BEEF_0000 + at as u64))
                .unwrap();
            assert_eq!((first.0, count), (at as u64, n as u32));
            at += n;
        }
        assert_eq!(writer.head(), Position(all.len() as u64));
        assert_segments_match(dir.path(), &expected, max);
    }

    // And the events path, which now serializes into the same batch.
    let dir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(dir.path(), Position(0), max).unwrap();
    for chunk in all.chunks(9) {
        writer.write_events(chunk).unwrap();
    }
    assert_segments_match(dir.path(), &expected, max);
}

#[test]
fn a_record_larger_than_a_segment_is_refused_with_the_old_message() {
    let dir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(dir.path(), Position(0), 400).unwrap();
    let mut big = events(1);
    big[0].payload = vec![7; 1000];
    let from_events = writer.write_events(&big).unwrap_err().to_string();
    let from_records = writer
        .write_batch(&batch_of(&big, 0))
        .unwrap_err()
        .to_string();
    assert_eq!(from_records, from_events);
    assert!(
        from_records.contains("exceeds the segment size"),
        "{from_records}"
    );
    assert_eq!(writer.head(), Position(0));
}

fn small(dir: &Path) -> Arc<EventStoreEngine> {
    Arc::new(
        EventStoreEngine::create_with_store_options(
            dir,
            &StoreOptions {
                max_segment_size: 4096,
                ..Default::default()
            },
        )
        .unwrap(),
    )
}

fn condition(marker: u64, orders: &[&str]) -> Option<AppendCondition> {
    Some(AppendCondition {
        consistency_marker: Position(marker),
        criteria: SourcingCondition {
            criteria: orders
                .iter()
                .map(|order| Criterion {
                    names: vec![],
                    tags: vec![tag("orderId", order)],
                })
                .collect(),
        },
    })
}

fn event(name: &str, order: &str, n: u32) -> AppendEvent {
    AppendEvent {
        identifier: format!("e-{n}"),
        name: name.into(),
        version: "1".into(),
        timestamp: 1_700_000_000_000 + n as i64,
        payload: vec![n as u8; 40 + (n as usize * 13) % 200],
        metadata: vec![("n".into(), n.to_string())],
        tags: vec![tag("orderId", order), tag("region", "EU")],
    }
}

fn outcome(result: Result<kronosdb_eventstore::append::AppendResponse, Error>) -> String {
    match result {
        Ok(r) => format!(
            "ok {} {} {}",
            r.first_position.0, r.count, r.consistency_marker.0
        ),
        Err(Error::ConsistencyConditionViolated {
            conflicting_position,
        }) => format!("conflict {}", conflicting_position.0),
        Err(other) => format!("error {other}"),
    }
}

fn all() -> SourcingCondition {
    SourcingCondition {
        criteria: vec![Criterion {
            names: vec![],
            tags: vec![],
        }],
    }
}

/// The same appends through both routes: same answers, same bytes on disk,
/// same reads, and the same again after a restart rebuilds the indexes from
/// the segments.
#[tokio::test]
async fn both_routes_answer_alike_and_leave_the_same_log() {
    let old_dir = tempfile::tempdir().unwrap();
    let new_dir = tempfile::tempdir().unwrap();
    let old = small(old_dir.path());
    let new = small(new_dir.path());

    #[allow(clippy::type_complexity)]
    let script: Vec<(Option<AppendCondition>, Vec<AppendEvent>)> = (0..200u32)
        .map(|i| {
            let order = format!("o-{}", i % 9);
            match i % 5 {
                // Conditional, usually clear.
                1 => (
                    condition(u64::from(i) / 2, &[&order]),
                    vec![event("OrderPlaced", &order, i)],
                ),
                // Conditional on the head: always clear.
                2 => (
                    condition(u64::from(i), &["nobody"]),
                    vec![
                        event("ItemAdded", &order, i),
                        event("ItemAdded", &order, i + 1000),
                    ],
                ),
                // Empty, with and without a condition.
                3 => (condition(0, &[&order]), vec![]),
                4 if i % 2 == 0 => (None, vec![]),
                _ => (
                    None,
                    (0..(i % 4 + 1))
                        .map(|k| event("OrderShipped", &order, i * 10 + k))
                        .collect(),
                ),
            }
        })
        .collect();

    for (step, (cond, evs)) in script.iter().enumerate() {
        let a = outcome(old.append(AppendRequest {
            condition: cond.clone(),
            events: evs.clone(),
        }));
        let b = outcome(
            new.append_records_async(RecordAppend {
                condition: cond.clone(),
                events: batch_of(evs, step as u64),
            })
            .await,
        );
        assert_eq!(a, b, "step {step}");
    }
    assert_eq!(old.head(), new.head());
    assert!(old.head().0 > 100);

    let segments = |dir: &Path| {
        let mut names: Vec<_> = std::fs::read_dir(dir)
            .unwrap()
            .flatten()
            .filter(|e| e.path().extension().is_some_and(|x| x == "seg"))
            .map(|e| e.path())
            .collect();
        names.sort();
        names
    };
    let (old_files, new_files) = (segments(old_dir.path()), segments(new_dir.path()));
    assert!(old_files.len() > 3, "the run rotated");
    assert_eq!(old_files.len(), new_files.len());
    for (a, b) in old_files.iter().zip(&new_files) {
        assert_eq!(a.file_name(), b.file_name());
        assert!(
            std::fs::read(a).unwrap() == std::fs::read(b).unwrap(),
            "{:?} differs between the routes",
            a.file_name()
        );
    }

    let read = |engine: &EventStoreEngine, c: &SourcingCondition| -> Vec<String> {
        engine
            .source(Position(0), c)
            .unwrap()
            .into_iter()
            .map(|e| {
                format!(
                    "{} {} {} {} {} {:?} {:?}",
                    e.position.0,
                    e.identifier,
                    e.name,
                    e.version,
                    e.timestamp,
                    e.payload,
                    e.metadata
                )
            })
            .collect()
    };
    let by_tag = SourcingCondition {
        criteria: vec![Criterion {
            names: vec!["OrderPlaced".into(), "ItemAdded".into()],
            tags: vec![tag("orderId", "o-3")],
        }],
    };
    let before = (read(&old, &all()), read(&new, &all()));
    assert_eq!(before.0, before.1);
    assert_eq!(read(&old, &by_tag), read(&new, &by_tag));
    assert!(!read(&new, &by_tag).is_empty());
    for position in 0..new.head().0 {
        assert_eq!(
            old.get_tags(Position(position)).unwrap(),
            new.get_tags(Position(position)).unwrap()
        );
    }

    // Restart: the raw route's live index answered the same as the rebuilt one.
    let head = new.head();
    new.shutdown();
    drop(new);
    let reopened = Arc::new(EventStoreEngine::open_with_options(new_dir.path(), 4096).unwrap());
    assert_eq!(reopened.head(), head);
    assert_eq!(read(&reopened, &all()), before.0);
    assert_eq!(read(&reopened, &by_tag), read(&old, &by_tag));
    // A DCB check against the rebuilt index still finds what the raw route wrote.
    let conflict = reopened
        .append_records_async(RecordAppend {
            condition: condition(0, &["o-3"]),
            events: batch_of(&[event("OrderPlaced", "o-3", 9999)], 0),
        })
        .await;
    let expected = old.append(AppendRequest {
        condition: condition(0, &["o-3"]),
        events: vec![event("OrderPlaced", "o-3", 9999)],
    });
    assert_eq!(outcome(conflict), outcome(expected));
}

/// The blocking raw entry point is the same append as the async one.
#[test]
fn the_blocking_record_append_matches_the_blocking_event_append() {
    let a_dir = tempfile::tempdir().unwrap();
    let b_dir = tempfile::tempdir().unwrap();
    let (a, b) = (small(a_dir.path()), small(b_dir.path()));
    for i in 0..30u32 {
        let evs = vec![event("OrderPlaced", &format!("o-{}", i % 4), i)];
        let cond = condition(u64::from(i) / 3, &["o-1"]);
        let x = outcome(a.append(AppendRequest {
            condition: cond.clone(),
            events: evs.clone(),
        }));
        let y = outcome(b.append_records(RecordAppend {
            condition: cond,
            events: batch_of(&evs, 0),
        }));
        assert_eq!(x, y, "step {i}");
    }
    assert_eq!(
        std::fs::read(segment_path(a_dir.path(), 0)).unwrap(),
        std::fs::read(segment_path(b_dir.path(), 0)).unwrap()
    );
}

// --- rejection parity ---------------------------------------------------

/// One record as a client could mangle it: `len | flags | body`.
fn raw_record(flags: u8, body: &[u8]) -> Vec<u8> {
    let mut record = ((1 + body.len()) as u32).to_le_bytes().to_vec();
    record.push(flags);
    record.extend_from_slice(body);
    record
}

fn good_body(event: &AppendEvent) -> Vec<u8> {
    let mut body = Vec::new();
    serialize_append_event(Position(0), event, &mut body);
    body
}

/// What both conversions say about `records` (framed and counted).
fn both(count: u32, records: &[u8]) -> (Result<usize, String>, Result<usize, String>) {
    let received = p::Records::from_raw(count, records).unwrap();
    let old = wire::append_events(&received)
        .map(|events| events.len())
        .map_err(|bad| bad.0);
    let backing = Bytes::copy_from_slice(records);
    let received = p::Records::from_raw(count, &backing).unwrap();
    let new = wire::record_batch(&received, &backing)
        .map(|batch| batch.len() as usize)
        .map_err(|bad| bad.0);
    (old, new)
}

#[test]
fn malformed_records_are_refused_with_the_same_messages() {
    let good = good_body(&event("OrderPlaced", "o-1", 1));
    let mut cases: Vec<(&str, Vec<Vec<u8>>)> = vec![
        ("an intact event", vec![raw_record(0, &good)]),
        ("flags 1", vec![raw_record(1, &good)]),
        ("a control record", vec![raw_record(0x21, &good)]),
        ("an empty body", vec![raw_record(0, &[])]),
        ("the position cut short", vec![raw_record(0, &good[..5])]),
        (
            "a body cut short",
            vec![raw_record(0, &good[..good.len() - 3])],
        ),
        ("a body cut in the middle", vec![raw_record(0, &good[..30])]),
        ("trailing bytes", {
            let mut body = good.clone();
            body.extend_from_slice(&[1, 2, 3]);
            vec![raw_record(0, &body)]
        }),
        (
            "a good record then a bad one",
            vec![raw_record(0, &good), raw_record(0, &good[..20])],
        ),
        (
            "a bad record then a reserved one",
            vec![
                raw_record(0, &good[..20]),
                raw_record(0, &good_body(&event("$sys", "o", 2))),
            ],
        ),
    ];
    // Text that is not UTF-8, in each place the format says text.
    let mut e = event("OrderPlaced", "o-1", 1);
    e.identifier = "ab".into();
    let mut body = good_body(&e);
    // identifier bytes start at 8 (position) + 2 (length)
    body[10] = 0xff;
    cases.push(("a non-UTF-8 identifier", vec![raw_record(0, &body)]));
    let mut body = good_body(&e);
    let name_at = 8 + 2 + e.identifier.len() + 2;
    body[name_at] = 0xc3;
    body[name_at + 1] = 0x28;
    cases.push(("a non-UTF-8 name", vec![raw_record(0, &body)]));
    let mut body = good_body(&e);
    let version_at = 8 + 2 + e.identifier.len() + 2 + e.name.len() + 2;
    body[version_at] = 0x80;
    cases.push(("a non-UTF-8 version", vec![raw_record(0, &body)]));
    let mut body = good_body(&e);
    let metadata_key_at =
        8 + 2 + 2 + 2 + e.identifier.len() + e.name.len() + e.version.len() + 8 + 2 + 2;
    body[metadata_key_at] = 0xfe;
    cases.push(("a non-UTF-8 metadata key", vec![raw_record(0, &body)]));
    // A tag's bytes may be anything.
    let mut binary = event("OrderPlaced", "o-1", 1);
    binary.tags = vec![Tag::new(vec![0xff, 0xfe], vec![0x80])];
    cases.push(("binary tags", vec![raw_record(0, &good_body(&binary))]));
    // A count that promises more than the body holds.
    let mut body = good_body(&e);
    let tag_count_at = body.len()
        - 4
        - e.payload.len()
        - 2 * 2 * e.tags.len()
        - e.tags
            .iter()
            .map(|t| t.key.len() + t.value.len())
            .sum::<usize>()
        - 2;
    body[tag_count_at] = 0xff;
    cases.push(("an inflated tag count", vec![raw_record(0, &body)]));
    // A payload length that overruns.
    let mut body = good_body(&e);
    let payload_len_at = body.len() - 4 - e.payload.len();
    body[payload_len_at..payload_len_at + 4].copy_from_slice(&u32::MAX.to_le_bytes());
    cases.push(("an overrunning payload", vec![raw_record(0, &body)]));

    let mut refused = 0;
    for (what, records) in cases {
        let bytes: Vec<u8> = records.concat();
        let (old, new) = both(records.len() as u32, &bytes);
        assert_eq!(old, new, "{what}");
        if let Err(message) = &old {
            refused += 1;
            assert!(message.starts_with("record "), "{what}: {message}");
        }
    }
    assert!(refused >= 12, "{refused}");
}

#[tokio::test]
async fn reserved_namespace_records_are_refused_with_the_same_errors() {
    let user = |name: &str, tags: Vec<Tag>| AppendEvent {
        tags,
        ..event(name, "o", 1)
    };
    let cases: Vec<(&str, Vec<AppendEvent>)> = vec![
        (
            "a system event type",
            vec![user("$schedule.created", vec![])],
        ),
        (
            "a system tag",
            vec![user("OrderPlaced", vec![tag("$sys", "1")])],
        ),
        (
            "a system tag after a user tag",
            vec![user(
                "OrderPlaced",
                vec![tag("orderId", "x"), tag("$a", "1")],
            )],
        ),
        (
            "the second event's type",
            vec![
                user("OrderPlaced", vec![]),
                user("$sys", vec![tag("orderId", "x")]),
            ],
        ),
        (
            "an earlier event's tag over a later event's type",
            vec![
                user("OrderPlaced", vec![tag("$first", "1")]),
                user("$second", vec![]),
            ],
        ),
        (
            "the type before the tags of one event",
            vec![user("$type", vec![tag("$tag", "1")])],
        ),
        ("a bare dollar", vec![user("$", vec![])]),
        (
            "a dollar inside a name",
            vec![user("Order$Placed", vec![tag("a$b", "$v")])],
        ),
    ];
    let dir = tempfile::tempdir().unwrap();
    let engine = small(dir.path());
    let mut refused = 0;
    for (what, events) in cases {
        let old = engine
            .append(AppendRequest {
                condition: None,
                events: events.clone(),
            })
            .map(|_| ());
        let new = engine
            .append_records_async(RecordAppend {
                condition: None,
                events: batch_of(&events, 0),
            })
            .await
            .map(|_| ());
        let blocking = engine
            .append_records(RecordAppend {
                condition: None,
                events: batch_of(&events, 0),
            })
            .map(|_| ());
        match (&old, &new, &blocking) {
            (
                Err(Error::ReservedNamespace { detail: a }),
                Err(Error::ReservedNamespace { detail: b }),
                Err(Error::ReservedNamespace { detail: c }),
            ) => {
                refused += 1;
                assert_eq!(a, b, "{what}");
                assert_eq!(a, c, "{what}");
            }
            // The last case is not in the namespace: a `$` anywhere but the
            // front is an ordinary character.
            (Ok(()), Ok(()), Ok(())) => assert!(what.contains("inside"), "{what}"),
            other => panic!("{what}: the routes disagree: {other:?}"),
        }
    }
    assert_eq!(refused, 7);
    // Refused appends wrote nothing; the two that were fine wrote theirs.
    assert_eq!(engine.head(), Position(3));

    // The condition is checked the same on both routes, after the events.
    let reserved_condition = Some(AppendCondition {
        consistency_marker: Position(0),
        criteria: SourcingCondition {
            criteria: vec![Criterion {
                names: vec!["$sys".into()],
                tags: vec![],
            }],
        },
    });
    let old = engine
        .append(AppendRequest {
            condition: reserved_condition.clone(),
            events: vec![event("OrderPlaced", "o", 5)],
        })
        .map(|_| ())
        .unwrap_err()
        .to_string();
    let new = engine
        .append_records_async(RecordAppend {
            condition: reserved_condition,
            events: batch_of(&[event("OrderPlaced", "o", 5)], 0),
        })
        .await
        .map(|_| ())
        .unwrap_err()
        .to_string();
    assert_eq!(old, new);
    assert!(new.contains("reserved"));
    engine.shutdown();
}

#[tokio::test]
async fn a_stopping_engine_refuses_record_appends_like_event_appends() {
    // The staging paths share one gate; this pins that the record route
    // goes through it (check_writable) after shutdown.
    let dir = tempfile::tempdir().unwrap();
    let engine = small(dir.path());
    engine
        .append_records_async(RecordAppend {
            condition: None,
            events: batch_of(&events(3), 0),
        })
        .await
        .unwrap();
    engine.shutdown();
    let refused = engine
        .append_records_async(RecordAppend {
            condition: None,
            events: batch_of(&events(1), 0),
        })
        .await
        .map(|_| ())
        .unwrap_err()
        .to_string();
    let refused_events = engine
        .append_async(
            AppendRequest {
                condition: None,
                events: events(1),
            },
            false,
        )
        .await
        .map(|_| ())
        .unwrap_err()
        .to_string();
    assert_eq!(refused, refused_events);
    assert!(refused.contains("shutting down"), "{refused}");
}
