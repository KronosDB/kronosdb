//! The log-wide index (ADR-0010) against the per-segment one and against a
//! model of the log: every read answers the same however far merging got,
//! a crash at any step of a merge leaves an index that opens without a
//! rebuild and answers the same, and merged tables are the same bytes
//! however and wherever they were built.

use std::collections::BTreeMap;
use std::path::Path;

use super::*;
use crate::append::AppendCondition;
use crate::criteria::Criterion;
use crate::event::AppendEvent;
use crate::log_index::MergeStep;
use crate::segment::merge::INDEX_DIR;

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// One event of the model: its type, tags, and whether it is a system
/// event (hidden from the client path).
#[derive(Clone, Debug)]
struct Logged {
    name: String,
    tags: Vec<Tag>,
    system: bool,
}

const TYPES: [&str; 4] = ["Placed", "Paid", "Shipped", "Noted"];

fn random_event(rng: &mut Rng, serial: u64) -> AppendEvent {
    let mut tags = Vec::new();
    if rng.below(5) != 0 {
        tags.push(Tag::from_str("k", &rng.below(12).to_string()));
    }
    if rng.below(2) == 0 {
        tags.push(Tag::from_str("j", &rng.below(3).to_string()));
    }
    if rng.below(3) == 0 {
        // A wide key space: most values appear once or twice in the log.
        tags.push(Tag::from_str("wide", &rng.below(4_000).to_string()));
    }
    AppendEvent {
        identifier: format!("e{serial}"),
        name: TYPES[rng.below(TYPES.len() as u64) as usize].into(),
        version: "1".into(),
        timestamp: 1_700_000_000_000 + serial as i64,
        payload: vec![b'p'; rng.below(120) as usize],
        metadata: vec![],
        tags,
    }
}

fn options(segment_size: u64, background: bool) -> StoreOptions {
    StoreOptions {
        max_segment_size: segment_size,
        background_index_merge: background,
        index_merge_rate: 0,
        ..StoreOptions::default()
    }
}

/// Appends a random log of about `events` events: client batches, now and
/// then a system event and a watermark checkpoint (a control record, so
/// record lengths have something to step over). Returns the model, by
/// position. `between` runs after every append.
fn grow(
    engine: &EventStoreEngine,
    seed: u64,
    events: u64,
    mut between: impl FnMut(&EventStoreEngine),
) -> Vec<Logged> {
    let mut rng = Rng(seed);
    let mut model = Vec::new();
    let mut serial = 0;
    while (model.len() as u64) < events {
        if rng.below(23) == 0 {
            let event = AppendEvent {
                identifier: format!("s{serial}"),
                name: "$sys.tick".into(),
                version: "1".into(),
                timestamp: 1_700_000_000_000,
                payload: vec![],
                metadata: vec![],
                tags: vec![Tag::from_str("$tick", &rng.below(5).to_string())],
            };
            serial += 1;
            engine
                .append_system(AppendRequest {
                    condition: None,
                    events: vec![event.clone()],
                })
                .unwrap();
            let mut tags = event.tags;
            tags.push(crate::system::marker());
            model.push(Logged {
                name: event.name,
                tags,
                system: true,
            });
        } else {
            let batch: Vec<AppendEvent> = (0..1 + rng.below(6))
                .map(|_| {
                    serial += 1;
                    random_event(&mut rng, serial)
                })
                .collect();
            for event in &batch {
                model.push(Logged {
                    name: event.name.clone(),
                    tags: event.tags.clone(),
                    system: false,
                });
            }
            engine
                .append(AppendRequest {
                    condition: None,
                    events: batch,
                })
                .unwrap();
        }
        if rng.below(31) == 0 {
            engine.persist_watermark_checkpoint().unwrap();
        }
        between(engine);
    }
    engine.seals.wait_idle();
    model
}

fn random_condition(rng: &mut Rng, client: bool) -> SourcingCondition {
    let criteria = (0..rng.below(4))
        .map(|_| {
            let mut tags = Vec::new();
            if rng.below(3) != 0 {
                tags.push(Tag::from_str("k", &rng.below(14).to_string()));
            }
            if rng.below(3) == 0 {
                tags.push(Tag::from_str("j", &rng.below(4).to_string()));
            }
            if rng.below(4) == 0 {
                tags.push(Tag::from_str("wide", &rng.below(4_200).to_string()));
            }
            if !client && rng.below(4) == 0 {
                tags.push(Tag::from_str("$tick", &rng.below(6).to_string()));
            }
            let mut names: Vec<String> = TYPES
                .iter()
                .chain(&["Missing"])
                .filter(|_| rng.below(4) == 0)
                .map(|n| n.to_string())
                .collect();
            if !client && rng.below(5) == 0 {
                names.push("$sys.tick".into());
            }
            Criterion { names, tags }
        })
        .collect();
    SourcingCondition { criteria }
}

fn matches(condition: &SourcingCondition, event: &Logged) -> bool {
    condition.criteria.iter().any(|c| {
        (c.names.is_empty() || c.names.contains(&event.name))
            && c.tags.iter().all(|t| event.tags.contains(t))
    })
}

/// Positions in `[from, to)` matching `condition`, system events included.
fn truth(model: &[Logged], condition: &SourcingCondition, from: u64, to: u64) -> Vec<u64> {
    (from..to.min(model.len() as u64))
        .filter(|&p| matches(condition, &model[p as usize]))
        .collect()
}

/// The positions of the records `read_records` copied.
fn record_positions(body: &[u8]) -> Vec<u64> {
    let mut out = Vec::new();
    let mut at = 0;
    while at < body.len() {
        let len = u32::from_le_bytes(body[at..at + 4].try_into().unwrap()) as usize;
        out.push(u64::from_le_bytes(
            body[at + 5..at + 13].try_into().unwrap(),
        ));
        at += 4 + len;
    }
    out
}

/// A subscriber catching up: pages of `read_records` after an index check,
/// as the native Subscribe handler does. The bytes it would send.
fn catch_up(engine: &EventStoreEngine, condition: &SourcingCondition, from: u64) -> Vec<u8> {
    let head = engine.head();
    let mut cursor = Position(from);
    let mut sent = Vec::new();
    while cursor < head {
        if !engine.has_matches_between(cursor, head, condition) {
            break;
        }
        let page = engine
            .read_records(
                condition,
                &PageBounds {
                    limit: 7,
                    ..PageBounds::new(cursor, head)
                },
                &mut sent,
            )
            .unwrap();
        cursor = match (page.full, page.scanned_to) {
            (true, Some(last)) => Position(last.0 + 1),
            _ => head,
        };
    }
    sent
}

/// Every question a read asks, of `engine`, answered for `queries` random
/// conditions: what must not depend on how the index is laid out.
fn answers(engine: &EventStoreEngine, seed: u64, queries: usize) -> Vec<String> {
    let mut rng = Rng(seed);
    let head = engine.head().0;
    let mut out = vec![format!("visible head {:?}", engine.visible_head())];
    for _ in 0..queries {
        let client = random_condition(&mut rng, true);
        let internal = random_condition(&mut rng, false);
        let from = rng.below(head + 1);
        let to = from + rng.below(head - from + 1);
        let source: Vec<u64> = engine
            .source(Position(from), &client)
            .unwrap()
            .iter()
            .map(|e| e.position.0)
            .collect();
        let internal_read: Vec<u64> = engine
            .source_internal(Position(from), &internal, usize::MAX)
            .unwrap()
            .iter()
            .map(|e| e.position.0)
            .collect();
        let latest = engine.latest_matching(&internal, Position(to)).unwrap();
        let between = engine.has_matches_between(Position(from), Position(to), &internal);
        let conflict = engine
            .scan_sealed(
                &AppendCondition {
                    consistency_marker: Position(from),
                    criteria: internal.clone(),
                },
                0,
                true,
            )
            .map(|(scan, _)| match scan {
                SealedScan::Conflict(p) => Some(p.0),
                _ => None,
            })
            .unwrap();
        let caught_up = catch_up(engine, &client, from);
        let plan = engine
            .plan_read(
                &internal,
                Position(from),
                Position(to),
                1 + rng.below(40) as usize,
            )
            .unwrap();
        out.push(format!(
            "{client:?} {internal:?} {from}..{to}: source {source:?} internal {internal_read:?} \
             latest {latest:?} between {between} conflict {conflict:?} \
             caught up {caught_up:?} plan {plan:?}"
        ));
    }
    out
}

/// The same answers, from the model.
fn check_against_model(engine: &EventStoreEngine, model: &[Logged], seed: u64, queries: usize) {
    let mut rng = Rng(seed);
    let head = engine.head().0;
    assert_eq!(head, model.len() as u64);
    let visible = model.iter().rposition(|e| !e.system).map_or(0, |p| p + 1);
    assert_eq!(engine.visible_head().0, visible as u64);
    let sealed_end = {
        let list = engine.segments.read();
        list.bases[list.sealed_count]
    };
    for _ in 0..queries {
        let client = random_condition(&mut rng, true);
        let internal = random_condition(&mut rng, false);
        let from = rng.below(head + 1);
        let to = from + rng.below(head - from + 1);
        let visible: Vec<u64> = truth(model, &client, from, head)
            .into_iter()
            .filter(|&p| !model[p as usize].system)
            .collect();
        let source: Vec<u64> = engine
            .source(Position(from), &client)
            .unwrap()
            .iter()
            .map(|e| e.position.0)
            .collect();
        assert_eq!(source, visible, "source {client:?} from {from}");
        let internal_truth = truth(model, &internal, from, head);
        let internal_read: Vec<u64> = engine
            .source_internal(Position(from), &internal, usize::MAX)
            .unwrap()
            .iter()
            .map(|e| e.position.0)
            .collect();
        assert_eq!(
            internal_read, internal_truth,
            "internal {internal:?} from {from}"
        );
        let in_range = truth(model, &internal, from, to);
        assert_eq!(
            engine.has_matches_between(Position(from), Position(to), &internal),
            !in_range.is_empty(),
            "between {internal:?} {from}..{to}"
        );
        assert_eq!(
            engine
                .latest_matching(&internal, Position(to))
                .unwrap()
                .map(|p| p.0),
            truth(model, &internal, 0, to).last().copied(),
            "latest {internal:?} below {to}"
        );
        let conflict = engine
            .scan_sealed(
                &AppendCondition {
                    consistency_marker: Position(from),
                    criteria: internal.clone(),
                },
                0,
                true,
            )
            .unwrap()
            .0;
        let expected = truth(model, &internal, from, sealed_end).first().copied();
        match conflict {
            SealedScan::Conflict(p) => assert_eq!(Some(p.0), expected),
            _ => assert_eq!(None, expected, "conflict {internal:?} from {from}"),
        }
        assert_eq!(
            record_positions(&catch_up(engine, &client, from)),
            visible,
            "catch up {client:?} from {from}"
        );
        let limit = 1 + rng.below(40) as usize;
        let plan = engine
            .plan_read(&internal, Position(from), Position(to), limit)
            .unwrap();
        let planned: Vec<u64> = plan.records.iter().map(|r| r.position).collect();
        let expected: Vec<u64> = in_range.iter().copied().take(limit).collect();
        assert_eq!(planned, expected, "plan {internal:?} {from}..{to}");
        assert_eq!(plan.complete, in_range.len() < limit);
    }
}

/// Every planned location is exactly its event's record in the segment
/// file: the header at the offset says that length, the CRC holds, and the
/// event is the planned position.
fn check_plan_against_files(engine: &EventStoreEngine, dir: &Path, condition: &SourcingCondition) {
    let plan = engine
        .plan_read(condition, Position(0), engine.head(), usize::MAX)
        .unwrap();
    assert!(plan.complete);
    let mut files: BTreeMap<u64, Vec<u8>> = BTreeMap::new();
    for location in &plan.records {
        let bytes = files.entry(location.segment_base).or_insert_with(|| {
            std::fs::read(segment::segment_path(dir, location.segment_base)).unwrap()
        });
        let at = location.offset as usize;
        let record = &bytes[at..at + location.len as usize];
        let header = crate::segment::record::parse_header(record[..9].try_into().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(header.total_len(), location.len as usize, "{location:?}");
        let payload = &record[9..];
        assert!(crate::segment::record::validate_crc(header, payload));
        match crate::segment::record::decode_native(header, payload).unwrap() {
            crate::segment::record::NativeRecord::Event { position } => {
                assert_eq!(position, location.position)
            }
            other => panic!("{location:?} is {other:?}"),
        }
    }
}

/// Merges one step at a time, checking the answers after each.
fn merge_stepwise(engine: &EventStoreEngine, expected: &[String], seed: u64) -> usize {
    let mut steps = 0;
    while engine.merge_index_step(|_| true).unwrap() {
        steps += 1;
        assert_eq!(
            answers(engine, seed, 40),
            expected[..41],
            "after step {steps}"
        );
    }
    steps
}

#[test]
fn merged_reads_answer_like_unmerged_ones_on_random_logs() {
    for (seed, events, segment_size) in
        [(1u64, 2_500u64, 2048u64), (7, 4_000, 4096), (99, 900, 1024)]
    {
        let unmerged_dir = tempfile::tempdir().unwrap();
        let merged_dir = tempfile::tempdir().unwrap();
        let unmerged = EventStoreEngine::create_with_store_options(
            unmerged_dir.path(),
            &options(segment_size, false),
        )
        .unwrap();
        let merged = EventStoreEngine::create_with_store_options(
            merged_dir.path(),
            &options(segment_size, false),
        )
        .unwrap();
        let model = grow(&unmerged, seed, events, |_| {});
        assert_eq!(grow(&merged, seed, events, |_| {}).len(), model.len());
        assert!(
            unmerged.segments.read().sealed_count > 64,
            "enough segments for two levels"
        );

        check_against_model(&unmerged, &model, seed * 3, 150);
        let expected = answers(&unmerged, seed * 5, 150);
        let steps = merge_stepwise(&merged, &expected, seed * 5);
        assert!(steps >= 2, "{steps} merges");
        let stats = merged.index_stats();
        assert!(stats.merged_tables >= 2, "{stats:?}");
        assert!(stats.segment_tables < 8, "{stats:?}");
        assert_eq!(answers(&merged, seed * 5, 150), expected);
        check_against_model(&merged, &model, seed * 3, 150);
        check_plan_against_files(
            &merged,
            merged_dir.path(),
            &SourcingCondition {
                criteria: vec![Criterion {
                    names: vec![],
                    tags: vec![Tag::from_str("k", "3")],
                }],
            },
        );
        check_plan_against_files(
            &merged,
            merged_dir.path(),
            &SourcingCondition {
                criteria: vec![Criterion {
                    names: vec![],
                    tags: vec![],
                }],
            },
        );
    }
}

/// While the log grows and merges run in the background, every read sees
/// the log as the model has it up to the head it read at.
#[test]
fn reads_racing_background_merges_stay_exact() {
    let dir = tempfile::tempdir().unwrap();
    let engine = Arc::new(
        EventStoreEngine::create_with_store_options(dir.path(), &options(1024, true)).unwrap(),
    );
    let model = Arc::new(parking_lot::RwLock::new(Vec::<Logged>::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let readers: Vec<_> = (0..3)
        .map(|r| {
            let (engine, model, stop) =
                (Arc::clone(&engine), Arc::clone(&model), Arc::clone(&stop));
            std::thread::spawn(move || {
                let mut rng = Rng(0xABC + r);
                let mut reads = 0;
                while !stop.load(Ordering::Relaxed) {
                    let condition = random_condition(&mut rng, true);
                    let model = model.read().clone();
                    let head = model.len() as u64;
                    let from = rng.below(head + 1);
                    let mut sent = Vec::new();
                    engine
                        .read_records(
                            &condition,
                            &PageBounds::new(Position(from), Position(head)),
                            &mut sent,
                        )
                        .unwrap();
                    let expected: Vec<u64> = truth(&model, &condition, from, head)
                        .into_iter()
                        .filter(|&p| !model[p as usize].system)
                        .collect();
                    assert_eq!(
                        record_positions(&sent),
                        expected,
                        "{condition:?} from {from}"
                    );
                    reads += 1;
                }
                reads
            })
        })
        .collect();
    let mut rng = Rng(5);
    for _ in 0..600 {
        let batch: Vec<AppendEvent> = (0..1 + rng.below(4))
            .map(|i| random_event(&mut rng, i))
            .collect();
        let logged: Vec<Logged> = batch
            .iter()
            .map(|e| Logged {
                name: e.name.clone(),
                tags: e.tags.clone(),
                system: false,
            })
            .collect();
        engine
            .append(AppendRequest {
                condition: None,
                events: batch,
            })
            .unwrap();
        model.write().extend(logged);
    }
    std::thread::sleep(std::time::Duration::from_millis(300));
    stop.store(true, Ordering::Relaxed);
    for reader in readers {
        assert!(reader.join().unwrap() > 0);
    }
    engine.merge_index().unwrap();
    assert!(engine.index_stats().merged_tables > 0);
    let model = model.read().clone();
    check_against_model(&engine, &model, 77, 60);
}

/// The index directory and every segment's index files, by name.
fn index_files(dir: &Path) -> BTreeMap<String, Vec<u8>> {
    let mut out = BTreeMap::new();
    for entry in std::fs::read_dir(dir.join(INDEX_DIR)).unwrap() {
        let entry = entry.unwrap();
        out.insert(
            format!("index/{}", entry.file_name().to_string_lossy()),
            std::fs::read(entry.path()).unwrap(),
        );
    }
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".idx") || name.ends_with(".keys") {
            out.insert(name, std::fs::read(entry.path()).unwrap());
        }
    }
    out
}

/// Merged tables are a function of the log: built in the background while
/// it grew, built one level at a time after every append, or built at once
/// at the end, from different inputs each time, they are the same files.
#[test]
fn merged_tables_are_byte_identical_however_they_were_built() {
    let dirs: Vec<_> = (0..3).map(|_| tempfile::tempdir().unwrap()).collect();
    let background =
        EventStoreEngine::create_with_store_options(dirs[0].path(), &options(1024, true)).unwrap();
    let eager =
        EventStoreEngine::create_with_store_options(dirs[1].path(), &options(1024, false)).unwrap();
    let late =
        EventStoreEngine::create_with_store_options(dirs[2].path(), &options(1024, false)).unwrap();
    grow(&background, 3, 1_500, |_| {});
    grow(&eager, 3, 1_500, |engine| {
        engine.merge_index().unwrap();
    });
    grow(&late, 3, 1_500, |_| {});
    for engine in [&background, &eager, &late] {
        engine.merge_index().unwrap();
    }
    assert!(
        eager.index_stats().merge_totals.tables_built
            > late.index_stats().merge_totals.tables_built
    );
    let files = index_files(dirs[2].path());
    assert!(
        files.keys().filter(|n| n.starts_with("index/")).count() >= 3,
        "{:?}",
        files.keys()
    );
    assert!(
        index_files(dirs[0].path()) == files,
        "background build differs"
    );
    assert!(
        index_files(dirs[1].path()) == files,
        "incremental build differs"
    );

    // Reopened, nothing changes; rebuilt from scratch, nothing changes.
    drop(late);
    let reopened =
        EventStoreEngine::open_with_store_options(dirs[2].path(), &options(1024, false)).unwrap();
    assert_eq!(reopened.merge_index().unwrap().tables_built, 0);
    assert!(
        index_files(dirs[2].path()) == files,
        "reopen changed the index"
    );
    drop(reopened);
    std::fs::remove_dir_all(dirs[2].path().join(INDEX_DIR)).unwrap();
    for entry in std::fs::read_dir(dirs[2].path()).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().is_some_and(|e| e == "keys" || e == "idx") {
            std::fs::remove_file(path).unwrap();
        }
    }
    let rebuilt =
        EventStoreEngine::open_with_store_options(dirs[2].path(), &options(1024, false)).unwrap();
    assert!(rebuilt.index_stats().rebuilt_at_open > 0);
    rebuilt.merge_index().unwrap();
    assert!(
        index_files(dirs[2].path()) == files,
        "a rebuild from the segments differs"
    );
}

/// (length, modification time) of every index file.
fn file_stamps(dir: &Path) -> BTreeMap<String, (u64, std::time::SystemTime)> {
    let mut out = BTreeMap::new();
    for sub in [dir.to_path_buf(), dir.join(INDEX_DIR)] {
        for entry in std::fs::read_dir(&sub).unwrap() {
            let entry = entry.unwrap();
            let meta = entry.metadata().unwrap();
            if meta.is_file() {
                out.insert(
                    entry.path().display().to_string(),
                    (meta.len(), meta.modified().unwrap()),
                );
            }
        }
    }
    out
}

#[test]
fn a_restart_rebuilds_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let engine =
        EventStoreEngine::create_with_store_options(dir.path(), &options(1024, false)).unwrap();
    let model = grow(&engine, 11, 1_200, |_| {});
    engine.merge_index().unwrap();
    let before = engine.index_stats();
    assert!(before.merged_tables > 0);
    engine.shutdown();
    drop(engine);
    let stamps = file_stamps(dir.path());

    std::thread::sleep(std::time::Duration::from_millis(20));
    let engine =
        EventStoreEngine::open_with_store_options(dir.path(), &options(1024, false)).unwrap();
    let after = engine.index_stats();
    assert_eq!(after.rebuilt_at_open, 0);
    assert_eq!(after.merged_tables, before.merged_tables);
    assert_eq!(after.segment_tables, before.segment_tables);
    assert_eq!(engine.merge_index().unwrap().tables_built, 0);
    // Not one index file was written, touched or removed.
    let now = file_stamps(dir.path());
    for (path, stamp) in &stamps {
        if path.ends_with(".seg") {
            continue;
        }
        assert_eq!(now.get(path), Some(stamp), "{path}");
    }
    check_against_model(&engine, &model, 12, 60);
}

/// A crash at every step of a merge: the store opens without rebuilding
/// anything, answers exactly, leaves nothing stray behind, and the merge it
/// lost is done again to the same bytes.
#[test]
fn a_crash_at_any_merge_step_leaves_a_consistent_index() {
    // The reference: the same log, merged without a crash.
    let reference = tempfile::tempdir().unwrap();
    let engine =
        EventStoreEngine::create_with_store_options(reference.path(), &options(1024, false))
            .unwrap();
    let model = grow(&engine, 21, 1_400, |_| {});
    engine.merge_index().unwrap();
    let expected = index_files(reference.path());
    drop(engine);

    for step in [
        MergeStep::TableWritten,
        MergeStep::ManifestWritten,
        MergeStep::Swapped,
        MergeStep::PartlyCleaned,
    ] {
        for merges_before in [0usize, 2] {
            let dir = tempfile::tempdir().unwrap();
            let engine =
                EventStoreEngine::create_with_store_options(dir.path(), &options(1024, false))
                    .unwrap();
            grow(&engine, 21, 1_400, |_| {});
            for _ in 0..merges_before {
                engine.merge_index_step(|_| true).unwrap();
            }
            assert!(
                engine.merge_index_step(|at| at != step).unwrap(),
                "{step:?}"
            );
            drop(engine);
            // Torn files a crash mid-write would leave.
            std::fs::write(dir.path().join(INDEX_DIR).join("MANIFEST.tmp"), b"torn").unwrap();
            std::fs::write(
                dir.path()
                    .join(INDEX_DIR)
                    .join("00000000000000000000-00000000000000000099.keys.tmp"),
                b"torn",
            )
            .unwrap();

            let engine =
                EventStoreEngine::open_with_store_options(dir.path(), &options(1024, false))
                    .unwrap();
            let stats = engine.index_stats();
            assert_eq!(stats.rebuilt_at_open, 0, "{step:?} after {merges_before}");
            check_against_model(&engine, &model, 31, 40);
            // Nothing stray: every file in the index directory is live, and
            // no covered segment kept its own key table.
            let on_disk = index_files(dir.path());
            let live: Vec<&String> = on_disk.keys().filter(|n| n.starts_with("index/")).collect();
            assert_eq!(live.len(), stats.merged_tables + 1, "{step:?}: {live:?}");
            let own_keys = on_disk
                .keys()
                .filter(|n| n.ends_with(".keys") && !n.starts_with("index/"))
                .count();
            assert_eq!(own_keys, stats.segment_tables, "{step:?}");
            engine.merge_index().unwrap();
            assert!(
                index_files(dir.path()) == expected,
                "{step:?} after {merges_before}"
            );
        }
    }
}

/// A merged table damaged on disk, or gone: open drops it, indexes its
/// segments on their own again (rebuilt from the segments), reads stay
/// exact, and the merge builds the same table again.
#[test]
fn a_damaged_or_missing_merged_table_is_rebuilt() {
    let dir = tempfile::tempdir().unwrap();
    let engine =
        EventStoreEngine::create_with_store_options(dir.path(), &options(1024, false)).unwrap();
    let model = grow(&engine, 41, 1_200, |_| {});
    engine.merge_index().unwrap();
    let expected = index_files(dir.path());
    let tables: Vec<String> = expected
        .keys()
        .filter(|n| n.starts_with("index/") && n.ends_with(".keys"))
        .cloned()
        .collect();
    assert!(tables.len() >= 2);
    drop(engine);

    // One flipped bit in the first table's header, the second table gone.
    // (Damage past the header is found by the read that reaches it, which
    // fails rather than answers: see the key table's own tests.)
    let first = dir.path().join(&tables[0]);
    let mut bytes = std::fs::read(&first).unwrap();
    bytes[20] ^= 0x20;
    std::fs::write(&first, bytes).unwrap();
    std::fs::remove_file(dir.path().join(&tables[1])).unwrap();

    let engine =
        EventStoreEngine::open_with_store_options(dir.path(), &options(1024, false)).unwrap();
    assert!(engine.index_stats().rebuilt_at_open > 0);
    check_against_model(&engine, &model, 42, 60);
    engine.merge_index().unwrap();
    assert!(index_files(dir.path()) == expected);
}

/// The read planner needs no segment body: with every sealed segment's file
/// emptied it still plans, and what it plans is what was there.
#[test]
fn planning_a_read_touches_no_sealed_segment_body() {
    let dir = tempfile::tempdir().unwrap();
    let engine =
        EventStoreEngine::create_with_store_options(dir.path(), &options(1024, false)).unwrap();
    grow(&engine, 51, 1_000, |_| {});
    engine.merge_index().unwrap();
    let condition = SourcingCondition {
        criteria: vec![Criterion {
            names: vec!["Paid".into()],
            tags: vec![],
        }],
    };
    let before = engine
        .plan_read(&condition, Position(0), engine.head(), usize::MAX)
        .unwrap();
    assert!(before.records.len() > 50);
    check_plan_against_files(&engine, dir.path(), &condition);
    let sealed: Vec<u64> = {
        let list = engine.segments.read();
        list.bases[..list.sealed_count].to_vec()
    };
    for base in sealed {
        std::fs::OpenOptions::new()
            .write(true)
            .open(segment::segment_path(dir.path(), base))
            .unwrap()
            .set_len(0)
            .unwrap();
    }
    let after = engine
        .plan_read(&condition, Position(0), engine.head(), usize::MAX)
        .unwrap();
    assert_eq!(after, before);
}

/// Merging follows the watermark, not the log: segments a quorum has not
/// committed past are left to their own key tables, and merged later.
#[test]
fn only_segments_ending_below_the_watermark_are_merged() {
    let dir = tempfile::tempdir().unwrap();
    let engine = EventStoreEngine::create_with_store_options(
        dir.path(),
        &StoreOptions {
            voters: vec![0, 1, 2],
            ..options(1024, false)
        },
    )
    .unwrap();
    // A three-voter store with no followers: nothing gets committed, so
    // appends are written but never acknowledged. Write directly.
    {
        let mut writer = engine.writer.lock();
        let mut rng = Rng(61);
        let old = writer.active_base_position();
        for i in 0..400 {
            let event = random_event(&mut rng, i);
            writer
                .write_batch(&crate::append::RecordBatch::from_events(&[event]))
                .unwrap();
        }
        engine.note_rotation(&writer, old);
        engine.local_tail.store(writer.head().0, Ordering::Release);
    }
    engine.sync_state.mark_pending();
    engine.sync_state.wake_sync_thread();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
    while engine
        .segments
        .read()
        .keys
        .values()
        .any(|k| matches!(k, SegmentKeys::Pinned(_)))
    {
        assert!(
            std::time::Instant::now() < deadline,
            "seals never finished: {:?} pending {} in flight {:?}",
            engine
                .segments
                .read()
                .keys
                .iter()
                .map(|(b, k)| (*b, matches!(k, SegmentKeys::Pinned(_))))
                .collect::<Vec<_>>(),
            engine.writer.lock().pending_seals().len(),
            engine.seals.in_flight.lock().unwrap()
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    let list = engine.segments.read().clone();
    assert!(list.sealed_count >= 16);
    assert_eq!(engine.watermark.get(), 0);
    assert_eq!(engine.merge_index().unwrap().tables_built, 0);

    // Commit up to exactly the end of the 16th segment: the first 8 end
    // below it and merge; the second 8 do not, since the last of them ends
    // at the watermark itself.
    let end_of_16th = list.bases[16];
    engine.adopt_watermark(engine.watermark.epoch(), Position(end_of_16th));
    engine.merge_index().unwrap();
    let tables = engine.segments.read().tables.clone();
    assert_eq!(tables.len(), 1);
    assert_eq!(
        (tables[0].base(), tables[0].end()),
        (list.bases[0], list.bases[8])
    );
    engine.adopt_watermark(engine.watermark.epoch(), Position(end_of_16th + 1));
    engine.merge_index().unwrap();
    let tables = engine.segments.read().tables.clone();
    assert_eq!(tables.len(), 2);
    assert_eq!(tables[1].end(), end_of_16th);
}
