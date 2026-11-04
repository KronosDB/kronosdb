//! Evicted segments (ADR-0011): a store whose oldest segment bodies live
//! only in the archive reads exactly what it read with them local.

use std::path::Path;

use super::*;
use crate::append::AppendRequest;
use crate::criteria::Criterion;
use crate::event::{AppendEvent, Tag};
use crate::remote::ArchiveStore;
use crate::tier::Archiver;

const SEGMENT_SIZE: u64 = 4 * 1024;
const FIRST_TIMESTAMP: i64 = 1_700_000_000_000;

fn archive_url(tier: &Path) -> String {
    url::Url::from_directory_path(tier).unwrap().to_string()
}

fn options(tier: Option<&Path>) -> StoreOptions {
    StoreOptions {
        max_segment_size: SEGMENT_SIZE,
        background_index_merge: false,
        index_merge_rate: 0,
        archive: tier.map(|tier| ArchiveStore::from_url(&archive_url(tier), None).unwrap()),
        ..StoreOptions::default()
    }
}

/// A store under `<root>/default` of 400 events in ~40 segments, every
/// event tagged with its order and timestamped one second after the last.
fn filled_store(root: &Path, tier: &Path) -> EventStoreEngine {
    let store =
        EventStoreEngine::create_with_store_options(&root.join("default"), &options(Some(tier)))
            .unwrap();
    fill(&store);
    store
}

/// Appends `filled_store`'s events, waits for their seals and merges.
fn fill(store: &EventStoreEngine) {
    for batch in 0..50u32 {
        store
            .append(AppendRequest {
                condition: None,
                events: (0..8u32)
                    .map(|i| {
                        let n = batch * 8 + i;
                        AppendEvent {
                            identifier: format!("e-{n}"),
                            name: "Placed".into(),
                            version: "1".into(),
                            timestamp: FIRST_TIMESTAMP + i64::from(n) * 1000,
                            payload: vec![b'p'; 200 + (n % 7) as usize * 40],
                            metadata: vec![],
                            tags: vec![Tag::from_str("order", &format!("o-{}", n % 13))],
                        }
                    })
                    .collect(),
            })
            .unwrap();
    }
    store.wait_for_seals();
    store.merge_index().unwrap();
}

fn archive(store: &EventStoreEngine, tier: &Path) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let report = runtime
        .block_on(
            Archiver::from_url(&archive_url(tier))
                .unwrap()
                .run_archive_pass("default", store),
        )
        .unwrap();
    assert!(report.segments > 0);
}

/// Evicts the oldest `count` segments.
fn evict(store: &EventStoreEngine, count: usize) {
    for segment in store.archivable_segments().iter().take(count) {
        store.evict_segment(segment.base).unwrap();
        assert!(!segment.path.exists());
    }
}

fn order(value: &str) -> SourcingCondition {
    SourcingCondition {
        criteria: vec![Criterion {
            names: vec![],
            tags: vec![Tag::from_str("order", value)],
        }],
    }
}

fn everything() -> SourcingCondition {
    SourcingCondition {
        criteria: vec![Criterion {
            names: vec![],
            tags: vec![],
        }],
    }
}

/// What the tests compare: every kind of read that touches segment bodies.
#[derive(Debug, PartialEq)]
struct Reads {
    all: Vec<(u64, String, Vec<u8>)>,
    orders: Vec<Vec<u64>>,
    tags: Vec<Vec<Tag>>,
    walked: Vec<(u64, Vec<u8>)>,
    sequence_at: Vec<Option<Position>>,
}

fn reads(store: &EventStoreEngine) -> Result<Reads, Error> {
    let head = store.watermark.get();
    let all = store
        .source(Position(0), &everything())?
        .into_iter()
        .map(|event| (event.position.0, event.identifier, event.payload))
        .collect();
    let orders = ["o-0", "o-5", "o-12"]
        .iter()
        .map(|value| {
            store
                .source(Position(0), &order(value))
                .map(|events| events.iter().map(|event| event.position.0).collect())
        })
        .collect::<Result<_, _>>()?;
    let tags = [0, 37, 211, 399]
        .iter()
        .map(|&position| store.get_tags(Position(position)))
        .collect::<Result<_, _>>()?;
    let mut walked = Vec::new();
    store.view.visit_events(0, head, |position, name, _, _| {
        walked.push((position, name.to_vec()));
        true
    })?;
    let sequence_at = [0, 1, 150, 399, 400, 10_000]
        .iter()
        .map(|&seconds| store.get_sequence_at(FIRST_TIMESTAMP + seconds * 1000))
        .collect::<Result<_, _>>()?;
    Ok(Reads {
        all,
        orders,
        tags,
        walked,
        sequence_at,
    })
}

#[test]
fn evicted_segments_read_like_local_ones() {
    let root = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let store = filled_store(root.path(), tier.path());
    let expected = reads(&store).unwrap();
    assert_eq!(expected.all.len(), 400);
    assert_eq!(expected.sequence_at[2], Some(Position(150)));

    archive(&store, tier.path());
    let segments = store.archivable_segments();
    assert!(segments.len() > 20, "{} segments", segments.len());
    evict(&store, segments.len() - 3);
    assert_eq!(store.evicted_end(), segments[segments.len() - 3].base);

    assert_eq!(reads(&store).unwrap(), expected);
    let remote = store.evicted.archive(0).unwrap();
    let requests = remote.requests();
    assert!(requests > 0);

    // A second round is served from the blocks fetched by the first.
    assert_eq!(reads(&store).unwrap(), expected);
    assert_eq!(remote.requests(), requests);
}

#[test]
fn a_reopened_store_finds_its_evicted_segments() {
    let root = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let dir = root.path().join("default");
    let store = filled_store(root.path(), tier.path());
    let expected = reads(&store).unwrap();
    archive(&store, tier.path());
    evict(&store, 10);
    let evicted_end = store.evicted_end();
    let evicted = store.archivable_segments()[..10].to_vec();
    store.shutdown();
    drop(store);

    // An evicted segment that also lost its companions gets them back from
    // the archive: they cannot be rebuilt from a body that is gone.
    let lost = &evicted[3];
    std::fs::remove_file(lost.path.with_extension("idx")).unwrap();
    let _ = std::fs::remove_file(lost.path.with_extension("keys"));

    let store =
        EventStoreEngine::open_with_store_options(&dir, &options(Some(tier.path()))).unwrap();
    assert_eq!(store.evicted_end(), evicted_end);
    assert!(lost.path.with_extension("idx").exists());
    assert_eq!(reads(&store).unwrap(), expected);
}

/// A store whose oldest segments are only in the archive does not open
/// without it, rather than failing every read of them later.
#[test]
fn a_store_with_evicted_segments_needs_its_archive_to_open() {
    let root = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let dir = root.path().join("default");
    let store = filled_store(root.path(), tier.path());
    archive(&store, tier.path());
    evict(&store, 5);
    store.shutdown();
    drop(store);

    let Err(error) = EventStoreEngine::open_with_store_options(&dir, &options(None)) else {
        panic!("opened without the archive its evicted segments are in");
    };
    assert!(matches!(error, Error::Rejected { .. }), "{error}");
}

/// Opening reads nothing from the archive while every evicted segment has
/// its locator and is indexed here, its own key table or a merged one.
#[cfg(unix)]
#[test]
fn a_store_with_its_index_opens_without_reaching_the_archive() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let dir = root.path().join("default");
    let store = filled_store(root.path(), tier.path());
    let expected = reads(&store).unwrap();
    archive(&store, tier.path());
    evict(&store, 10);
    let evicted = store.archivable_segments()[..10].to_vec();
    assert!(
        evicted
            .iter()
            .any(|segment| !segment.path.with_extension("keys").exists()),
        "some evicted segment should be indexed by a merged table only"
    );
    store.shutdown();
    drop(store);

    // Key tables in the archive cannot be read now.
    let index = tier.path().join("default").join("index");
    std::fs::set_permissions(&index, std::fs::Permissions::from_mode(0o000)).unwrap();
    let opened = EventStoreEngine::open_with_store_options(&dir, &options(Some(tier.path())));
    std::fs::set_permissions(&index, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(reads(&opened.unwrap()).unwrap(), expected);
}

/// A body that cannot be deleted stays a body: the evicted prefix does not
/// move past it, and the store reopens as it was.
#[cfg(unix)]
#[test]
fn a_body_that_cannot_be_deleted_is_not_evicted() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let dir = root.path().join("default");
    let store = filled_store(root.path(), tier.path());
    let expected = reads(&store).unwrap();
    archive(&store, tier.path());
    evict(&store, 2);
    let evicted_end = store.evicted_end();
    let next = store.archivable_segments()[2].clone();

    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let refused = store.evict_segment(next.base);
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(refused.is_err());
    assert!(next.path.exists());
    assert_eq!(store.evicted_end(), evicted_end);
    assert_eq!(reads(&store).unwrap(), expected);

    store.evict_segment(next.base).unwrap();
    store.shutdown();
    drop(store);
    let store =
        EventStoreEngine::open_with_store_options(&dir, &options(Some(tier.path()))).unwrap();
    assert_eq!(store.evicted_end(), next.end);
    assert_eq!(reads(&store).unwrap(), expected);
}

#[test]
fn a_damaged_archived_record_is_corrupted() {
    let root = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let store = filled_store(root.path(), tier.path());
    archive(&store, tier.path());
    evict(&store, 1);

    let object = tier
        .path()
        .join("default")
        .join("segments")
        .join(format!("{:020}.seg", 0));
    let mut bytes = std::fs::read(&object).unwrap();
    let at = bytes.len() / 2;
    bytes[at] ^= 0xff;
    std::fs::write(&object, bytes).unwrap();

    let Err(error) = store.source(Position(0), &everything()) else {
        panic!("read an evicted segment");
    };
    assert!(matches!(error, Error::Corrupted { .. }), "{error}");
}

#[test]
fn only_the_oldest_final_segment_with_a_body_is_evicted() {
    let root = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let store = filled_store(root.path(), tier.path());
    archive(&store, tier.path());
    let segments = store.archivable_segments();

    let error = store.evict_segment(segments[1].base).unwrap_err();
    assert!(matches!(error, Error::Rejected { .. }), "{error}");
    assert!(segments[1].path.exists());

    let active = *store.segments.read().bases.last().unwrap();
    let error = store.evict_segment(active).unwrap_err();
    assert!(matches!(error, Error::Rejected { .. }), "{error}");

    store.evict_segment(segments[0].base).unwrap();
    store.evict_segment(segments[1].base).unwrap();
    assert_eq!(store.evicted_end(), segments[1].end);
}

fn local_bytes(store: &EventStoreEngine) -> u64 {
    store
        .sealed_bodies()
        .iter()
        .map(|segment| std::fs::metadata(&segment.path).unwrap().len())
        .sum()
}

fn evict_down_to(
    store: Arc<EventStoreEngine>,
    tier: &Path,
    max_local_bytes: u64,
) -> Result<crate::tier::EvictionPassReport, Error> {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        Archiver::from_url(&archive_url(tier))
            .unwrap()
            .run_eviction_pass(&[("default".to_string(), store)], max_local_bytes)
            .await
    })
}

#[test]
fn eviction_keeps_the_oldest_segments_in_the_archive_only() {
    let root = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let store = Arc::new(filled_store(root.path(), tier.path()));
    let expected = reads(&store).unwrap();
    archive(&store, tier.path());
    let before = local_bytes(&store);
    let cap = before / 2;

    let report = evict_down_to(Arc::clone(&store), tier.path(), cap).unwrap();
    assert!(report.evicted > 0);
    assert!(report.local_bytes <= cap, "{report:?}");
    assert_eq!(report.local_bytes, local_bytes(&store));
    assert_eq!(report.freed, before - report.local_bytes);
    // The evicted segments are exactly the oldest ones.
    let segments = store.archivable_segments();
    for segment in &segments {
        assert_eq!(segment.path.exists(), segment.base >= store.evicted_end());
    }
    assert_eq!(reads(&store).unwrap(), expected);

    // Under the cap, a pass does nothing.
    let again = evict_down_to(Arc::clone(&store), tier.path(), cap).unwrap();
    assert_eq!(again.evicted, 0);
}

#[test]
fn segments_not_yet_archived_are_kept() {
    let root = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let store = Arc::new(filled_store(root.path(), tier.path()));
    let report = evict_down_to(Arc::clone(&store), tier.path(), 0).unwrap();
    assert_eq!(report.evicted, 0);
    assert_eq!(report.waiting, vec!["default".to_string()]);
    assert_eq!(store.evicted_end(), 0);
}

#[test]
fn a_segment_unlike_its_archived_copy_is_kept() {
    let root = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let store = Arc::new(filled_store(root.path(), tier.path()));
    archive(&store, tier.path());
    let summary = tier
        .path()
        .join("default")
        .join("segments")
        .join(format!("{:020}.sum", 0));
    let mut archived: crate::tier::SegmentSummary =
        serde_json::from_slice(&std::fs::read(&summary).unwrap()).unwrap();
    archived.blake3 = "0".repeat(64);
    std::fs::write(&summary, serde_json::to_vec(&archived).unwrap()).unwrap();

    let error = evict_down_to(Arc::clone(&store), tier.path(), 0).unwrap_err();
    assert!(matches!(error, Error::Corrupted { .. }), "{error}");
    assert_eq!(store.evicted_end(), 0);
    assert!(store.archivable_segments()[0].path.exists());
}

/// A read across many evicted segments plans ahead and fetches them all at
/// once: against a store that answers every request after a delay, it takes
/// a few delays, not one per segment. So does a scan of the evicted log.
#[test]
fn a_read_across_evicted_segments_waits_about_one_round_trip() {
    use object_store::ObjectStore;
    use object_store::throttle::{ThrottleConfig, ThrottledStore};

    const DELAY: std::time::Duration = std::time::Duration::from_millis(100);
    let root = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let dir = root.path().join("default");
    let store = filled_store(root.path(), tier.path());
    archive(&store, tier.path());
    let segments = store.archivable_segments();
    evict(&store, segments.len() - 1);
    let evicted = segments.len() - 1;
    assert!(evicted >= 20, "{evicted} segments evicted");
    store.shutdown();
    drop(store);

    let slow: Arc<dyn ObjectStore> = Arc::new(ThrottledStore::new(
        in_memory_copy(tier.path()),
        ThrottleConfig {
            wait_get_per_call: DELAY,
            ..ThrottleConfig::default()
        },
    ));
    let archive =
        ArchiveStore::over_store(slow, object_store::path::Path::default(), None).unwrap();
    let store = EventStoreEngine::open_with_store_options(
        &dir,
        &StoreOptions {
            archive: Some(archive),
            ..options(None)
        },
    )
    .unwrap();

    // Order o-0 is in one batch of ten events in thirteen, so it is
    // spread over most segments.
    let started = std::time::Instant::now();
    let read = store.source(Position(0), &order("o-0")).unwrap();
    let took = started.elapsed();
    assert!(!read.is_empty());
    let touched = {
        let list = store.segments.read();
        let mut bases: Vec<u64> = read
            .iter()
            .map(|event| {
                let at = list.bases.partition_point(|&b| b <= event.position.0) - 1;
                list.bases[at]
            })
            .collect();
        bases.dedup();
        bases.len()
    };
    eprintln!("a read over {touched} evicted segments took {took:?}, delay {DELAY:?}");
    assert!(touched >= 10, "the read touched only {touched} segments");
    assert!(
        took < DELAY * 4,
        "a read over {touched} evicted segments took {took:?}, {} delays",
        took.as_millis() / DELAY.as_millis()
    );

    let started = std::time::Instant::now();
    assert_eq!(store.source(Position(0), &everything()).unwrap().len(), 400);
    let took = started.elapsed();
    eprintln!("a scan over {evicted} evicted segments took {took:?}");
    assert!(
        took < DELAY * 4,
        "a scan over {evicted} evicted segments took {took:?}"
    );

    // A subscription catching up walks the log the same way.
    let fresh = EventStoreEngine::open_with_store_options(
        &dir,
        &StoreOptions {
            archive: Some(
                ArchiveStore::over_store(
                    Arc::new(ThrottledStore::new(
                        in_memory_copy(tier.path()),
                        ThrottleConfig {
                            wait_get_per_call: DELAY,
                            ..ThrottleConfig::default()
                        },
                    )),
                    object_store::path::Path::default(),
                    None,
                )
                .unwrap(),
            ),
            ..options(None)
        },
    )
    .unwrap();
    let head = fresh.watermark.get();
    let started = std::time::Instant::now();
    let mut walked = 0;
    fresh
        .view
        .visit_events(0, head, |_, _, _, _| {
            walked += 1;
            true
        })
        .unwrap();
    let took = started.elapsed();
    eprintln!("a subscription walk over {evicted} evicted segments took {took:?}");
    assert_eq!(walked, 400);
    assert!(took < DELAY * 4, "a subscription walk took {took:?}");
}

/// The archive under `tier`, copied into a store in memory.
fn in_memory_copy(tier: &Path) -> Arc<object_store::memory::InMemory> {
    use object_store::ObjectStoreExt;
    let memory = Arc::new(object_store::memory::InMemory::new());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut dirs = vec![tier.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
                continue;
            }
            let name = path
                .strip_prefix(tier)
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            let bytes = std::fs::read(&path).unwrap();
            runtime
                .block_on(memory.put(&object_store::path::Path::from(name), bytes.into()))
                .unwrap();
        }
    }
    memory
}

/// A download that dies (its store panics) answers the read with an error
/// instead of leaving it waiting.
#[test]
fn a_download_that_dies_fails_the_read() {
    use object_store::ObjectStore;
    use object_store::throttle::{ThrottleConfig, ThrottledStore};

    let root = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let dir = root.path().join("default");
    let store = filled_store(root.path(), tier.path());
    archive(&store, tier.path());
    evict(&store, 3);
    store.shutdown();
    drop(store);

    // The throttle does not support a file store's answers and panics.
    let dying: Arc<dyn ObjectStore> = Arc::new(ThrottledStore::new(
        object_store::local::LocalFileSystem::new_with_prefix(tier.path()).unwrap(),
        ThrottleConfig::default(),
    ));
    let archive =
        ArchiveStore::over_store(dying, object_store::path::Path::default(), None).unwrap();
    let store = EventStoreEngine::open_with_store_options(
        &dir,
        &StoreOptions {
            archive: Some(archive),
            ..options(None)
        },
    )
    .unwrap();
    let Err(error) = store.source(Position(0), &everything()) else {
        panic!("read evicted segments from a store that panics");
    };
    assert!(matches!(error, Error::Unavailable { .. }), "{error}");
}

/// Every step of the archive against the object store at
/// `KRONOSDB_TEST_ARCHIVE_URL`, configured by its provider's environment
/// variables: S3 or an S3-compatible store, GCS, Azure, or their
/// emulators. Skipped when unset. Each run writes under a prefix of its own.
#[test]
fn an_archive_at_the_environments_url_serves_every_step() {
    let Ok(base) = std::env::var("KRONOSDB_TEST_ARCHIVE_URL") else {
        eprintln!("skipped: KRONOSDB_TEST_ARCHIVE_URL is not set");
        return;
    };
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let url = format!("{}/run-{run}", base.trim_end_matches('/'));
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("default");
    let with_archive = || StoreOptions {
        archive: Some(ArchiveStore::from_url(&url, None).unwrap()),
        ..options(None)
    };
    let store = EventStoreEngine::create_with_store_options(&dir, &with_archive()).unwrap();
    fill(&store);
    let expected = reads(&store).unwrap();

    // Archiving: summaries created only if absent, a second pass finds
    // everything there, and superseded tables are deleted.
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let archiver = Archiver::from_url(&url)
        .unwrap()
        .with_delete_grace(std::time::Duration::ZERO);
    let first = runtime
        .block_on(archiver.run_archive_pass("default", &store))
        .unwrap();
    assert!(first.segments > 20, "{first:?}");
    let again = Archiver::from_url(&url)
        .unwrap()
        .with_delete_grace(std::time::Duration::ZERO);
    let second = runtime
        .block_on(again.run_archive_pass("default", &store))
        .unwrap();
    assert_eq!(second.segments, 0, "{second:?}");
    assert_eq!(second.archived_end, first.archived_end);

    // What a joining node reads: the listing, summaries, whole segments.
    let remote = store.evicted.archive(0).unwrap();
    let listing = remote.listing().unwrap();
    let segments = store.archivable_segments();
    assert_eq!(listing.segments.len(), segments.len());
    assert!(!listing.tables.is_empty());
    let bases: Vec<u64> = segments.iter().map(|segment| segment.base).collect();
    for summary in remote.summaries(&bases).unwrap() {
        let summary = summary.expect("every archived segment has a summary");
        remote.fetch_segment(&summary).unwrap();
    }

    // Eviction and ranged reads, before and after a restart.
    evict(&store, segments.len() - 3);
    assert_eq!(reads(&store).unwrap(), expected);
    store.shutdown();
    drop(store);
    let store = EventStoreEngine::open_with_store_options(&dir, &with_archive()).unwrap();
    assert_eq!(store.evicted_end(), segments[segments.len() - 3].base);
    assert_eq!(reads(&store).unwrap(), expected);
}

/// A read from below the end of the evicted prefix may wait on the
/// archive; the server reads those off its async runtime. One from past it
/// reads only local segments.
#[test]
fn reads_from_evicted_positions_are_told_apart() {
    use crate::api::EventStore;
    let root = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let store = filled_store(root.path(), tier.path());
    assert!(
        !store.reads_archive_from(Position(0)),
        "nothing is evicted yet"
    );
    archive(&store, tier.path());
    evict(&store, 5);
    let evicted_end = store.evicted_end();
    assert!(store.reads_archive_from(Position(0)));
    assert!(store.reads_archive_from(Position(evicted_end - 1)));
    assert!(!store.reads_archive_from(Position(evicted_end)));
    assert_eq!(
        store.final_end(),
        store.archivable_segments().last().unwrap().end
    );
    assert!(store.sealed_body_bytes() > 0);
}

/// On a node that evicts, the oldest segment that still has its body can be
/// evicted while a read of it runs, so a read from there may wait on the
/// archive too.
#[test]
fn reads_from_the_oldest_local_segment_may_reach_the_archive_on_a_capped_node() {
    use crate::api::EventStore;
    let root = tempfile::tempdir().unwrap();
    let tier = tempfile::tempdir().unwrap();
    let store = EventStoreEngine::create_with_store_options(
        &root.path().join("default"),
        &StoreOptions {
            archive: Some(
                ArchiveStore::from_url(&archive_url(tier.path()), Some(1 << 30)).unwrap(),
            ),
            ..options(None)
        },
    )
    .unwrap();
    fill(&store);
    let segments = store.archivable_segments();
    assert!(store.reads_archive_from(Position(0)));
    assert!(store.reads_archive_from(Position(segments[0].end - 1)));
    assert!(!store.reads_archive_from(Position(segments[0].end)));
    archive(&store, tier.path());
    evict(&store, 2);
    assert!(store.reads_archive_from(Position(segments[2].end - 1)));
    assert!(!store.reads_archive_from(Position(segments[2].end)));
}
