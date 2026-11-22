//! A retried append is recognised by its events' identifiers and answered
//! with the positions the first attempt got, instead of being written again.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

use super::*;
use crate::append::{AppendCondition, AppendRequest};
use crate::criteria::{Criterion, SourcingCondition};
use crate::event::{AppendEvent, Tag};

fn event(identifier: &str, payload: &str) -> AppendEvent {
    AppendEvent {
        identifier: identifier.into(),
        name: "OrderPlaced".into(),
        version: "1.0".into(),
        timestamp: 1712345678000,
        payload: payload.as_bytes().to_vec(),
        metadata: Default::default(),
        tags: vec![Tag::from_str("order", "o-1")],
    }
}

/// An event no condition in these tests looks for.
fn untagged(identifier: &str) -> AppendEvent {
    AppendEvent {
        tags: vec![],
        ..event(identifier, "some payload that takes a little room")
    }
}

fn plain(events: Vec<AppendEvent>) -> AppendRequest {
    AppendRequest {
        condition: None,
        events,
    }
}

/// Fails if an event tagged `order=o-1` exists after `marker`.
fn unless_ordered_after(marker: u64, events: Vec<AppendEvent>) -> AppendRequest {
    AppendRequest {
        condition: Some(AppendCondition {
            consistency_marker: Position(marker),
            criteria: SourcingCondition {
                criteria: vec![Criterion {
                    names: vec![],
                    tags: vec![Tag::from_str("order", "o-1")],
                }],
            },
        }),
        events,
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

fn stored(store: &EventStoreEngine) -> Vec<String> {
    store
        .source(Position(0), &everything())
        .unwrap()
        .into_iter()
        .map(|event| event.identifier)
        .collect()
}

fn with_window(dir: &Path, recent_events: usize) -> EventStoreEngine {
    EventStoreEngine::create_with_store_options(
        dir,
        &StoreOptions {
            recent_events,
            ..StoreOptions::default()
        },
    )
    .unwrap()
}

#[test]
fn a_retried_append_is_answered_with_the_positions_it_already_has() {
    let dir = tempfile::tempdir().unwrap();
    let store = EventStoreEngine::create(dir.path()).unwrap();
    store.append(plain(vec![event("before", "x")])).unwrap();

    let first = store
        .append(plain(vec![event("a", "one"), event("b", "two")]))
        .unwrap();
    store.append(plain(vec![event("after", "y")])).unwrap();
    let retried = store
        .append(plain(vec![event("a", "one"), event("b", "two")]))
        .unwrap();

    assert_eq!(retried.first_position, first.first_position);
    assert_eq!(retried.count, first.count);
    assert_eq!(retried.consistency_marker, first.consistency_marker);
    assert_eq!(stored(&store), ["before", "a", "b", "after"]);
    assert_eq!(store.head(), Position(4));
}

/// Without this a conditional append that was committed, retried, would be
/// refused by its own events.
#[test]
fn a_retried_conditional_append_succeeds_again_instead_of_conflicting_with_itself() {
    let dir = tempfile::tempdir().unwrap();
    let store = EventStoreEngine::create(dir.path()).unwrap();
    let read_up_to = store
        .append(plain(vec![untagged("before")]))
        .unwrap()
        .consistency_marker
        .0;

    let first = store
        .append(unless_ordered_after(read_up_to, vec![event("a", "one")]))
        .unwrap();
    let retried = store
        .append(unless_ordered_after(read_up_to, vec![event("a", "one")]))
        .unwrap();
    assert_eq!(retried.first_position, first.first_position);
    assert_eq!(stored(&store), ["before", "a"]);

    // Another append under the same condition is a different append, and
    // still conflicts.
    let other = store.append(unless_ordered_after(read_up_to, vec![event("c", "three")]));
    assert!(matches!(
        other,
        Err(Error::ConsistencyConditionViolated { .. })
    ));
}

/// The first attempt's segment has been sealed since: the sealed half of the
/// condition check runs before the writer lock, and finds the retry's own
/// events there.
#[test]
fn a_retried_conditional_append_succeeds_again_after_its_segment_was_sealed() {
    let dir = tempfile::tempdir().unwrap();
    let store = EventStoreEngine::create_with_store_options(
        dir.path(),
        &StoreOptions {
            max_segment_size: 4 * 1024,
            ..StoreOptions::default()
        },
    )
    .unwrap();
    let read_up_to = store
        .append(plain(vec![untagged("before")]))
        .unwrap()
        .consistency_marker
        .0;
    let first = store
        .append(unless_ordered_after(read_up_to, vec![event("a", "one")]))
        .unwrap();
    for i in 0..100 {
        store
            .append(plain(vec![untagged(&format!("filler-{i}"))]))
            .unwrap();
    }
    assert!(
        store.segments.read().sealed_count >= 1,
        "the segment sealed"
    );

    let retried = store
        .append(unless_ordered_after(read_up_to, vec![event("a", "one")]))
        .unwrap();
    assert_eq!(retried.first_position, first.first_position);
    assert_eq!(store.head(), Position(102), "nothing was written");
}

#[test]
fn only_the_same_events_are_a_retry() {
    let dir = tempfile::tempdir().unwrap();
    let store = EventStoreEngine::create(dir.path()).unwrap();
    store
        .append(plain(vec![event("a", "one"), event("b", "two")]))
        .unwrap();

    // The same identifier with other content.
    store.append(plain(vec![event("a", "changed")])).unwrap();
    // Part of an earlier append with something new.
    store
        .append(plain(vec![event("b", "two"), event("c", "three")]))
        .unwrap();
    // No identifier: written every time.
    store.append(plain(vec![event("", "anonymous")])).unwrap();
    store.append(plain(vec![event("", "anonymous")])).unwrap();

    assert_eq!(stored(&store), ["a", "b", "a", "b", "c", "", ""]);
}

#[test]
fn a_retry_older_than_the_window_is_written_again() {
    let dir = tempfile::tempdir().unwrap();
    let store = with_window(dir.path(), 2);
    store.append(plain(vec![event("a", "one")])).unwrap();
    store.append(plain(vec![event("b", "two")])).unwrap();
    store.append(plain(vec![event("c", "three")])).unwrap();

    // `b` is still among the last two events; `a` is not.
    store.append(plain(vec![event("b", "two")])).unwrap();
    assert_eq!(stored(&store), ["a", "b", "c"]);
    store.append(plain(vec![event("a", "one")])).unwrap();
    assert_eq!(stored(&store), ["a", "b", "c", "a"]);
}

#[test]
fn a_window_of_zero_turns_it_off() {
    let dir = tempfile::tempdir().unwrap();
    let store = with_window(dir.path(), 0);
    store.append(plain(vec![event("a", "one")])).unwrap();
    store.append(plain(vec![event("a", "one")])).unwrap();
    assert_eq!(stored(&store), ["a", "a"]);
}

#[test]
fn a_retry_is_recognised_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let first = {
        let store = EventStoreEngine::create(dir.path()).unwrap();
        store.append(plain(vec![event("before", "x")])).unwrap();
        let first = store
            .append(plain(vec![event("a", "one"), event("b", "two")]))
            .unwrap();
        store.shutdown();
        first
    };

    let store = EventStoreEngine::open(dir.path()).unwrap();
    let retried = store
        .append(plain(vec![event("a", "one"), event("b", "two")]))
        .unwrap();
    assert_eq!(retried.first_position, first.first_position);
    assert_eq!(stored(&store), ["before", "a", "b"]);
}

/// Opening a log reads no sealed segment's body, so after a restart the
/// window is the open segment: what was sealed before is written again.
#[test]
fn after_a_restart_the_window_is_the_open_segment() {
    let dir = tempfile::tempdir().unwrap();
    let options = StoreOptions {
        max_segment_size: 4 * 1024,
        ..StoreOptions::default()
    };
    let (open_from, tail) = {
        let store = EventStoreEngine::create_with_store_options(dir.path(), &options).unwrap();
        for i in 0..200 {
            store
                .append(plain(vec![event(&format!("e{i}"), "some payload bytes")]))
                .unwrap();
        }
        assert!(store.segments.read().sealed_count >= 2, "several segments");
        let open_from = store.writer.lock().active_base_position();
        store.shutdown();
        (open_from, 200)
    };
    assert!(open_from > 3 && open_from < tail - 1, "{open_from}");

    let store = EventStoreEngine::open_with_store_options(dir.path(), &options).unwrap();
    // In the open segment: recognised.
    let last = store
        .append(plain(vec![event("e199", "some payload bytes")]))
        .unwrap();
    assert_eq!(last.first_position, Position(199));
    let first_open = store
        .append(plain(vec![event(
            &format!("e{open_from}"),
            "some payload bytes",
        )]))
        .unwrap();
    assert_eq!(first_open.first_position, Position(open_from));
    assert_eq!(store.head(), Position(tail));
    // Sealed before the restart: written again.
    let sealed = store
        .append(plain(vec![event("e3", "some payload bytes")]))
        .unwrap();
    assert_eq!(sealed.first_position, Position(tail));
}

/// A node that received the events from the leader recognises the retry once
/// it leads: the client does not know, or care, who wrote the first attempt.
#[test]
fn a_retry_is_recognised_by_a_node_that_got_the_events_by_replication() {
    let options = StoreOptions {
        voters: vec![0, 1, 2],
        ..StoreOptions::default()
    };
    let leader_dir = tempfile::tempdir().unwrap();
    let follower_dir = tempfile::tempdir().unwrap();
    let leader = EventStoreEngine::create_with_store_options(leader_dir.path(), &options).unwrap();
    let follower =
        EventStoreEngine::create_with_store_options(follower_dir.path(), &options).unwrap();

    leader.begin_replication_epoch(1, [0, 1, 2]).unwrap();
    leader.persist_epoch_change(1, 0).unwrap();
    // Staged only: with three voters nothing commits without a quorum, and
    // the retry is about what is in the log, not what is committed.
    let staged = leader
        .append_stage(plain(vec![event("a", "one"), event("b", "two")]))
        .unwrap();
    let first = staged.outcome.as_ref().unwrap().first_position;
    leader.drain_pending().unwrap();

    // What a Tail session does: the leader's bytes, applied as they are.
    follower.begin_replication_epoch(1, [0, 1, 2]).unwrap();
    let (_, slices) = leader.replication_catchup_slices(Position(0)).unwrap();
    for slice in slices {
        if slice.first_position == slice.segment_base && slice.segment_base > 0 {
            follower
                .rotate_replicated(1, Position(slice.segment_base))
                .unwrap();
        }
        let mut file = File::open(&slice.path).unwrap();
        file.seek(SeekFrom::Start(slice.byte_start)).unwrap();
        let mut bytes = vec![0u8; (slice.byte_end - slice.byte_start) as usize];
        file.read_exact(&mut bytes).unwrap();
        follower
            .apply_replicated_records(
                1,
                slice.segment_base,
                Position(slice.first_position),
                &bytes,
            )
            .unwrap();
    }
    assert_eq!(follower.local_tail(), Position(2));

    // The follower takes over, and the client retries against it.
    follower.begin_replication_epoch(2, [0, 1, 2]).unwrap();
    let retried = follower
        .append_stage(plain(vec![event("a", "one"), event("b", "two")]))
        .unwrap();
    assert_eq!(retried.outcome.as_ref().unwrap().first_position, first);
    assert_eq!(follower.local_tail(), Position(2), "nothing was written");
}

#[test]
fn a_retry_of_events_that_were_cut_from_the_log_is_written() {
    let options = StoreOptions {
        voters: vec![0, 1, 2],
        ..StoreOptions::default()
    };
    let dir = tempfile::tempdir().unwrap();
    let store = EventStoreEngine::create_with_store_options(dir.path(), &options).unwrap();
    store.begin_replication_epoch(1, [0, 1, 2]).unwrap();
    store.append_stage(plain(vec![event("kept", "x")])).unwrap();
    store.append_stage(plain(vec![event("a", "one")])).unwrap();
    store.drain_pending().unwrap();

    // A new leader never had `a`: this node cuts it away.
    store.truncate_to(Position(1)).unwrap();
    let again = store.append_stage(plain(vec![event("a", "one")])).unwrap();
    assert_eq!(
        again.outcome.as_ref().unwrap().first_position,
        Position(1),
        "written at the new end of the log"
    );
    assert_eq!(store.local_tail(), Position(2));
    // And what was kept is still recognised.
    let kept = store.append_stage(plain(vec![event("kept", "x")])).unwrap();
    assert_eq!(kept.outcome.as_ref().unwrap().first_position, Position(0));
    assert_eq!(store.local_tail(), Position(2));
}

/// The server's own events are not a client's retry: a snapshot of the same
/// key, for one, is written each time.
#[test]
fn the_servers_own_appends_are_written_every_time() {
    let dir = tempfile::tempdir().unwrap();
    let store = EventStoreEngine::create(dir.path()).unwrap();
    let system = || AppendEvent {
        identifier: "same".into(),
        name: "$test".into(),
        version: "1".into(),
        timestamp: 1,
        payload: vec![1],
        metadata: Default::default(),
        tags: vec![],
    };
    store.append_system(plain(vec![system()])).unwrap();
    store.append_system(plain(vec![system()])).unwrap();
    assert_eq!(store.local_tail(), Position(2));
}

/// A retry staged in the same hold of the writer lock as the first attempt,
/// whose bytes have not reached the file yet.
#[test]
fn a_retry_in_the_same_group_as_the_first_attempt_is_recognised() {
    let dir = tempfile::tempdir().unwrap();
    let store = EventStoreEngine::create(dir.path()).unwrap();
    let group = vec![
        prepare_client_append(plain(vec![event("a", "one")])).unwrap(),
        prepare_client_append(plain(vec![event("b", "two")])).unwrap(),
        prepare_client_append(plain(vec![event("a", "one")])).unwrap(),
    ];
    let firsts: Vec<u64> = store
        .stage_group(group)
        .into_iter()
        .map(|staging| match staging {
            Staging::Done(Ok(staged)) => staged.outcome.unwrap().first_position.0,
            _ => panic!("every append stages"),
        })
        .collect();
    assert_eq!(firsts, [0, 1, 0]);
    assert_eq!(store.local_tail(), Position(2));
}

#[test]
fn recognised_retries_are_counted_and_not_as_appended_events() {
    let dir = tempfile::tempdir().unwrap();
    let store = EventStoreEngine::create(dir.path()).unwrap();
    store.append(plain(vec![event("a", "one")])).unwrap();
    store.append(plain(vec![event("a", "one")])).unwrap();
    let metrics = store.metrics_snapshot();
    assert_eq!(metrics.retried_appends, 1);
    assert_eq!(metrics.events_appended, 1);
}
