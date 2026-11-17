//! `truncate_to_matching` refuses cuts it must not make with an error, not a
//! panic: the follower calls it on a blocking thread, where a panic only
//! surfaces as an opaque "task panicked".

use super::*;
use crate::append::AppendRequest;
use crate::event::AppendEvent;

fn event(name: &str) -> AppendEvent {
    AppendEvent {
        identifier: format!("id-{name}"),
        name: name.into(),
        version: "1.0".into(),
        timestamp: 1712345678000,
        payload: vec![1, 2, 3],
        metadata: Default::default(),
        tags: vec![],
    }
}

fn engine_with_events(dir: &std::path::Path, count: usize) -> EventStoreEngine {
    let store = EventStoreEngine::create(dir).unwrap();
    store
        .append(AppendRequest {
            condition: None,
            events: (0..count).map(|i| event(&format!("E{i}"))).collect(),
        })
        .unwrap();
    store
}

#[test]
fn a_cut_below_the_watermark_is_an_error_not_a_panic() {
    let dir = tempfile::tempdir().unwrap();
    let store = engine_with_events(dir.path(), 3);
    let watermark = store.watermark.get();
    assert!(watermark > 0, "the appended events are committed");
    let tail_before = store.local_tail().0;

    let error = store
        .truncate_to_matching(Position(0), None)
        .expect_err("cutting committed data must be refused");
    assert!(matches!(error, Error::Internal { .. }), "{error:?}");
    let message = error.to_string();
    assert!(
        message.contains(&format!("watermark {watermark}")) && message.contains("cut at 0"),
        "names the positions: {message}"
    );
    assert!(message.contains("snapshot"), "says what to do: {message}");
    assert_eq!(store.local_tail().0, tail_before, "nothing was cut");
    store.shutdown();
}

#[test]
fn a_cut_beyond_the_tail_is_not_reported_as_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let store = engine_with_events(dir.path(), 2);
    let beyond = store.local_tail().0 + 10;

    let error = store
        .truncate_to_matching(Position(beyond), None)
        .expect_err("a cut past the tail is refused");
    assert!(matches!(error, Error::Internal { .. }), "{error:?}");
    assert!(
        error.to_string().contains(&beyond.to_string()),
        "names the position: {error}"
    );
    store.shutdown();
}
