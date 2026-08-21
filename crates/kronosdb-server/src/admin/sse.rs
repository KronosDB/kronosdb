//! Server-sent events for the admin console.
//!
//! One `/sse` connection per browser tab replaces per-fragment polling.
//! Every `POLL_INTERVAL` the handler fingerprints each topic's backing
//! data and emits a `tick` event naming the topic when the fingerprint
//! changes. The frontend re-fires the tick as an htmx `sse-<topic>`
//! trigger on `<body>`, so fragments refresh only when something they
//! display actually changed.

use std::collections::hash_map::DefaultHasher;
use std::convert::Infallible;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use axum::extract::State;
use axum::response::IntoResponse;
use axum::response::sse::{Event, KeepAlive, Sse};
use tokio_stream::wrappers::ReceiverStream;

use super::AdminState;

/// How often topic fingerprints are recomputed. This bounds tick latency,
/// not client traffic — nothing is sent while fingerprints are unchanged.
const POLL_INTERVAL: Duration = Duration::from_secs(2);

const TOPICS: &[&str] = &[
    "stats",
    "contexts",
    "clients",
    "commands",
    "queries",
    "subscriptions",
    "processors",
    "events",
];

pub async fn sse_handler(State(state): State<AdminState>) -> impl IntoResponse {
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<Event, Infallible>>(32);

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(POLL_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut last: Vec<u64> = vec![0; TOPICS.len()];
        let mut first = true;

        loop {
            interval.tick().await;
            for (i, topic) in TOPICS.iter().enumerate() {
                let fp = fingerprint(&state, topic);
                if fp != last[i] {
                    last[i] = fp;
                    // The page just rendered fresh data; skip the initial burst.
                    if first {
                        continue;
                    }
                    let event = Event::default().event("tick").data(*topic);
                    if tx.send(Ok(event)).await.is_err() {
                        return; // Client disconnected.
                    }
                }
            }
            first = false;
        }
    });

    Sse::new(ReceiverStream::new(rx)).keep_alive(KeepAlive::default())
}

/// A cheap, order-stable digest of the data behind one topic. Collisions
/// only delay a refresh until the next real change — never corrupt data —
/// so a 64-bit std hash is plenty.
fn fingerprint(state: &AdminState, topic: &str) -> u64 {
    let mut h = DefaultHasher::new();
    match topic {
        "stats" | "events" | "contexts" => {
            // Heads/tails move on every append; context list changes on
            // create. stats/chart/context tables all key off the same data.
            topic.hash(&mut h);
            for name in state.contexts.list_contexts() {
                name.hash(&mut h);
                if let Ok(store) = state.contexts.get_context(&name) {
                    store.head().0.hash(&mut h);
                    store.tail().0.hash(&mut h);
                }
            }
            if topic == "stats" {
                state.client_registry.client_count().hash(&mut h);
            }
        }
        "clients" => {
            for c in state.client_registry.list_client_details() {
                c.client_id.0.hash(&mut h);
                c.component_name.0.hash(&mut h);
                c.version.hash(&mut h);
                c.has_active_stream.hash(&mut h);
            }
        }
        "commands" => {
            for d in state.messaging.all_command_details() {
                hash_message_type_detail(&d, &mut h);
            }
        }
        "queries" => {
            for d in state.messaging.all_query_details() {
                hash_message_type_detail(&d, &mut h);
            }
        }
        "subscriptions" => {
            for s in state.messaging.all_subscription_stats() {
                s.subscription_id.hash(&mut h);
                s.query_name.hash(&mut h);
                s.bus.hash(&mut h);
                s.handler_client_id.0.hash(&mut h);
            }
        }
        "processors" => {
            for view in state.processor_registry.list_aggregated() {
                view.processor_name.hash(&mut h);
                view.mode.hash(&mut h);
                for inst in &view.instances {
                    inst.running.hash(&mut h);
                    inst.error.hash(&mut h);
                    for seg in &inst.segments {
                        seg.segment_id.hash(&mut h);
                        seg.token_position.hash(&mut h);
                        seg.caught_up.hash(&mut h);
                        seg.replaying.hash(&mut h);
                    }
                }
            }
        }
        _ => {}
    }
    h.finish()
}

fn hash_message_type_detail(
    d: &kronosdb_messaging::handler::MessageTypeDetail,
    h: &mut impl Hasher,
) {
    d.name.hash(h);
    d.bus.hash(h);
    for handler in &d.handlers {
        handler.client_id.hash(h);
        handler.available_permits.hash(h);
    }
    d.metrics.dispatched.hash(h);
    d.metrics.succeeded.hash(h);
    d.metrics.failed.hash(h);
}
