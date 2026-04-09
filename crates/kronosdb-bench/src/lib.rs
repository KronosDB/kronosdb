//! Shared helpers for KronosDB benchmarks.
//!
//! Models a realistic e-commerce domain with orders, payments, and shipments.
//! Event streams are tagged by entity ID (orderId, customerId) — the standard
//! DCB pattern where queries are always "give me all events for entity X".
//!
//! Typical DCB stream sizes are 5-50 events per entity (order lifecycle,
//! user activity, etc.), not millions. The scale challenge is having millions
//! of *entities*, each with a small number of events spread across many segments.

use kronosdb_eventstore::append::AppendRequest;
use kronosdb_eventstore::criteria::{Criterion, SourcingCondition};
use kronosdb_eventstore::event::{AppendEvent, Position, Tag};
use kronosdb_eventstore::store::{EventStoreEngine, StoreOptions};
use std::path::Path;

/// Domain event types for an order lifecycle.
const ORDER_EVENT_TYPES: &[&str] = &[
    "OrderPlaced",
    "PaymentInitiated",
    "PaymentReceived",
    "InventoryReserved",
    "OrderConfirmed",
    "OrderPacked",
    "OrderShipped",
    "OrderDelivered",
];

/// Creates a store populated with realistic e-commerce events.
///
/// Each order gets a full lifecycle of 8 events, tagged with:
/// - `orderId=order-{i}` (unique per order)
/// - `customerId=cust-{i % num_customers}` (shared across orders)
///
/// This models the real DCB pattern where you source by orderId (selective,
/// 8 events) or by customerId (broader, many orders per customer).
pub fn create_ecommerce_store(
    dir: &Path,
    num_orders: usize,
    num_customers: usize,
    opts: &StoreOptions,
) -> EventStoreEngine {
    let store = EventStoreEngine::create_with_store_options(dir, opts).unwrap();
    let events_per_order = ORDER_EVENT_TYPES.len();

    // Append in batches. Interleave orders to simulate realistic arrival
    // (not all events for one order, then all for the next).
    let batch_size = 500;
    let total_events = num_orders * events_per_order;
    let mut event_idx = 0;

    // Phase 1: Place all orders (OrderPlaced events come first).
    // Phase 2-8: Subsequent lifecycle events arrive interleaved.
    for phase in 0..events_per_order {
        let mut remaining = num_orders;
        while remaining > 0 {
            let current = remaining.min(batch_size);
            let mut events = Vec::with_capacity(current);

            for _ in 0..current {
                let order_id = num_orders - remaining;
                let customer_id = order_id % num_customers;
                let event_type = ORDER_EVENT_TYPES[phase];

                events.push(AppendEvent {
                    identifier: format!("evt-{event_idx}"),
                    name: event_type.into(),
                    version: "1.0".into(),
                    timestamp: 1712345678000 + event_idx as i64,
                    payload: make_payload(event_type, order_id),
                    metadata: vec![],
                    tags: vec![
                        Tag::from_str("orderId", &format!("order-{order_id}")),
                        Tag::from_str("customerId", &format!("cust-{customer_id}")),
                    ],
                });
                event_idx += 1;
                remaining -= 1;
            }

            store
                .append(AppendRequest {
                    condition: None,
                    events,
                })
                .unwrap();
        }
    }

    assert_eq!(event_idx, total_events);
    store
}

/// Creates a multi-segment store by using a small segment size.
/// Returns the store reopened with default (large) segment options.
pub fn create_multi_segment_store(
    dir: &Path,
    num_orders: usize,
    num_customers: usize,
    events_per_segment: usize,
) -> EventStoreEngine {
    let bytes_per_event = 400u64; // conservative estimate
    let segment_size = (events_per_segment as u64 * bytes_per_event) + 1024;

    let small_opts = StoreOptions {
        max_segment_size: segment_size,
        ..Default::default()
    };

    let store = create_ecommerce_store(dir, num_orders, num_customers, &small_opts);
    let head = store.head();
    drop(store);

    let store = EventStoreEngine::open_with_store_options(dir, &StoreOptions::default()).unwrap();
    assert_eq!(store.head(), head);
    store
}

/// "Source all events for this order" — the most common DCB query.
/// Highly selective: returns ~8 events from potentially millions.
pub fn order_condition(order_id: usize) -> SourcingCondition {
    SourcingCondition {
        criteria: vec![Criterion {
            names: vec![],
            tags: vec![Tag::from_str("orderId", &format!("order-{order_id}"))],
        }],
    }
}

/// "Source all events for this customer" — broader query spanning many orders.
pub fn customer_condition(customer_id: usize) -> SourcingCondition {
    SourcingCondition {
        criteria: vec![Criterion {
            names: vec![],
            tags: vec![Tag::from_str("customerId", &format!("cust-{customer_id}"))],
        }],
    }
}

/// "Source specific event types for an order" — decision model sourcing.
/// E.g., "has this order been paid?" checks for PaymentReceived events.
pub fn order_payment_condition(order_id: usize) -> SourcingCondition {
    SourcingCondition {
        criteria: vec![Criterion {
            names: vec!["PaymentReceived".into()],
            tags: vec![Tag::from_str("orderId", &format!("order-{order_id}"))],
        }],
    }
}

/// "Source events for entity that doesn't exist" — bloom filter best case.
pub fn nonexistent_order_condition() -> SourcingCondition {
    SourcingCondition {
        criteria: vec![Criterion {
            names: vec![],
            tags: vec![Tag::from_str("orderId", "order-99999999")],
        }],
    }
}

/// Generates a realistic-ish payload for an event type.
fn make_payload(event_type: &str, order_id: usize) -> Vec<u8> {
    // Real payloads are JSON/protobuf, typically 200-2000 bytes.
    // We use ~300 bytes to model the common case.
    let json = format!(
        r#"{{"type":"{}","orderId":"order-{}","amount":4999,"currency":"SEK","items":[{{"sku":"ITEM-001","qty":2}},{{"sku":"ITEM-002","qty":1}}]}}"#,
        event_type, order_id
    );
    let mut payload = json.into_bytes();
    // Pad to ~300 bytes for consistency.
    payload.resize(300, 0);
    payload
}
