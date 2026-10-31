//! Subscription queries: an initial result, then updates until either side
//! ends it.

use std::collections::HashMap;
use std::sync::Arc;

use kronosdb_messaging::api::SubscriptionQueryDispatcher;
use kronosdb_messaging::query::QueryResult;
use kronosdb_messaging::subscription::{SubscriptionError, SubscriptionQuery, SubscriptionUpdate};
use kronosdb_messaging::types::ClientId;
use tokio::sync::{mpsc, oneshot};

use super::Hub;
use super::bus::Bus;
use super::peers::RemoteFeed;
use super::types::{Fault, FaultKind, SubscriptionEvent, ToQueryHandler};

/// A handler's event, as the update the bus accounts permits for and
/// delivers. The initial result travels apart from the updates.
pub(super) fn update_of(
    subscription_id: &str,
    event: SubscriptionEvent,
) -> Option<SubscriptionUpdate> {
    Some(match event {
        SubscriptionEvent::Initial(_) => return None,
        SubscriptionEvent::Update(update) => SubscriptionUpdate {
            subscription_id: subscription_id.to_string(),
            ..update
        },
        SubscriptionEvent::Complete => SubscriptionUpdate {
            subscription_id: subscription_id.to_string(),
            payload: None,
            metadata: HashMap::new(),
            error_code: None,
            error: None,
            complete: true,
        },
        SubscriptionEvent::Failed { error_code, error } => SubscriptionUpdate {
            subscription_id: subscription_id.to_string(),
            payload: None,
            metadata: HashMap::new(),
            error_code: Some(error_code),
            error,
            complete: true,
        },
    })
}

/// An update the bus delivers, as the event it tells the subscriber.
pub(super) fn event_of(update: SubscriptionUpdate) -> SubscriptionEvent {
    if let Some(error_code) = update.error_code {
        SubscriptionEvent::Failed {
            error_code,
            error: update.error,
        }
    } else if update.complete {
        SubscriptionEvent::Complete
    } else {
        SubscriptionEvent::Update(update)
    }
}

fn subscription_fault(error: SubscriptionError) -> Fault {
    let kind = match &error {
        SubscriptionError::NoHandlerAvailable { .. }
        | SubscriptionError::NoPermitsAvailable { .. } => FaultKind::Unavailable,
        SubscriptionError::SubscriptionNotFound { .. } => FaultKind::NotFound,
        SubscriptionError::DuplicateSubscription { .. } => FaultKind::AlreadyExists,
    };
    Fault::new(kind, error.to_string())
}

/// What a subscriber receives, wherever its handler is. Dropping it
/// cancels the subscription and tells the handler to stop.
pub struct SubscriptionFeed {
    source: Source,
}

enum Source {
    Local {
        bus: Arc<Bus>,
        id: String,
        /// The client serving the subscription.
        handler: String,
        /// The handler's initial result, until it has come (or never will).
        initial: Option<oneshot::Receiver<QueryResult>>,
        updates: mpsc::Receiver<SubscriptionUpdate>,
    },
    Remote(RemoteFeed),
}

impl SubscriptionFeed {
    /// The next event: the initial result first, then updates, then the
    /// end. `None` once the subscription is over.
    pub async fn next(&mut self) -> Option<Result<SubscriptionEvent, Fault>> {
        match &mut self.source {
            Source::Local {
                initial, updates, ..
            } => loop {
                if let Some(waiting) = initial {
                    tokio::select! {
                        // An update can only follow an initial result the
                        // handler already sent, so the initial result wins.
                        biased;
                        result = waiting => {
                            *initial = None;
                            if let Ok(result) = result {
                                return Some(Ok(SubscriptionEvent::Initial(result)));
                            }
                        }
                        update = updates.recv() => {
                            return update.map(|u| Ok(event_of(u)));
                        }
                    }
                } else {
                    return updates.recv().await.map(|u| Ok(event_of(u)));
                }
            },
            Source::Remote(feed) => feed.events.recv().await,
        }
    }

    /// The subscriber will take `permits` more updates.
    pub fn grant(&self, permits: u64) {
        match &self.source {
            Source::Local {
                bus, id, handler, ..
            } => {
                bus.engine
                    .grant_subscription_permits(id, permits.min(i64::MAX as u64) as i64);
                // The handler learns of it too, so it can hold updates
                // instead of having the bus drop them.
                bus.nudge_query_handler(
                    handler,
                    ToQueryHandler::SubscriptionPermits {
                        subscription_id: id.clone(),
                        permits,
                    },
                );
            }
            Source::Remote(feed) => {
                let _ = feed.permits.send(permits);
            }
        }
    }
}

impl Drop for SubscriptionFeed {
    fn drop(&mut self) {
        if let Source::Local { bus, id, .. } = &self.source {
            bus.initials.remove(id);
            if let Some(handler) = bus.engine.cancel_subscription(id) {
                bus.nudge_query_handler(
                    &handler.0,
                    ToQueryHandler::CancelSubscription {
                        subscription_id: id.clone(),
                    },
                );
            }
        }
        // A remote feed's relay ends with its receiver.
    }
}

impl Hub {
    /// Opens a subscription query on whichever handler the bus (or the
    /// fabric, when only another node has one) picks.
    pub async fn open_subscription(
        &self,
        bus: &Arc<Bus>,
        subscription: SubscriptionQuery,
    ) -> Result<SubscriptionFeed, Fault> {
        // A subscription binds one handler, so this is point-to-point:
        // locality-preferred, relayed when only remote handlers exist.
        let route = self.router.query(&bus.name, &subscription.query_name, true);
        if !route.local
            && let Some((node, targets)) = route.remote.into_iter().next()
        {
            let target = targets.into_iter().next().unwrap_or_default();
            let feed = self
                .peers
                .relay_subscription(node, &bus.name, target, subscription)
                .await?;
            return Ok(SubscriptionFeed {
                source: Source::Remote(feed),
            });
        }
        self.open_local_subscription(bus, subscription, None).await
    }

    /// A subscription another node chose this node's `target` handler for.
    pub async fn open_relayed_subscription(
        &self,
        bus: &Arc<Bus>,
        target: ClientId,
        subscription: SubscriptionQuery,
    ) -> Result<SubscriptionFeed, Fault> {
        self.open_local_subscription(bus, subscription, Some(target))
            .await
    }

    async fn open_local_subscription(
        &self,
        bus: &Arc<Bus>,
        subscription: SubscriptionQuery,
        target: Option<ClientId>,
    ) -> Result<SubscriptionFeed, Fault> {
        let id = subscription.subscription_id.clone();
        let handler_view = Arc::new(subscription.clone());
        let opened = match &target {
            Some(target) => bus.engine.subscribe_to(subscription, target),
            None => bus.engine.subscribe(subscription),
        };
        let (pending, updates) = opened.map_err(subscription_fault)?;
        let handler = pending
            .target_handlers
            .first()
            .map(|h| h.0.clone())
            .unwrap_or_default();
        let (initial_tx, initial_rx) = oneshot::channel();
        bus.initials.insert(id.clone(), initial_tx);
        // From here the feed owns the subscription: dropping it, on any
        // path below, cancels it.
        let feed = SubscriptionFeed {
            source: Source::Local {
                bus: Arc::clone(bus),
                id,
                handler: handler.clone(),
                initial: Some(initial_rx),
                updates,
            },
        };
        let delivered = bus
            .to_query_handler(&handler, ToQueryHandler::Subscription(handler_view))
            .await;
        if delivered.is_err() {
            return Err(Fault::unavailable(format!(
                "handler '{handler}' has no stream on this node"
            )));
        }
        Ok(feed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kronosdb_messaging::types::{ErrorDetail, Payload};

    fn payload(data: &'static [u8]) -> Payload {
        Payload {
            payload_type: "String".into(),
            revision: "1".into(),
            data: bytes::Bytes::from_static(data),
        }
    }

    fn update(complete: bool, error_code: Option<&str>) -> SubscriptionUpdate {
        SubscriptionUpdate {
            subscription_id: "s1".into(),
            payload: Some(payload(b"u")),
            metadata: HashMap::new(),
            error_code: error_code.map(String::from),
            error: error_code.map(|c| ErrorDetail {
                message: "boom".into(),
                location: "handler".into(),
                details: vec![],
                error_code: c.into(),
            }),
            complete,
        }
    }

    #[test]
    fn events_become_the_updates_the_bus_accounts_for() {
        assert!(
            update_of(
                "s1",
                SubscriptionEvent::Initial(QueryResult {
                    message_id: String::new(),
                    request_id: "s1".into(),
                    error_code: None,
                    error: None,
                    payload: None,
                    metadata: HashMap::new(),
                    processing_instructions: vec![],
                })
            )
            .is_none(),
            "the initial result is not an update"
        );
        let u = update_of("s1", SubscriptionEvent::Complete).unwrap();
        assert!(u.complete && u.error_code.is_none());
        let u = update_of(
            "s1",
            SubscriptionEvent::Failed {
                error_code: "E1".into(),
                error: None,
            },
        )
        .unwrap();
        assert!(u.complete && u.error_code.as_deref() == Some("E1"));
        // An update is stamped with the subscription it arrived for.
        let u = update_of("other", SubscriptionEvent::Update(update(false, None))).unwrap();
        assert_eq!(u.subscription_id, "other");
        assert!(!u.complete);
    }

    #[test]
    fn updates_become_the_events_subscribers_see() {
        assert!(matches!(
            event_of(update(false, None)),
            SubscriptionEvent::Update(_)
        ));
        assert!(matches!(
            event_of(update(true, None)),
            SubscriptionEvent::Complete
        ));
        // An error ends the subscription even when the flag is unset.
        assert!(matches!(
            event_of(update(false, Some("E1"))),
            SubscriptionEvent::Failed { .. }
        ));
        assert!(event_of(update(true, Some("E1"))).is_terminal());
    }
}
