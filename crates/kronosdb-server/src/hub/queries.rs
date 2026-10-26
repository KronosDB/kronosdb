//! Dispatching a query to its handlers and gathering what they answer.

use std::sync::Arc;
use std::time::Duration;

use kronosdb_messaging::query::{PendingQuery, Query, QueryError, QueryResult};
use kronosdb_messaging::types::ClientId;
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tokio::time::Instant;

use super::Hub;
use super::bus::{Bus, Collector};
use super::peers::QueryLeg;
use super::routing::{QueryFanout, effective_timeout};
use super::types::{Fault, FaultKind, ToQueryHandler};

/// How many results may wait for the dispatcher to take them.
const RESULT_QUEUE: usize = 64;

/// Why the bus would not take a query, as a fault.
pub(super) fn query_fault(error: QueryError) -> Fault {
    Fault::new(FaultKind::Unavailable, error.to_string())
}

/// The results of a query as the handlers produce them. Ends when every
/// handler is done, or at the deadline. Dropping it gives the query up:
/// handlers still working on it are told to stop.
pub struct QueryStream {
    results: mpsc::Receiver<Result<QueryResult, Fault>>,
    deadline: Instant,
    /// Whether a local handler's silence at the deadline is an error. (A
    /// remote leg that runs out of time just ends.)
    timeout_is_error: bool,
    finished: bool,
    /// Dropped with the stream, which gives the query up if it is unfinished.
    _giving_up: Option<GiveUp>,
}

/// What dropping an unfinished query does.
struct GiveUp {
    bus: Arc<Bus>,
    request_id: String,
}

impl Drop for GiveUp {
    fn drop(&mut self) {
        // Still filed means some handler has not finished: it is told to
        // stop, and the entry goes.
        if let Some((_, collector)) = self.bus.collectors.remove(&self.request_id) {
            for handler in collector.outstanding.lock().drain(..) {
                self.bus.nudge_query_handler(
                    &handler,
                    ToQueryHandler::CancelQuery {
                        request_id: self.request_id.clone(),
                    },
                );
            }
        }
    }
}

impl QueryStream {
    /// The next result. `None` once every handler is done; an error at the
    /// deadline when a local handler never finished.
    pub async fn next(&mut self) -> Option<Result<QueryResult, Fault>> {
        if self.finished {
            return None;
        }
        match tokio::time::timeout_at(self.deadline, self.results.recv()).await {
            Ok(Some(item)) => Some(item),
            Ok(None) => {
                self.finished = true;
                None
            }
            Err(_) => {
                self.finished = true;
                self.timeout_is_error
                    .then(|| Err(Fault::timeout("query response timed out")))
            }
        }
    }
}

impl Hub {
    /// Sends a query to its handlers — here, and on other nodes that have
    /// some — and returns the stream their results arrive on.
    pub async fn dispatch_query(&self, bus: &Arc<Bus>, query: Query) -> Result<QueryStream, Fault> {
        let timeout = effective_timeout(&query.processing_instructions, self.config.query_timeout);
        // The plan from the replicated routing table: scatter-gather spans
        // every node with registered handlers; point-to-point prefers this
        // node.
        let point_to_point = query.expected_results == 1;
        let fanout = self.router.query(&bus.name, &query.name, point_to_point);
        tracing::debug!(
            query = %query.name,
            local = fanout.local,
            remote_nodes = fanout.remote.len(),
            "query fanout"
        );
        let deadline = Instant::now() + timeout;
        let request_id = query.message_id.clone();
        let (results, rx) = mpsc::channel(RESULT_QUEUE);
        let QueryFanout { local, remote } = fanout;
        let remote_only = !local && !remote.is_empty();
        let has_remote = !remote.is_empty();

        // The remote legs start before the local dispatch so a local permit
        // wait cannot delay them.
        for (node, targets) in remote {
            let peers = Arc::clone(&self.peers);
            let sink = results.clone();
            let bus_name = Arc::clone(&bus.name);
            let query = query.clone();
            tokio::spawn(async move {
                peers
                    .forward_query(
                        QueryLeg {
                            node,
                            bus: &bus_name,
                            target_client_ids: targets,
                            query,
                            timeout,
                            report_failure: point_to_point,
                        },
                        sink,
                    )
                    .await;
            });
        }

        // A bounded wait for a flow-control grant when no local handler can
        // accept, as for commands. A local failure is tolerated when remote
        // targets exist (scatter-gather is best-effort per handler), and a
        // remote-only route skips the local bus entirely.
        let mut local_active = false;
        if !remote_only {
            let wait = if has_remote { Duration::ZERO } else { timeout };
            let name = query.name.clone();
            match bus.engine.query_bus().dispatch_wait(query, wait).await {
                Ok(pending) => {
                    self.deliver_query(bus, pending, results.clone()).await;
                    local_active = true;
                }
                Err(e) if has_remote => {
                    tracing::debug!(query = %name, error = %e, "local query dispatch skipped");
                }
                Err(e) => return Err(query_fault(e)),
            }
        }
        Ok(QueryStream {
            results: rx,
            deadline,
            timeout_is_error: local_active,
            finished: false,
            _giving_up: local_active.then(|| GiveUp {
                bus: Arc::clone(bus),
                request_id,
            }),
        })
    }

    /// A query another node chose this node's `targets` for. Their results
    /// end the stream when they are all in, or at `budget`.
    pub async fn dispatch_forwarded_query(
        &self,
        bus: &Arc<Bus>,
        targets: Vec<ClientId>,
        query: Query,
        budget: Duration,
    ) -> Result<QueryStream, Fault> {
        let request_id = query.message_id.clone();
        let pending = bus
            .engine
            .query_bus()
            .dispatch_to_wait(query, &targets, budget)
            .await
            .map_err(query_fault)?;
        let (results, rx) = mpsc::channel(RESULT_QUEUE);
        self.deliver_query(bus, pending, results).await;
        Ok(QueryStream {
            results: rx,
            deadline: Instant::now() + budget,
            timeout_is_error: false,
            finished: false,
            _giving_up: Some(GiveUp {
                bus: Arc::clone(bus),
                request_id,
            }),
        })
    }

    /// Files the collector for a dispatched query, then hands the query to
    /// each local handler it was assigned. The collector goes first so no
    /// answer can race past it.
    async fn deliver_query(
        &self,
        bus: &Bus,
        pending: PendingQuery,
        results: mpsc::Sender<Result<QueryResult, Fault>>,
    ) {
        let request_id = pending.query.message_id.clone();
        let targets: Vec<String> = pending.target_handlers.into_iter().map(|h| h.0).collect();
        bus.collectors.insert(
            request_id.clone(),
            Arc::new(Collector {
                results,
                outstanding: Mutex::new(targets.clone()),
            }),
        );
        let query = Arc::new(pending.query);
        for target in targets {
            let sent = bus
                .to_query_handler(&target, ToQueryHandler::Query(Arc::clone(&query)))
                .await;
            // A handler with no stream here will never answer.
            if sent.is_err() {
                bus.handler_done(&target, &request_id);
            }
        }
    }
}
