//! The vocabulary of the hub: what it says to handlers and clients, and how
//! it reports a failure. Plain data, no I/O, nothing about any wire.

use std::fmt;
use std::sync::Arc;

use kronosdb_eventstore::raft::handler_registry::HandlerKind;
use kronosdb_messaging::command::Command;
use kronosdb_messaging::query::{Query, QueryResult};
use kronosdb_messaging::subscription::{SubscriptionQuery, SubscriptionUpdate};
use kronosdb_messaging::types::ErrorDetail;

/// Why the hub refused or lost a request. Adapters turn this into whatever
/// their wire reports failures with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    /// Not ready, no handler, nowhere to send it. Retrying may help.
    Unavailable,
    /// The server's state forbids this otherwise valid request.
    Rejected,
    InvalidArgument,
    NotFound,
    AlreadyExists,
    /// A capacity limit.
    ResourceExhausted,
    /// The request's own deadline passed.
    Timeout,
    Internal,
}

#[derive(Debug, Clone)]
pub struct Fault {
    pub kind: FaultKind,
    pub message: String,
}

impl Fault {
    pub fn new(kind: FaultKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::new(FaultKind::Unavailable, message)
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(FaultKind::InvalidArgument, message)
    }

    pub fn timeout(message: impl Into<String>) -> Self {
        Self::new(FaultKind::Timeout, message)
    }
}

impl fmt::Display for Fault {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for Fault {}

/// Who a handler stream or platform stream speaks for.
#[derive(Debug, Clone)]
pub struct Who {
    pub client_id: String,
    pub component_name: String,
}

/// What the hub sends down a command handler's stream.
#[derive(Debug)]
pub enum ToCommandHandler {
    /// A command to answer.
    Command(Command),
}

/// What the hub sends down a query handler's stream.
#[derive(Debug)]
pub enum ToQueryHandler {
    /// A query to answer. Shared: a scatter-gather query goes to every
    /// handler without being copied for each.
    Query(Arc<Query>),
    /// The sender gave up on this query.
    CancelQuery { request_id: String },
    /// A subscription query to answer with an initial result and updates.
    Subscription(Arc<SubscriptionQuery>),
    /// The subscriber granted more update credits.
    SubscriptionPermits {
        subscription_id: String,
        permits: u64,
    },
    /// The subscriber went away.
    CancelSubscription { subscription_id: String },
}

/// How a subscription query progresses, from the handler to the subscriber.
#[derive(Debug, Clone)]
pub enum SubscriptionEvent {
    Initial(QueryResult),
    Update(SubscriptionUpdate),
    Complete,
    Failed {
        error_code: String,
        error: Option<ErrorDetail>,
    },
}

impl SubscriptionEvent {
    /// Whether nothing follows this event.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Complete | Self::Failed { .. })
    }
}

/// What the hub sends a platform client.
#[derive(Debug, Clone)]
pub enum ToClient {
    Node(NodeInfo),
    Topology(TopologyChange),
    Instruction(ProcessorInstruction),
    /// The server is going away; reconnect elsewhere.
    ReconnectRequested,
}

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node_name: String,
    pub version: String,
    pub contexts: Vec<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopologyEvent {
    HandlerRegistered,
    HandlerDeregistered,
}

/// A handler registration appeared or went away.
#[derive(Debug, Clone)]
pub struct TopologyChange {
    pub event: TopologyEvent,
    /// Empty when a whole client's handlers went.
    pub message_type: String,
    pub kind: HandlerKind,
    pub client_id: String,
    pub component_name: String,
}

/// The protocol's processor actions. The admin console sends pause, start,
/// split and merge; the others wait for an operator surface that uses them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub enum ProcessorAction {
    Pause,
    Start,
    ReleaseSegment,
    ReportStatus,
    SplitSegment,
    MergeSegment,
}

/// What an operator asks of a processor.
#[derive(Debug, Clone)]
pub struct ProcessorInstruction {
    pub instruction_id: String,
    pub action: ProcessorAction,
    pub processor_name: String,
    /// The segment the action applies to; 0 when it applies to the processor.
    pub segment: u32,
}

/// Who a platform client says it is.
#[derive(Debug, Clone)]
pub struct ClientIdentity {
    pub client_id: String,
    pub component_name: String,
    pub version: String,
}

/// A client's report on one event processor.
#[derive(Debug, Clone)]
pub struct ProcessorReport {
    pub processor_name: String,
    pub mode: String,
    pub running: bool,
    pub error: bool,
    pub streaming: bool,
    pub token_store_identifier: String,
    pub segments: Vec<SegmentReport>,
}

#[derive(Debug, Clone)]
pub struct SegmentReport {
    pub segment_id: u32,
    pub caught_up: bool,
    pub replaying: bool,
    pub one_part_of: u32,
    pub token_position: i64,
    pub error_state: String,
}
