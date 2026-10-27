//! Messaging frames to hub types and back. Plain functions, no I/O.
//!
//! Coming in, a frame's payload bytes are views into the buffer it arrived
//! in, so a command passes through the hub without its payload being
//! copied. Going out, the frame borrows from the hub's value.

use bytes::Bytes;
use kronosdb_messaging::command::{Command, CommandResult};
use kronosdb_messaging::query::{Query, QueryResult};
use kronosdb_messaging::subscription::{SubscriptionQuery, SubscriptionUpdate};
use kronosdb_messaging::types::{
    ClientId, ComponentName, ErrorDetail, Metadata, MetadataValue, Payload, ProcessingInstruction,
    ProcessingKey,
};
use kronosdb_protocol as p;
use kronosdb_protocol::ErrorCode;
use kronosdb_protocol::net::Failure;

use crate::hub::{
    Fault, FaultKind, ProcessorAction, SubscriptionEvent, expected_results, routing_key_of,
};

/// The buffer a frame arrived in, to slice payloads out of.
#[derive(Clone, Copy)]
pub struct Source<'a>(pub &'a Bytes);

impl Source<'_> {
    fn bytes(&self, data: &[u8]) -> Bytes {
        self.0.slice_ref(data)
    }
}

// ── Frames to hub types ────────────────────────────────────────────────

pub fn payload(src: Source<'_>, p: &p::Payload<'_>) -> Payload {
    Payload {
        payload_type: p.type_name.to_string(),
        revision: p.revision.to_string(),
        data: src.bytes(p.data),
    }
}

fn metadata_value(src: Source<'_>, v: &p::MetadataValue<'_>) -> MetadataValue {
    match v {
        p::MetadataValue::Text(s) => MetadataValue::Text(s.to_string()),
        p::MetadataValue::Number(n) => MetadataValue::Number(*n),
        p::MetadataValue::Boolean(b) => MetadataValue::Boolean(*b),
        p::MetadataValue::Double(d) => MetadataValue::Double(*d),
        p::MetadataValue::Object(o) => MetadataValue::Bytes(payload(src, o)),
    }
}

fn metadata(src: Source<'_>, entries: &[p::MessageMetadata<'_>]) -> Metadata {
    entries
        .iter()
        .map(|m| (m.key.to_string(), metadata_value(src, &m.value)))
        .collect()
}

fn instructions(
    src: Source<'_>,
    instructions: &[p::ProcessingInstruction<'_>],
) -> Vec<ProcessingInstruction> {
    instructions
        .iter()
        .map(|pi| ProcessingInstruction {
            key: match pi.key {
                p::ProcessingKey::PRIORITY => ProcessingKey::Priority,
                p::ProcessingKey::TIMEOUT => ProcessingKey::Timeout,
                p::ProcessingKey::NR_OF_RESULTS => ProcessingKey::NrOfResults,
                // Routing key, and any key this version does not know.
                _ => ProcessingKey::RoutingKey,
            },
            value: pi.value.as_ref().map(|v| metadata_value(src, v)),
        })
        .collect()
}

fn error_detail(e: &p::ErrorDetail<'_>) -> ErrorDetail {
    ErrorDetail {
        message: e.message.to_string(),
        location: e.location.to_string(),
        details: e.details.iter().map(|d| d.to_string()).collect(),
        error_code: e.error_code.to_string(),
    }
}

/// An empty error code means success.
fn error_code(code: &str) -> Option<String> {
    (!code.is_empty()).then(|| code.to_string())
}

pub fn command(src: Source<'_>, c: &p::Command<'_>) -> Command {
    let processing_instructions = instructions(src, &c.instructions);
    Command {
        message_id: c.message_id.to_string(),
        name: c.name.to_string(),
        timestamp: c.timestamp,
        payload: payload(src, &c.payload),
        metadata: metadata(src, &c.metadata),
        routing_key: routing_key_of(&processing_instructions),
        processing_instructions,
        client_id: ClientId(c.client_id.to_string()),
        component_name: ComponentName(c.component_name.to_string()),
    }
}

pub fn command_result(src: Source<'_>, r: &p::CommandResult<'_>) -> CommandResult {
    CommandResult {
        message_id: r.message_id.to_string(),
        request_id: r.request_id.to_string(),
        error_code: error_code(r.error_code),
        error: r.error.as_ref().map(error_detail),
        payload: r.payload.as_ref().map(|p| payload(src, p)),
        metadata: metadata(src, &r.metadata),
        processing_instructions: instructions(src, &r.instructions),
    }
}

pub fn query(src: Source<'_>, q: &p::Query<'_>) -> Query {
    let processing_instructions = instructions(src, &q.instructions);
    Query {
        message_id: q.message_id.to_string(),
        name: q.name.to_string(),
        timestamp: q.timestamp,
        payload: payload(src, &q.payload),
        metadata: metadata(src, &q.metadata),
        client_id: ClientId(q.client_id.to_string()),
        component_name: ComponentName(q.component_name.to_string()),
        // NrOfResults of 1 selects point-to-point routing.
        expected_results: expected_results(&processing_instructions),
        processing_instructions,
    }
}

pub fn query_result(src: Source<'_>, r: &p::QueryResult<'_>) -> QueryResult {
    QueryResult {
        message_id: r.message_id.to_string(),
        request_id: r.request_id.to_string(),
        error_code: error_code(r.error_code),
        error: r.error.as_ref().map(error_detail),
        payload: r.payload.as_ref().map(|p| payload(src, p)),
        metadata: metadata(src, &r.metadata),
        processing_instructions: instructions(src, &r.instructions),
    }
}

pub fn subscription(src: Source<'_>, s: &p::SubscriptionQuery<'_>) -> SubscriptionQuery {
    SubscriptionQuery {
        subscription_id: s.subscription_id.to_string(),
        query_name: s.query.name.to_string(),
        timestamp: s.query.timestamp,
        payload: payload(src, &s.query.payload),
        metadata: metadata(src, &s.query.metadata),
        client_id: ClientId(s.query.client_id.to_string()),
        component_name: ComponentName(s.query.component_name.to_string()),
        initial_permits: s.permits.min(i64::MAX as u64) as i64,
    }
}

pub fn subscription_event(
    src: Source<'_>,
    subscription_id: &str,
    e: &p::SubscriptionEvent<'_>,
) -> SubscriptionEvent {
    match e {
        p::SubscriptionEvent::InitialResult(r) => SubscriptionEvent::Initial(query_result(src, r)),
        p::SubscriptionEvent::Update(u) => SubscriptionEvent::Update(SubscriptionUpdate {
            subscription_id: subscription_id.to_string(),
            payload: u.payload.as_ref().map(|p| payload(src, p)),
            metadata: metadata(src, &u.metadata),
            error_code: error_code(u.error_code),
            error: u.error.as_ref().map(error_detail),
            complete: false,
        }),
        p::SubscriptionEvent::Complete(_) => SubscriptionEvent::Complete,
        p::SubscriptionEvent::CompleteExceptionally(f) => SubscriptionEvent::Failed {
            error_code: f.error_code.to_string(),
            error: f.error.as_ref().map(error_detail),
        },
    }
}

// ── Hub types to frames ────────────────────────────────────────────────

pub fn wire_payload(p: &Payload) -> p::Payload<'_> {
    p::Payload {
        type_name: &p.payload_type,
        revision: &p.revision,
        data: &p.data,
    }
}

fn wire_metadata_value(v: &MetadataValue) -> p::MetadataValue<'_> {
    match v {
        MetadataValue::Text(s) => p::MetadataValue::Text(s),
        MetadataValue::Number(n) => p::MetadataValue::Number(*n),
        MetadataValue::Boolean(b) => p::MetadataValue::Boolean(*b),
        MetadataValue::Double(d) => p::MetadataValue::Double(*d),
        MetadataValue::Bytes(o) => p::MetadataValue::Object(wire_payload(o)),
    }
}

fn wire_metadata(m: &Metadata) -> Vec<p::MessageMetadata<'_>> {
    m.iter()
        .map(|(k, v)| p::MessageMetadata {
            key: k,
            value: wire_metadata_value(v),
        })
        .collect()
}

fn wire_instructions(list: &[ProcessingInstruction]) -> Vec<p::ProcessingInstruction<'_>> {
    list.iter()
        .map(|pi| p::ProcessingInstruction {
            key: match pi.key {
                ProcessingKey::RoutingKey => p::ProcessingKey::ROUTING_KEY,
                ProcessingKey::Priority => p::ProcessingKey::PRIORITY,
                ProcessingKey::Timeout => p::ProcessingKey::TIMEOUT,
                ProcessingKey::NrOfResults => p::ProcessingKey::NR_OF_RESULTS,
            },
            value: pi.value.as_ref().map(wire_metadata_value),
        })
        .collect()
}

pub fn wire_error(e: &ErrorDetail) -> p::ErrorDetail<'_> {
    p::ErrorDetail {
        message: &e.message,
        location: &e.location,
        details: e.details.iter().map(String::as_str).collect(),
        error_code: &e.error_code,
    }
}

pub fn wire_command(c: &Command) -> p::Command<'_> {
    p::Command {
        message_id: &c.message_id,
        name: &c.name,
        timestamp: c.timestamp,
        payload: wire_payload(&c.payload),
        metadata: wire_metadata(&c.metadata),
        instructions: wire_instructions(&c.processing_instructions),
        client_id: &c.client_id.0,
        component_name: &c.component_name.0,
    }
}

pub fn wire_command_result(r: &CommandResult) -> p::CommandResult<'_> {
    p::CommandResult {
        message_id: &r.message_id,
        request_id: &r.request_id,
        error_code: r.error_code.as_deref().unwrap_or(""),
        error: r.error.as_ref().map(wire_error),
        payload: r.payload.as_ref().map(wire_payload),
        metadata: wire_metadata(&r.metadata),
        instructions: wire_instructions(&r.processing_instructions),
    }
}

pub fn wire_query(q: &Query) -> p::Query<'_> {
    p::Query {
        message_id: &q.message_id,
        name: &q.name,
        timestamp: q.timestamp,
        payload: wire_payload(&q.payload),
        metadata: wire_metadata(&q.metadata),
        instructions: wire_instructions(&q.processing_instructions),
        client_id: &q.client_id.0,
        component_name: &q.component_name.0,
    }
}

pub fn wire_query_result(r: &QueryResult) -> p::QueryResult<'_> {
    p::QueryResult {
        message_id: &r.message_id,
        request_id: &r.request_id,
        error_code: r.error_code.as_deref().unwrap_or(""),
        error: r.error.as_ref().map(wire_error),
        payload: r.payload.as_ref().map(wire_payload),
        metadata: wire_metadata(&r.metadata),
        instructions: wire_instructions(&r.processing_instructions),
    }
}

/// A subscription as its handler receives it: the query's message id is the
/// subscription id.
pub fn wire_subscription(s: &SubscriptionQuery) -> p::SubscriptionQuery<'_> {
    p::SubscriptionQuery {
        subscription_id: &s.subscription_id,
        query: p::Query {
            message_id: &s.subscription_id,
            name: &s.query_name,
            timestamp: s.timestamp,
            payload: wire_payload(&s.payload),
            metadata: wire_metadata(&s.metadata),
            instructions: vec![],
            client_id: &s.client_id.0,
            component_name: &s.component_name.0,
        },
        permits: s.initial_permits.max(0) as u64,
    }
}

pub fn wire_subscription_event(e: &SubscriptionEvent) -> p::SubscriptionEvent<'_> {
    match e {
        SubscriptionEvent::Initial(r) => p::SubscriptionEvent::InitialResult(wire_query_result(r)),
        SubscriptionEvent::Update(u) => p::SubscriptionEvent::Update(p::QueryUpdate {
            message_id: &u.subscription_id,
            payload: u.payload.as_ref().map(wire_payload),
            metadata: wire_metadata(&u.metadata),
            client_id: "",
            component_name: "",
            error_code: u.error_code.as_deref().unwrap_or(""),
            error: u.error.as_ref().map(wire_error),
        }),
        SubscriptionEvent::Complete => p::SubscriptionEvent::Complete(p::SubscriptionEnd {
            client_id: "",
            component_name: "",
        }),
        SubscriptionEvent::Failed { error_code, error } => {
            p::SubscriptionEvent::CompleteExceptionally(p::SubscriptionFailure {
                client_id: "",
                component_name: "",
                error_code,
                error: error.as_ref().map(wire_error),
            })
        }
    }
}

pub fn wire_action(a: ProcessorAction) -> p::ProcessorAction {
    match a {
        ProcessorAction::Pause => p::ProcessorAction::PAUSE,
        ProcessorAction::Start => p::ProcessorAction::START,
        ProcessorAction::ReleaseSegment => p::ProcessorAction::RELEASE_SEGMENT,
        ProcessorAction::ReportStatus => p::ProcessorAction::REPORT_STATUS,
        ProcessorAction::SplitSegment => p::ProcessorAction::SPLIT_SEGMENT,
        ProcessorAction::MergeSegment => p::ProcessorAction::MERGE_SEGMENT,
    }
}

// ── Failures ───────────────────────────────────────────────────────────

/// How a hub fault is reported on the wire.
pub fn failure(fault: Fault) -> Failure {
    let code = match fault.kind {
        // The protocol has no deadline code; the message says which it was.
        FaultKind::Unavailable | FaultKind::Timeout => ErrorCode::UNAVAILABLE,
        FaultKind::Rejected => ErrorCode::REJECTED,
        FaultKind::InvalidArgument => ErrorCode::INVALID_ARGUMENT,
        FaultKind::NotFound => ErrorCode::NOT_FOUND,
        FaultKind::AlreadyExists => ErrorCode::ALREADY_EXISTS,
        FaultKind::ResourceExhausted => ErrorCode::RESOURCE_EXHAUSTED,
        FaultKind::Internal => ErrorCode::INTERNAL,
    };
    Failure::new(code, fault.message)
}

/// The fault a failure on the wire stands for, when a peer reports one.
pub fn fault(failure: &Failure) -> Fault {
    let kind = match failure.code {
        ErrorCode::UNAVAILABLE | ErrorCode::CANCELLED => FaultKind::Unavailable,
        ErrorCode::REJECTED => FaultKind::Rejected,
        ErrorCode::INVALID_ARGUMENT => FaultKind::InvalidArgument,
        ErrorCode::NOT_FOUND => FaultKind::NotFound,
        ErrorCode::ALREADY_EXISTS => FaultKind::AlreadyExists,
        ErrorCode::RESOURCE_EXHAUSTED => FaultKind::ResourceExhausted,
        _ => FaultKind::Internal,
    };
    Fault::new(kind, failure.message.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kronosdb_protocol::net::{OwnedFrame, encode};
    use std::collections::HashMap;

    /// Sends a frame through the codec the way a connection would, so the
    /// payload slices are real views into a received buffer.
    fn received(frame: &p::Frame<'_>) -> OwnedFrame {
        let bytes = encode(1, 0, frame).unwrap();
        OwnedFrame::from_bytes(bytes).unwrap()
    }

    fn sample_command() -> Command {
        let mut metadata = Metadata::new();
        metadata.insert("trace".into(), MetadataValue::Text("t-1".into()));
        metadata.insert("n".into(), MetadataValue::Number(-7));
        metadata.insert("ok".into(), MetadataValue::Boolean(true));
        metadata.insert("ratio".into(), MetadataValue::Double(0.5));
        metadata.insert(
            "blob".into(),
            MetadataValue::Bytes(Payload {
                payload_type: "Blob".into(),
                revision: "2".into(),
                data: Bytes::from_static(b"\x00\x01"),
            }),
        );
        Command {
            message_id: "cmd-1".into(),
            name: "PlaceOrder".into(),
            timestamp: 1_761_300_000_000,
            payload: Payload {
                payload_type: "PlaceOrder".into(),
                revision: "1".into(),
                data: Bytes::from_static(b"{\"id\":1}"),
            },
            metadata,
            processing_instructions: vec![
                ProcessingInstruction {
                    key: ProcessingKey::RoutingKey,
                    value: Some(MetadataValue::Text("order-1".into())),
                },
                ProcessingInstruction {
                    key: ProcessingKey::Timeout,
                    value: Some(MetadataValue::Number(5000)),
                },
            ],
            routing_key: None,
            client_id: ClientId("caller".into()),
            component_name: ComponentName("shop".into()),
        }
    }

    #[test]
    fn a_command_survives_the_wire() {
        let sent = sample_command();
        let frame = received(&p::Frame::HandleCommand(p::HandleCommand {
            command: wire_command(&sent),
        }));
        let p::Frame::HandleCommand(h) = frame.decode().unwrap() else {
            panic!("not a HandleCommand");
        };
        let got = command(Source(frame.bytes()), &h.command);
        assert_eq!(got.message_id, "cmd-1");
        assert_eq!(got.name, "PlaceOrder");
        assert_eq!(got.timestamp, sent.timestamp);
        assert_eq!(&got.payload.data[..], b"{\"id\":1}");
        assert_eq!(got.client_id.0, "caller");
        assert_eq!(got.component_name.0, "shop");
        // The routing key is lifted out of the instructions.
        assert_eq!(got.routing_key.unwrap().0, "order-1");
        assert_eq!(got.processing_instructions.len(), 2);
        assert_eq!(got.metadata.len(), 5);
        assert!(matches!(got.metadata["n"], MetadataValue::Number(-7)));
        assert!(
            matches!(&got.metadata["blob"], MetadataValue::Bytes(b) if &b.data[..] == b"\x00\x01")
        );
    }

    #[test]
    fn payloads_are_views_into_the_received_frame() {
        let sent = sample_command();
        let frame = received(&p::Frame::DispatchCommand(p::DispatchCommand {
            bus: 0,
            command: wire_command(&sent),
        }));
        let p::Frame::DispatchCommand(d) = frame.decode().unwrap() else {
            panic!();
        };
        let got = command(Source(frame.bytes()), &d.command);
        let frame_range = frame.bytes().as_ptr_range();
        assert!(frame_range.contains(&got.payload.data.as_ptr()), "no copy");
    }

    #[test]
    fn results_keep_their_error_code_and_detail() {
        let failed = CommandResult {
            message_id: "m".into(),
            request_id: "cmd-1".into(),
            error_code: Some("KRONOSDB-4006".into()),
            error: Some(ErrorDetail {
                message: "gone".into(),
                location: "node".into(),
                details: vec!["a".into(), "b".into()],
                error_code: "KRONOSDB-4006".into(),
            }),
            payload: None,
            metadata: Metadata::new(),
            processing_instructions: vec![],
        };
        let frame = received(&p::Frame::CommandResponse(p::CommandResponse {
            result: wire_command_result(&failed),
        }));
        let p::Frame::CommandResponse(r) = frame.decode().unwrap() else {
            panic!();
        };
        let got = command_result(Source(frame.bytes()), &r.result);
        assert_eq!(got.error_code.as_deref(), Some("KRONOSDB-4006"));
        assert_eq!(got.error.unwrap().details, vec!["a", "b"]);
        assert!(got.payload.is_none());

        // Success carries an empty code, which reads back as none.
        let ok = CommandResult {
            error_code: None,
            error: None,
            ..failed
        };
        let frame = received(&p::Frame::CommandResponse(p::CommandResponse {
            result: wire_command_result(&ok),
        }));
        let p::Frame::CommandResponse(r) = frame.decode().unwrap() else {
            panic!();
        };
        assert!(
            command_result(Source(frame.bytes()), &r.result)
                .error_code
                .is_none()
        );
    }

    #[test]
    fn a_query_asking_for_one_result_is_point_to_point() {
        let mut q = sample_command();
        q.processing_instructions = vec![ProcessingInstruction {
            key: ProcessingKey::NrOfResults,
            value: Some(MetadataValue::Number(1)),
        }];
        let query_out = Query {
            message_id: q.message_id,
            name: "GetOrder".into(),
            timestamp: 0,
            payload: q.payload,
            metadata: Metadata::new(),
            processing_instructions: q.processing_instructions,
            client_id: q.client_id,
            component_name: q.component_name,
            expected_results: 1,
        };
        let frame = received(&p::Frame::HandleQuery(p::HandleQuery {
            query: wire_query(&query_out),
        }));
        let p::Frame::HandleQuery(h) = frame.decode().unwrap() else {
            panic!();
        };
        assert_eq!(query(Source(frame.bytes()), &h.query).expected_results, 1);
    }

    #[test]
    fn subscription_events_round_trip() {
        let events = [
            SubscriptionEvent::Complete,
            SubscriptionEvent::Failed {
                error_code: "E".into(),
                error: None,
            },
            SubscriptionEvent::Update(SubscriptionUpdate {
                subscription_id: "s1".into(),
                payload: Some(Payload {
                    payload_type: "T".into(),
                    revision: "1".into(),
                    data: Bytes::from_static(b"x"),
                }),
                metadata: HashMap::new(),
                error_code: None,
                error: None,
                complete: false,
            }),
        ];
        for sent in events {
            let frame = received(&p::Frame::SubscriptionQueryEvent(
                p::SubscriptionQueryEvent {
                    subscription_id: "s1",
                    event: wire_subscription_event(&sent),
                },
            ));
            let p::Frame::SubscriptionQueryEvent(e) = frame.decode().unwrap() else {
                panic!();
            };
            let got = subscription_event(Source(frame.bytes()), e.subscription_id, &e.event);
            match (&sent, &got) {
                (SubscriptionEvent::Complete, SubscriptionEvent::Complete) => {}
                (
                    SubscriptionEvent::Failed { error_code: a, .. },
                    SubscriptionEvent::Failed { error_code: b, .. },
                ) => assert_eq!(a, b),
                (SubscriptionEvent::Update(a), SubscriptionEvent::Update(b)) => {
                    assert_eq!(a.subscription_id, b.subscription_id);
                    assert_eq!(
                        a.payload.as_ref().unwrap().data,
                        b.payload.as_ref().unwrap().data
                    );
                }
                other => panic!("{other:?}"),
            }
        }
    }

    #[test]
    fn a_subscription_reaches_its_handler_with_its_id_as_the_message_id() {
        let sent = SubscriptionQuery {
            subscription_id: "sub-1".into(),
            query_name: "Count".into(),
            timestamp: 9,
            payload: Payload {
                payload_type: "Count".into(),
                revision: "1".into(),
                data: Bytes::from_static(b"q"),
            },
            metadata: Metadata::new(),
            client_id: ClientId("sub".into()),
            component_name: ComponentName("ui".into()),
            initial_permits: 16,
        };
        let frame = received(&p::Frame::HandleSubscriptionQuery(
            p::HandleSubscriptionQuery {
                subscription: wire_subscription(&sent),
            },
        ));
        let p::Frame::HandleSubscriptionQuery(h) = frame.decode().unwrap() else {
            panic!();
        };
        assert_eq!(h.subscription.query.message_id, "sub-1");
        let got = subscription(Source(frame.bytes()), &h.subscription);
        assert_eq!(got.subscription_id, "sub-1");
        assert_eq!(got.query_name, "Count");
        assert_eq!(got.initial_permits, 16);
    }

    #[test]
    fn faults_map_to_the_codes_clients_retry_on() {
        let code = |kind| failure(Fault::new(kind, "m")).code;
        assert_eq!(code(FaultKind::Unavailable), ErrorCode::UNAVAILABLE);
        assert_eq!(code(FaultKind::Timeout), ErrorCode::UNAVAILABLE);
        assert_eq!(
            code(FaultKind::ResourceExhausted),
            ErrorCode::RESOURCE_EXHAUSTED
        );
        assert_eq!(code(FaultKind::AlreadyExists), ErrorCode::ALREADY_EXISTS);
        // And back, for what a peer reports.
        let back = fault(&Failure::new(ErrorCode::UNAVAILABLE, "down"));
        assert_eq!(back.kind, FaultKind::Unavailable);
    }
}
