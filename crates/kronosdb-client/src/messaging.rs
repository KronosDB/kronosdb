//! Commands, queries, subscription queries and the platform.
//!
//! A [`Bus`] is a messaging bus bound on the connection. A dispatcher sends
//! on it ([`Bus::dispatch_command`], [`Bus::dispatch_query`],
//! [`Bus::subscription_query`]); a handler serves it over a long-lived
//! stream ([`Bus::command_handlers`], [`Bus::query_handlers`]) that grants
//! the server permits back automatically as it answers.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use bytes::Bytes;
use kronosdb_protocol as p;
use kronosdb_protocol::net::client::{ClientStream, StreamWriter};
use kronosdb_protocol::net::{NetError, OwnedFrame};

use crate::{Client, Result, expect, unexpected};

// ── Messages ───────────────────────────────────────────────────────────

/// A serialized application object: opaque to the server.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Payload {
    pub type_name: String,
    pub revision: String,
    pub data: Bytes,
}

impl Payload {
    pub fn new(type_name: impl Into<String>, data: impl Into<Bytes>) -> Self {
        Self {
            type_name: type_name.into(),
            revision: "1".into(),
            data: data.into(),
        }
    }

    fn wire(&self) -> p::Payload<'_> {
        p::Payload {
            type_name: &self.type_name,
            revision: &self.revision,
            data: &self.data,
        }
    }

    fn from_wire(frame: &OwnedFrame, w: &p::Payload<'_>) -> Self {
        Self {
            type_name: w.type_name.to_string(),
            revision: w.revision.to_string(),
            data: frame.bytes().slice_ref(w.data),
        }
    }
}

/// A metadata value, typed so any runtime's values travel losslessly.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Text(String),
    Number(i64),
    Boolean(bool),
    Double(f64),
    Object(Payload),
}

impl Value {
    fn wire(&self) -> p::MetadataValue<'_> {
        match self {
            Value::Text(s) => p::MetadataValue::Text(s),
            Value::Number(n) => p::MetadataValue::Number(*n),
            Value::Boolean(b) => p::MetadataValue::Boolean(*b),
            Value::Double(d) => p::MetadataValue::Double(*d),
            Value::Object(o) => p::MetadataValue::Object(o.wire()),
        }
    }

    fn from_wire(frame: &OwnedFrame, w: &p::MetadataValue<'_>) -> Self {
        match w {
            p::MetadataValue::Text(s) => Value::Text(s.to_string()),
            p::MetadataValue::Number(n) => Value::Number(*n),
            p::MetadataValue::Boolean(b) => Value::Boolean(*b),
            p::MetadataValue::Double(d) => Value::Double(*d),
            p::MetadataValue::Object(o) => Value::Object(Payload::from_wire(frame, o)),
        }
    }
}

/// Why an application handler failed.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ErrorDetail {
    pub message: String,
    pub location: String,
    pub details: Vec<String>,
    pub error_code: String,
}

impl ErrorDetail {
    fn wire(&self) -> p::ErrorDetail<'_> {
        p::ErrorDetail {
            message: &self.message,
            location: &self.location,
            details: self.details.iter().map(String::as_str).collect(),
            error_code: &self.error_code,
        }
    }

    fn from_wire(w: &p::ErrorDetail<'_>) -> Self {
        Self {
            message: w.message.to_string(),
            location: w.location.to_string(),
            details: w.details.iter().map(|d| d.to_string()).collect(),
            error_code: w.error_code.to_string(),
        }
    }
}

type Metadata = Vec<(String, Value)>;

fn wire_metadata(metadata: &Metadata) -> Vec<p::MessageMetadata<'_>> {
    metadata
        .iter()
        .map(|(k, v)| p::MessageMetadata {
            key: k,
            value: v.wire(),
        })
        .collect()
}

fn metadata_from_wire(frame: &OwnedFrame, w: &[p::MessageMetadata<'_>]) -> Metadata {
    w.iter()
        .map(|m| (m.key.to_string(), Value::from_wire(frame, &m.value)))
        .collect()
}

/// A command or a query: what a dispatcher sends and a handler receives.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Message {
    pub message_id: String,
    pub name: String,
    pub timestamp: i64,
    pub payload: Payload,
    pub metadata: Metadata,
    /// Commands with the same key go to the same handler.
    pub routing_key: Option<String>,
    /// How long the dispatcher will wait, in milliseconds.
    pub timeout_ms: Option<i64>,
    /// A query wanting this many results; 1 asks a single handler.
    pub nr_of_results: Option<i64>,
    pub priority: Option<i64>,
    pub client_id: String,
    pub component_name: String,
}

pub type Command = Message;
pub type Query = Message;

impl Message {
    pub fn new(message_id: impl Into<String>, name: impl Into<String>, payload: Payload) -> Self {
        Self {
            message_id: message_id.into(),
            name: name.into(),
            payload,
            ..Default::default()
        }
    }

    pub fn with_routing_key(mut self, key: impl Into<String>) -> Self {
        self.routing_key = Some(key.into());
        self
    }

    fn instructions(&self) -> Vec<p::ProcessingInstruction<'_>> {
        let mut list = Vec::new();
        if let Some(key) = &self.routing_key {
            list.push(p::ProcessingInstruction {
                key: p::ProcessingKey::ROUTING_KEY,
                value: Some(p::MetadataValue::Text(key)),
            });
        }
        for (key, value) in [
            (p::ProcessingKey::PRIORITY, self.priority),
            (p::ProcessingKey::TIMEOUT, self.timeout_ms),
            (p::ProcessingKey::NR_OF_RESULTS, self.nr_of_results),
        ] {
            if let Some(n) = value {
                list.push(p::ProcessingInstruction {
                    key,
                    value: Some(p::MetadataValue::Number(n)),
                });
            }
        }
        list
    }

    fn wire_command(&self) -> p::Command<'_> {
        p::Command {
            message_id: &self.message_id,
            name: &self.name,
            timestamp: self.timestamp,
            payload: self.payload.wire(),
            metadata: wire_metadata(&self.metadata),
            instructions: self.instructions(),
            client_id: &self.client_id,
            component_name: &self.component_name,
        }
    }

    fn wire_query(&self) -> p::Query<'_> {
        p::Query {
            message_id: &self.message_id,
            name: &self.name,
            timestamp: self.timestamp,
            payload: self.payload.wire(),
            metadata: wire_metadata(&self.metadata),
            instructions: self.instructions(),
            client_id: &self.client_id,
            component_name: &self.component_name,
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn from_wire(
        frame: &OwnedFrame,
        message_id: &str,
        name: &str,
        timestamp: i64,
        payload: &p::Payload<'_>,
        metadata: &[p::MessageMetadata<'_>],
        instructions: &[p::ProcessingInstruction<'_>],
        client_id: &str,
        component_name: &str,
    ) -> Self {
        let number = |key: p::ProcessingKey| {
            instructions.iter().find_map(|i| match (&i.key, &i.value) {
                (k, Some(p::MetadataValue::Number(n))) if *k == key => Some(*n),
                _ => None,
            })
        };
        Self {
            message_id: message_id.to_string(),
            name: name.to_string(),
            timestamp,
            payload: Payload::from_wire(frame, payload),
            metadata: metadata_from_wire(frame, metadata),
            routing_key: instructions.iter().find_map(|i| match (&i.key, &i.value) {
                (&p::ProcessingKey::ROUTING_KEY, Some(p::MetadataValue::Text(t))) => {
                    Some(t.to_string())
                }
                _ => None,
            }),
            timeout_ms: number(p::ProcessingKey::TIMEOUT),
            nr_of_results: number(p::ProcessingKey::NR_OF_RESULTS),
            priority: number(p::ProcessingKey::PRIORITY),
            client_id: client_id.to_string(),
            component_name: component_name.to_string(),
        }
    }

    fn from_command(frame: &OwnedFrame, c: &p::Command<'_>) -> Self {
        Self::from_wire(
            frame,
            c.message_id,
            c.name,
            c.timestamp,
            &c.payload,
            &c.metadata,
            &c.instructions,
            c.client_id,
            c.component_name,
        )
    }

    fn from_query(frame: &OwnedFrame, q: &p::Query<'_>) -> Self {
        Self::from_wire(
            frame,
            q.message_id,
            q.name,
            q.timestamp,
            &q.payload,
            &q.metadata,
            &q.instructions,
            q.client_id,
            q.component_name,
        )
    }
}

/// What a handler answered, to a command or a query.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Reply {
    pub message_id: String,
    /// The command or query this answers.
    pub request_id: String,
    /// Empty on success.
    pub error_code: String,
    pub error: Option<ErrorDetail>,
    pub payload: Option<Payload>,
    pub metadata: Metadata,
}

pub type CommandResult = Reply;
pub type QueryResult = Reply;

impl Reply {
    /// A successful answer to `request_id` carrying `payload`.
    pub fn ok(request_id: impl Into<String>, payload: Payload) -> Self {
        Self {
            request_id: request_id.into(),
            payload: Some(payload),
            ..Default::default()
        }
    }

    /// A failed answer to `request_id`.
    pub fn failed(
        request_id: impl Into<String>,
        error_code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        let error_code = error_code.into();
        Self {
            request_id: request_id.into(),
            error: Some(ErrorDetail {
                message: message.into(),
                error_code: error_code.clone(),
                ..Default::default()
            }),
            error_code,
            ..Default::default()
        }
    }

    pub fn is_ok(&self) -> bool {
        self.error_code.is_empty()
    }

    fn wire_command(&self) -> p::CommandResult<'_> {
        p::CommandResult {
            message_id: &self.message_id,
            request_id: &self.request_id,
            error_code: &self.error_code,
            error: self.error.as_ref().map(ErrorDetail::wire),
            payload: self.payload.as_ref().map(Payload::wire),
            metadata: wire_metadata(&self.metadata),
            instructions: vec![],
        }
    }

    fn wire_query(&self) -> p::QueryResult<'_> {
        p::QueryResult {
            message_id: &self.message_id,
            request_id: &self.request_id,
            error_code: &self.error_code,
            error: self.error.as_ref().map(ErrorDetail::wire),
            payload: self.payload.as_ref().map(Payload::wire),
            metadata: wire_metadata(&self.metadata),
            instructions: vec![],
        }
    }

    fn from_command(frame: &OwnedFrame, r: &p::CommandResult<'_>) -> Self {
        Self {
            message_id: r.message_id.to_string(),
            request_id: r.request_id.to_string(),
            error_code: r.error_code.to_string(),
            error: r.error.as_ref().map(ErrorDetail::from_wire),
            payload: r.payload.as_ref().map(|p| Payload::from_wire(frame, p)),
            metadata: metadata_from_wire(frame, &r.metadata),
        }
    }

    fn from_query(frame: &OwnedFrame, r: &p::QueryResult<'_>) -> Self {
        Self {
            message_id: r.message_id.to_string(),
            request_id: r.request_id.to_string(),
            error_code: r.error_code.to_string(),
            error: r.error.as_ref().map(ErrorDetail::from_wire),
            payload: r.payload.as_ref().map(|p| Payload::from_wire(frame, p)),
            metadata: metadata_from_wire(frame, &r.metadata),
        }
    }
}

// ── Buses ──────────────────────────────────────────────────────────────

impl Client {
    /// The `default` bus, bound from the start.
    pub fn default_bus(&self) -> Bus {
        Bus {
            client: self.clone(),
            id: 0,
        }
    }

    /// A bus by name, bound on first use.
    pub async fn bus(&self, name: &str) -> Result<Bus> {
        let mut buses = self.shared.buses.lock().await;
        if let Some(&id) = buses.get(name) {
            return Ok(Bus {
                client: self.clone(),
                id,
            });
        }
        let id = u16::try_from(buses.len()).map_err(|_| NetError::Unexpected {
            kind: 0,
            context: "more than 65536 buses on one connection",
        })?;
        let reply = self
            .shared
            .conn
            .request(&p::Frame::BindBus(p::BindBus { id, name }))
            .await?;
        expect(&reply, p::BusBound::KIND, "instead of BusBound")?;
        buses.insert(name.to_string(), id);
        Ok(Bus {
            client: self.clone(),
            id,
        })
    }
}

/// One messaging bus on a client's connection.
#[derive(Clone)]
pub struct Bus {
    client: Client,
    id: u16,
}

/// Sent with every handler stream and subscription: who is asking.
fn instruction_id(counter: &mut u64) -> String {
    *counter += 1;
    format!("i{counter}")
}

impl Bus {
    fn conn(&self) -> &kronosdb_protocol::net::client::Connection {
        &self.client.shared.conn
    }

    /// Sends a command and waits for the handler's result. A handler that
    /// fails, vanishes or takes too long is still an `Ok` result, with the
    /// error code that says so (`KRONOSDB-4005` timed out, `KRONOSDB-4006`
    /// handler lost); an `Err` means the command reached no handler.
    pub async fn dispatch_command(&self, command: &Command) -> Result<CommandResult> {
        let reply = self
            .conn()
            .request(&p::Frame::DispatchCommand(p::DispatchCommand {
                bus: self.id,
                command: command.wire_command(),
            }))
            .await?;
        match reply.decode()? {
            p::Frame::CommandResponse(r) => Ok(Reply::from_command(&reply, &r.result)),
            _ => Err(unexpected(&reply, "instead of CommandResponse")),
        }
    }

    /// Sends a query to its handlers and returns the stream of their results.
    pub async fn dispatch_query(&self, query: &Query) -> Result<QueryResults> {
        let stream = self
            .conn()
            .open(
                &p::Frame::DispatchQuery(p::DispatchQuery {
                    bus: self.id,
                    query: query.wire_query(),
                }),
                true,
            )
            .await?;
        Ok(QueryResults { stream })
    }

    /// Opens a subscription query: an initial result, then updates, until
    /// the handler completes it or the stream is cancelled. Update permits
    /// are granted back as updates are taken.
    pub async fn subscription_query(
        &self,
        subscription_id: &str,
        query: &Query,
        permits: u32,
    ) -> Result<SubscriptionQueryStream> {
        let stream = self
            .conn()
            .open(
                &p::Frame::OpenSubscriptionQuery(p::OpenSubscriptionQuery {
                    bus: self.id,
                    subscription: p::SubscriptionQuery {
                        subscription_id,
                        query: query.wire_query(),
                        permits: permits as u64,
                    },
                }),
                false,
            )
            .await?;
        Ok(SubscriptionQueryStream {
            stream,
            permits: PermitPool::new(permits),
            outstanding: permits as u64,
        })
    }

    /// Opens a command handler stream for this client.
    pub async fn command_handlers(
        &self,
        client_id: &str,
        component_name: &str,
        permits: u32,
    ) -> Result<CommandHandlers> {
        let stream = self
            .conn()
            .open(
                &p::Frame::OpenCommandHandlers(p::OpenCommandHandlers {
                    bus: self.id,
                    client_id,
                    component_name,
                }),
                false,
            )
            .await?;
        // One pool of permits for the whole stream, whatever it subscribes to.
        stream
            .send(&p::Frame::Permits(p::Permits { count: permits }))
            .await?;
        Ok(CommandHandlers {
            stream,
            permits: PermitPool::new(permits),
            backlog: Vec::new(),
            instructions: 0,
        })
    }

    /// Opens a query handler stream for this client.
    pub async fn query_handlers(
        &self,
        client_id: &str,
        component_name: &str,
        permits: u32,
    ) -> Result<QueryHandlers> {
        let stream = self
            .conn()
            .open(
                &p::Frame::OpenQueryHandlers(p::OpenQueryHandlers {
                    bus: self.id,
                    client_id,
                    component_name,
                }),
                false,
            )
            .await?;
        // One pool of permits for the whole stream, whatever it subscribes to.
        stream
            .send(&p::Frame::Permits(p::Permits { count: permits }))
            .await?;
        Ok(QueryHandlers {
            stream,
            permits: PermitPool::new(permits),
            backlog: Vec::new(),
            instructions: 0,
        })
    }
}

// ── Permits ────────────────────────────────────────────────────────────

/// The permits a handler grants the server: a pool that is topped up once
/// half of it has been answered.
struct PermitPool {
    size: u32,
    answered: AtomicU32,
}

impl PermitPool {
    fn new(size: u32) -> Arc<Self> {
        Arc::new(Self {
            size,
            answered: AtomicU32::new(0),
        })
    }

    /// One more answered. How many permits to grant back, if it is time.
    fn answered(&self) -> Option<u32> {
        let n = self.answered.fetch_add(1, Ordering::AcqRel) + 1;
        if n >= self.size.div_ceil(2).max(1) {
            // Take whatever has built up; a racing answer finds zero.
            let granted = self.answered.swap(0, Ordering::AcqRel);
            (granted > 0).then_some(granted)
        } else {
            None
        }
    }
}

/// The server's acknowledgement of an instruction, read off a handler stream
/// while anything else that arrives is kept for the caller.
async fn await_ack(
    stream: &mut ClientStream,
    instruction: &str,
    backlog: &mut Vec<OwnedFrame>,
    keep: impl Fn(&OwnedFrame) -> bool,
) -> Result<()> {
    loop {
        let Some(frame) = stream.next().await? else {
            return Err(NetError::Closed);
        };
        if let Ok(p::Frame::ServerAck(a)) = frame.decode() {
            if a.ack.instruction_id == instruction {
                return if a.ack.success {
                    Ok(())
                } else {
                    Err(NetError::Failed(kronosdb_protocol::net::Failure::new(
                        p::ErrorCode::REJECTED,
                        a.ack
                            .error
                            .map(|e| e.message.to_string())
                            .unwrap_or_else(|| "the server refused the instruction".into()),
                    )))
                };
            }
        } else if keep(&frame) {
            backlog.push(frame);
        }
    }
}

// ── Command handlers ───────────────────────────────────────────────────

/// A command handler stream. Subscribe to the command names it serves, then
/// take commands and answer each.
pub struct CommandHandlers {
    stream: ClientStream,
    permits: Arc<PermitPool>,
    backlog: Vec<OwnedFrame>,
    instructions: u64,
}

/// A command to answer.
pub struct IncomingCommand {
    pub command: Command,
    writer: StreamWriter,
    permits: Arc<PermitPool>,
}

impl IncomingCommand {
    /// Answers the command and, when enough are answered, grants the server
    /// more permits.
    pub async fn respond(self, result: &CommandResult) -> Result<()> {
        let mut result = result.clone();
        if result.request_id.is_empty() {
            result.request_id = self.command.message_id.clone();
        }
        self.writer
            .send(&p::Frame::CommandHandled(p::CommandHandled {
                result: result.wire_command(),
            }))
            .await?;
        if let Some(granted) = self.permits.answered() {
            self.writer
                .send(&p::Frame::Permits(p::Permits { count: granted }))
                .await?;
        }
        Ok(())
    }
}

impl CommandHandlers {
    /// Serves `command` from now on, once the server acknowledges it. It
    /// shares the stream's permits with every other command.
    pub async fn subscribe(&mut self, command: &str, load_factor: u32) -> Result<()> {
        let id = instruction_id(&mut self.instructions);
        self.stream
            .send(&p::Frame::SubscribeCommand(p::SubscribeCommand {
                instruction_id: &id,
                command,
                load_factor,
            }))
            .await?;
        await_ack(&mut self.stream, &id, &mut self.backlog, |f| {
            f.kind() == p::HandleCommand::KIND
        })
        .await
    }

    pub async fn unsubscribe(&mut self, command: &str) -> Result<()> {
        let id = instruction_id(&mut self.instructions);
        self.stream
            .send(&p::Frame::UnsubscribeCommand(p::UnsubscribeCommand {
                instruction_id: &id,
                command,
            }))
            .await?;
        await_ack(&mut self.stream, &id, &mut self.backlog, |f| {
            f.kind() == p::HandleCommand::KIND
        })
        .await
    }

    /// The next command. `None` when the stream has ended.
    pub async fn next(&mut self) -> Result<Option<IncomingCommand>> {
        loop {
            let frame = if self.backlog.is_empty() {
                match self.stream.next().await? {
                    Some(frame) => frame,
                    None => return Ok(None),
                }
            } else {
                self.backlog.remove(0)
            };
            if let p::Frame::HandleCommand(h) = frame.decode()? {
                return Ok(Some(IncomingCommand {
                    command: Message::from_command(&frame, &h.command),
                    writer: self.stream.writer(),
                    permits: Arc::clone(&self.permits),
                }));
            }
        }
    }
}

// ── Queries ────────────────────────────────────────────────────────────

/// A query's results as the handlers produce them.
pub struct QueryResults {
    stream: ClientStream,
}

impl QueryResults {
    /// The next result. `None` once every handler is done.
    pub async fn next(&mut self) -> Result<Option<QueryResult>> {
        loop {
            let Some(frame) = self.stream.next().await? else {
                return Ok(None);
            };
            match frame.decode()? {
                p::Frame::QueryResponse(r) => {
                    return Ok(Some(Reply::from_query(&frame, &r.result)));
                }
                p::Frame::QueryEnd(_) => return Ok(None),
                _ => {}
            }
        }
    }

    /// Every result, up to the end.
    pub async fn collect(mut self) -> Result<Vec<QueryResult>> {
        let mut all = Vec::new();
        while let Some(result) = self.next().await? {
            all.push(result);
        }
        Ok(all)
    }

    /// Gives the query up; handlers still working on it are told to stop.
    pub async fn cancel(self) {
        self.stream.cancel().await
    }
}

/// A query handler stream.
pub struct QueryHandlers {
    stream: ClientStream,
    permits: Arc<PermitPool>,
    backlog: Vec<OwnedFrame>,
    instructions: u64,
}

/// A query to answer with any number of results, then [`done`](IncomingQuery::done).
pub struct IncomingQuery {
    pub query: Query,
    writer: StreamWriter,
    permits: Arc<PermitPool>,
}

impl IncomingQuery {
    pub async fn send(&self, result: &QueryResult) -> Result<()> {
        let mut result = result.clone();
        if result.request_id.is_empty() {
            result.request_id = self.query.message_id.clone();
        }
        self.writer
            .send(&p::Frame::QueryHandled(p::QueryHandled {
                result: result.wire_query(),
            }))
            .await
    }

    /// Says every result has been sent, and grants the server more permits
    /// when enough queries are answered.
    pub async fn done(self) -> Result<()> {
        self.writer
            .send(&p::Frame::QueryDone(p::QueryDone {
                message_id: "",
                request_id: &self.query.message_id,
            }))
            .await?;
        if let Some(granted) = self.permits.answered() {
            self.writer
                .send(&p::Frame::Permits(p::Permits { count: granted }))
                .await?;
        }
        Ok(())
    }

    /// One result, then done.
    pub async fn respond(self, result: &QueryResult) -> Result<()> {
        self.send(result).await?;
        self.done().await
    }
}

/// A subscription query to answer: an initial result, updates, an end.
pub struct IncomingSubscription {
    pub subscription_id: String,
    pub query: Query,
    /// Updates the subscriber will take before it grants more.
    pub permits: u64,
    writer: StreamWriter,
}

impl IncomingSubscription {
    async fn event(&self, event: p::SubscriptionEvent<'_>) -> Result<()> {
        self.writer
            .send(&p::Frame::SubscriptionQueryHandled(
                p::SubscriptionQueryHandled {
                    subscription_id: &self.subscription_id,
                    event,
                },
            ))
            .await
    }

    pub async fn initial(&self, result: &QueryResult) -> Result<()> {
        let mut result = result.clone();
        if result.request_id.is_empty() {
            result.request_id = self.subscription_id.clone();
        }
        self.event(p::SubscriptionEvent::InitialResult(result.wire_query()))
            .await
    }

    pub async fn update(&self, payload: Payload) -> Result<()> {
        self.event(p::SubscriptionEvent::Update(p::QueryUpdate {
            message_id: &self.subscription_id,
            payload: Some(payload.wire()),
            metadata: vec![],
            client_id: "",
            component_name: "",
            error_code: "",
            error: None,
        }))
        .await
    }

    pub async fn complete(&self) -> Result<()> {
        self.event(p::SubscriptionEvent::Complete(p::SubscriptionEnd {
            client_id: "",
            component_name: "",
        }))
        .await
    }

    pub async fn fail(&self, error_code: &str, message: &str) -> Result<()> {
        let error = ErrorDetail {
            message: message.into(),
            error_code: error_code.into(),
            ..Default::default()
        };
        self.event(p::SubscriptionEvent::CompleteExceptionally(
            p::SubscriptionFailure {
                client_id: "",
                component_name: "",
                error_code,
                error: Some(error.wire()),
            },
        ))
        .await
    }
}

/// What a query handler stream delivers.
pub enum QueryHandlerEvent {
    Query(IncomingQuery),
    Subscription(IncomingSubscription),
    /// The dispatcher gave up on this query; stop working on it.
    CancelQuery(String),
    /// The subscriber went away.
    CancelSubscription(String),
    /// The subscriber granted more update permits.
    SubscriptionPermits {
        subscription_id: String,
        permits: u64,
    },
    /// The server allows more results on a streaming query.
    QueryPermits {
        request_id: String,
        permits: u64,
    },
}

impl QueryHandlers {
    pub async fn subscribe(&mut self, query: &str, result_name: &str) -> Result<()> {
        let id = instruction_id(&mut self.instructions);
        self.stream
            .send(&p::Frame::SubscribeQuery(p::SubscribeQuery {
                instruction_id: &id,
                query,
                result_name,
            }))
            .await?;
        await_ack(&mut self.stream, &id, &mut self.backlog, |f| {
            is_query_handler_event(f.kind())
        })
        .await
    }

    pub async fn unsubscribe(&mut self, query: &str, result_name: &str) -> Result<()> {
        let id = instruction_id(&mut self.instructions);
        self.stream
            .send(&p::Frame::UnsubscribeQuery(p::UnsubscribeQuery {
                instruction_id: &id,
                query,
                result_name,
            }))
            .await?;
        await_ack(&mut self.stream, &id, &mut self.backlog, |f| {
            is_query_handler_event(f.kind())
        })
        .await
    }

    /// The next thing the server asks of this handler. `None` when the
    /// stream has ended.
    pub async fn next(&mut self) -> Result<Option<QueryHandlerEvent>> {
        loop {
            let frame = if self.backlog.is_empty() {
                match self.stream.next().await? {
                    Some(frame) => frame,
                    None => return Ok(None),
                }
            } else {
                self.backlog.remove(0)
            };
            let writer = || self.stream.writer();
            let event = match frame.decode()? {
                p::Frame::HandleQuery(h) => QueryHandlerEvent::Query(IncomingQuery {
                    query: Message::from_query(&frame, &h.query),
                    writer: writer(),
                    permits: Arc::clone(&self.permits),
                }),
                p::Frame::HandleSubscriptionQuery(h) => {
                    QueryHandlerEvent::Subscription(IncomingSubscription {
                        subscription_id: h.subscription.subscription_id.to_string(),
                        query: Message::from_query(&frame, &h.subscription.query),
                        permits: h.subscription.permits,
                        writer: writer(),
                    })
                }
                p::Frame::CancelQuery(c) => {
                    QueryHandlerEvent::CancelQuery(c.request_id.to_string())
                }
                p::Frame::CancelSubscriptionQuery(c) => {
                    QueryHandlerEvent::CancelSubscription(c.subscription_id.to_string())
                }
                p::Frame::SubscriptionQueryPermits(c) => QueryHandlerEvent::SubscriptionPermits {
                    subscription_id: c.subscription_id.to_string(),
                    permits: c.permits,
                },
                p::Frame::QueryPermits(c) => QueryHandlerEvent::QueryPermits {
                    request_id: c.request_id.to_string(),
                    permits: c.permits,
                },
                _ => continue,
            };
            return Ok(Some(event));
        }
    }
}

fn is_query_handler_event(kind: u16) -> bool {
    [
        p::HandleQuery::KIND,
        p::HandleSubscriptionQuery::KIND,
        p::CancelQuery::KIND,
        p::CancelSubscriptionQuery::KIND,
        p::SubscriptionQueryPermits::KIND,
        p::QueryPermits::KIND,
    ]
    .contains(&kind)
}

// ── Subscription queries ───────────────────────────────────────────────

/// What a subscription query delivers.
#[derive(Debug, Clone, PartialEq)]
pub enum SubscriptionEvent {
    Initial(QueryResult),
    Update {
        payload: Option<Payload>,
        metadata: Metadata,
    },
    /// The handler ended the subscription.
    Complete,
    /// The handler ended the subscription with an error.
    Failed {
        error_code: String,
        error: Option<ErrorDetail>,
    },
}

/// A live subscription query. Update permits are granted back as updates are
/// taken, so the handler stays within the permits it was given.
pub struct SubscriptionQueryStream {
    stream: ClientStream,
    permits: Arc<PermitPool>,
    outstanding: u64,
}

impl SubscriptionQueryStream {
    /// The next event; `None` after the terminal one.
    pub async fn next(&mut self) -> Result<Option<SubscriptionEvent>> {
        loop {
            let Some(frame) = self.stream.next().await? else {
                return Ok(None);
            };
            let p::Frame::SubscriptionQueryEvent(e) = frame.decode()? else {
                continue;
            };
            let event = match &e.event {
                p::SubscriptionEvent::InitialResult(r) => {
                    SubscriptionEvent::Initial(Reply::from_query(&frame, r))
                }
                p::SubscriptionEvent::Update(u) => {
                    self.outstanding = self.outstanding.saturating_sub(1);
                    if self.outstanding <= (self.permits.size / 2) as u64 {
                        let top_up = self.permits.size as u64 - self.outstanding;
                        self.stream
                            .send(&p::Frame::Permits(p::Permits {
                                count: top_up as u32,
                            }))
                            .await?;
                        self.outstanding += top_up;
                    }
                    SubscriptionEvent::Update {
                        payload: u.payload.as_ref().map(|p| Payload::from_wire(&frame, p)),
                        metadata: metadata_from_wire(&frame, &u.metadata),
                    }
                }
                p::SubscriptionEvent::Complete(_) => SubscriptionEvent::Complete,
                p::SubscriptionEvent::CompleteExceptionally(f) => SubscriptionEvent::Failed {
                    error_code: f.error_code.to_string(),
                    error: f.error.as_ref().map(ErrorDetail::from_wire),
                },
            };
            return Ok(Some(event));
        }
    }

    /// Ends the subscription and waits for the server to confirm.
    pub async fn cancel(self) {
        self.stream.cancel().await
    }
}

// ── Platform ───────────────────────────────────────────────────────────

/// Who this client is, for the admin console.
#[derive(Debug, Clone, Default)]
pub struct Identity {
    pub client_id: String,
    pub component_name: String,
    pub version: String,
    pub tags: Vec<(String, String)>,
}

impl Identity {
    fn wire(&self) -> p::ClientIdentity<'_> {
        p::ClientIdentity {
            client_id: &self.client_id,
            component_name: &self.component_name,
            version: &self.version,
            tags: self
                .tags
                .iter()
                .map(|(k, v)| p::MetadataEntry { key: k, value: v })
                .collect(),
        }
    }
}

/// What a node says about itself.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeInfo {
    pub node_name: String,
    pub version: String,
    pub contexts: Vec<String>,
}

/// One segment of an event processor.
#[derive(Debug, Clone, Default)]
pub struct SegmentStatus {
    pub segment_id: u32,
    pub caught_up: bool,
    pub replaying: bool,
    pub one_part_of: u32,
    pub token_position: i64,
    pub error_state: String,
}

/// An event processor's state, reported on the platform stream.
#[derive(Debug, Clone, Default)]
pub struct ProcessorStatus {
    pub processor_name: String,
    pub mode: String,
    pub active_threads: u32,
    pub available_threads: u32,
    pub running: bool,
    pub error: bool,
    pub streaming: bool,
    pub token_store_identifier: String,
    pub segments: Vec<SegmentStatus>,
}

/// What the server asks of a processor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessorAction {
    Pause,
    Start,
    ReleaseSegment,
    ReportStatus,
    SplitSegment,
    MergeSegment,
    /// An action this version does not know.
    Other(u8),
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProcessorInstruction {
    pub instruction_id: String,
    pub action: ProcessorAction,
    pub processor_name: String,
    /// 0 when the action applies to the processor.
    pub segment: u32,
}

/// A handler registration appeared or went away somewhere in the cluster.
#[derive(Debug, Clone, PartialEq)]
pub struct TopologyChange {
    pub change_type: String,
    pub message_type: String,
    pub handler_kind: String,
    pub client_id: String,
    pub component_name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PlatformEvent {
    /// The node this stream is on.
    Node(NodeInfo),
    Topology(TopologyChange),
    Instruction(ProcessorInstruction),
    /// The node is going away; connect to another.
    ReconnectRequested,
}

impl Client {
    /// Introduces this client to the node.
    pub async fn register(&self, identity: &Identity) -> Result<NodeInfo> {
        let reply = self
            .shared
            .conn
            .request(&p::Frame::GetPlatformInfo(p::GetPlatformInfo {
                client: identity.wire(),
            }))
            .await?;
        match reply.decode()? {
            p::Frame::PlatformInfo(i) => Ok(NodeInfo {
                node_name: i.node_name.to_string(),
                version: i.version.to_string(),
                contexts: i.contexts.iter().map(|c| c.to_string()).collect(),
            }),
            _ => Err(unexpected(&reply, "instead of PlatformInfo")),
        }
    }

    /// Opens the platform stream: report processors on it, receive the
    /// server's instructions. The server expects to hear from the client
    /// regularly; [`Platform::next`] answers its heartbeats.
    pub async fn open_platform(&self, identity: &Identity) -> Result<Platform> {
        let stream = self
            .shared
            .conn
            .open(
                &p::Frame::OpenPlatform(p::OpenPlatform {
                    client: identity.wire(),
                }),
                false,
            )
            .await?;
        Ok(Platform { stream })
    }
}

/// A client's platform stream.
pub struct Platform {
    stream: ClientStream,
}

impl Platform {
    /// The next thing the server says. Heartbeats are answered here and not
    /// returned, so keep calling this while the client is up.
    pub async fn next(&mut self) -> Result<Option<PlatformEvent>> {
        loop {
            let Some(frame) = self.stream.next().await? else {
                return Ok(None);
            };
            let event = match frame.decode()? {
                p::Frame::ServerHeartbeat(_) => {
                    self.heartbeat().await?;
                    continue;
                }
                p::Frame::NodeInfo(i) => PlatformEvent::Node(NodeInfo {
                    node_name: i.node_name.to_string(),
                    version: i.version.to_string(),
                    contexts: i.contexts.iter().map(|c| c.to_string()).collect(),
                }),
                p::Frame::TopologyChange(t) => PlatformEvent::Topology(TopologyChange {
                    change_type: t.change_type.to_string(),
                    message_type: t.message_type.to_string(),
                    handler_kind: t.handler_kind.to_string(),
                    client_id: t.client_id.to_string(),
                    component_name: t.component_name.to_string(),
                }),
                p::Frame::ProcessorInstruction(i) => {
                    PlatformEvent::Instruction(ProcessorInstruction {
                        instruction_id: i.instruction_id.to_string(),
                        action: match i.action {
                            p::ProcessorAction::PAUSE => ProcessorAction::Pause,
                            p::ProcessorAction::START => ProcessorAction::Start,
                            p::ProcessorAction::RELEASE_SEGMENT => ProcessorAction::ReleaseSegment,
                            p::ProcessorAction::REPORT_STATUS => ProcessorAction::ReportStatus,
                            p::ProcessorAction::SPLIT_SEGMENT => ProcessorAction::SplitSegment,
                            p::ProcessorAction::MERGE_SEGMENT => ProcessorAction::MergeSegment,
                            other => ProcessorAction::Other(other.0),
                        },
                        processor_name: i.processor_name.to_string(),
                        segment: i.segment,
                    })
                }
                p::Frame::RequestReconnect(_) => PlatformEvent::ReconnectRequested,
                _ => continue,
            };
            return Ok(Some(event));
        }
    }

    pub async fn heartbeat(&self) -> Result<()> {
        self.stream
            .send(&p::Frame::PlatformHeartbeat(p::PlatformHeartbeat {}))
            .await
    }

    /// Reports an event processor's state.
    pub async fn report(&self, status: &ProcessorStatus) -> Result<()> {
        let segments = status
            .segments
            .iter()
            .map(|s| p::SegmentStatus {
                segment_id: s.segment_id,
                caught_up: s.caught_up,
                replaying: s.replaying,
                one_part_of: s.one_part_of,
                token_position: s.token_position,
                error_state: &s.error_state,
            })
            .collect();
        self.stream
            .send(&p::Frame::ProcessorStatus(p::ProcessorStatus {
                processor_name: &status.processor_name,
                mode: &status.mode,
                active_threads: status.active_threads,
                available_threads: status.available_threads,
                running: status.running,
                error: status.error,
                streaming: status.streaming,
                token_store_identifier: &status.token_store_identifier,
                segments,
            }))
            .await
    }

    /// Reports how an instruction went.
    pub async fn answer(&self, instruction_id: &str, success: bool) -> Result<()> {
        self.stream
            .send(&p::Frame::InstructionResult(p::InstructionResult {
                instruction_id,
                success,
                error: None,
            }))
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_pool_grants_back_once_half_is_answered() {
        let pool = PermitPool::new(10);
        let grants: Vec<Option<u32>> = (0..10).map(|_| pool.answered()).collect();
        assert_eq!(
            grants,
            vec![
                None,
                None,
                None,
                None,
                Some(5),
                None,
                None,
                None,
                None,
                Some(5)
            ]
        );
        // A pool of one grants after every answer.
        let one = PermitPool::new(1);
        assert_eq!(one.answered(), Some(1));
        assert_eq!(one.answered(), Some(1));
    }

    #[test]
    fn instructions_carry_the_routing_key_and_hints() {
        let mut m = Message::new("c1", "Do", Payload::new("Do", Bytes::from_static(b"x")))
            .with_routing_key("order-1");
        m.timeout_ms = Some(2_000);
        m.nr_of_results = Some(1);
        let keys: Vec<u8> = m.instructions().iter().map(|i| i.key.0).collect();
        assert_eq!(keys, vec![0, 2, 3]);
        assert!(m.instructions().len() == 3);
    }

    #[test]
    fn a_failed_reply_says_so() {
        let r = Reply::failed("c1", "E-1", "no");
        assert!(!r.is_ok());
        assert_eq!(r.error.unwrap().error_code, "E-1");
        assert!(Reply::ok("c1", Payload::default()).is_ok());
    }
}
