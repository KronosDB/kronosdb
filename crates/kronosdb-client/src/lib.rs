//! A Rust client for KronosDB over its native protocol.
//!
//! [`Client::connect`] opens one connection; every method on it and on the
//! [`Context`]s it binds multiplexes over that connection, so one client is
//! shared by a whole process. Events read back are views into the frame they
//! arrived in ([`EventBatch`]): nothing is copied out until the caller does.

mod messaging;

use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use kronosdb_protocol as p;
use kronosdb_protocol::net::client::{self, ClientStream, Connection};
use kronosdb_protocol::net::{NetError, OwnedFrame};
use tokio::sync::Mutex;

pub use kronosdb_protocol::net::Failure;
pub use kronosdb_protocol::net::client::{ConnectOptions, Credential, DEFAULT_WINDOW};
pub use kronosdb_protocol::net::rules::MIN_WINDOW;
pub use kronosdb_protocol::{ErrorCode, EventRecord};
pub use messaging::*;

pub type Error = NetError;
pub type Result<T> = std::result::Result<T, Error>;

/// An event to append.
#[derive(Debug, Clone, Default)]
pub struct NewEvent {
    pub identifier: String,
    pub name: String,
    pub version: String,
    pub timestamp: i64,
    pub metadata: Vec<(String, String)>,
    pub tags: Vec<(Vec<u8>, Vec<u8>)>,
    pub payload: Bytes,
}

impl NewEvent {
    fn record(&self) -> p::EventRecord<'_> {
        p::EventRecord {
            position: 0,
            identifier: &self.identifier,
            name: &self.name,
            version: &self.version,
            timestamp: self.timestamp,
            metadata: self
                .metadata
                .iter()
                .map(|(k, v)| p::MetadataEntry { key: k, value: v })
                .collect(),
            tags: self
                .tags
                .iter()
                .map(|(k, v)| p::Tag { key: k, value: v })
                .collect(),
            payload: &self.payload,
        }
    }
}

/// Events of the given types (any type when empty) carrying all the tags.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Criterion {
    pub names: Vec<String>,
    pub tags: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Criterion {
    pub fn tag(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            names: vec![],
            tags: vec![(key.into(), value.into())],
        }
    }

    fn wire(&self) -> p::Criterion<'_> {
        p::Criterion {
            names: self.names.iter().map(String::as_str).collect(),
            tags: self
                .tags
                .iter()
                .map(|(k, v)| p::Tag { key: k, value: v })
                .collect(),
        }
    }
}

fn wire_criteria(criteria: &[Criterion]) -> Vec<p::Criterion<'_>> {
    criteria.iter().map(Criterion::wire).collect()
}

/// "Fail the append if anything matching `criteria` exists at or after `marker`."
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Condition {
    pub marker: u64,
    pub criteria: Vec<Criterion>,
}

/// How an append ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Appended {
    /// The events are durable; `marker` conditions the next append.
    Ok {
        first_position: u64,
        count: u32,
        marker: u64,
    },
    /// The condition failed; nothing was written.
    Conflict { position: u64 },
}

/// A batch of events as it arrived; iterating borrows from it.
#[derive(Debug, Clone)]
pub struct EventBatch {
    frame: OwnedFrame,
}

/// An event that outlives the batch it arrived in. Its payload still shares
/// the batch's buffer; only the small fields are copied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Event {
    pub position: u64,
    pub identifier: String,
    pub name: String,
    pub version: String,
    pub timestamp: i64,
    pub metadata: Vec<(String, String)>,
    pub tags: Vec<(Vec<u8>, Vec<u8>)>,
    pub payload: Bytes,
}

impl EventBatch {
    fn events(&self) -> p::Events<'_> {
        match self.frame.decode() {
            Ok(p::Frame::Events(e)) => e,
            _ => unreachable!("checked when the batch arrived"),
        }
    }

    pub fn len(&self) -> u32 {
        self.events().events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The events, borrowed from the frame.
    pub fn iter(&self) -> impl Iterator<Item = p::EventRecord<'_>> {
        self.events()
            .events
            .iter()
            .map(|r| r.event().expect("the server sends whole events"))
    }

    /// The consistency marker, on a read's last batch.
    pub fn marker(&self) -> Option<u64> {
        self.events().marker
    }

    /// The events as owned values; payloads stay views into the batch.
    pub fn to_events(&self) -> Vec<Event> {
        let frame = self.frame.bytes();
        self.iter()
            .map(|e| Event {
                position: e.position,
                identifier: e.identifier.to_string(),
                name: e.name.to_string(),
                version: e.version.to_string(),
                timestamp: e.timestamp,
                metadata: e
                    .metadata
                    .iter()
                    .map(|m| (m.key.to_string(), m.value.to_string()))
                    .collect(),
                tags: e
                    .tags
                    .iter()
                    .map(|t| (t.key.to_vec(), t.value.to_vec()))
                    .collect(),
                payload: frame.slice_ref(e.payload),
            })
            .collect()
    }
}

/// What a principal may do, as the server sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    pub roles: Vec<String>,
    /// `None`: every context.
    pub contexts: Option<Vec<String>>,
    /// `None`: every bus.
    pub buses: Option<Vec<String>>,
}

/// Who the server resolved the connection's credential to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Whoami {
    pub subject: String,
    pub source: String,
    pub grants: Vec<Grant>,
    pub authentication_enabled: bool,
}

/// Opaque state a client folded from the log, and the position it summarizes
/// up to (next-exclusive).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub state: Bytes,
    pub position: u64,
}

impl Snapshot {
    fn from_wire(snapshot: p::Snapshot<'_>) -> Self {
        Self {
            state: Bytes::copy_from_slice(snapshot.state),
            position: snapshot.position,
        }
    }
}

fn events_frame(frame: OwnedFrame) -> Result<EventBatch> {
    match frame.decode()? {
        p::Frame::Events(_) => Ok(EventBatch { frame }),
        _ => Err(NetError::Unexpected {
            kind: frame.kind(),
            context: "instead of Events",
        }),
    }
}

struct Shared {
    conn: Connection,
    contexts: Mutex<HashMap<String, u16>>,
    buses: Mutex<HashMap<String, u16>>,
}

/// A connection to a KronosDB node. Cheap to clone.
#[derive(Clone)]
pub struct Client {
    shared: Arc<Shared>,
}

impl Client {
    pub async fn connect(addr: &str, options: &ConnectOptions) -> Result<Self> {
        let conn = client::connect(addr, options).await?;
        Ok(Self::from_connection(conn))
    }

    pub fn from_connection(conn: Connection) -> Self {
        let mut contexts = HashMap::new();
        contexts.insert("default".to_string(), 0);
        let mut buses = HashMap::new();
        buses.insert("default".to_string(), 0);
        Self {
            shared: Arc::new(Shared {
                conn,
                contexts: Mutex::new(contexts),
                buses: Mutex::new(buses),
            }),
        }
    }

    pub fn connection(&self) -> &Connection {
        &self.shared.conn
    }

    /// The `default` context, bound from the start.
    pub fn default_context(&self) -> Context {
        Context {
            client: self.clone(),
            id: 0,
        }
    }

    /// A context by name, bound on first use.
    pub async fn context(&self, name: &str) -> Result<Context> {
        let mut contexts = self.shared.contexts.lock().await;
        if let Some(&id) = contexts.get(name) {
            return Ok(Context {
                client: self.clone(),
                id,
            });
        }
        let id = u16::try_from(contexts.len()).map_err(|_| NetError::Unexpected {
            kind: 0,
            context: "more than 65536 contexts on one connection",
        })?;
        let reply = self
            .shared
            .conn
            .request(&p::Frame::BindContext(p::BindContext { id, name }))
            .await?;
        expect(&reply, p::ContextBound::KIND, "instead of ContextBound")?;
        contexts.insert(name.to_string(), id);
        Ok(Context {
            client: self.clone(),
            id,
        })
    }

    /// Who the server resolved this connection to, and what it may do.
    pub async fn who_am_i(&self) -> Result<Whoami> {
        let reply = self
            .shared
            .conn
            .request(&p::Frame::WhoAmI(p::WhoAmI {}))
            .await?;
        let owned = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        match reply.decode()? {
            p::Frame::Identity(i) => Ok(Whoami {
                subject: i.subject.to_string(),
                source: i.source.to_string(),
                grants: i
                    .grants
                    .iter()
                    .map(|g| Grant {
                        roles: owned(&g.roles),
                        contexts: (!g.all_contexts).then(|| owned(&g.contexts)),
                        buses: (!g.all_buses).then(|| owned(&g.buses)),
                    })
                    .collect(),
                authentication_enabled: i.authentication_enabled,
            }),
            _ => Err(unexpected(&reply, "instead of Identity")),
        }
    }
}

fn unexpected(frame: &OwnedFrame, context: &'static str) -> NetError {
    NetError::Unexpected {
        kind: frame.kind(),
        context,
    }
}

fn expect(frame: &OwnedFrame, kind: u16, context: &'static str) -> Result<()> {
    if frame.kind() == kind {
        Ok(())
    } else {
        Err(unexpected(frame, context))
    }
}

/// One event store context on a client's connection.
#[derive(Clone)]
pub struct Context {
    client: Client,
    id: u16,
}

impl Context {
    fn conn(&self) -> &Connection {
        &self.client.shared.conn
    }

    pub async fn append(
        &self,
        events: &[NewEvent],
        condition: Option<&Condition>,
    ) -> Result<Appended> {
        let mut records = p::RecordsBuf::new();
        for event in events {
            records.push(&event.record())?;
        }
        let condition = condition.map(|c| p::Condition {
            marker: c.marker,
            criteria: wire_criteria(&c.criteria),
        });
        let reply = self
            .conn()
            .request(&p::Frame::Append(p::Append {
                context: self.id,
                condition,
                events: records.as_records(),
            }))
            .await?;
        match reply.decode()? {
            p::Frame::Appended(a) => Ok(Appended::Ok {
                first_position: a.first_position,
                count: a.count,
                marker: a.marker,
            }),
            p::Frame::AppendConflict(c) => Ok(Appended::Conflict {
                position: c.conflicting_position,
            }),
            _ => Err(unexpected(&reply, "instead of Appended")),
        }
    }

    /// Reads matching events from `from` up to the head as of now.
    pub async fn source(
        &self,
        from: u64,
        criteria: &[Criterion],
        batch_size: u32,
    ) -> Result<Source> {
        let stream = self
            .conn()
            .open(
                &p::Frame::Source(p::Source {
                    context: self.id,
                    from,
                    criteria: wire_criteria(criteria),
                    batch_size,
                }),
                true,
            )
            .await?;
        Ok(Source { stream })
    }

    /// Reads a whole result: every batch, and the marker.
    pub async fn source_all(
        &self,
        from: u64,
        criteria: &[Criterion],
    ) -> Result<(Vec<EventBatch>, u64)> {
        let mut source = self.source(from, criteria, 0).await?;
        let mut batches = Vec::new();
        while let Some(batch) = source.next().await? {
            if let Some(marker) = batch.marker() {
                batches.push(batch);
                return Ok((batches, marker));
            }
            batches.push(batch);
        }
        Err(NetError::Closed)
    }

    pub async fn subscribe(&self, from: u64, options: SubscribeOptions) -> Result<Subscription> {
        let stream = self
            .conn()
            .open(
                &p::Frame::Subscribe(p::Subscribe {
                    context: self.id,
                    from,
                    criteria: wire_criteria(&options.criteria),
                    exclude_names: options.exclude_names.iter().map(String::as_str).collect(),
                    batch_size: options.batch_size,
                    progress_interval: options.progress_interval_ms,
                }),
                true,
            )
            .await?;
        Ok(Subscription { stream })
    }

    /// Stores `state` under `key` as a snapshot of the log up to `position`,
    /// the marker of the read it was folded from. Returns where the record
    /// landed in the log.
    pub async fn append_snapshot(&self, key: &[u8], state: &[u8], position: u64) -> Result<u64> {
        let reply = self
            .conn()
            .request(&p::Frame::AppendSnapshot(p::AppendSnapshot {
                context: self.id,
                key,
                state,
                position,
            }))
            .await?;
        match reply.decode()? {
            p::Frame::SnapshotAppended(a) => Ok(a.position),
            _ => Err(unexpected(&reply, "instead of SnapshotAppended")),
        }
    }

    /// The latest snapshot under `key`, if any.
    pub async fn get_snapshot(&self, key: &[u8]) -> Result<Option<Snapshot>> {
        let reply = self
            .conn()
            .request(&p::Frame::GetSnapshot(p::GetSnapshot {
                context: self.id,
                key,
            }))
            .await?;
        match reply.decode()? {
            p::Frame::SnapshotResult(r) => Ok(r.snapshot.map(Snapshot::from_wire)),
            _ => Err(unexpected(&reply, "instead of SnapshotResult")),
        }
    }

    /// A read that starts from the latest snapshot under `key`: the snapshot
    /// first, then the matching events from where it leaves off.
    pub async fn snapshotted_source(
        &self,
        key: &[u8],
        criteria: &[Criterion],
        batch_size: u32,
    ) -> Result<SnapshottedSource> {
        let mut stream = self
            .conn()
            .open(
                &p::Frame::SnapshottedSource(p::SnapshottedSource {
                    context: self.id,
                    criteria: wire_criteria(criteria),
                    key,
                    batch_size,
                }),
                true,
            )
            .await?;
        let first = stream.next().await?.ok_or(NetError::Closed)?;
        let snapshot = match first.decode()? {
            p::Frame::SnapshotResult(r) => r.snapshot.map(Snapshot::from_wire),
            _ => return Err(unexpected(&first, "instead of SnapshotResult")),
        };
        Ok(SnapshottedSource {
            snapshot,
            source: Source { stream },
        })
    }

    pub async fn head(&self) -> Result<u64> {
        let reply = self
            .conn()
            .request(&p::Frame::GetHead(p::GetHead { context: self.id }))
            .await?;
        match reply.decode()? {
            p::Frame::Head(h) => Ok(h.position),
            _ => Err(unexpected(&reply, "instead of Head")),
        }
    }

    /// The tags of the event at `position`.
    pub async fn tags(&self, position: u64) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let reply = self
            .conn()
            .request(&p::Frame::GetTags(p::GetTags {
                context: self.id,
                position,
            }))
            .await?;
        match reply.decode()? {
            p::Frame::Tags(t) => Ok(t
                .tags
                .iter()
                .map(|t| (t.key.to_vec(), t.value.to_vec()))
                .collect()),
            _ => Err(unexpected(&reply, "instead of Tags")),
        }
    }

    pub async fn tail(&self) -> Result<u64> {
        let reply = self
            .conn()
            .request(&p::Frame::GetTail(p::GetTail { context: self.id }))
            .await?;
        match reply.decode()? {
            p::Frame::Tail(t) => Ok(t.position),
            _ => Err(unexpected(&reply, "instead of Tail")),
        }
    }
}

/// A read in progress.
pub struct Source {
    stream: ClientStream,
}

impl Source {
    /// The next batch; the last one carries the marker. `None` after it.
    pub async fn next(&mut self) -> Result<Option<EventBatch>> {
        match self.stream.next().await? {
            Some(frame) => events_frame(frame).map(Some),
            None => Ok(None),
        }
    }
}

/// A read that started from a snapshot, if there was one.
pub struct SnapshottedSource {
    /// The snapshot the events continue from.
    pub snapshot: Option<Snapshot>,
    source: Source,
}

impl SnapshottedSource {
    /// The next batch; the last one carries the marker. `None` after it.
    pub async fn next(&mut self) -> Result<Option<EventBatch>> {
        self.source.next().await
    }
}

#[derive(Debug, Clone, Default)]
pub struct SubscribeOptions {
    pub criteria: Vec<Criterion>,
    pub exclude_names: Vec<String>,
    /// Most events per batch; 0 lets the server choose.
    pub batch_size: u32,
    /// Report progress through quiet stretches this often; 0 never.
    pub progress_interval_ms: u32,
}

/// What a subscription delivers.
#[derive(Debug, Clone)]
pub enum Delivery {
    Events(EventBatch),
    /// Everything before this position that matches has been delivered.
    Progress(u64),
}

/// A live subscription. The server stays at most the connection's window
/// ahead of what is taken from it.
pub struct Subscription {
    stream: ClientStream,
}

impl Subscription {
    pub async fn next(&mut self) -> Result<Option<Delivery>> {
        let Some(frame) = self.stream.next().await? else {
            return Ok(None);
        };
        match frame.decode()? {
            p::Frame::Events(_) => Ok(Some(Delivery::Events(EventBatch { frame }))),
            p::Frame::Progress(pr) => Ok(Some(Delivery::Progress(pr.position))),
            _ => Err(unexpected(&frame, "on a subscription")),
        }
    }

    /// Ends the subscription and waits for the server to confirm.
    pub async fn cancel(self) {
        self.stream.cancel().await
    }
}
