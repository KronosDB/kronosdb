/// A message payload. Opaque to the messaging engine — just bytes with type info.
#[derive(Debug, Clone)]
pub struct Payload {
    /// The type identifier (e.g., "com.example.CreateOrderCommand").
    pub payload_type: String,
    /// The revision/version of the serialized form.
    pub revision: String,
    /// The serialized data.
    pub data: Vec<u8>,
}

/// A metadata value. Supports multiple types to allow lossless transport
/// of metadata from any language runtime (Java objects, .NET types, etc.).
#[derive(Debug, Clone)]
pub enum MetadataValue {
    Text(String),
    Number(i64),
    Boolean(bool),
    Double(f64),
    /// A serialized object (type + revision + bytes).
    Bytes(Payload),
}

/// Metadata attached to messages. Opaque to KronosDB — just transported.
pub type Metadata = std::collections::HashMap<String, MetadataValue>;

/// A processing instruction — routing/priority/timeout hints attached to messages.
#[derive(Debug, Clone)]
pub struct ProcessingInstruction {
    pub key: ProcessingKey,
    pub value: Option<MetadataValue>,
}

/// Processing instruction keys.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessingKey {
    RoutingKey,
    Priority,
    Timeout,
    NrOfResults,
}

/// Identifies a connected client instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClientId(pub String);

/// Identifies a component (application) type. Multiple client instances
/// can share the same component name (e.g., "order-service" running on 3 nodes).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ComponentName(pub String);

/// A routing key for consistent hashing. Commands with the same routing key
/// always go to the same handler instance.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoutingKey(pub String);

/// Structured error detail. Mirrors the gRPC ErrorMessage so the full error
/// context (exception class, stack trace, location) survives the round-trip
/// through KronosDB without being flattened to a string.
#[derive(Debug, Clone)]
pub struct ErrorDetail {
    /// Human-readable error message.
    pub message: String,
    /// Where the error occurred (handler component, server, etc.).
    pub location: String,
    /// Additional context — root cause chain, exception class names, stack frames.
    pub details: Vec<String>,
    /// Error code (e.g. "AXONIQ-4002" or application-specific).
    pub error_code: String,
}
