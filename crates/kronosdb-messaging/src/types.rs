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

/// Metadata key-value pairs attached to messages.
pub type Metadata = Vec<(String, String)>;

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
