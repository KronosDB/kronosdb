/// A tag is a key-value pair that describes an event.
/// Tags are the primary mechanism for querying and for DCB consistency checks.
/// They are stored in a separate mutable index, not embedded in event data.
///
/// Both key and value are raw bytes, allowing domain-specific encoding.
/// Common usage: key="orderId", value=b"abc-123"
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Tag {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl Tag {
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
        }
    }

    /// Convenience constructor for string key-value tags.
    pub fn from_str(key: &str, value: &str) -> Self {
        Self {
            key: key.as_bytes().to_vec(),
            value: value.as_bytes().to_vec(),
        }
    }
}
