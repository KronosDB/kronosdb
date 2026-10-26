//! The primitives the generated code is built on: a zero-copy reader,
//! length-checked writers, stored event records, and frame framing.

use std::fmt;

use crate::generated::{EventRecord, Frame};

/// Bytes every connection starts with, before the client's Hello.
pub const PREFACE: &[u8; 8] = b"KRONOSDB";

/// `len u32 | kind u16 | flags u8 | stream u32`; `len` counts everything after itself.
pub const FRAME_HEADER_LEN: usize = 11;

/// Set on the last frame a sender will send on a stream.
pub const FLAG_END: u8 = 0x01;

/// Bytes that are not a valid encoding of what was expected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    /// The input ended `needed` bytes short at offset `at`.
    Truncated { needed: usize, at: usize },
    /// A string field is not UTF-8.
    InvalidUtf8 { at: usize },
    /// A bool or option byte other than 0 or 1.
    InvalidFlagByte { value: u8, at: usize },
    /// A record whose length field is zero (it must count at least `flags`).
    ZeroLengthRecord { at: usize },
    /// A record body with bytes after its last field.
    TrailingRecordBytes { extra: usize },
    /// A frame whose length field is shorter than the header.
    ShortFrame { len: u32 },
    /// A union tag this version does not define.
    UnknownVariant {
        union: &'static str,
        tag: u8,
        at: usize,
    },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::Truncated { needed, at } => {
                write!(f, "truncated: {needed} more bytes needed at offset {at}")
            }
            DecodeError::InvalidUtf8 { at } => {
                write!(f, "invalid UTF-8 in the string at offset {at}")
            }
            DecodeError::InvalidFlagByte { value, at } => {
                write!(f, "expected 0 or 1 at offset {at}, got {value}")
            }
            DecodeError::ZeroLengthRecord { at } => write!(f, "zero-length record at offset {at}"),
            DecodeError::TrailingRecordBytes { extra } => {
                write!(f, "record body has {extra} bytes after its last field")
            }
            DecodeError::ShortFrame { len } => {
                write!(f, "frame length {len} is shorter than its header")
            }
            DecodeError::UnknownVariant { union, tag, at } => {
                write!(f, "{union} has no variant with tag {tag} (offset {at})")
            }
        }
    }
}

impl std::error::Error for DecodeError {}

/// A value too long for the length field its schema type gives it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodeError {
    /// `Struct.field` that overflowed.
    pub field: &'static str,
    pub len: usize,
    pub max: usize,
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} is {} long; the limit is {}",
            self.field, self.len, self.max
        )
    }
}

impl std::error::Error for EncodeError {}

/// Reads little-endian fields, borrowing strings and bytes from the input.
pub struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    pub fn position(&self) -> usize {
        self.pos
    }

    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// The bytes read since offset `start`.
    pub fn consumed_since(&self, start: usize) -> &'a [u8] {
        &self.buf[start..self.pos]
    }

    pub fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if n > self.remaining() {
            return Err(DecodeError::Truncated {
                needed: n - self.remaining(),
                at: self.pos,
            });
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], DecodeError> {
        Ok(self.take(N)?.try_into().expect("take returned N bytes"))
    }

    pub fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.take(1)?[0])
    }

    pub fn u16(&mut self) -> Result<u16, DecodeError> {
        Ok(u16::from_le_bytes(self.array()?))
    }

    pub fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    pub fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    pub fn i64(&mut self) -> Result<i64, DecodeError> {
        Ok(i64::from_le_bytes(self.array()?))
    }

    pub fn f64(&mut self) -> Result<f64, DecodeError> {
        Ok(f64::from_le_bytes(self.array()?))
    }

    fn flag(&mut self) -> Result<bool, DecodeError> {
        let at = self.pos;
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            value => Err(DecodeError::InvalidFlagByte { value, at }),
        }
    }

    pub fn bool(&mut self) -> Result<bool, DecodeError> {
        self.flag()
    }

    /// Reads an `opt<T>` presence byte.
    pub fn present(&mut self) -> Result<bool, DecodeError> {
        self.flag()
    }

    pub fn len16(&mut self) -> Result<usize, DecodeError> {
        Ok(self.u16()? as usize)
    }

    pub fn len32(&mut self) -> Result<usize, DecodeError> {
        Ok(self.u32()? as usize)
    }

    fn utf8(&mut self, len: usize) -> Result<&'a str, DecodeError> {
        let at = self.pos;
        std::str::from_utf8(self.take(len)?).map_err(|_| DecodeError::InvalidUtf8 { at })
    }

    pub fn str16(&mut self) -> Result<&'a str, DecodeError> {
        let len = self.len16()?;
        self.utf8(len)
    }

    pub fn str32(&mut self) -> Result<&'a str, DecodeError> {
        let len = self.len32()?;
        self.utf8(len)
    }

    pub fn bytes16(&mut self) -> Result<&'a [u8], DecodeError> {
        let len = self.len16()?;
        self.take(len)
    }

    pub fn bytes32(&mut self) -> Result<&'a [u8], DecodeError> {
        let len = self.len32()?;
        self.take(len)
    }
}

pub fn put_u8(w: &mut Vec<u8>, v: u8) {
    w.push(v);
}

pub fn put_u16(w: &mut Vec<u8>, v: u16) {
    w.extend_from_slice(&v.to_le_bytes());
}

pub fn put_u32(w: &mut Vec<u8>, v: u32) {
    w.extend_from_slice(&v.to_le_bytes());
}

pub fn put_u64(w: &mut Vec<u8>, v: u64) {
    w.extend_from_slice(&v.to_le_bytes());
}

pub fn put_i64(w: &mut Vec<u8>, v: i64) {
    w.extend_from_slice(&v.to_le_bytes());
}

pub fn put_f64(w: &mut Vec<u8>, v: f64) {
    w.extend_from_slice(&v.to_le_bytes());
}

pub fn put_len16(w: &mut Vec<u8>, len: usize, field: &'static str) -> Result<(), EncodeError> {
    let n = u16::try_from(len).map_err(|_| EncodeError {
        field,
        len,
        max: u16::MAX as usize,
    })?;
    put_u16(w, n);
    Ok(())
}

pub fn put_len32(w: &mut Vec<u8>, len: usize, field: &'static str) -> Result<(), EncodeError> {
    let n = u32::try_from(len).map_err(|_| EncodeError {
        field,
        len,
        max: u32::MAX as usize,
    })?;
    put_u32(w, n);
    Ok(())
}

pub fn put_str16(w: &mut Vec<u8>, s: &str, field: &'static str) -> Result<(), EncodeError> {
    put_bytes16(w, s.as_bytes(), field)
}

pub fn put_str32(w: &mut Vec<u8>, s: &str, field: &'static str) -> Result<(), EncodeError> {
    put_bytes32(w, s.as_bytes(), field)
}

pub fn put_bytes16(w: &mut Vec<u8>, b: &[u8], field: &'static str) -> Result<(), EncodeError> {
    put_len16(w, b.len(), field)?;
    w.extend_from_slice(b);
    Ok(())
}

pub fn put_bytes32(w: &mut Vec<u8>, b: &[u8], field: &'static str) -> Result<(), EncodeError> {
    put_len32(w, b.len(), field)?;
    w.extend_from_slice(b);
    Ok(())
}

#[cfg(test)]
pub(crate) fn hex(b: &[u8]) -> String {
    b.iter().map(|x| format!("{x:02x}")).collect()
}

/// CRC32C of a record's `flags` and body: what the stored record's leading
/// `crc` field holds on disk and in replication.
pub fn record_crc(flags: u8, body: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(&[flags]), body)
}

/// One event record as it travels to and from clients: `len u32 | flags u8 |
/// body`. On disk the same bytes follow the record's CRC, so a stored record
/// goes on the wire as the slice that starts four bytes in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordRef<'a> {
    pub flags: u8,
    pub body: &'a [u8],
}

impl<'a> RecordRef<'a> {
    /// `len u32 | flags u8`.
    pub const HEADER_LEN: usize = 5;

    pub fn decode(r: &mut Reader<'a>) -> Result<Self, DecodeError> {
        let at = r.position();
        let len = r.u32()? as usize;
        if len == 0 {
            return Err(DecodeError::ZeroLengthRecord { at });
        }
        let flags = r.u8()?;
        let body = r.take(len - 1)?;
        Ok(Self { flags, body })
    }

    pub fn encode(&self, w: &mut Vec<u8>) -> Result<(), EncodeError> {
        put_len32(w, 1 + self.body.len(), "record")?;
        put_u8(w, self.flags);
        w.extend_from_slice(self.body);
        Ok(())
    }

    /// Bytes this record occupies, header included.
    pub fn encoded_len(&self) -> usize {
        Self::HEADER_LEN + self.body.len()
    }

    /// The CRC the record carries on disk.
    pub fn crc(&self) -> u32 {
        record_crc(self.flags, self.body)
    }

    /// Decodes the body as an event. Every byte of the body must be used.
    pub fn event(&self) -> Result<EventRecord<'a>, DecodeError> {
        let mut r = Reader::new(self.body);
        let event = EventRecord::decode(&mut r)?;
        match r.remaining() {
            0 => Ok(event),
            extra => Err(DecodeError::TrailingRecordBytes { extra }),
        }
    }
}

/// `records`: a count and that many stored records, borrowed as one slice.
///
/// Decoding checks every record's framing, so iteration cannot fail. The
/// server builds one straight from segment bytes; nothing is re-encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Records<'a> {
    count: u32,
    bytes: &'a [u8],
}

impl<'a> Records<'a> {
    pub fn decode(r: &mut Reader<'a>) -> Result<Self, DecodeError> {
        let count = r.u32()?;
        let start = r.position();
        for _ in 0..count {
            RecordRef::decode(r)?;
        }
        Ok(Self {
            count,
            bytes: r.consumed_since(start),
        })
    }

    /// Wraps `count` records already laid out back to back in `bytes`,
    /// checking their framing.
    pub fn from_raw(count: u32, bytes: &'a [u8]) -> Result<Self, DecodeError> {
        let mut r = Reader::new(bytes);
        for _ in 0..count {
            RecordRef::decode(&mut r)?;
        }
        if r.remaining() != 0 {
            return Err(DecodeError::TrailingRecordBytes {
                extra: r.remaining(),
            });
        }
        Ok(Self { count, bytes })
    }

    pub fn encode(&self, w: &mut Vec<u8>) -> Result<(), EncodeError> {
        put_u32(w, self.count);
        w.extend_from_slice(self.bytes);
        Ok(())
    }

    pub fn len(&self) -> u32 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// The records' bytes, back to back, exactly as on disk.
    pub fn bytes(&self) -> &'a [u8] {
        self.bytes
    }

    pub fn iter(&self) -> RecordIter<'a> {
        RecordIter {
            reader: Reader::new(self.bytes),
            left: self.count,
        }
    }
}

pub struct RecordIter<'a> {
    reader: Reader<'a>,
    left: u32,
}

impl<'a> Iterator for RecordIter<'a> {
    type Item = RecordRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.left == 0 {
            return None;
        }
        self.left -= 1;
        Some(
            RecordRef::decode(&mut self.reader)
                .expect("framing was checked when the records were built"),
        )
    }
}

/// Builds a `records` value from events, for clients appending.
#[derive(Debug, Default, Clone)]
pub struct RecordsBuf {
    count: u32,
    bytes: Vec<u8>,
}

impl RecordsBuf {
    pub fn new() -> Self {
        Self::default()
    }

    /// Appends `event` as a record.
    pub fn push(&mut self, event: &EventRecord<'_>) -> Result<(), EncodeError> {
        let start = self.bytes.len();
        put_u32(&mut self.bytes, 0);
        put_u8(&mut self.bytes, 0);
        event.encode(&mut self.bytes)?;
        let len = self.bytes.len() - start - 4;
        let len = u32::try_from(len).map_err(|_| EncodeError {
            field: "record",
            len,
            max: u32::MAX as usize,
        })?;
        self.bytes[start..start + 4].copy_from_slice(&len.to_le_bytes());
        self.count += 1;
        Ok(())
    }

    /// Appends a record that is already encoded, such as one copied from a
    /// segment.
    pub fn push_raw(&mut self, record: RecordRef<'_>) -> Result<(), EncodeError> {
        record.encode(&mut self.bytes)?;
        self.count += 1;
        Ok(())
    }

    pub fn len(&self) -> u32 {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Encoded size so far, for cutting batches by bytes.
    pub fn byte_len(&self) -> usize {
        self.bytes.len()
    }

    pub fn as_records(&self) -> Records<'_> {
        Records {
            count: self.count,
            bytes: &self.bytes,
        }
    }

    pub fn clear(&mut self) {
        self.count = 0;
        self.bytes.clear();
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    pub kind: u16,
    pub flags: u8,
    pub stream: u32,
}

impl FrameHeader {
    pub fn is_end(&self) -> bool {
        self.flags & FLAG_END != 0
    }
}

/// Appends a whole frame, header and body, to `w`.
pub fn encode_frame(
    w: &mut Vec<u8>,
    stream: u32,
    flags: u8,
    frame: &Frame<'_>,
) -> Result<(), EncodeError> {
    let start = w.len();
    put_u32(w, 0);
    put_u16(w, frame.kind());
    put_u8(w, flags);
    put_u32(w, stream);
    frame.encode_body(w)?;
    let len = w.len() - start - 4;
    let len = u32::try_from(len).map_err(|_| EncodeError {
        field: "frame",
        len,
        max: u32::MAX as usize,
    })?;
    w[start..start + 4].copy_from_slice(&len.to_le_bytes());
    Ok(())
}

/// The first whole frame in a buffer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SplitFrame<'a> {
    pub header: FrameHeader,
    pub body: &'a [u8],
    /// Bytes the frame occupies, header included: where the next one starts.
    pub len: usize,
}

/// Splits the first frame off `buf`. `Ok(None)` until the whole frame has arrived.
pub fn split_frame(buf: &[u8]) -> Result<Option<SplitFrame<'_>>, DecodeError> {
    if buf.len() < FRAME_HEADER_LEN {
        return Ok(None);
    }
    let len = u32::from_le_bytes(buf[0..4].try_into().unwrap());
    if (len as usize) < FRAME_HEADER_LEN - 4 {
        return Err(DecodeError::ShortFrame { len });
    }
    let total = 4 + len as usize;
    if buf.len() < total {
        return Ok(None);
    }
    let header = FrameHeader {
        kind: u16::from_le_bytes(buf[4..6].try_into().unwrap()),
        flags: buf[6],
        stream: u32::from_le_bytes(buf[7..11].try_into().unwrap()),
    };
    Ok(Some(SplitFrame {
        header,
        body: &buf[FRAME_HEADER_LEN..total],
        len: total,
    }))
}
