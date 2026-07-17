//! Shared physical-record parsing for segment recovery, replication, and scans.

use crate::error::Error;

use super::format::{self, ControlRecord};
use super::{RECORD_HEADER_SIZE, flags};

#[derive(Debug, Clone, Copy)]
pub struct RecordHeader {
    pub stored_crc: u32,
    pub flags: u8,
    pub payload_len: usize,
}

impl RecordHeader {
    pub fn total_len(self) -> usize {
        RECORD_HEADER_SIZE + self.payload_len
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeRecord {
    Event { position: u64 },
    Control(ControlRecord),
}

/// Decodes a physical header. An all-zero preallocated tail returns `None`.
pub fn parse_header(bytes: &[u8; RECORD_HEADER_SIZE]) -> Result<Option<RecordHeader>, Error> {
    let record_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
    if record_len == 0 {
        return Ok(None);
    }
    Ok(Some(RecordHeader {
        stored_crc: u32::from_le_bytes(bytes[0..4].try_into().unwrap()),
        flags: bytes[8],
        payload_len: record_len - 1,
    }))
}

pub fn validate_crc(header: RecordHeader, payload: &[u8]) -> bool {
    if payload.len() != header.payload_len {
        return false;
    }
    let mut crc = crc32c::crc32c(&[header.flags]);
    crc = crc32c::crc32c_append(crc, payload);
    crc == header.stored_crc
}

/// Validates the native record type and returns its logical meaning.
pub fn decode_native(header: RecordHeader, payload: &[u8]) -> Result<NativeRecord, Error> {
    if payload.len() != header.payload_len {
        return Err(Error::Corrupted {
            message: "segment record payload length mismatch".into(),
        });
    }
    if flags::is_event(header.flags) {
        if payload.len() < 8 {
            return Err(Error::Corrupted {
                message: "event segment record has no position".into(),
            });
        }
        return Ok(NativeRecord::Event {
            position: u64::from_le_bytes(payload[0..8].try_into().unwrap()),
        });
    }
    if flags::is_control(header.flags) {
        return Ok(NativeRecord::Control(format::deserialize_control(
            header.flags,
            payload,
        )?));
    }
    Err(Error::Corrupted {
        message: format!("unknown segment record flags 0x{:02x}", header.flags),
    })
}
