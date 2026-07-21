//! Proto ↔ internal conversions shared by the command and query services.

use std::collections::HashMap;
use std::time::Duration;

use kronosdb_messaging::types::{
    ErrorDetail, Metadata, MetadataValue, Payload, ProcessingInstruction, ProcessingKey,
    instruction_number,
};

pub fn proto_mv_to_internal(v: crate::proto::kronosdb::MetadataValue) -> MetadataValue {
    match v.data {
        Some(crate::proto::kronosdb::metadata_value::Data::TextValue(s)) => MetadataValue::Text(s),
        Some(crate::proto::kronosdb::metadata_value::Data::NumberValue(n)) => {
            MetadataValue::Number(n)
        }
        Some(crate::proto::kronosdb::metadata_value::Data::BooleanValue(b)) => {
            MetadataValue::Boolean(b)
        }
        Some(crate::proto::kronosdb::metadata_value::Data::DoubleValue(d)) => {
            MetadataValue::Double(d)
        }
        Some(crate::proto::kronosdb::metadata_value::Data::BytesValue(obj)) => {
            MetadataValue::Bytes(Payload {
                payload_type: obj.r#type,
                revision: obj.revision,
                data: obj.data,
            })
        }
        None => MetadataValue::Text(String::new()),
    }
}

pub fn internal_mv_to_proto(v: &MetadataValue) -> crate::proto::kronosdb::MetadataValue {
    let data = match v {
        MetadataValue::Text(s) => Some(crate::proto::kronosdb::metadata_value::Data::TextValue(
            s.clone(),
        )),
        MetadataValue::Number(n) => Some(
            crate::proto::kronosdb::metadata_value::Data::NumberValue(*n),
        ),
        MetadataValue::Boolean(b) => Some(
            crate::proto::kronosdb::metadata_value::Data::BooleanValue(*b),
        ),
        MetadataValue::Double(d) => Some(
            crate::proto::kronosdb::metadata_value::Data::DoubleValue(*d),
        ),
        MetadataValue::Bytes(p) => Some(crate::proto::kronosdb::metadata_value::Data::BytesValue(
            crate::proto::kronosdb::SerializedObject {
                r#type: p.payload_type.clone(),
                revision: p.revision.clone(),
                data: p.data.clone(),
            },
        )),
    };
    crate::proto::kronosdb::MetadataValue { data }
}

pub fn proto_metadata_to_internal(
    meta: HashMap<String, crate::proto::kronosdb::MetadataValue>,
) -> Metadata {
    meta.into_iter()
        .map(|(k, v)| (k, proto_mv_to_internal(v)))
        .collect()
}

pub fn internal_metadata_to_proto(
    meta: &Metadata,
) -> HashMap<String, crate::proto::kronosdb::MetadataValue> {
    meta.iter()
        .map(|(k, v)| (k.clone(), internal_mv_to_proto(v)))
        .collect()
}

pub fn proto_pk_to_internal(key: i32) -> ProcessingKey {
    match key {
        1 => ProcessingKey::Priority,
        2 => ProcessingKey::Timeout,
        3 => ProcessingKey::NrOfResults,
        _ => ProcessingKey::RoutingKey, // 0 and unknown
    }
}

pub fn internal_pk_to_proto(key: ProcessingKey) -> i32 {
    match key {
        ProcessingKey::RoutingKey => 0,
        ProcessingKey::Priority => 1,
        ProcessingKey::Timeout => 2,
        ProcessingKey::NrOfResults => 3,
    }
}

pub fn proto_pi_to_internal(
    pis: Vec<crate::proto::kronosdb::ProcessingInstruction>,
) -> Vec<ProcessingInstruction> {
    pis.into_iter()
        .map(|pi| ProcessingInstruction {
            key: proto_pk_to_internal(pi.key),
            value: pi.value.map(proto_mv_to_internal),
        })
        .collect()
}

pub fn internal_pi_to_proto(
    pis: &[ProcessingInstruction],
) -> Vec<crate::proto::kronosdb::ProcessingInstruction> {
    pis.iter()
        .map(|pi| crate::proto::kronosdb::ProcessingInstruction {
            key: internal_pk_to_proto(pi.key),
            value: pi.value.as_ref().map(internal_mv_to_proto),
        })
        .collect()
}

pub fn proto_error_to_detail(e: crate::proto::kronosdb::ErrorMessage) -> ErrorDetail {
    ErrorDetail {
        message: e.message,
        location: e.location,
        details: e.details,
        error_code: e.error_code,
    }
}

pub fn detail_to_proto_error(e: &ErrorDetail) -> crate::proto::kronosdb::ErrorMessage {
    crate::proto::kronosdb::ErrorMessage {
        message: e.message.clone(),
        location: e.location.clone(),
        details: e.details.clone(),
        error_code: e.error_code.clone(),
    }
}

/// Effective request deadline: the client's `Timeout` processing instruction
/// (milliseconds, clamped to [1s, 1h]) when present, else the server default.
pub fn effective_timeout(
    instructions: &[ProcessingInstruction],
    server_default: Duration,
) -> Duration {
    instruction_number(instructions, ProcessingKey::Timeout)
        .map(|ms| Duration::from_millis(ms.clamp(1_000, 3_600_000) as u64))
        .unwrap_or(server_default)
}

/// Expected result count from the `NrOfResults` processing instruction.
/// `1` selects point-to-point routing; anything else (absent, 0, negative,
/// or >1) is scatter-gather (`-1`).
pub fn expected_results(instructions: &[ProcessingInstruction]) -> i32 {
    match instruction_number(instructions, ProcessingKey::NrOfResults) {
        Some(1) => 1,
        _ => -1,
    }
}
