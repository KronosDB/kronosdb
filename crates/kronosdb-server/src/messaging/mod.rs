pub mod command_service;
mod convert;
pub mod fabric;
pub mod query_service;

/// gRPC metadata header naming the messaging bus to route to.
///
/// Buses are independent of event store contexts (ADR-0006): consumers pick
/// a bus per RPC, so one bus can be shared across many contexts, or used
/// with no event store at all. Absent header routes to the default bus.
const BUS_HEADER: &str = "kronosdb-bus";
const DEFAULT_BUS: &str = "default";

/// Resolves the target bus for a messaging RPC from its metadata.
fn bus_from_metadata(metadata: &tonic::metadata::MetadataMap) -> String {
    metadata
        .get(BUS_HEADER)
        .and_then(|v| v.to_str().ok())
        .unwrap_or(DEFAULT_BUS)
        .to_string()
}
