//! Generated protobuf types and gRPC service definitions.

/// Common types shared across services (SerializedObject, MetadataValue, etc.)
// result_large_err: generated service methods return Result<_, tonic::Status>;
// clippy 1.98 flags the Status variant's size in code we don't author.
#[allow(clippy::enum_variant_names, clippy::result_large_err)]
pub mod kronosdb {
    tonic::include_proto!("kronosdb");

    pub mod eventstore {
        tonic::include_proto!("kronosdb.eventstore");
    }

    pub mod command {
        tonic::include_proto!("kronosdb.command");
    }

    pub mod query {
        tonic::include_proto!("kronosdb.query");
    }

    pub mod platform {
        tonic::include_proto!("kronosdb.platform");
    }

    pub mod scheduler {
        tonic::include_proto!("kronosdb.scheduler");
    }

    pub mod fabric {
        tonic::include_proto!("kronosdb.fabric");
    }
}
