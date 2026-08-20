//! Generated protobuf types and gRPC service definitions.

/// Common types shared across services (SerializedObject, MetadataValue, etc.)
#[allow(clippy::enum_variant_names)]
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
}
