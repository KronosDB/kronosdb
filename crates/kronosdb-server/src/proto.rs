/// Generated protobuf types and gRPC service definitions.

/// Common types shared across services (SerializedObject, MetadataValue, etc.)
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
}
