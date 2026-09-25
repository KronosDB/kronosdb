fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        // Payload fields decode to `Bytes`, not `Vec<u8>`: an event handed to
        // N subscribers is then N refcount bumps, not N copies, and a decoded
        // request borrows the receive buffer instead of copying out of it.
        .bytes([
            ".kronosdb.eventstore.Event.payload",
            ".kronosdb.SerializedObject.data",
        ])
        .compile_protos(
            &[
                format!("{proto_dir}/common.proto"),
                format!("{proto_dir}/eventstore.proto"),
                format!("{proto_dir}/command.proto"),
                format!("{proto_dir}/query.proto"),
                format!("{proto_dir}/platform.proto"),
                format!("{proto_dir}/scheduler.proto"),
                format!("{proto_dir}/fabric.proto"),
            ],
            &[proto_dir],
        )?;

    Ok(())
}
