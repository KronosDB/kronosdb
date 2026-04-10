fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";

    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                format!("{proto_dir}/common.proto"),
                format!("{proto_dir}/eventstore.proto"),
                format!("{proto_dir}/command.proto"),
                format!("{proto_dir}/query.proto"),
                format!("{proto_dir}/platform.proto"),
                format!("{proto_dir}/snapshot.proto"),
            ],
            &[proto_dir],
        )?;

    Ok(())
}
