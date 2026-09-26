fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";

    tonic_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(
            &[
                format!("{proto_dir}/common.proto"),
                format!("{proto_dir}/eventstore.proto"),
                format!("{proto_dir}/platform.proto"),
            ],
            &[proto_dir],
        )?;

    Ok(())
}
