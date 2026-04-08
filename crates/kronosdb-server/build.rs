fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";

    // Compile event store proto only for now.
    // Command and query protos reference common.proto types that need
    // special module mapping — will be added when messaging is implemented.
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[format!("{proto_dir}/eventstore.proto")],
            &[proto_dir],
        )?;

    Ok(())
}
