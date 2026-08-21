//! Messaging-fabric e2e (ADR-0007 Tier 2): a command dispatched on node A
//! reaches a handler connected to node B and the response flows back.
//!
//! Two real server processes form a Raft cluster. The handler registers on
//! B only; its registration replicates through the metadata Raft into A's
//! routing table, A's dispatch resolves the remote owner and forwards over
//! the fabric service. Node death is asserted to fail fast (retriable
//! error), not hang.

use std::net::TcpListener;
use std::process::{Child, Command as ProcCommand, Stdio};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;

// Generated proto code; same allows as src/proto.rs.
#[allow(clippy::enum_variant_names, clippy::result_large_err)]
mod pb {
    tonic::include_proto!("kronosdb");
    pub mod command {
        tonic::include_proto!("kronosdb.command");
    }
}

use pb::command::command_service_client::CommandServiceClient;

struct Node {
    child: Child,
    grpc_addr: String,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn spawn_node(node_id: u64, grpc_port: u16, admin_port: u16, peers: &[(u64, u16)]) -> Node {
    let data_dir = format!(
        "{}/fabric-{}-node-{}",
        env!("CARGO_TARGET_TMPDIR"),
        std::process::id(),
        node_id
    );
    let peer_str = peers
        .iter()
        .map(|(id, port)| format!("{id}=127.0.0.1:{port}"))
        .collect::<Vec<_>>()
        .join(",");

    let mut cmd = ProcCommand::new(env!("CARGO_BIN_EXE_kronosdb-server"));
    cmd.env("KRONOSDB_LISTEN", format!("127.0.0.1:{grpc_port}"))
        .env("KRONOSDB_ADMIN_LISTEN", format!("127.0.0.1:{admin_port}"))
        .env("KRONOSDB_DATA_DIR", &data_dir)
        .env("KRONOSDB_NODE_NAME", format!("fabric-node-{node_id}"))
        .env("KRONOSDB_CLUSTER_NODE_ID", node_id.to_string())
        .env("KRONOSDB_CLUSTER_PEERS", &peer_str)
        .env("RUST_LOG", "warn");
    if std::env::var_os("KRONOSDB_TEST_LOGS").is_some() {
        cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    } else {
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
    }
    let child = cmd.spawn().expect("spawn kronosdb-server");
    Node {
        child,
        grpc_addr: format!("http://127.0.0.1:{grpc_port}"),
    }
}

async fn connect(addr: &str) -> Channel {
    for _ in 0..100 {
        if let Ok(channel) = Channel::from_shared(addr.to_string())
            .unwrap()
            .connect()
            .await
        {
            return channel;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("could not connect to {addr}");
}

/// Opens a command handler stream on `channel`, subscribes to
/// `command_name`, grants permits, and echoes every received command's
/// payload back as its response.
async fn start_echo_handler(channel: Channel, command_name: &str, client_id: &str) {
    let mut client = CommandServiceClient::new(channel);
    let (tx, rx) = mpsc::channel(64);

    tx.send(pb::command::CommandHandlerOutbound {
        request: Some(pb::command::command_handler_outbound::Request::Subscribe(
            pb::command::CommandSubscription {
                message_id: "sub-1".into(),
                command: command_name.into(),
                component_name: "fabric-test".into(),
                client_id: client_id.into(),
                load_factor: 100,
            },
        )),
        instruction_id: String::new(),
    })
    .await
    .unwrap();
    tx.send(pb::command::CommandHandlerOutbound {
        request: Some(pb::command::command_handler_outbound::Request::FlowControl(
            pb::FlowControl {
                client_id: client_id.into(),
                permits: 1_000,
            },
        )),
        instruction_id: String::new(),
    })
    .await
    .unwrap();

    let mut inbound = client
        .open_stream(ReceiverStream::new(rx))
        .await
        .expect("open handler stream")
        .into_inner();

    tokio::spawn(async move {
        while let Ok(Some(msg)) = inbound.message().await {
            if let Some(pb::command::command_handler_inbound::Request::Command(cmd)) = msg.request {
                let response = pb::command::CommandHandlerOutbound {
                    request: Some(
                        pb::command::command_handler_outbound::Request::CommandResponse(
                            pb::command::CommandResponse {
                                message_identifier: format!("resp-{}", cmd.message_identifier),
                                request_identifier: cmd.message_identifier,
                                error_code: String::new(),
                                error_message: None,
                                payload: cmd.payload,
                                metadata: Default::default(),
                                processing_instructions: vec![],
                            },
                        ),
                    ),
                    instruction_id: String::new(),
                };
                if tx.send(response).await.is_err() {
                    break;
                }
            }
        }
    });
}

fn make_command(seq: u32, name: &str, routing_key: Option<&str>) -> pb::command::Command {
    let processing_instructions = routing_key
        .map(|key| {
            vec![pb::ProcessingInstruction {
                key: pb::ProcessingKey::RoutingKey as i32,
                value: Some(pb::MetadataValue {
                    data: Some(pb::metadata_value::Data::TextValue(key.to_string())),
                }),
            }]
        })
        .unwrap_or_default();
    pb::command::Command {
        message_identifier: format!("fabric-cmd-{seq}"),
        name: name.into(),
        timestamp: 0,
        payload: Some(pb::SerializedObject {
            r#type: name.into(),
            revision: "1".into(),
            data: format!("payload-{seq}").into_bytes(),
        }),
        metadata: Default::default(),
        processing_instructions,
        client_id: "fabric-dispatcher".into(),
        component_name: "fabric-test".into(),
    }
}

/// Retries dispatch until the handler registration has replicated and the
/// cluster has settled — registration rides a Raft round and leader
/// election takes a moment after spawn.
async fn dispatch_until_ok(
    client: &mut CommandServiceClient<Channel>,
    name: &str,
    routing_key: Option<&str>,
) -> pb::command::CommandResponse {
    let mut seq = 0u32;
    for _ in 0..150 {
        seq += 1;
        match client.dispatch(make_command(seq, name, routing_key)).await {
            Ok(response) => return response.into_inner(),
            Err(_) => tokio::time::sleep(Duration::from_millis(200)).await,
        }
    }
    panic!("dispatch never succeeded for {name}");
}

#[tokio::test(flavor = "multi_thread")]
async fn command_dispatched_on_a_reaches_handler_on_b() {
    let grpc_a = free_port();
    let grpc_b = free_port();
    let admin_a = free_port();
    let admin_b = free_port();
    let peers = [(1u64, grpc_a), (2u64, grpc_b)];

    let node_a = spawn_node(1, grpc_a, admin_a, &peers);
    let node_b = spawn_node(2, grpc_b, admin_b, &peers);

    let channel_a = connect(&node_a.grpc_addr).await;
    let channel_b = connect(&node_b.grpc_addr).await;

    // Handler lives ONLY on node B.
    start_echo_handler(channel_b.clone(), "FabricEcho", "handler-on-b").await;

    // Dispatch through node A: routing table says the owner is on B, so
    // the command crosses the fabric and the response crosses back.
    let mut dispatcher = CommandServiceClient::new(channel_a.clone());
    let response = dispatch_until_ok(&mut dispatcher, "FabricEcho", None).await;
    let payload = response.payload.expect("echoed payload");
    assert!(
        String::from_utf8_lossy(&payload.data).starts_with("payload-"),
        "expected echoed payload, got {payload:?}"
    );

    // Keyed dispatch takes the ring path to the same remote handler.
    let response = dispatch_until_ok(&mut dispatcher, "FabricEcho", Some("order-42")).await;
    assert!(response.payload.is_some());

    // Kill the owning node: dispatch must fail fast with a retriable
    // error, not hang. (The registry row lingers until membership or
    // restart cleanup — the forward itself surfaces the failure.)
    drop(node_b);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let result = tokio::time::timeout(
        Duration::from_secs(10),
        CommandServiceClient::new(channel_a).dispatch(make_command(9_999, "FabricEcho", None)),
    )
    .await;
    match result {
        Ok(Err(_)) => {}
        Ok(Ok(_)) => panic!("dispatch succeeded against a dead handler node"),
        Err(_) => panic!("dispatch hung after handler node death"),
    }
}
