mod admin;
mod auth;
mod config;
mod eventstore;
mod handler_registry;
mod manifest;
mod messaging;
mod platform;
mod processor;
mod proto;

use std::sync::Arc;
use std::time::Duration;

use tokio::signal;
use tonic::transport::{Certificate, Identity, Server, ServerTlsConfig};
use tracing::{error, info, warn};

use kronosdb_eventstore::context::ContextManager;
use kronosdb_eventstore::raft::cluster::{ClusterConfig, ClusterManager, NodeType, PeerConfig};
use kronosdb_eventstore::raft::network::RAFT_MAX_MESSAGE_BYTES;
use kronosdb_eventstore::raft::proto::raft_transport_server::RaftTransportServer;
use kronosdb_eventstore::raft::transport::RaftTransportService;
use kronosdb_eventstore::raft::types::default_raft_config;
use kronosdb_eventstore::replication::PEER_MAX_MESSAGE_BYTES;
use kronosdb_eventstore::replication::peer::{PeerTlsConfig, PeerTransportConfig};
use kronosdb_eventstore::replication::proto::segment_replication_server::SegmentReplicationServer;
use kronosdb_eventstore::replication::service::SegmentReplicationService;
use kronosdb_eventstore::store::StoreOptions;
use kronosdb_messaging::client::ClientRegistry;
use kronosdb_messaging::manager::MessagingManager;

use crate::config::ServerConfig;
use crate::eventstore::service::EventStoreService;
use crate::eventstore::snapshot_service::SnapshotServiceImpl;
use crate::messaging::command_service::CommandServiceImpl;
use crate::messaging::query_service::QueryServiceImpl;
use crate::platform::service::{ClientChannelRegistry, PlatformServiceImpl, spawn_reaper};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize structured logging. KRONOSDB_LOG_FORMAT=json switches to
    // newline-delimited JSON for log aggregation (Loki, CloudWatch, etc.).
    let env_filter = || {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| "kronosdb=info,warn".into())
    };
    if std::env::var("KRONOSDB_LOG_FORMAT").as_deref() == Ok("json") {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter())
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter())
            .init();
    }

    let config = ServerConfig::parse()?;

    // Native segment quorum topology is required before contexts open: it
    // determines recovery's initial watermark (a clustered node may not treat
    // its local durable tail as quorum-committed).
    let node_id = config.cluster_node_id.unwrap_or(1);
    let voter_ids: Vec<u64> = if config.cluster_peers.is_empty() {
        vec![node_id]
    } else {
        config.cluster_peers.iter().map(|peer| peer.id).collect()
    };

    // Create the context manager and ensure a default context exists.
    let store_options = StoreOptions {
        max_segment_size: config.segment_size,
        index_cache_size: config.index_cache_size,
        bloom_cache_size: config.bloom_cache_size,
        group_commit_interval_ms: config.group_commit_ms,
        node_id,
        voters: voter_ids,
    };
    let contexts = Arc::new(ContextManager::with_options(
        &config.data_dir,
        store_options,
    )?);
    if !contexts.context_exists("default") {
        contexts.create_context("default")?;
    }

    // Apply the declarative manifest (if configured) BEFORE Raft init, the
    // same way the default context is bootstrapped. Idempotent: existing
    // contexts are untouched, nothing is ever deleted. In a cluster, every
    // node must be given the same manifest.
    if let Some(ref manifest_path) = config.manifest {
        let manifest = manifest::load(manifest_path)?;
        let created = manifest::apply(&manifest, &contexts)?;
        tracing::info!(
            manifest = %manifest_path.display(),
            declared = manifest.contexts.len(),
            created = ?created,
            "applied declarative manifest"
        );
    }

    // Create the cluster manager — every node is always a Raft node.
    // A node with no peers starts as a single-node cluster (instant leader).
    let node_type = match config.cluster_node_type.as_str() {
        "passive-backup" => NodeType::PassiveBackup,
        _ => NodeType::Standard,
    };

    let voters: Vec<PeerConfig> = if config.cluster_peers.is_empty() {
        // Single-node: this node is the only voter.
        vec![PeerConfig {
            id: node_id,
            addr: config.listen_addr.to_string(),
        }]
    } else {
        config
            .cluster_peers
            .iter()
            .map(|p| PeerConfig {
                id: p.id,
                addr: p.addr.clone(),
            })
            .collect()
    };

    let learners: Vec<PeerConfig> = config
        .cluster_learners
        .iter()
        .map(|p| PeerConfig {
            id: p.id,
            addr: p.addr.clone(),
        })
        .collect();

    let peer_tls = match (&config.tls_cert, &config.tls_key) {
        (Some(cert_path), Some(key_path)) => Some(PeerTlsConfig {
            ca_certificate: config.tls_ca.as_ref().map(std::fs::read).transpose()?,
            identity_certificate: std::fs::read(cert_path)?,
            identity_key: std::fs::read(key_path)?,
        }),
        _ => None,
    };
    let peer_transport = PeerTransportConfig {
        tls: peer_tls,
        access_token: config.access_token.clone(),
    };

    let cluster_config = ClusterConfig {
        node_id,
        node_type,
        advertise_addr: config.listen_addr.to_string(),
        voters,
        learners,
        raft_config: default_raft_config(),
        peer_transport,
    };

    // Log what the durability configuration means for this node. This is the
    // operational "nicety" — a one-liner that makes it obvious whether the operator
    // has chosen a safe, crash-tolerant setup. Strict-only: Window mode is
    // out-of-scope per PROJECT.md non-goals.
    log_durability_summary(&cluster_config, config.group_commit_ms);

    let cluster = Arc::new(ClusterManager::new(Arc::clone(&contexts), cluster_config));

    // One shared metadata Raft group per node; every context registers its
    cluster.init_raft().await?;
    for ctx_name in contexts.list_contexts() {
        cluster.register_context(&ctx_name)?;
    }

    cluster.bootstrap().await?;

    for learner in &config.cluster_learners {
        let _ = cluster.add_learner(learner.id, learner.addr.clone()).await;
    }

    // Create the messaging manager (per-context) and client registries.
    let messaging = Arc::new(MessagingManager::new());
    let client_registry = Arc::new(ClientRegistry::new());
    let channel_registry = Arc::new(ClientChannelRegistry::new());
    let processor_registry = Arc::new(processor::ProcessorRegistry::new());
    let handler_stream_registry = Arc::new(handler_registry::HandlerStreamRegistry::new());

    // Build gRPC services.
    let event_store_service = EventStoreService::new(Arc::clone(&cluster));
    let snapshot_service = SnapshotServiceImpl::new(Arc::clone(&cluster));
    let command_service = CommandServiceImpl::new(
        Arc::clone(&messaging),
        Duration::from_secs(config.command_timeout_secs),
        Arc::clone(&channel_registry),
        Arc::clone(&handler_stream_registry),
    );
    let query_service = QueryServiceImpl::new(
        Arc::clone(&messaging),
        Duration::from_secs(config.query_timeout_secs),
        Arc::clone(&channel_registry),
        Arc::clone(&handler_stream_registry),
    );

    let heartbeat_interval = Duration::from_secs(config.heartbeat_interval_secs);
    let heartbeat_timeout = Duration::from_secs(config.heartbeat_timeout_secs);

    let context_names = {
        let contexts = Arc::clone(&contexts);
        Arc::new(move || contexts.list_contexts()) as Arc<dyn Fn() -> Vec<String> + Send + Sync>
    };
    // Platform service uses the default context for client cleanup.
    let default_platform = messaging.get_platform("default");
    let platform_service = PlatformServiceImpl::new(
        Arc::clone(&client_registry),
        Arc::clone(&channel_registry),
        Arc::clone(&processor_registry),
        Arc::clone(&messaging),
        Arc::clone(&handler_stream_registry),
        default_platform.clone(),
        context_names,
        config.node_name.clone(),
        heartbeat_interval,
        heartbeat_timeout,
    );

    // Spawn background heartbeat reaper.
    let _reaper = spawn_reaper(
        Arc::clone(&client_registry),
        default_platform,
        heartbeat_timeout,
    );

    // Background safety-net sweep: any in-flight command older than 2× the
    // per-request timeout gets a KRONOSDB-4005 failure. The dispatch RPC
    // already enforces command_timeout per request, but the sweep catches
    // any leaks (e.g. caller dropped without timeout firing).
    let sweep_messaging = Arc::clone(&messaging);
    let sweep_timeout = Duration::from_secs(config.command_timeout_secs.saturating_mul(2));
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(30));
        interval.tick().await; // first tick fires immediately — discard.
        loop {
            interval.tick().await;
            for ctx_name in sweep_messaging.list_contexts() {
                let platform = sweep_messaging.get_platform(&ctx_name);
                let swept = platform.sweep_command_timeouts(sweep_timeout);
                if !swept.is_empty() {
                    warn!(
                        context = %ctx_name,
                        swept = swept.len(),
                        "command sweep: cancelled in-flight commands past deadline"
                    );
                }
            }
        }
    });

    // Start admin HTTP server in the background.
    let admin_auth = Arc::new(admin::auth::AuthRuntime::new(&config.admin_auth));
    let admin_state = admin::AdminState {
        config: config.clone(),
        contexts: Arc::clone(&contexts),
        client_registry: Arc::clone(&client_registry),
        messaging: Arc::clone(&messaging),
        cluster: Arc::clone(&cluster),
        processor_registry: Arc::clone(&processor_registry),
        channel_registry: Arc::clone(&channel_registry),
        started_at: std::time::Instant::now(),
        auth: admin_auth,
    };
    tokio::spawn(async move {
        if let Err(e) = admin::start_admin_server(admin_state).await {
            error!(error = %e, "admin server failed");
        }
    });

    let nodes = if cluster.is_multi_node() {
        "multi-node"
    } else {
        "single-node"
    };
    let tls_enabled = config.tls_cert.is_some() && config.tls_key.is_some();
    let auth_enabled = config.access_token.is_some();
    info!(
        version = env!("CARGO_PKG_VERSION"),
        listen = %config.listen_addr,
        cluster = nodes,
        node_id = node_id,
        node = %config.node_name,
        data_dir = %config.data_dir.display(),
        admin = %config.admin_listen_addr,
        tls = tls_enabled,
        auth = auth_enabled,
        contexts = ?contexts.list_contexts(),
        "KronosDB starting"
    );

    // Build auth interceptor (no-op when access_token is None).
    let auth = auth::make_auth_interceptor(config.access_token.clone());

    // Import generated gRPC server types.
    use crate::proto::kronosdb::command::command_service_server::CommandServiceServer;
    use crate::proto::kronosdb::eventstore::event_store_server::EventStoreServer;
    use crate::proto::kronosdb::platform::platform_service_server::PlatformServiceServer;
    use crate::proto::kronosdb::query::query_service_server::QueryServiceServer;
    use crate::proto::kronosdb::snapshot::snapshot_store_server::SnapshotStoreServer;

    // Configure TLS and gRPC keepalive. The ping cadence keeps idle
    // connector streams from being silently dropped by middleboxes while
    // tolerating multi-second CPU saturation: a timeout shorter than a bad
    // scheduling stall turns overload into connection kills for every
    // stream on the connection. tonic delegates PING handling to h2.
    let make_builder = || {
        Server::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(10)))
            .http2_keepalive_timeout(Some(Duration::from_secs(20)))
            .tcp_keepalive(Some(Duration::from_secs(60)))
            .http2_adaptive_window(Some(true))
            // DCB clients legitimately issue very high rates of short Source
            // reads and cancel the stream once they have what they need. With
            // h2's default budget (20), that pattern trips the Rapid-Reset
            // mitigation ("too_many_internal_resets") and kills the whole
            // connection under load.
            .http2_max_pending_accept_reset_streams(Some(2048))
    };

    let mut server = if let (Some(cert_path), Some(key_path)) = (&config.tls_cert, &config.tls_key)
    {
        let cert = std::fs::read(cert_path)
            .map_err(|e| format!("failed to read TLS cert '{}': {e}", cert_path.display()))?;
        let key = std::fs::read(key_path)
            .map_err(|e| format!("failed to read TLS key '{}': {e}", key_path.display()))?;
        let identity = Identity::from_pem(&cert, &key);

        let mut tls = ServerTlsConfig::new().identity(identity);

        if let Some(ca_path) = &config.tls_ca {
            let ca = std::fs::read(ca_path)
                .map_err(|e| format!("failed to read TLS CA '{}': {e}", ca_path.display()))?;
            let ca_cert = Certificate::from_pem(&ca);
            tls = tls.client_ca_root(ca_cert);
            info!("mTLS enabled (client certificate verification)");
        }

        make_builder().tls_config(tls)?
    } else {
        if config.tls_cert.is_some() || config.tls_key.is_some() {
            warn!("both --tls-cert and --tls-key must be set for TLS; running without TLS");
        }
        make_builder()
    };

    // Build gRPC router with auth interceptor on client-facing services.
    let mut router = server
        .add_service(EventStoreServer::with_interceptor(
            event_store_service,
            auth.clone(),
        ))
        .add_service(SnapshotStoreServer::with_interceptor(
            snapshot_service,
            auth.clone(),
        ))
        .add_service(CommandServiceServer::with_interceptor(
            command_service,
            auth.clone(),
        ))
        .add_service(QueryServiceServer::with_interceptor(
            query_service,
            auth.clone(),
        ))
        .add_service(PlatformServiceServer::with_interceptor(
            platform_service,
            auth.clone(),
        ));

    // Raft transport — always enabled (every node is a Raft node).
    let raft_node = cluster
        .raft_node()
        .expect("shared Raft node must be initialized");
    let raft_transport = RaftTransportService::new(Arc::clone(&raft_node));
    let raft_server = RaftTransportServer::new(raft_transport)
        .max_decoding_message_size(RAFT_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(RAFT_MAX_MESSAGE_BYTES);
    let raft_server =
        tonic::service::interceptor::InterceptedService::new(raft_server, auth.clone());
    router = router.add_service(raft_server);

    // Native segment Tail transport uses the same authentication, TLS, and
    // message limits as the metadata control plane.
    let segment_replication = SegmentReplicationService::new(
        Arc::clone(&contexts),
        cluster.replication_control(),
        config.replication_inflight_bytes,
    );
    let replication_server = SegmentReplicationServer::new(segment_replication)
        .max_decoding_message_size(PEER_MAX_MESSAGE_BYTES)
        .max_encoding_message_size(PEER_MAX_MESSAGE_BYTES);
    let replication_server =
        tonic::service::interceptor::InterceptedService::new(replication_server, auth);
    router = router.add_service(replication_server);

    // grpc.health.v1 — unauthenticated by design (kubelet probes and gRPC
    // client-side health checking). Overall status tracks Raft leadership:
    // SERVING only while a leader is known, mirroring the admin `/ready`.
    let (health_reporter, health_service) = tonic_health::server::health_reporter();
    router = router.add_service(health_service);
    {
        let health_cluster = Arc::clone(&cluster);
        let reporter = health_reporter.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                interval.tick().await;
                let serving = health_cluster.native_ready();
                if serving {
                    reporter
                        .set_service_status("", tonic_health::ServingStatus::Serving)
                        .await;
                } else {
                    reporter
                        .set_service_status("", tonic_health::ServingStatus::NotServing)
                        .await;
                }
            }
        });
    }

    let cr = Arc::clone(&channel_registry);
    let raft_for_shutdown = Arc::clone(&raft_node);
    let contexts_for_shutdown = Arc::clone(&contexts);
    let drain_deadline = Duration::from_secs(config.drain_deadline_secs);
    let shutdown = async move {
        shutdown_signal().await;
        info!(
            deadline_secs = drain_deadline.as_secs(),
            "shutdown signal received: draining"
        );
        // 1. Ask all connected clients to reconnect elsewhere.
        cr.request_reconnect_all().await;
        // 2. Shut the Raft core down cleanly — on multi-node clusters the
        //    peers detect the departure at the next election timeout instead
        //    of waiting on a dead TCP peer.
        let _ = raft_for_shutdown.shutdown().await;
        // 3. Stop the engines: new appends are rejected, sync threads do a
        //    final fsync pass releasing in-flight writers.
        contexts_for_shutdown.shutdown_all();
        // 4. Watchdog: tonic's drain waits for open connections, and the
        //    platform/command/query streams are indefinitely long-lived. If
        //    they don't close within the deadline, exit anyway — acked
        //    writes are already durable, and kubelet's SIGKILL would be
        //    less orderly than this.
        tokio::spawn(async move {
            tokio::time::sleep(drain_deadline).await;
            tracing::warn!("drain deadline exceeded with connections still open; exiting now");
            std::process::exit(0);
        });
    };

    // Bind with SO_REUSEADDR so restarts don't fail with "address already in use".
    let listener = {
        let socket = socket2::Socket::new(
            socket2::Domain::for_address(config.listen_addr),
            socket2::Type::STREAM,
            Some(socket2::Protocol::TCP),
        )?;
        socket.set_reuse_address(true)?;
        socket.set_nonblocking(true)?;
        socket.bind(&config.listen_addr.into())?;
        socket.listen(1024)?;
        let std_listener: std::net::TcpListener = socket.into();
        tokio::net::TcpListener::from_std(std_listener)?
    };

    // tonic applies tcp_nodelay/tcp_keepalive only to sockets accepted by its
    // own TcpIncoming; with a user-supplied incoming stream they must be set
    // per accepted socket here. Without nodelay, Nagle + the client's delayed
    // ACK holds every response that follows a small h2 frame for ~40ms.
    let incoming = {
        use tokio_stream::StreamExt;
        tokio_stream::wrappers::TcpListenerStream::new(listener).map(|conn| {
            conn.inspect(|stream| {
                let _ = stream.set_nodelay(true);
                let keepalive = socket2::TcpKeepalive::new().with_time(Duration::from_secs(60));
                let _ = socket2::SockRef::from(stream).set_tcp_keepalive(&keepalive);
            })
        })
    };
    cluster.start_replication()?;
    router
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await?;

    info!("KronosDB shut down gracefully");
    Ok(())
}

pub(crate) async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("received Ctrl+C, shutting down"),
        _ = terminate => info!("received SIGTERM, shutting down"),
    }
}

/// Logs a single structured summary of the durability configuration at
/// startup. Strict-only — window/loss-window durability is out-of-scope
/// for this project (see PROJECT.md non-goals). If a gated window mode
/// lands in v2 DUR-01/DUR-02, this helper gets new arms at that time.
fn log_durability_summary(cluster: &ClusterConfig, group_commit_ms: u64) {
    let voter_count = cluster.voters.len();
    let multi_node = voter_count > 1;

    if multi_node {
        info!(
            durability = "strict",
            voters = voter_count,
            group_commit_ms,
            "multi-node native durability: every write waits for a quorum-durable segment cursor"
        );
    } else {
        info!(
            durability = "strict",
            voters = voter_count,
            group_commit_ms,
            "single-node, strict durability: every write blocks until fsync — crash-safe"
        );
    }
}
