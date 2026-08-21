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
mod scheduler_service;

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
use crate::messaging::command_service::CommandServiceImpl;
use crate::messaging::fabric as messaging_fabric;
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
    match config.ack_mode.as_str() {
        "auto" => kronosdb_eventstore::configure_ack_mode(kronosdb_eventstore::AckMode::Auto),
        // "replicated" is a deprecated alias from the mode's prototype era.
        "written" | "replicated" => {
            kronosdb_eventstore::configure_ack_mode(kronosdb_eventstore::AckMode::Written)
        }
        "durable" => kronosdb_eventstore::configure_ack_mode(kronosdb_eventstore::AckMode::Durable),
        other => {
            return Err(format!(
                "invalid ack-mode {other:?}: expected \"auto\", \"written\", or \"durable\""
            )
            .into());
        }
    }

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

    // Create the messaging manager (named buses, independent of event store
    // contexts — ADR-0006) and client registries.
    let messaging = Arc::new(MessagingManager::with_permit_wait(
        config.messaging_permit_wait,
    ));
    let client_registry = Arc::new(ClientRegistry::new());
    let channel_registry = Arc::new(ClientChannelRegistry::new());
    let processor_registry = Arc::new(processor::ProcessorRegistry::new());
    let handler_stream_registry = Arc::new(handler_registry::HandlerStreamRegistry::new());

    // Build gRPC services.
    let event_store_service =
        EventStoreService::new(Arc::clone(&cluster), config.max_snapshot_size);
    let scheduler_service = scheduler_service::SchedulerServiceImpl::new(Arc::clone(&cluster));
    // Command handler delivery channels are shared with the fabric service
    // so forwarded commands reach locally-connected handlers (ADR-0007).
    let command_handler_streams: messaging_fabric::CommandHandlerStreams =
        Arc::new(dashmap::DashMap::new());
    let query_handler_streams: messaging_fabric::QueryHandlerStreams =
        Arc::new(dashmap::DashMap::new());
    let pending_queries: messaging_fabric::PendingQueries = Arc::new(dashmap::DashMap::new());
    let fabric_router = Arc::new(messaging_fabric::FabricRouter::new(Arc::clone(&cluster)));
    let command_service = CommandServiceImpl::new(
        Arc::clone(&messaging),
        Duration::from_secs(config.command_timeout_secs),
        Arc::clone(&channel_registry),
        Arc::clone(&handler_stream_registry),
        Arc::clone(&command_handler_streams),
        Arc::clone(&cluster),
        Arc::clone(&fabric_router),
    );
    let query_service = QueryServiceImpl::new(
        Arc::clone(&messaging),
        Duration::from_secs(config.query_timeout_secs),
        Arc::clone(&channel_registry),
        Arc::clone(&handler_stream_registry),
        Arc::clone(&query_handler_streams),
        Arc::clone(&pending_queries),
        Arc::clone(&cluster),
        Arc::clone(&fabric_router),
    );
    let fabric_service = messaging_fabric::FabricServiceImpl::new(
        Arc::clone(&messaging),
        Arc::clone(&command_handler_streams),
        Arc::clone(&query_handler_streams),
        Arc::clone(&pending_queries),
    );

    // Drop any handler rows this node stranded in a previous life — a
    // crashed process cannot deregister its clients. Retries in the
    // background until the control plane has a leader to accept it.
    {
        let clear_cluster = Arc::clone(&cluster);
        tokio::spawn(async move {
            for attempt in 1..=10u32 {
                match clear_cluster.clear_node_handlers().await {
                    Ok(()) => return,
                    Err(e) if attempt == 10 => {
                        warn!(error = %e, "fabric: startup ClearNodeHandlers failed; stale rows may linger until membership change");
                    }
                    Err(_) => tokio::time::sleep(Duration::from_millis(500 * attempt as u64)).await,
                }
            }
        });
    }

    let heartbeat_interval = Duration::from_secs(config.heartbeat_interval_secs);
    let heartbeat_timeout = Duration::from_secs(config.heartbeat_timeout_secs);

    let context_names = {
        let contexts = Arc::clone(&contexts);
        Arc::new(move || contexts.list_contexts()) as Arc<dyn Fn() -> Vec<String> + Send + Sync>
    };
    let platform_service = PlatformServiceImpl::new(
        Arc::clone(&client_registry),
        Arc::clone(&channel_registry),
        Arc::clone(&processor_registry),
        Arc::clone(&messaging),
        Arc::clone(&handler_stream_registry),
        Arc::clone(&cluster),
        context_names,
        config.node_name.clone(),
        heartbeat_interval,
        heartbeat_timeout,
    );

    // Spawn background heartbeat reaper (cleans dead clients off every bus
    // and out of the replicated routing table).
    let _reaper = spawn_reaper(
        Arc::clone(&client_registry),
        Arc::clone(&messaging),
        Arc::clone(&cluster),
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
            for bus_name in sweep_messaging.list_buses() {
                let platform = sweep_messaging.get_platform(&bus_name);
                let swept = platform.sweep_command_timeouts(sweep_timeout);
                if !swept.is_empty() {
                    warn!(
                        bus = %bus_name,
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
        activity: Arc::new(admin::activity::ActivityTracker::new()),
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
    use crate::proto::kronosdb::scheduler::scheduler_service_server::SchedulerServiceServer;

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
        ))
        .add_service(SchedulerServiceServer::with_interceptor(
            scheduler_service,
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
        tonic::service::interceptor::InterceptedService::new(replication_server, auth.clone());
    router = router.add_service(replication_server);

    // Messaging fabric — internode command/query forwarding (ADR-0007).
    // Same auth and TLS as the other peer transports.
    use crate::proto::kronosdb::fabric::messaging_fabric_server::MessagingFabricServer;
    router = router.add_service(MessagingFabricServer::with_interceptor(
        fabric_service,
        auth,
    ));

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
    if let Some(backup_url) = &config.backup_url {
        cluster.start_backup(kronosdb_eventstore::tier::TierConfig {
            url: backup_url.clone(),
            interval: Duration::from_secs(config.backup_interval_secs),
        })?;
        info!(url = %backup_url, interval_secs = config.backup_interval_secs, "segment backup enabled");
    }
    cluster.start_scheduler(Duration::from_secs(1));
    if let Some(ref manifest_path) = config.manifest {
        spawn_manifest_watch(Arc::clone(&cluster), manifest_path.clone());
    }
    router
        .serve_with_incoming_shutdown(incoming, shutdown)
        .await?;

    info!("KronosDB shut down gracefully");
    Ok(())
}

/// Watches the manifest file and applies additions live, so declaring a new
/// context in a GitOps-managed ConfigMap materializes it without a restart.
///
/// Strictly additive, like the startup apply: an entry disappearing from the
/// manifest never deletes or unloads anything — it is logged as drift and
/// nothing more. Runtime creation goes through the replicated control plane
/// (unlike the pre-Raft startup apply), and only the claimed leader proposes,
/// so a cluster whose ConfigMap updates on every node does not race three
/// identical proposals. Followers converge through consensus; after failover
/// the new leader picks up any pending additions on its next tick.
fn spawn_manifest_watch(cluster: Arc<ClusterManager>, path: std::path::PathBuf) {
    tokio::spawn(async move {
        let mut last_contents = std::fs::read_to_string(&path).unwrap_or_default();
        loop {
            tokio::time::sleep(Duration::from_secs(10)).await;

            // Content comparison, not mtime: Kubernetes updates mounted
            // ConfigMaps by atomically swapping a symlinked directory.
            let contents = match std::fs::read_to_string(&path) {
                Ok(contents) => contents,
                // Transient: the file can vanish mid-swap.
                Err(_) => continue,
            };
            let changed = contents != last_contents;

            let manifest = match toml::from_str::<manifest::Manifest>(&contents) {
                Ok(manifest) => manifest,
                Err(error) => {
                    if changed {
                        tracing::warn!(%error, manifest = %path.display(),
                            "manifest changed but does not parse; keeping previous state");
                        last_contents = contents;
                    }
                    continue;
                }
            };

            let existing = cluster.context_manager().list_contexts();
            if changed {
                last_contents = contents;
                let undeclared = manifest::undeclared(&manifest, &existing);
                if !undeclared.is_empty() {
                    tracing::info!(contexts = ?undeclared,
                        "manifest no longer declares existing contexts; they keep \
                         running — deletion is an explicit admin operation");
                }
            }

            // Reconciled every tick, not only on change: a creation that
            // failed (or arrived while this node was not leader) is retried
            // here, and a new leader picks up pending additions on its first
            // tick. The steady-state cost is a list + diff, no proposals.
            if !cluster.is_writable_leader() {
                continue;
            }
            for name in manifest::missing(&manifest, &existing) {
                match cluster.create_context_replicated(&name).await {
                    Ok(()) => {
                        tracing::info!(context = name, "context created from manifest change")
                    }
                    Err(error) => tracing::warn!(%error, context = name,
                        "manifest context creation failed; retrying next tick"),
                }
            }
        }
    });
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
