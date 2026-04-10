#![allow(dead_code, clippy::all)]

mod admin;
mod auth;
mod config;
mod eventstore;
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
use kronosdb_eventstore::raft::transport::RaftTransportService;
use kronosdb_eventstore::raft::types::default_raft_config;
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
    // Initialize structured logging.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "kronosdb=info,warn".into()),
        )
        .init();

    let config = ServerConfig::parse()?;

    // Create the context manager and ensure a default context exists.
    let store_options = StoreOptions {
        max_segment_size: config.segment_size,
        index_cache_size: config.index_cache_size,
        bloom_cache_size: config.bloom_cache_size,
        group_commit_interval_ms: config.group_commit_ms,
        ..Default::default()
    };
    let contexts = Arc::new(ContextManager::with_options(
        &config.data_dir,
        store_options,
    )?);
    if !contexts.context_exists("default") {
        contexts.create_context("default")?;
    }

    // Create the cluster manager — every node is always a Raft node.
    // A node with no peers starts as a single-node cluster (instant leader).
    let node_id = config.cluster_node_id.unwrap_or(1);
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

    let cluster_config = ClusterConfig {
        node_id,
        node_type,
        advertise_addr: config.listen_addr.to_string(),
        voters,
        learners,
        raft_config: default_raft_config(),
    };

    let cluster = Arc::new(ClusterManager::new(Arc::clone(&contexts), cluster_config));

    for ctx_name in contexts.list_contexts() {
        cluster.init_context(&ctx_name).await?;
    }

    cluster.bootstrap().await?;

    for learner in &config.cluster_learners {
        let _ = cluster
            .add_learner("default", learner.id, learner.addr.clone())
            .await;
    }

    // Create the messaging manager (per-context) and client registries.
    let messaging = Arc::new(MessagingManager::new());
    let client_registry = Arc::new(ClientRegistry::new());
    let channel_registry = Arc::new(ClientChannelRegistry::new());
    let processor_registry = Arc::new(processor::ProcessorRegistry::new());

    // Build gRPC services.
    let event_store_service = EventStoreService::new(Arc::clone(&cluster));
    let snapshot_service = SnapshotServiceImpl::new(Arc::clone(&cluster));
    let command_service = CommandServiceImpl::new(
        Arc::clone(&messaging),
        Duration::from_secs(config.command_timeout_secs),
    );
    let query_service = QueryServiceImpl::new(
        Arc::clone(&messaging),
        Duration::from_secs(config.query_timeout_secs),
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

    // Start admin HTTP server in the background.
    let admin_state = admin::AdminState {
        config: config.clone(),
        contexts: Arc::clone(&contexts),
        client_registry: Arc::clone(&client_registry),
        messaging: Arc::clone(&messaging),
        cluster: Arc::clone(&cluster),
        processor_registry: Arc::clone(&processor_registry),
        channel_registry: Arc::clone(&channel_registry),
        started_at: std::time::Instant::now(),
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

    // Configure TLS if cert and key are provided.
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

        Server::builder().tls_config(tls)?
    } else {
        if config.tls_cert.is_some() || config.tls_key.is_some() {
            warn!("both --tls-cert and --tls-key must be set for TLS; running without TLS");
        }
        Server::builder()
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
            auth,
        ));

    // Raft transport — always enabled (every node is a Raft node).
    let raft_node = cluster
        .get_raft_node("default")
        .expect("default context Raft node must be initialized");
    let raft_transport = RaftTransportService::new(raft_node);
    router = router.add_service(raft_transport.into_server());

    let cr = Arc::clone(&channel_registry);
    let shutdown = async move {
        shutdown_signal().await;
        // Ask all connected clients to reconnect before shutting down.
        cr.request_reconnect_all().await;
    };

    router
        .serve_with_shutdown(config.listen_addr, shutdown)
        .await?;

    info!("KronosDB shut down gracefully");
    Ok(())
}

async fn shutdown_signal() {
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
