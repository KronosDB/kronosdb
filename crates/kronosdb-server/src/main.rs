mod admin;
mod auth;
mod config;
mod hub;
mod manifest;
mod native;
mod processor;

use std::sync::Arc;
use std::time::Duration;

use tokio::signal;
use tracing::{error, info, warn};

use kronosdb_eventstore::context::ContextManager;
use kronosdb_eventstore::raft::cluster::{ClusterConfig, ClusterManager, NodeType, PeerConfig};
use kronosdb_eventstore::raft::types::default_raft_config;
use kronosdb_eventstore::replication::peer::{PeerTlsConfig, PeerTransportConfig};
use kronosdb_eventstore::replication::service::SegmentReplicationService;
use kronosdb_eventstore::store::StoreOptions;
use kronosdb_messaging::client::ClientRegistry;
use kronosdb_messaging::manager::MessagingManager;

use crate::config::ServerConfig;
use crate::hub::{Hub, HubConfig};

// Many small, short-lived allocations cross threads (frames built on one
// worker, freed on another); glibc's arenas contend on them.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

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

    // Both `ring` (our TLS) and `aws-lc-rs` (object_store) end up compiled in,
    // and rustls refuses to guess between them — without this, the first TLS
    // handshake setup panics. Err just means a provider is already installed.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let config = ServerConfig::parse()?;
    let ack_mode = match config.ack_mode.as_str() {
        "auto" => kronosdb_eventstore::AckMode::Auto,
        // "replicated" is a deprecated alias from the mode's prototype era.
        "written" | "replicated" => kronosdb_eventstore::AckMode::Written,
        "durable" => kronosdb_eventstore::AckMode::Durable,
        other => {
            return Err(format!(
                "invalid ack-mode {other:?}: expected \"auto\", \"written\", or \"durable\""
            )
            .into());
        }
    };
    kronosdb_eventstore::configure_ack_mode(ack_mode);

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
        mapping_budget: kronosdb_eventstore::cache::MappingBudget::shared(),
        read_mode: config.read_mode,
        block_cache_bytes: config.block_cache_bytes,
        block_size: config.block_size,
        readahead_blocks: config.readahead_blocks,
        group_commit_interval_ms: config.group_commit_ms,
        node_id,
        voters: voter_ids,
        index_merge_rate: config.index_merge_rate,
        background_index_merge: true,
    };
    let contexts = Arc::new(ContextManager::with_options(
        &config.data_dir,
        store_options,
    )?);

    // Look at the hardware now that the data dir is ours (locked) and before
    // anything appends: the disk probe decides what `ack-mode = auto` means
    // in a cluster. Best-effort; this never fails startup.
    // Blocking is fine here: nothing else is running yet, and the probe is
    // bounded (it runs on its own thread with a timeout).
    let hardware = {
        let report =
            kronosdb_eventstore::hardware::check(&kronosdb_eventstore::hardware::CheckInput {
                data_dir: &config.data_dir,
                cache_budget_bytes: config.block_cache_bytes,
                ack_mode,
                dev: config.dev,
            });
        report.ack.apply();
        report.log();
        Arc::new(report)
    };

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
    // contexts — ADR-0006), the registries the admin console reads, and the
    // hub that runs commands, queries and the platform over them.
    let messaging = Arc::new(MessagingManager::with_permit_wait(
        config.messaging_permit_wait,
    ));
    let client_registry = Arc::new(ClientRegistry::new());
    let processor_registry = Arc::new(processor::ProcessorRegistry::new());

    let heartbeat_interval = Duration::from_secs(config.heartbeat_interval_secs);
    let heartbeat_timeout = Duration::from_secs(config.heartbeat_timeout_secs);
    let context_names = {
        let contexts = Arc::clone(&contexts);
        Arc::new(move || contexts.list_contexts()) as Arc<dyn Fn() -> Vec<String> + Send + Sync>
    };
    let hub = Hub::new(
        HubConfig {
            node_name: config.node_name.clone(),
            command_timeout: Duration::from_secs(config.command_timeout_secs),
            query_timeout: Duration::from_secs(config.query_timeout_secs),
            heartbeat_interval,
            heartbeat_timeout,
        },
        hub::Shared {
            manager: Arc::clone(&messaging),
            client_registry: Arc::clone(&client_registry),
            processors: Arc::clone(&processor_registry),
        },
        Arc::clone(&cluster),
        Arc::new(native::peers::NativePeers::new(Arc::clone(&cluster))),
        context_names,
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

    // Clean dead clients off every bus and out of the replicated routing
    // table.
    let _reaper = hub.spawn_reaper();

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
    let authenticator = Arc::new(auth::Authenticator::new(&config.identity)?);
    let admin_auth = Arc::new(admin::auth::AuthRuntime::new(
        &config.admin_auth,
        Arc::clone(&authenticator),
    ));
    let admin_state = admin::AdminState {
        config: config.clone(),
        contexts: Arc::clone(&contexts),
        client_registry: Arc::clone(&client_registry),
        messaging: Arc::clone(&messaging),
        cluster: Arc::clone(&cluster),
        processor_registry: Arc::clone(&processor_registry),
        hub: Arc::clone(&hub),
        started_at: std::time::Instant::now(),
        auth: admin_auth,
        identities: Arc::clone(&authenticator),
        activity: Arc::new(admin::activity::ActivityTracker::new()),
        hardware,
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
    let auth_methods = authenticator.describe();
    if auth_methods.is_empty() {
        warn!(
            "authentication is DISABLED — anyone who can reach the client port can read \
             and append events. Configure [security] access-token, tokens, issuers, or mtls \
             grants before exposing it."
        );
    }
    info!(
        version = env!("CARGO_PKG_VERSION"),
        listen = %config.listen_addr,
        cluster = nodes,
        node_id = node_id,
        node = %config.node_name,
        data_dir = %config.data_dir.display(),
        admin = %config.admin_listen_addr,
        tls = tls_enabled,
        auth = ?auth_methods,
        contexts = ?contexts.list_contexts(),
        "KronosDB starting"
    );

    let tls = match (&config.tls_cert, &config.tls_key) {
        (Some(cert), Some(key)) => {
            let optional = config.tls_client_auth == auth::config::ClientAuth::Optional;
            if config.tls_ca.is_some() {
                if optional && !authenticator.enabled() {
                    // Optional certificates with nothing else checking
                    // identity would quietly turn the CA into decoration.
                    return Err("tls-client-auth = \"optional\" requires [security] \
                                authentication (tokens, issuers, or mtls grants) to be configured"
                        .into());
                }
                info!(
                    client_certs = if optional { "optional" } else { "required" },
                    "mTLS enabled (client certificate verification)"
                );
            }
            Some(native::tls::acceptor(
                cert,
                key,
                config.tls_ca.as_deref(),
                optional,
            )?)
        }
        (None, None) => None,
        _ => {
            warn!("both --tls-cert and --tls-key must be set for TLS; running without TLS");
            None
        }
    };

    let raft_node = cluster
        .raft_node()
        .expect("shared Raft node must be initialized");

    let (native_stop, native_stop_rx) = tokio::sync::watch::channel(false);
    let connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let open_connections = Arc::clone(&connections);
    let shutdown_hub = Arc::clone(&hub);
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
        let _ = native_stop.send(true);
        shutdown_hub.clients.request_reconnect_all().await;
        // 2. Shut the Raft core down cleanly — on multi-node clusters the
        //    peers detect the departure at the next election timeout instead
        //    of waiting on a dead TCP peer.
        let _ = raft_for_shutdown.shutdown().await;
        // 3. Stop the engines: new appends are rejected, sync threads do a
        //    final fsync pass releasing in-flight writers.
        contexts_for_shutdown.shutdown_all();
        // 4. Give connections the drain deadline to close after GoAway;
        //    platform/command/query streams are indefinitely long-lived, so
        //    exit anyway when it passes — acked writes are already durable,
        //    and kubelet's SIGKILL would be less orderly than this.
        let drained = tokio::time::timeout(drain_deadline, async {
            while open_connections.load(std::sync::atomic::Ordering::Relaxed) > 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        });
        if drained.await.is_err() {
            warn!("drain deadline exceeded with connections still open; exiting now");
        }
    };

    let listener = bind_listener(config.listen_addr)?;
    let native_service = Arc::new(native::Native::new(native::Deps {
        cluster: Arc::clone(&cluster),
        auth: Arc::clone(&authenticator),
        raft: Arc::clone(&raft_node),
        replication: Arc::new(SegmentReplicationService::new(
            Arc::clone(&contexts),
            cluster.replication_control(),
            config.replication_inflight_bytes,
        )),
        max_snapshot_size: config.max_snapshot_size,
        hub: Arc::clone(&hub),
        schedule_projections: Default::default(),
    }));
    let native_config = Arc::new(kronosdb_protocol::net::server::ServerConfig {
        server_name: config.node_name.clone(),
        tls,
        ..Default::default()
    });
    tokio::spawn(accept_loop(
        listener,
        native_service,
        native_config,
        native_stop_rx,
        Arc::clone(&connections),
    ));
    info!(addr = %config.listen_addr, "listening");

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
    shutdown.await;

    info!("KronosDB shut down gracefully");
    Ok(())
}

/// Accepts client and peer connections until `stop`, counting the ones
/// still open so shutdown can wait for them to drain.
async fn accept_loop(
    listener: tokio::net::TcpListener,
    service: Arc<native::Native>,
    config: Arc<kronosdb_protocol::net::server::ServerConfig>,
    mut stop: tokio::sync::watch::Receiver<bool>,
    open: Arc<std::sync::atomic::AtomicUsize>,
) {
    use std::sync::atomic::Ordering;
    loop {
        let (tcp, addr) = tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok(accepted) => accepted,
                Err(_) => continue,
            },
            _ = stop.wait_for(|stop| *stop) => return,
        };
        // Without nodelay, Nagle + the client's delayed ACK holds every
        // response that follows a small frame for ~40ms.
        let _ = tcp.set_nodelay(true);
        let keepalive = socket2::TcpKeepalive::new().with_time(Duration::from_secs(60));
        let _ = socket2::SockRef::from(&tcp).set_tcp_keepalive(&keepalive);
        let (service, config, stop, open) = (
            Arc::clone(&service),
            Arc::clone(&config),
            stop.clone(),
            Arc::clone(&open),
        );
        open.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(async move {
            kronosdb_protocol::net::server::connection(tcp, addr, service, config, stop).await;
            open.fetch_sub(1, Ordering::Relaxed);
        });
    }
}

/// Binds with SO_REUSEADDR so restarts don't fail with "address already in use".
fn bind_listener(addr: std::net::SocketAddr) -> std::io::Result<tokio::net::TcpListener> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(addr),
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    let std_listener: std::net::TcpListener = socket.into();
    tokio::net::TcpListener::from_std(std_listener)
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
            "multi-node native durability: every write waits for the quorum's segment cursors; \
             whether they must be fsynced or only written is the ack mode logged above"
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
