use std::net::SocketAddr;
use std::path::PathBuf;

use clap::Parser;
use serde::Deserialize;

/// KronosDB — a distributed DCB event store.
#[derive(Parser, Debug)]
#[command(name = "kronosdb", version, about)]
struct Cli {
    /// gRPC listen address (default 127.0.0.1:50051).
    #[arg(long, env = "KRONOSDB_LISTEN")]
    listen: Option<SocketAddr>,

    /// Data directory for event store contexts (default "data").
    #[arg(long, env = "KRONOSDB_DATA_DIR")]
    data_dir: Option<PathBuf>,

    /// Server node name (default "kronosdb-0").
    #[arg(long, env = "KRONOSDB_NODE_NAME")]
    node_name: Option<String>,

    /// Admin HTTP listen address (default 127.0.0.1:9240).
    #[arg(long, env = "KRONOSDB_ADMIN_LISTEN")]
    admin_listen: Option<SocketAddr>,

    /// Segment size in bytes (e.g. 268435456 for 256MB).
    #[arg(long, env = "KRONOSDB_SEGMENT_SIZE")]
    segment_size: Option<u64>,

    /// Number of segment indices to cache.
    #[arg(long, env = "KRONOSDB_INDEX_CACHE_SIZE")]
    index_cache_size: Option<usize>,

    /// Number of bloom filters to cache.
    #[arg(long, env = "KRONOSDB_BLOOM_CACHE_SIZE")]
    bloom_cache_size: Option<usize>,

    /// Maximum snapshot state size in bytes (default 4MB). Snapshot records
    /// share the log with events; oversized blobs are rejected at the RPC.
    #[arg(long, env = "KRONOSDB_MAX_SNAPSHOT_SIZE")]
    max_snapshot_size: Option<u64>,

    /// Append acknowledgement mode. "written": ack once a quorum has
    /// written the events to its log (fsync trails by one group-commit
    /// wave). "durable": ack only once a quorum has fsynced. "auto"
    /// (default): durable on a single voter, written at two or more — the
    /// ack is never backed by page cache alone unless "written" is chosen
    /// explicitly.
    #[arg(long, env = "KRONOSDB_ACK_MODE")]
    ack_mode: Option<String>,

    /// Extra group-commit coalescing window in milliseconds. Commits are
    /// event-driven: 0 (default) fsyncs as soon as writes are pending, and
    /// concurrent writes batch on the fsync duration. A positive value adds
    /// that much latency in exchange for larger waves.
    #[arg(long, env = "KRONOSDB_GROUP_COMMIT_MS")]
    group_commit_ms: Option<u64>,

    /// Object-store URL for sealed-segment backup (ADR-0002 stage 1), e.g.
    /// file:///backups/kronosdb or s3://bucket/prefix. Unset = backups off.
    #[arg(long, env = "KRONOSDB_BACKUP_URL")]
    backup_url: Option<String>,

    /// Seconds between backup passes on the claimed leader (default 30).
    #[arg(long, env = "KRONOSDB_BACKUP_INTERVAL_SECS")]
    backup_interval_secs: Option<u64>,

    /// Command dispatch timeout in seconds.
    #[arg(long, env = "KRONOSDB_COMMAND_TIMEOUT")]
    command_timeout: Option<u64>,

    /// Query dispatch timeout in seconds.
    #[arg(long, env = "KRONOSDB_QUERY_TIMEOUT")]
    query_timeout: Option<u64>,

    /// Heartbeat interval in seconds.
    #[arg(long, env = "KRONOSDB_HEARTBEAT_INTERVAL")]
    heartbeat_interval: Option<u64>,

    /// Heartbeat timeout in seconds.
    #[arg(long, env = "KRONOSDB_HEARTBEAT_TIMEOUT")]
    heartbeat_timeout: Option<u64>,

    /// Graceful-shutdown drain deadline in seconds. After SIGTERM the server
    /// waits at most this long for open connections to close before exiting.
    /// Set terminationGracePeriodSeconds slightly above this on Kubernetes.
    #[arg(long, default_value = "20", env = "KRONOSDB_DRAIN_DEADLINE")]
    drain_deadline_secs: u64,

    /// Path to config file (TOML).
    #[arg(long, short, env = "KRONOSDB_CONFIG")]
    config: Option<PathBuf>,

    /// Path to a declarative manifest (TOML) whose resources — e.g. event
    /// store contexts — are ensured to exist at startup.
    #[arg(long, env = "KRONOSDB_MANIFEST")]
    manifest: Option<PathBuf>,

    // --- Cluster options ---
    /// Enable clustering with this node ID (u64).
    #[arg(long, env = "KRONOSDB_CLUSTER_NODE_ID")]
    cluster_node_id: Option<u64>,

    /// Node type: "standard" (voter) or "passive-backup" (learner).
    #[arg(long, default_value = "standard", env = "KRONOSDB_CLUSTER_NODE_TYPE")]
    cluster_node_type: String,

    /// Maximum unacknowledged raw segment bytes per follower Tail session.
    #[arg(long, env = "KRONOSDB_REPLICATION_INFLIGHT_BYTES")]
    replication_inflight_bytes: Option<u64>,

    /// Cluster voter peers as "id=addr" pairs (e.g. "1=127.0.0.1:50051,2=127.0.0.1:50052").
    #[arg(long, env = "KRONOSDB_CLUSTER_PEERS", value_delimiter = ',')]
    cluster_peers: Vec<String>,

    /// Cluster learner peers as "id=addr" pairs.
    #[arg(long, env = "KRONOSDB_CLUSTER_LEARNERS", value_delimiter = ',')]
    cluster_learners: Vec<String>,

    // --- Security options ---
    /// Access token for gRPC authentication. If set, clients must send this token
    /// in the `kronosdb-token` metadata header. If not set, auth is disabled.
    #[arg(long, env = "KRONOSDB_ACCESS_TOKEN")]
    access_token: Option<String>,

    /// Path to TLS certificate file (PEM).
    #[arg(long, env = "KRONOSDB_TLS_CERT")]
    tls_cert: Option<PathBuf>,

    /// Path to TLS private key file (PEM).
    #[arg(long, env = "KRONOSDB_TLS_KEY")]
    tls_key: Option<PathBuf>,

    /// Path to TLS CA certificate for client verification (mTLS).
    #[arg(long, env = "KRONOSDB_TLS_CA")]
    tls_ca: Option<PathBuf>,

    // --- Admin auth options (see also [admin.auth] / [admin.oidc] in TOML) ---
    /// Admin console/API auth mode: "none", "token", or "oidc".
    #[arg(long, env = "KRONOSDB_ADMIN_AUTH_MODE")]
    admin_auth_mode: Option<String>,

    /// Static bearer token for admin auth mode "token".
    #[arg(long, env = "KRONOSDB_ADMIN_TOKEN")]
    admin_token: Option<String>,

    /// OIDC issuer URL (e.g. https://keycloak.example.com/realms/platform).
    #[arg(long, env = "KRONOSDB_OIDC_ISSUER")]
    oidc_issuer: Option<String>,

    /// OIDC client id.
    #[arg(long, env = "KRONOSDB_OIDC_CLIENT_ID")]
    oidc_client_id: Option<String>,

    /// OIDC client secret (omit for public clients; PKCE is always used).
    #[arg(long, env = "KRONOSDB_OIDC_CLIENT_SECRET")]
    oidc_client_secret: Option<String>,

    /// OIDC redirect URL registered at the IdP (default: derived from Host).
    #[arg(long, env = "KRONOSDB_OIDC_REDIRECT_URL")]
    oidc_redirect_url: Option<String>,

    /// Dotted path to the roles claim (e.g. "realm_access.roles").
    #[arg(long, env = "KRONOSDB_OIDC_ROLE_CLAIM")]
    oidc_role_claim: Option<String>,

    /// Role required for admin access.
    #[arg(long, env = "KRONOSDB_OIDC_REQUIRED_ROLE")]
    oidc_required_role: Option<String>,

    /// Expected `aud` claim for bearer tokens (unset = not enforced).
    #[arg(long, env = "KRONOSDB_OIDC_AUDIENCE")]
    oidc_audience: Option<String>,

    /// HMAC secret for admin session cookies (unset = random per boot).
    #[arg(long, env = "KRONOSDB_OIDC_COOKIE_SECRET")]
    oidc_cookie_secret: Option<String>,
}

/// TOML config file structure.
#[derive(Deserialize, Default, Debug)]
struct ConfigFile {
    listen: Option<String>,
    #[serde(rename = "data-dir")]
    data_dir: Option<String>,
    #[serde(rename = "node-name")]
    node_name: Option<String>,
    manifest: Option<String>,
    #[serde(rename = "replication-inflight-bytes")]
    replication_inflight_bytes: Option<u64>,

    #[serde(default)]
    storage: StorageConfig,

    #[serde(default)]
    backup: BackupFileConfig,

    #[serde(default)]
    timeouts: TimeoutConfig,

    #[serde(default)]
    admin: AdminConfig,

    #[serde(default)]
    security: SecurityConfig,
}

#[derive(Deserialize, Default, Debug)]
struct StorageConfig {
    #[serde(rename = "segment-size")]
    segment_size: Option<u64>,
    #[serde(rename = "index-cache-size")]
    index_cache_size: Option<usize>,
    #[serde(rename = "bloom-cache-size")]
    bloom_cache_size: Option<usize>,
    #[serde(rename = "ack-mode")]
    ack_mode: Option<String>,
    #[serde(rename = "max-snapshot-size")]
    max_snapshot_size: Option<u64>,
}

#[derive(Deserialize, Default, Debug)]
struct BackupFileConfig {
    url: Option<String>,
    #[serde(rename = "interval-secs")]
    interval_secs: Option<u64>,
}

#[derive(Deserialize, Default, Debug)]
struct TimeoutConfig {
    #[serde(rename = "command")]
    command_timeout: Option<u64>,
    #[serde(rename = "query")]
    query_timeout: Option<u64>,
    #[serde(rename = "heartbeat-interval")]
    heartbeat_interval: Option<u64>,
    #[serde(rename = "heartbeat-timeout")]
    heartbeat_timeout: Option<u64>,
}

#[derive(Deserialize, Default, Debug)]
struct AdminConfig {
    listen: Option<String>,
    #[serde(default)]
    auth: AdminAuthFile,
    #[serde(default)]
    oidc: OidcFile,
}

#[derive(Deserialize, Default, Debug)]
struct AdminAuthFile {
    /// "none" (default), "token", or "oidc".
    mode: Option<String>,
    token: Option<String>,
}

#[derive(Deserialize, Default, Debug)]
struct OidcFile {
    issuer: Option<String>,
    #[serde(rename = "client-id")]
    client_id: Option<String>,
    #[serde(rename = "client-secret")]
    client_secret: Option<String>,
    #[serde(rename = "redirect-url")]
    redirect_url: Option<String>,
    scopes: Option<Vec<String>>,
    #[serde(rename = "role-claim")]
    role_claim: Option<String>,
    #[serde(rename = "required-role")]
    required_role: Option<String>,
    audience: Option<String>,
    #[serde(rename = "cookie-secret")]
    cookie_secret: Option<String>,
    #[serde(rename = "session-ttl-minutes")]
    session_ttl_minutes: Option<u64>,
}

/// How the admin console + admin API authenticate callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminAuthMode {
    /// No authentication (default). The server logs a prominent warning —
    /// only acceptable when the admin port is unreachable from untrusted
    /// networks (localhost, strict NetworkPolicy).
    None,
    /// Static bearer token (constant-time compared).
    Token,
    /// OpenID Connect: authorization-code + PKCE for the console (session
    /// cookie), JWT bearer validation against the IdP's JWKS for the API.
    Oidc,
}

/// Resolved admin auth configuration.
#[derive(Debug, Clone)]
pub struct AdminAuthConfig {
    pub mode: AdminAuthMode,
    /// Static token for `mode = token`.
    pub token: Option<String>,
    /// OIDC settings for `mode = oidc`.
    pub oidc: Option<OidcConfig>,
}

/// OIDC provider settings ("admin realm" — e.g. a Keycloak realm).
#[derive(Debug, Clone)]
pub struct OidcConfig {
    /// Issuer URL; discovery is fetched from
    /// `{issuer}/.well-known/openid-configuration`.
    pub issuer: String,
    pub client_id: String,
    /// Optional for public clients (PKCE is always used).
    pub client_secret: Option<String>,
    /// Callback URL registered at the IdP. Derived from the request's Host
    /// (and X-Forwarded-Proto) when unset.
    pub redirect_url: Option<String>,
    pub scopes: Vec<String>,
    /// Dotted path to the roles claim, e.g. "realm_access.roles" (Keycloak).
    pub role_claim: Option<String>,
    /// Role required to access the console/API. Unset = any authenticated
    /// user of the realm.
    pub required_role: Option<String>,
    /// Expected `aud` for bearer tokens. Unset = audience not enforced.
    pub audience: Option<String>,
    /// HMAC key for session cookies. Random per boot when unset (sessions
    /// don't survive restarts).
    pub cookie_secret: Option<String>,
    pub session_ttl_minutes: u64,
}

#[derive(Deserialize, Default, Debug)]
struct SecurityConfig {
    #[serde(rename = "access-token")]
    access_token: Option<String>,
    #[serde(rename = "tls-cert")]
    tls_cert: Option<String>,
    #[serde(rename = "tls-key")]
    tls_key: Option<String>,
    #[serde(rename = "tls-ca")]
    tls_ca: Option<String>,
}

/// Parsed cluster peer: id + address.
#[derive(Debug, Clone)]
pub struct PeerEntry {
    pub id: u64,
    pub addr: String,
}

/// Resolved server configuration. All values are concrete.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub listen_addr: SocketAddr,
    pub data_dir: PathBuf,
    pub node_name: String,
    pub admin_listen_addr: SocketAddr,
    pub segment_size: u64,
    pub index_cache_size: usize,
    /// "auto" (default), "written", or "durable"; see the CLI flag docs.
    pub ack_mode: String,
    pub bloom_cache_size: usize,
    /// Maximum snapshot state size in bytes; larger appends are rejected.
    pub max_snapshot_size: u64,
    pub group_commit_ms: u64,
    /// Object-store URL for sealed-segment backup; None = backups disabled.
    pub backup_url: Option<String>,
    pub backup_interval_secs: u64,
    pub command_timeout_secs: u64,
    pub query_timeout_secs: u64,
    pub heartbeat_interval_secs: u64,
    pub heartbeat_timeout_secs: u64,
    /// Max seconds to wait for connections to drain after SIGTERM.
    pub drain_deadline_secs: u64,
    /// If set, clustering is enabled.
    pub cluster_node_id: Option<u64>,
    pub cluster_node_type: String,
    pub replication_inflight_bytes: u64,
    pub cluster_peers: Vec<PeerEntry>,
    pub cluster_learners: Vec<PeerEntry>,
    /// Access token for gRPC auth. None = open access.
    pub access_token: Option<String>,
    /// TLS certificate file path.
    pub tls_cert: Option<PathBuf>,
    /// TLS private key file path.
    pub tls_key: Option<PathBuf>,
    /// TLS CA certificate for client verification (mTLS).
    pub tls_ca: Option<PathBuf>,
    /// Declarative manifest applied at startup, if any.
    pub manifest: Option<PathBuf>,
    /// Admin console/API authentication.
    pub admin_auth: AdminAuthConfig,
}

impl ServerConfig {
    /// Parses configuration from CLI args → env vars → config file → defaults.
    /// Priority: CLI > env > config file > defaults.
    pub fn parse() -> Result<Self, Box<dyn std::error::Error>> {
        let cli = Cli::parse();

        // Load config file if specified.
        let file_config = if let Some(ref path) = cli.config {
            let contents = std::fs::read_to_string(path)
                .map_err(|e| format!("failed to read config file '{}': {e}", path.display()))?;
            toml::from_str::<ConfigFile>(&contents)
                .map_err(|e| format!("failed to parse config file '{}': {e}", path.display()))?
        } else {
            // Try default location. A malformed file is an error, not a
            // silent fallback to defaults — a typo'd config must not boot
            // the server with settings the operator didn't choose.
            if let Ok(contents) = std::fs::read_to_string("kronosdb.toml") {
                toml::from_str::<ConfigFile>(&contents)
                    .map_err(|e| format!("failed to parse config file 'kronosdb.toml': {e}"))?
            } else {
                ConfigFile::default()
            }
        };

        // Defaults.
        const DEFAULT_SEGMENT_SIZE: u64 = 256 * 1024 * 1024;
        const DEFAULT_INDEX_CACHE_SIZE: usize = 50;
        const DEFAULT_BLOOM_CACHE_SIZE: usize = 200;
        const DEFAULT_BACKUP_INTERVAL_SECS: u64 = 30;
        const DEFAULT_COMMAND_TIMEOUT: u64 = 30;
        const DEFAULT_QUERY_TIMEOUT: u64 = 30;
        const DEFAULT_HEARTBEAT_INTERVAL: u64 = 5;
        const DEFAULT_HEARTBEAT_TIMEOUT: u64 = 15;
        const DEFAULT_MAX_SNAPSHOT_SIZE: u64 = 4 * 1024 * 1024;

        // CLI overrides file config. clap already handles env vars.
        // Every value resolves CLI/env > file > default.

        let cluster_peers = parse_peer_list(&cli.cluster_peers)?;
        let cluster_learners = parse_peer_list(&cli.cluster_learners)?;
        let admin_auth = resolve_admin_auth(&cli, &file_config)?;

        let listen_addr = match cli.listen {
            Some(addr) => addr,
            None => match file_config.listen {
                Some(ref s) => s
                    .parse()
                    .map_err(|e| format!("invalid `listen` address '{s}' in config file: {e}"))?,
                None => "127.0.0.1:50051".parse().unwrap(),
            },
        };
        let admin_listen_addr = match cli.admin_listen {
            Some(addr) => addr,
            None => match file_config.admin.listen {
                Some(ref s) => s.parse().map_err(|e| {
                    format!("invalid `[admin] listen` address '{s}' in config file: {e}")
                })?,
                None => "127.0.0.1:9240".parse().unwrap(),
            },
        };

        match cli.cluster_node_type.as_str() {
            "standard" | "passive-backup" => {}
            other => {
                return Err(format!(
                    "invalid --cluster-node-type '{other}': expected \"standard\" or \"passive-backup\""
                )
                .into());
            }
        }

        Ok(Self {
            listen_addr,
            data_dir: cli
                .data_dir
                .or(file_config.data_dir.map(PathBuf::from))
                .unwrap_or_else(|| PathBuf::from("data")),
            node_name: cli
                .node_name
                .or(file_config.node_name)
                .unwrap_or_else(|| "kronosdb-0".to_string()),
            admin_listen_addr,
            segment_size: cli
                .segment_size
                .or(file_config.storage.segment_size)
                .unwrap_or(DEFAULT_SEGMENT_SIZE),
            index_cache_size: cli
                .index_cache_size
                .or(file_config.storage.index_cache_size)
                .unwrap_or(DEFAULT_INDEX_CACHE_SIZE),
            bloom_cache_size: cli
                .bloom_cache_size
                .or(file_config.storage.bloom_cache_size)
                .unwrap_or(DEFAULT_BLOOM_CACHE_SIZE),
            max_snapshot_size: cli
                .max_snapshot_size
                .or(file_config.storage.max_snapshot_size)
                .unwrap_or(DEFAULT_MAX_SNAPSHOT_SIZE),
            ack_mode: cli
                .ack_mode
                // A set-but-empty env var (e.g. compose passthrough
                // defaults) means "not configured", not mode "".
                .filter(|mode| !mode.is_empty())
                .or(file_config.storage.ack_mode)
                .unwrap_or_else(|| "auto".to_string()),
            group_commit_ms: cli.group_commit_ms.unwrap_or(0),
            backup_url: cli.backup_url.or(file_config.backup.url),
            backup_interval_secs: cli
                .backup_interval_secs
                .or(file_config.backup.interval_secs)
                .unwrap_or(DEFAULT_BACKUP_INTERVAL_SECS),
            command_timeout_secs: cli
                .command_timeout
                .or(file_config.timeouts.command_timeout)
                .unwrap_or(DEFAULT_COMMAND_TIMEOUT),
            query_timeout_secs: cli
                .query_timeout
                .or(file_config.timeouts.query_timeout)
                .unwrap_or(DEFAULT_QUERY_TIMEOUT),
            heartbeat_interval_secs: cli
                .heartbeat_interval
                .or(file_config.timeouts.heartbeat_interval)
                .unwrap_or(DEFAULT_HEARTBEAT_INTERVAL),
            heartbeat_timeout_secs: cli
                .heartbeat_timeout
                .or(file_config.timeouts.heartbeat_timeout)
                .unwrap_or(DEFAULT_HEARTBEAT_TIMEOUT),
            drain_deadline_secs: cli.drain_deadline_secs,
            cluster_node_id: cli.cluster_node_id,
            cluster_node_type: cli.cluster_node_type,
            replication_inflight_bytes: cli
                .replication_inflight_bytes
                .or(file_config.replication_inflight_bytes)
                .unwrap_or(64 * 1024 * 1024),
            cluster_peers,
            cluster_learners,
            access_token: cli.access_token.or(file_config.security.access_token),
            tls_cert: cli
                .tls_cert
                .or(file_config.security.tls_cert.map(PathBuf::from)),
            tls_key: cli
                .tls_key
                .or(file_config.security.tls_key.map(PathBuf::from)),
            tls_ca: cli
                .tls_ca
                .or(file_config.security.tls_ca.map(PathBuf::from)),
            // CLI/env > config file > default discovery of ./kronosdb-manifest.toml.
            manifest: cli
                .manifest
                .or(file_config.manifest.map(PathBuf::from))
                .or_else(|| {
                    let default = PathBuf::from("kronosdb-manifest.toml");
                    default.exists().then_some(default)
                }),
            admin_auth,
        })
    }
}

/// Resolves admin auth from CLI/env (priority) and the TOML file.
fn resolve_admin_auth(
    cli: &Cli,
    file: &ConfigFile,
) -> Result<AdminAuthConfig, Box<dyn std::error::Error>> {
    let mode_str = cli
        .admin_auth_mode
        .clone()
        .or_else(|| file.admin.auth.mode.clone())
        .unwrap_or_else(|| "none".to_string());
    let mode = match mode_str.as_str() {
        "none" => AdminAuthMode::None,
        "token" => AdminAuthMode::Token,
        "oidc" => AdminAuthMode::Oidc,
        other => {
            return Err(format!(
                "invalid admin auth mode '{other}': expected 'none', 'token', or 'oidc'"
            )
            .into());
        }
    };

    let token = cli
        .admin_token
        .clone()
        .or_else(|| file.admin.auth.token.clone());
    if mode == AdminAuthMode::Token && token.is_none() {
        return Err(
            "admin auth mode 'token' requires --admin-token / KRONOSDB_ADMIN_TOKEN / [admin.auth] token"
                .into(),
        );
    }

    let oidc = if mode == AdminAuthMode::Oidc {
        let issuer = cli
            .oidc_issuer
            .clone()
            .or_else(|| file.admin.oidc.issuer.clone())
            .ok_or("admin auth mode 'oidc' requires --oidc-issuer / [admin.oidc] issuer")?;
        let client_id = cli
            .oidc_client_id
            .clone()
            .or_else(|| file.admin.oidc.client_id.clone())
            .ok_or("admin auth mode 'oidc' requires --oidc-client-id / [admin.oidc] client-id")?;
        Some(OidcConfig {
            issuer: issuer.trim_end_matches('/').to_string(),
            client_id,
            client_secret: cli
                .oidc_client_secret
                .clone()
                .or_else(|| file.admin.oidc.client_secret.clone()),
            redirect_url: cli
                .oidc_redirect_url
                .clone()
                .or_else(|| file.admin.oidc.redirect_url.clone()),
            scopes: file
                .admin
                .oidc
                .scopes
                .clone()
                .unwrap_or_else(|| vec!["openid".into(), "profile".into(), "email".into()]),
            role_claim: cli
                .oidc_role_claim
                .clone()
                .or_else(|| file.admin.oidc.role_claim.clone()),
            required_role: cli
                .oidc_required_role
                .clone()
                .or_else(|| file.admin.oidc.required_role.clone()),
            audience: cli
                .oidc_audience
                .clone()
                .or_else(|| file.admin.oidc.audience.clone()),
            cookie_secret: cli
                .oidc_cookie_secret
                .clone()
                .or_else(|| file.admin.oidc.cookie_secret.clone()),
            session_ttl_minutes: file.admin.oidc.session_ttl_minutes.unwrap_or(480),
        })
    } else {
        None
    };

    Ok(AdminAuthConfig { mode, token, oidc })
}

/// Parses "id=addr" peer entries from CLI/config.
pub fn parse_peer_list(entries: &[String]) -> Result<Vec<PeerEntry>, Box<dyn std::error::Error>> {
    let mut peers = Vec::new();
    for entry in entries {
        let parts: Vec<&str> = entry.splitn(2, '=').collect();
        if parts.len() != 2 {
            return Err(format!("invalid peer format '{}', expected 'id=addr'", entry).into());
        }
        let id: u64 = parts[0]
            .parse()
            .map_err(|_| format!("invalid peer id '{}' in '{}'", parts[0], entry))?;
        peers.push(PeerEntry {
            id,
            addr: parts[1].to_string(),
        });
    }
    Ok(peers)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_peer_list_valid() {
        let input = vec![
            "1=127.0.0.1:50051".to_string(),
            "2=127.0.0.1:50052".to_string(),
            "3=10.0.0.1:50053".to_string(),
        ];
        let peers = parse_peer_list(&input).unwrap();
        assert_eq!(peers.len(), 3);
        assert_eq!(peers[0].id, 1);
        assert_eq!(peers[0].addr, "127.0.0.1:50051");
        assert_eq!(peers[2].id, 3);
        assert_eq!(peers[2].addr, "10.0.0.1:50053");
    }

    #[test]
    fn parse_peer_list_empty() {
        let peers = parse_peer_list(&[]).unwrap();
        assert!(peers.is_empty());
    }

    #[test]
    fn parse_peer_list_invalid_format() {
        let input = vec!["no-equals-sign".to_string()];
        assert!(parse_peer_list(&input).is_err());
    }

    #[test]
    fn parse_peer_list_invalid_id() {
        let input = vec!["abc=127.0.0.1:50051".to_string()];
        assert!(parse_peer_list(&input).is_err());
    }

    #[test]
    fn parse_peer_list_addr_with_equals() {
        // Edge case: addr contains '=' (shouldn't happen but test splitn(2))
        let input = vec!["1=host=50051".to_string()];
        let peers = parse_peer_list(&input).unwrap();
        assert_eq!(peers[0].addr, "host=50051");
    }

    #[test]
    fn toml_config_parses() {
        let toml = r#"
listen = "0.0.0.0:50051"

[storage]
segment-size = 134217728
index-cache-size = 100
bloom-cache-size = 500

[timeouts]
command = 60
query = 45
heartbeat-interval = 3
heartbeat-timeout = 10

[admin]
listen = "0.0.0.0:9090"
"#;
        let config: ConfigFile = toml::from_str(toml).unwrap();
        assert_eq!(config.storage.segment_size, Some(134217728));
        assert_eq!(config.storage.index_cache_size, Some(100));
        assert_eq!(config.storage.bloom_cache_size, Some(500));
        assert_eq!(config.timeouts.command_timeout, Some(60));
        assert_eq!(config.timeouts.query_timeout, Some(45));
        assert_eq!(config.timeouts.heartbeat_interval, Some(3));
        assert_eq!(config.timeouts.heartbeat_timeout, Some(10));
    }

    #[test]
    fn toml_config_defaults_on_empty() {
        let config: ConfigFile = toml::from_str("").unwrap();
        assert!(config.storage.segment_size.is_none());
        assert!(config.timeouts.command_timeout.is_none());
        assert!(config.security.access_token.is_none());
        assert!(config.security.tls_cert.is_none());
    }

    #[test]
    fn toml_config_parses_security() {
        let toml = r#"
[security]
access-token = "my-secret"
tls-cert = "/etc/kronosdb/cert.pem"
tls-key = "/etc/kronosdb/key.pem"
tls-ca = "/etc/kronosdb/ca.pem"
"#;
        let config: ConfigFile = toml::from_str(toml).unwrap();
        assert_eq!(config.security.access_token.as_deref(), Some("my-secret"));
        assert_eq!(
            config.security.tls_cert.as_deref(),
            Some("/etc/kronosdb/cert.pem")
        );
        assert_eq!(
            config.security.tls_key.as_deref(),
            Some("/etc/kronosdb/key.pem")
        );
        assert_eq!(
            config.security.tls_ca.as_deref(),
            Some("/etc/kronosdb/ca.pem")
        );
    }
}
