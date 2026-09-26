//! `[security]` identity configuration: static tokens, trusted OIDC issuers,
//! and grants. Deserialized straight from the TOML file — these lists have
//! no CLI/env form (the legacy `access-token` and the TLS paths do, and stay
//! in `crate::config`).

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::Deserialize;

/// What a principal may do. `admin` implies `write` implies `read`; `peer`
/// stands apart — it guards the internode services (Raft transport, segment
/// replication, messaging fabric) and is never implied by another role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Read,
    Write,
    Admin,
    Peer,
}

impl Role {
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Read => "read",
            Role::Write => "write",
            Role::Admin => "admin",
            Role::Peer => "peer",
        }
    }
}

/// `[[security.tokens]]` — a named static secret with its own roles. The
/// low-tech option: CI jobs, local dev, anything without an identity
/// provider. `token-file` suits mounted Kubernetes secrets.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TokenConfig {
    pub name: String,
    pub token: Option<String>,
    #[serde(rename = "token-file")]
    pub token_file: Option<PathBuf>,
    pub roles: Vec<Role>,
    #[serde(default)]
    pub contexts: Option<Vec<String>>,
    #[serde(default)]
    pub buses: Option<Vec<String>>,
}

/// `[[security.issuers]]` — an OIDC issuer whose JWTs are trusted. Covers
/// human SSO (Google, Entra, Keycloak, Okta, Auth0) and workload identity
/// (Kubernetes projected service-account tokens on GKE/EKS/AKS, GitHub
/// Actions, SPIFFE JWT-SVIDs) alike: they are all signed JWTs with a JWKS.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IssuerConfig {
    /// Referenced by grants (`source = "<name>"`).
    pub name: String,
    /// Must equal the token's `iss` claim exactly.
    pub issuer: String,
    /// Accepted `aud` values. Empty = audience not enforced, which is only
    /// safe when grants pin the caller through claims instead.
    #[serde(default)]
    pub audiences: Vec<String>,
    /// Skip discovery and fetch keys from here (an in-cluster Kubernetes API
    /// server: `https://kubernetes.default.svc/openid/v1/jwks`).
    #[serde(rename = "jwks-uri")]
    pub jwks_uri: Option<String>,
    /// Keys from a local file instead of the network (air-gapped, SPIRE
    /// bundles). Re-read on the normal refresh cadence.
    #[serde(rename = "jwks-file")]
    pub jwks_file: Option<PathBuf>,
    /// Extra root CA (PEM) for the discovery/JWKS fetch.
    #[serde(rename = "ca-file")]
    pub ca_file: Option<PathBuf>,
    /// Bearer token file sent with the discovery/JWKS fetch — the Kubernetes
    /// API server only serves its JWKS to authenticated callers by default.
    #[serde(rename = "jwks-bearer-token-file")]
    pub jwks_bearer_token_file: Option<PathBuf>,
    /// Claim that names the principal (default `sub`; `email` reads better
    /// for Google accounts).
    #[serde(rename = "subject-claim")]
    pub subject_claim: Option<String>,
    /// Dotted path to a roles claim (Keycloak: `realm_access.roles`).
    #[serde(rename = "role-claim")]
    pub role_claim: Option<String>,
    /// IdP role name → KronosDB role. Roles granted this way are unscoped.
    #[serde(rename = "role-map", default)]
    pub role_map: BTreeMap<String, Role>,
}

/// `[[security.grants]]` — binds principals from one source to roles,
/// optionally scoped to contexts and buses.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantConfig {
    /// An issuer `name`, or `"mtls"` for client-certificate identities.
    pub source: String,
    /// Glob (`*` wildcard) over the principal's subject. For mTLS every
    /// certificate name is tried: URI SANs (SPIFFE IDs), DNS SANs, then CN.
    #[serde(default = "any")]
    pub subject: String,
    /// Claims that must all match (JWT sources only). Scalars compare by
    /// equality; an array claim matches when it contains the value.
    #[serde(default)]
    pub claims: BTreeMap<String, toml::Value>,
    pub roles: Vec<Role>,
    /// Context name globs (absent = all contexts).
    #[serde(default)]
    pub contexts: Option<Vec<String>>,
    /// Bus name globs (absent = all buses).
    #[serde(default)]
    pub buses: Option<Vec<String>>,
}

fn any() -> String {
    "*".to_string()
}

/// Whether a client certificate is demanded at the TLS handshake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientAuth {
    /// Every connection must present a certificate signed by `tls-ca`.
    Required,
    /// Certificates are verified when presented but not demanded, so mTLS
    /// workloads and bearer-token callers can share the port.
    Optional,
}

/// Resolved identity configuration handed to [`super::Authenticator`].
#[derive(Debug, Clone, Default)]
pub struct IdentityConfig {
    /// Legacy `[security] access-token`: full access including `peer`.
    pub access_token: Option<String>,
    pub tokens: Vec<TokenConfig>,
    pub issuers: Vec<IssuerConfig>,
    pub grants: Vec<GrantConfig>,
}

pub const MTLS_SOURCE: &str = "mtls";

impl IdentityConfig {
    /// True once any credential is configured. With nothing configured the
    /// server keeps its historical open-access behavior.
    pub fn enabled(&self) -> bool {
        self.access_token.is_some()
            || !self.tokens.is_empty()
            || !self.issuers.is_empty()
            || !self.grants.is_empty()
    }

    pub fn has_mtls_grants(&self) -> bool {
        self.grants.iter().any(|g| g.source == MTLS_SOURCE)
    }

    /// Catches configurations that would boot and then lock everyone out.
    pub fn validate(&self, clustered: bool, mtls_available: bool) -> Result<(), String> {
        let mut names = std::collections::HashSet::new();
        for issuer in &self.issuers {
            if issuer.name == MTLS_SOURCE {
                return Err("[[security.issuers]] name \"mtls\" is reserved".into());
            }
            if !names.insert(issuer.name.as_str()) {
                return Err(format!(
                    "[[security.issuers]] name {:?} is declared twice",
                    issuer.name
                ));
            }
            if issuer.jwks_uri.is_some() && issuer.jwks_file.is_some() {
                return Err(format!(
                    "issuer {:?}: set jwks-uri or jwks-file, not both",
                    issuer.name
                ));
            }
        }
        for token in &self.tokens {
            if token.token.is_some() == token.token_file.is_some() {
                return Err(format!(
                    "[[security.tokens]] {:?}: set exactly one of token / token-file",
                    token.name
                ));
            }
            if token.roles.is_empty() {
                return Err(format!(
                    "[[security.tokens]] {:?}: roles is empty",
                    token.name
                ));
            }
        }
        for grant in &self.grants {
            if grant.roles.is_empty() {
                return Err(format!(
                    "[[security.grants]] for source {:?}: roles is empty",
                    grant.source
                ));
            }
            if grant.source == MTLS_SOURCE {
                if !mtls_available {
                    return Err(
                        "a [[security.grants]] entry uses source \"mtls\" but tls-cert, \
                         tls-key and tls-ca are not all set"
                            .into(),
                    );
                }
                if !grant.claims.is_empty() {
                    return Err("mtls grants cannot match on claims".into());
                }
            } else if !names.contains(grant.source.as_str()) {
                return Err(format!(
                    "[[security.grants]] source {:?} is not a declared issuer (or \"mtls\")",
                    grant.source
                ));
            }
        }

        // Peers authenticate to each other with the legacy access token or
        // with their TLS identity; nothing else is available to them.
        let peer_path = self.access_token.is_some()
            || self
                .grants
                .iter()
                .any(|g| g.source == MTLS_SOURCE && g.roles.contains(&Role::Peer));
        if clustered && self.enabled() && !peer_path {
            return Err(
                "authentication is enabled on a clustered node but peers have no way in: \
                 set [security] access-token, or add a [[security.grants]] entry with \
                 source = \"mtls\" and roles = [\"peer\"] matching the nodes' certificates"
                    .into(),
            );
        }
        Ok(())
    }
}
