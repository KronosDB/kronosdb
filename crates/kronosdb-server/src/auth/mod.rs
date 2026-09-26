//! Identity and authorization for the gRPC plane.
//!
//! One tower layer ([`layer::AuthLayer`]) sits in front of every service. It
//! turns whatever credential the caller presents into a [`Principal`], checks
//! the principal's grants against what the method needs ([`policy`]), and
//! stores the principal in the request extensions for handlers.
//!
//! Credentials, all optional, all combinable on one port:
//! - **Static tokens** — legacy `[security] access-token` (full access, the
//!   credential peers use) and named `[[security.tokens]]` with their own
//!   roles. Sent as `kronosdb-token: <t>` or `authorization: Bearer <t>`.
//! - **OIDC JWTs** — any number of trusted `[[security.issuers]]`. Human SSO
//!   and workload identity are the same mechanism: Google / Entra / Keycloak
//!   ID tokens, Kubernetes projected service-account tokens (GKE, EKS, AKS
//!   workload identity), GitHub Actions, SPIFFE JWT-SVIDs. Nothing here is
//!   cloud-specific; a cloud is just an issuer URL.
//! - **mTLS** — a client certificate verified against `tls-ca`; the identity
//!   is its SPIFFE ID / DNS SAN / CN, bound to roles by `source = "mtls"`
//!   grants.
//!
//! With nothing configured the server stays open, as it always has.

pub mod config;
pub mod jwt;
pub mod layer;
pub mod mtls;
pub mod policy;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use config::{GrantConfig, IdentityConfig, IssuerConfig, MTLS_SOURCE, Role};
use jwt::{Audience, JwksSource, JwksVerifier};
use policy::{Access, Grant, Scope};

/// Verified-credential caches are flushed wholesale past this many entries;
/// simpler than LRU and the working set (live tokens, live client certs) is
/// far smaller.
const CACHE_CAP: usize = 4096;
/// Upper bound on how long a verified JWT is trusted without re-verifying,
/// whatever its `exp` says.
const JWT_CACHE_TTL: Duration = Duration::from_secs(300);

/// Who is calling, and what they may do.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    pub subject: String,
    /// `"anonymous"`, `"token"`, `"mtls"`, or an issuer name.
    pub source: String,
    pub grants: Vec<Grant>,
}

impl Principal {
    /// The caller when authentication is not configured: everything allowed.
    fn anonymous() -> Self {
        Self {
            subject: "anonymous".into(),
            source: "anonymous".into(),
            grants: vec![Grant::unscoped(vec![Role::Admin, Role::Peer])],
        }
    }

    pub fn allows(&self, access: Access, scope: Scope, target: &str) -> bool {
        // Reaching this point at all means the caller was verified.
        matches!(access, Access::Public | Access::Authenticated)
            || self.grants.iter().any(|g| g.allows(access, scope, target))
    }

    /// Cluster-wide admin (the admin HTTP plane's requirement).
    pub fn is_admin(&self) -> bool {
        self.allows(Access::Admin, Scope::Global, "")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// No credential, or one that doesn't verify.
    Unauthenticated(String),
    /// A verified caller without a grant covering the request.
    PermissionDenied(String),
}

/// What a request carries that could identify its sender.
#[derive(Default)]
pub struct Credentials<'a> {
    /// From `authorization: Bearer` or `kronosdb-token`.
    pub token: Option<&'a str>,
    /// Leaf client certificate (DER), already chain-verified by rustls.
    pub client_cert: Option<&'a [u8]>,
}

struct StaticToken {
    digest: [u8; 32],
    principal: Arc<Principal>,
}

struct Issuer {
    cfg: IssuerConfig,
    verifier: JwksVerifier,
}

#[derive(Default)]
pub struct AuthStats {
    pub token: AtomicU64,
    pub jwt: AtomicU64,
    pub mtls: AtomicU64,
    pub unauthenticated: AtomicU64,
    pub denied: AtomicU64,
}

pub struct Authenticator {
    enabled: bool,
    anonymous: Arc<Principal>,
    tokens: Vec<StaticToken>,
    issuers: Vec<Issuer>,
    grants: Vec<GrantConfig>,
    mtls: bool,
    jwt_cache: DashMap<[u8; 32], (Arc<Principal>, u64)>,
    cert_cache: DashMap<Vec<u8>, Arc<Principal>>,
    pub stats: AuthStats,
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn digest(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

impl Authenticator {
    /// Performs NO network I/O; issuer keys are fetched on first use.
    pub fn new(cfg: &IdentityConfig) -> Result<Self, String> {
        let mut tokens = Vec::new();
        if let Some(token) = &cfg.access_token {
            tokens.push(StaticToken {
                digest: digest(token),
                principal: Arc::new(Principal {
                    subject: "access-token".into(),
                    source: "token".into(),
                    grants: vec![Grant::unscoped(vec![Role::Admin, Role::Peer])],
                }),
            });
        }
        for t in &cfg.tokens {
            let secret = match (&t.token, &t.token_file) {
                (Some(token), _) => token.clone(),
                (None, Some(path)) => std::fs::read_to_string(path)
                    .map_err(|e| format!("token {:?}: reading {}: {e}", t.name, path.display()))?
                    .trim()
                    .to_string(),
                (None, None) => unreachable!("validated"),
            };
            if secret.is_empty() {
                return Err(format!("token {:?} is empty", t.name));
            }
            tokens.push(StaticToken {
                digest: digest(&secret),
                principal: Arc::new(Principal {
                    subject: t.name.clone(),
                    source: "token".into(),
                    grants: vec![Grant {
                        roles: t.roles.clone(),
                        contexts: t.contexts.clone(),
                        buses: t.buses.clone(),
                    }],
                }),
            });
        }

        let mut issuers = Vec::new();
        for i in &cfg.issuers {
            let source = match (&i.jwks_file, &i.jwks_uri) {
                (Some(path), _) => JwksSource::File(path.clone()),
                (None, Some(uri)) => JwksSource::Uri(uri.clone()),
                (None, None) => JwksSource::Discovery,
            };
            let ca = i
                .ca_file
                .as_ref()
                .map(|p| {
                    std::fs::read(p)
                        .map_err(|e| format!("issuer {:?}: reading {}: {e}", i.name, p.display()))
                })
                .transpose()?;
            if i.audiences.is_empty() {
                tracing::warn!(
                    issuer = %i.name,
                    "issuer has no audiences: any token this issuer signs for ANY service is \
                     accepted — make sure its grants pin callers through subject/claims"
                );
            }
            issuers.push(Issuer {
                verifier: JwksVerifier::with_http(
                    &i.issuer,
                    source,
                    ca.as_deref(),
                    i.jwks_bearer_token_file.clone(),
                )?,
                cfg: i.clone(),
            });
        }

        Ok(Self {
            enabled: cfg.enabled(),
            anonymous: Arc::new(Principal::anonymous()),
            tokens,
            issuers,
            grants: cfg.grants.clone(),
            mtls: cfg.has_mtls_grants(),
            jwt_cache: DashMap::new(),
            cert_cache: DashMap::new(),
            stats: AuthStats::default(),
        })
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    /// The open-access principal, when authentication is not configured.
    /// `None` once it is — public methods then run without a principal.
    pub fn open_principal(&self) -> Option<Arc<Principal>> {
        (!self.enabled).then(|| Arc::clone(&self.anonymous))
    }

    /// Who has access, without the secrets: the `\du` of KronosDB. Token
    /// values and JWKS material never leave the process.
    pub fn access_summary(&self) -> serde_json::Value {
        let scope = |patterns: &Option<Vec<String>>| match patterns {
            None => serde_json::json!("*"),
            Some(patterns) => serde_json::json!(patterns),
        };
        let roles = |roles: &[Role]| roles.iter().map(|r| r.as_str()).collect::<Vec<_>>();
        serde_json::json!({
            "enabled": self.enabled,
            "tokens": self.tokens.iter().map(|t| {
                let grant = &t.principal.grants[0];
                serde_json::json!({
                    "name": t.principal.subject,
                    "roles": roles(&grant.roles),
                    "contexts": scope(&grant.contexts),
                    "buses": scope(&grant.buses),
                })
            }).collect::<Vec<_>>(),
            "issuers": self.issuers.iter().map(|i| serde_json::json!({
                "name": i.cfg.name,
                "issuer": i.cfg.issuer,
                "audiences": i.cfg.audiences,
                "subject_claim": i.cfg.subject_claim.as_deref().unwrap_or("sub"),
                "role_map": i.cfg.role_map.iter()
                    .map(|(from, to)| (from.clone(), to.as_str()))
                    .collect::<std::collections::BTreeMap<_, _>>(),
            })).collect::<Vec<_>>(),
            "grants": self.grants.iter().map(|g| serde_json::json!({
                "source": g.source,
                "subject": g.subject,
                "claims": g.claims.iter()
                    .map(|(k, v)| (k.clone(), v.to_string()))
                    .collect::<std::collections::BTreeMap<_, _>>(),
                "roles": roles(&g.roles),
                "contexts": scope(&g.contexts),
                "buses": scope(&g.buses),
            })).collect::<Vec<_>>(),
        })
    }

    /// One line per configured method, for the startup log and settings page.
    pub fn describe(&self) -> Vec<String> {
        let mut out = Vec::new();
        if !self.tokens.is_empty() {
            out.push(format!("static tokens ({})", self.tokens.len()));
        }
        for i in &self.issuers {
            out.push(format!("oidc:{} ({})", i.cfg.name, i.cfg.issuer));
        }
        if self.mtls {
            out.push("mtls".into());
        }
        out
    }

    /// Resolves the caller. A presented token that fails is final — it does
    /// not fall through to the client certificate, so a bad token is never
    /// masked by a good one.
    pub async fn authenticate(&self, creds: Credentials<'_>) -> Result<Arc<Principal>, AuthError> {
        if !self.enabled {
            return Ok(Arc::clone(&self.anonymous));
        }
        let result = if let Some(token) = creds.token {
            self.authenticate_token(token).await
        } else if let Some(der) = creds.client_cert
            && self.mtls
        {
            self.authenticate_cert(der)
        } else {
            Err(AuthError::Unauthenticated(
                "no credential: send authorization: Bearer <token>, kronosdb-token, or a \
                 client certificate"
                    .into(),
            ))
        };
        if result.is_err() {
            self.stats.unauthenticated.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Static token or JWT. Never yields the anonymous principal, so it is
    /// safe for callers (the admin plane) to use when `enabled()` is false.
    pub async fn authenticate_token(&self, token: &str) -> Result<Arc<Principal>, AuthError> {
        // Every digest is compared, without early exit: timing reveals
        // neither which token matched nor how much of one was right.
        let presented = digest(token);
        let mut matched = None;
        for t in &self.tokens {
            if bool::from(presented.ct_eq(&t.digest)) {
                matched = Some(&t.principal);
            }
        }
        if let Some(principal) = matched {
            self.stats.token.fetch_add(1, Ordering::Relaxed);
            return Ok(Arc::clone(principal));
        }

        if self.issuers.is_empty() || token.bytes().filter(|b| *b == b'.').count() != 2 {
            return Err(AuthError::Unauthenticated("invalid access token".into()));
        }

        let now = now_unix();
        if let Some(entry) = self.jwt_cache.get(&presented)
            && entry.1 > now
        {
            self.stats.jwt.fetch_add(1, Ordering::Relaxed);
            return Ok(Arc::clone(&entry.0));
        }

        let iss = jwt::unverified_issuer(token)
            .ok_or_else(|| AuthError::Unauthenticated("jwt has no iss claim".into()))?;
        let issuer = self
            .issuers
            .iter()
            .find(|i| i.cfg.issuer == iss)
            .ok_or_else(|| AuthError::Unauthenticated(format!("untrusted issuer {iss:?}")))?;
        let claims = issuer
            .verifier
            .verify(token, Audience::OneOf(&issuer.cfg.audiences))
            .await
            .map_err(AuthError::Unauthenticated)?;

        let principal = Arc::new(self.jwt_principal(issuer, &claims)?);
        let exp = claims.get("exp").and_then(|v| v.as_u64()).unwrap_or(now);
        if self.jwt_cache.len() >= CACHE_CAP {
            self.jwt_cache.clear();
        }
        self.jwt_cache.insert(
            presented,
            (
                Arc::clone(&principal),
                exp.min(now + JWT_CACHE_TTL.as_secs()),
            ),
        );
        tracing::debug!(subject = %principal.subject, issuer = %issuer.cfg.name, "jwt verified");
        self.stats.jwt.fetch_add(1, Ordering::Relaxed);
        Ok(principal)
    }

    fn jwt_principal(
        &self,
        issuer: &Issuer,
        claims: &serde_json::Value,
    ) -> Result<Principal, AuthError> {
        let subject_claim = issuer.cfg.subject_claim.as_deref().unwrap_or("sub");
        let subject = jwt::claim_at(claims, subject_claim)
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                AuthError::Unauthenticated(format!("jwt has no string {subject_claim:?} claim"))
            })?
            .to_string();

        let mut grants = Vec::new();
        let mapped: Vec<Role> = jwt::extract_roles(claims, issuer.cfg.role_claim.as_deref())
            .iter()
            .filter_map(|r| issuer.cfg.role_map.get(r).copied())
            .collect();
        if !mapped.is_empty() {
            grants.push(Grant::unscoped(mapped));
        }
        for g in &self.grants {
            if g.source == issuer.cfg.name
                && policy::glob_match(&g.subject, &subject)
                && g.claims
                    .iter()
                    .all(|(path, want)| claim_matches(jwt::claim_at(claims, path), want))
            {
                grants.push(Grant {
                    roles: g.roles.clone(),
                    contexts: g.contexts.clone(),
                    buses: g.buses.clone(),
                });
            }
        }
        Ok(Principal {
            subject,
            source: issuer.cfg.name.clone(),
            grants,
        })
    }

    fn authenticate_cert(&self, der: &[u8]) -> Result<Arc<Principal>, AuthError> {
        if let Some(principal) = self.cert_cache.get(der) {
            self.stats.mtls.fetch_add(1, Ordering::Relaxed);
            return Ok(Arc::clone(&principal));
        }
        let identity = mtls::CertIdentity::from_der(der).map_err(AuthError::Unauthenticated)?;
        let grants: Vec<Grant> = self
            .grants
            .iter()
            .filter(|g| {
                g.source == MTLS_SOURCE
                    && identity
                        .names
                        .iter()
                        .any(|name| policy::glob_match(&g.subject, name))
            })
            .map(|g| Grant {
                roles: g.roles.clone(),
                contexts: g.contexts.clone(),
                buses: g.buses.clone(),
            })
            .collect();
        let principal = Arc::new(Principal {
            subject: identity.primary().to_string(),
            source: MTLS_SOURCE.into(),
            grants,
        });
        if self.cert_cache.len() >= CACHE_CAP {
            self.cert_cache.clear();
        }
        self.cert_cache.insert(der.to_vec(), Arc::clone(&principal));
        self.stats.mtls.fetch_add(1, Ordering::Relaxed);
        Ok(principal)
    }

    /// Checks `principal` against what `path` needs.
    pub fn authorize(
        &self,
        principal: &Principal,
        path: &str,
        context: &str,
        bus: &str,
    ) -> Result<(), AuthError> {
        let (access, scope) = policy::classify(path);
        let target = match scope {
            Scope::Context => context,
            Scope::Bus => bus,
            Scope::Global => "",
        };
        if principal.allows(access, scope, target) {
            return Ok(());
        }
        self.stats.denied.fetch_add(1, Ordering::Relaxed);
        let on = match scope {
            Scope::Context => format!(" on context {target:?}"),
            Scope::Bus => format!(" on bus {target:?}"),
            Scope::Global => String::new(),
        };
        Err(AuthError::PermissionDenied(format!(
            "{} ({}) lacks {access:?} access{on}",
            principal.subject, principal.source
        )))
    }
}

/// Scalars compare by equality; an array claim matches when it contains the
/// wanted value (`groups`, `amr`, ...).
fn claim_matches(actual: Option<&serde_json::Value>, want: &toml::Value) -> bool {
    let Some(actual) = actual else { return false };
    if let Some(items) = actual.as_array() {
        return items.iter().any(|item| claim_matches(Some(item), want));
    }
    match want {
        toml::Value::String(s) => actual.as_str() == Some(s),
        toml::Value::Boolean(b) => actual.as_bool() == Some(*b),
        toml::Value::Integer(i) => actual.as_i64() == Some(*i),
        _ => false,
    }
}

#[cfg(test)]
mod tests;
