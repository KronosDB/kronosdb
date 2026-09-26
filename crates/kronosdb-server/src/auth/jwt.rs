//! JWT verification against an issuer's JWKS. Shared by the gRPC identity
//! layer and the admin console's OIDC mode.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use jsonwebtoken::Algorithm;
use jsonwebtoken::jwk::JwkSet;
use serde::Deserialize;
use tokio::sync::RwLock;

const JWKS_REFRESH_INTERVAL: Duration = Duration::from_secs(3600);
/// Floor between refetches triggered by an unknown `kid`. Key rotation is
/// picked up within this window; tokens with made-up kids can't turn every
/// request into an outbound fetch.
const JWKS_MISS_COOLDOWN: Duration = Duration::from_secs(30);
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Deserialize, Clone)]
pub struct Discovery {
    pub jwks_uri: String,
    pub authorization_endpoint: Option<String>,
    pub token_endpoint: Option<String>,
}

/// Where the signing keys come from.
pub enum JwksSource {
    /// `{issuer}/.well-known/openid-configuration` → `jwks_uri`.
    Discovery,
    Uri(String),
    File(PathBuf),
}

pub enum Audience<'a> {
    /// Audience not enforced.
    Any,
    OneOf(&'a [String]),
}

pub struct JwksVerifier {
    issuer: String,
    source: JwksSource,
    http: reqwest::Client,
    bearer_token_file: Option<PathBuf>,
    discovery: tokio::sync::OnceCell<Discovery>,
    jwks: RwLock<Option<(JwkSet, Instant)>>,
}

impl JwksVerifier {
    /// Performs NO network I/O — keys are fetched lazily on first use so an
    /// unreachable IdP can't block boot.
    pub fn new(issuer: &str, source: JwksSource) -> Self {
        Self::with_http(issuer, source, None, None).expect("default http client")
    }

    pub fn with_http(
        issuer: &str,
        source: JwksSource,
        ca_pem: Option<&[u8]>,
        bearer_token_file: Option<PathBuf>,
    ) -> Result<Self, String> {
        let mut http = reqwest::Client::builder().timeout(FETCH_TIMEOUT);
        if let Some(pem) = ca_pem {
            let cert = reqwest::Certificate::from_pem(pem)
                .map_err(|e| format!("issuer {issuer}: bad ca-file: {e}"))?;
            http = http.add_root_certificate(cert);
        }
        Ok(Self {
            issuer: issuer.to_string(),
            source,
            http: http.build().map_err(|e| format!("http client: {e}"))?,
            bearer_token_file,
            discovery: tokio::sync::OnceCell::new(),
            jwks: RwLock::new(None),
        })
    }

    pub fn http(&self) -> &reqwest::Client {
        &self.http
    }

    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        url: &str,
        what: &str,
    ) -> Result<T, String> {
        let mut req = self.http.get(url);
        if let Some(path) = &self.bearer_token_file {
            // Read per fetch: projected service-account tokens rotate.
            let token = std::fs::read_to_string(path)
                .map_err(|e| format!("{what}: reading {}: {e}", path.display()))?;
            req = req.bearer_auth(token.trim());
        }
        req.send()
            .await
            .map_err(|e| format!("{what} fetch failed: {e}"))?
            .error_for_status()
            .map_err(|e| format!("{what} fetch failed: {e}"))?
            .json::<T>()
            .await
            .map_err(|e| format!("{what} parse failed: {e}"))
    }

    pub async fn discovery(&self) -> Result<&Discovery, String> {
        self.discovery
            .get_or_try_init(|| async {
                let url = format!(
                    "{}/.well-known/openid-configuration",
                    self.issuer.trim_end_matches('/')
                );
                self.get_json::<Discovery>(&url, "oidc discovery").await
            })
            .await
    }

    async fn load_jwks(&self) -> Result<JwkSet, String> {
        match &self.source {
            JwksSource::File(path) => {
                let bytes = tokio::fs::read(path)
                    .await
                    .map_err(|e| format!("jwks file {}: {e}", path.display()))?;
                serde_json::from_slice(&bytes).map_err(|e| format!("jwks parse failed: {e}"))
            }
            JwksSource::Uri(uri) => self.get_json(uri, "jwks").await,
            JwksSource::Discovery => {
                let uri = self.discovery().await?.jwks_uri.clone();
                self.get_json(&uri, "jwks").await
            }
        }
    }

    /// Returns the JWKS, refreshing when stale or when `kid` is unknown.
    async fn jwks(&self, kid: Option<&str>) -> Result<JwkSet, String> {
        {
            let cached = self.jwks.read().await;
            if let Some((set, fetched_at)) = cached.as_ref() {
                let age = fetched_at.elapsed();
                let has_kid = kid.is_none_or(|k| set.find(k).is_some());
                if age < JWKS_REFRESH_INTERVAL && (has_kid || age < JWKS_MISS_COOLDOWN) {
                    return Ok(set.clone());
                }
            }
        }
        match self.load_jwks().await {
            Ok(set) => {
                *self.jwks.write().await = Some((set.clone(), Instant::now()));
                Ok(set)
            }
            // An IdP blip must not lock out tokens signed by keys we
            // already hold.
            Err(e) => match self.jwks.read().await.as_ref() {
                Some((set, _)) => {
                    tracing::warn!(issuer = %self.issuer, error = %e, "jwks refresh failed; using cached keys");
                    Ok(set.clone())
                }
                None => Err(e),
            },
        }
    }

    /// Validates signature, issuer, expiry, and audience. Returns the claims.
    pub async fn verify(
        &self,
        token: &str,
        audience: Audience<'_>,
    ) -> Result<serde_json::Value, String> {
        let header =
            jsonwebtoken::decode_header(token).map_err(|e| format!("bad jwt header: {e}"))?;
        // JWKS keys are public keys. A symmetric algorithm here would mean
        // verifying an HMAC with material anyone can download.
        if matches!(
            header.alg,
            Algorithm::HS256 | Algorithm::HS384 | Algorithm::HS512
        ) {
            return Err("symmetric jwt algorithms are not accepted".into());
        }
        let jwks = self.jwks(header.kid.as_deref()).await?;
        let jwk = match &header.kid {
            Some(kid) => jwks.find(kid).ok_or("jwt kid not found in jwks")?,
            None => jwks.keys.first().ok_or("empty jwks")?,
        };
        let decoding_key =
            jsonwebtoken::DecodingKey::from_jwk(jwk).map_err(|e| format!("bad jwk: {e}"))?;

        let mut validation = jsonwebtoken::Validation::new(header.alg);
        validation.set_issuer(&[&self.issuer]);
        match audience {
            Audience::OneOf(auds) if !auds.is_empty() => validation.set_audience(auds),
            _ => validation.validate_aud = false,
        }

        jsonwebtoken::decode::<serde_json::Value>(token, &decoding_key, &validation)
            .map(|data| data.claims)
            .map_err(|e| format!("jwt validation failed: {e}"))
    }
}

/// Reads the `iss` claim WITHOUT verifying anything — only good for picking
/// which issuer's keys to verify against.
pub fn unverified_issuer(token: &str) -> Option<String> {
    use base64::Engine as _;
    let payload = token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .ok()?;
    let claims: serde_json::Value = serde_json::from_slice(&bytes).ok()?;
    claims.get("iss")?.as_str().map(String::from)
}

/// Walks a dotted path (`realm_access.roles`) through the claims.
pub fn claim_at<'a>(claims: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    // Try the literal key first: some IdPs use namespaced claim names that
    // contain dots (`https://example.com/roles`).
    if let Some(v) = claims.get(path) {
        return Some(v);
    }
    let mut node = claims;
    for part in path.split('.') {
        node = node.get(part)?;
    }
    Some(node)
}

/// String members of an array claim at `path` (default `roles`).
pub fn extract_roles(claims: &serde_json::Value, role_claim: Option<&str>) -> Vec<String> {
    claim_at(claims, role_claim.unwrap_or("roles"))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_roles_dotted_path() {
        let claims = serde_json::json!({
            "realm_access": { "roles": ["kronosdb-admin", "user"] }
        });
        assert_eq!(
            extract_roles(&claims, Some("realm_access.roles")),
            vec!["kronosdb-admin".to_string(), "user".to_string()]
        );
        assert!(extract_roles(&claims, Some("resource_access.app.roles")).is_empty());
    }

    #[test]
    fn extract_roles_default_top_level() {
        let claims = serde_json::json!({ "roles": ["a"] });
        assert_eq!(extract_roles(&claims, None), vec!["a".to_string()]);
    }

    #[test]
    fn claim_at_prefers_literal_dotted_key() {
        let claims = serde_json::json!({ "https://x.io/roles": ["a"], "a": { "b": 1 } });
        assert!(claim_at(&claims, "https://x.io/roles").is_some());
        assert_eq!(claim_at(&claims, "a.b"), Some(&serde_json::json!(1)));
    }

    #[test]
    fn unverified_issuer_reads_payload() {
        use base64::Engine as _;
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(br#"{"iss":"https://accounts.google.com"}"#);
        let token = format!("e30.{payload}.sig");
        assert_eq!(
            unverified_issuer(&token).as_deref(),
            Some("https://accounts.google.com")
        );
        assert_eq!(unverified_issuer("not-a-jwt"), None);
    }
}
