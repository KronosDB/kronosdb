//! Authentication for the admin console and admin API.
//!
//! Decoupled from the console/API handlers: everything happens in a single
//! axum middleware (`require_auth`) plus three `/auth/*` routes. The rest of
//! the admin surface never sees auth concerns.
//!
//! Modes (config `[admin.auth] mode`, `--admin-auth-mode`, `KRONOSDB_ADMIN_AUTH_MODE`):
//! - `none`  — no auth (default; a prominent warning is logged at startup).
//! - `token` — static bearer token, constant-time compared. Browsers may
//!   bootstrap a session with `?access_token=<token>` once; APIs send
//!   `Authorization: Bearer <token>`.
//! - `oidc`  — OpenID Connect against an "admin realm" (Keycloak, Auth0,
//!   Entra ID, ...). The console uses the authorization-code flow with PKCE
//!   and an HMAC-signed session cookie; API calls send a JWT access token as
//!   `Authorization: Bearer`, validated against the IdP's JWKS (issuer, exp,
//!   optional audience, optional required role).
//!
//! `/health`, `/ready`, `/metrics`, and `/auth/*` are always reachable —
//! probes and scrapers don't authenticate.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::extract::{Query, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::get;
use axum_extra::extract::cookie::{Cookie, Key, SameSite, SignedCookieJar};
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256, Sha512};
use subtle::ConstantTimeEq;
use tokio::sync::RwLock;

use crate::config::{AdminAuthConfig, AdminAuthMode, OidcConfig};

use super::AdminState;

const SESSION_COOKIE: &str = "kdb_session";
const FLOW_COOKIE: &str = "kdb_auth_flow";
const FLOW_TTL_SECS: u64 = 600;
const JWKS_REFRESH_INTERVAL: Duration = Duration::from_secs(3600);

// ───────────────────────────── runtime ─────────────────────────────

/// Long-lived auth state carried on `AdminState`.
pub struct AuthRuntime {
    pub mode: AdminAuthMode,
    token: Option<String>,
    key: Key,
    session_ttl: Duration,
    oidc: Option<OidcRuntime>,
}

impl AuthRuntime {
    /// Builds the runtime. Performs NO network I/O — OIDC discovery is
    /// fetched lazily on first use so an unreachable IdP can't block boot.
    pub fn new(cfg: &AdminAuthConfig) -> Self {
        let key = match cfg.oidc.as_ref().and_then(|o| o.cookie_secret.as_deref()) {
            // Stretch the configured secret to the 64 bytes Key::from wants.
            Some(secret) => Key::from(&Sha512::digest(secret.as_bytes())),
            None => Key::generate(),
        };
        let session_ttl = Duration::from_secs(
            cfg.oidc
                .as_ref()
                .map(|o| o.session_ttl_minutes)
                .unwrap_or(480)
                * 60,
        );
        match cfg.mode {
            AdminAuthMode::None => tracing::warn!(
                "admin console/API auth is DISABLED — anyone who can reach the admin port \
                 can browse events and mutate cluster membership. Set [admin.auth] mode to \
                 'token' or 'oidc' (KRONOSDB_ADMIN_AUTH_MODE) before exposing it."
            ),
            AdminAuthMode::Token => tracing::info!("admin auth: static token"),
            AdminAuthMode::Oidc => tracing::info!(
                issuer = %cfg.oidc.as_ref().map(|o| o.issuer.as_str()).unwrap_or(""),
                "admin auth: OIDC"
            ),
        }
        Self {
            mode: cfg.mode.clone(),
            token: cfg.token.clone(),
            key,
            session_ttl,
            oidc: cfg.oidc.as_ref().map(OidcRuntime::new),
        }
    }

    fn token_matches(&self, presented: &str) -> bool {
        match &self.token {
            Some(expected) => expected.as_bytes().ct_eq(presented.as_bytes()).into(),
            None => false,
        }
    }
}

// ───────────────────────────── session ─────────────────────────────

#[derive(Serialize, Deserialize)]
struct Session {
    sub: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    roles: Vec<String>,
    /// Unix seconds.
    exp: u64,
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn read_session(auth: &AuthRuntime, headers: &HeaderMap) -> Option<Session> {
    let jar = SignedCookieJar::from_headers(headers, auth.key.clone());
    let cookie = jar.get(SESSION_COOKIE)?;
    let session: Session = serde_json::from_str(cookie.value()).ok()?;
    (session.exp > now_unix()).then_some(session)
}

fn session_cookie(auth: &AuthRuntime, session: &Session) -> SignedCookieJar {
    let jar = SignedCookieJar::new(auth.key.clone());
    let value = serde_json::to_string(session).unwrap_or_default();
    jar.add(
        Cookie::build((SESSION_COOKIE, value))
            .path("/")
            .http_only(true)
            .same_site(SameSite::Lax),
    )
}

fn clear_session(auth: &AuthRuntime, headers: &HeaderMap) -> SignedCookieJar {
    // The jar must be built from the request so the session cookie exists as
    // an "original" — cookie-jar removal only emits a removal Set-Cookie for
    // cookies it knows about; removing from an empty jar is a silent no-op.
    SignedCookieJar::from_headers(headers, auth.key.clone())
        .remove(Cookie::build((SESSION_COOKIE, "")).path("/"))
}

// ─────────────────────────── middleware ────────────────────────────

fn is_public(path: &str) -> bool {
    matches!(path, "/health" | "/ready" | "/metrics") || path.starts_with("/auth/")
}

fn wants_html(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|accept| accept.contains("text/html"))
        .unwrap_or(false)
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
}

fn unauthorized(msg: &str) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        [(header::CONTENT_TYPE, "application/json")],
        format!("{{\"error\":\"unauthorized\",\"detail\":\"{msg}\"}}"),
    )
        .into_response()
}

pub async fn require_auth(State(state): State<AdminState>, req: Request, next: Next) -> Response {
    let auth = &state.auth;
    let path = req.uri().path().to_string();

    if is_public(&path) || auth.mode == AdminAuthMode::None {
        return next.run(req).await;
    }

    // A valid session cookie satisfies every mode.
    if read_session(auth, req.headers()).is_some() {
        return next.run(req).await;
    }

    match auth.mode {
        AdminAuthMode::Token => {
            if let Some(token) = bearer(req.headers()) {
                if auth.token_matches(token) {
                    return next.run(req).await;
                }
                return unauthorized("invalid token");
            }
            // Browser bootstrap: GET ...?access_token=<token> once, then a
            // session cookie carries the rest of the visit.
            if let Some(presented) = query_param(req.uri().query(), "access_token") {
                if auth.token_matches(&presented) {
                    let session = Session {
                        sub: "token-admin".into(),
                        name: "token-admin".into(),
                        roles: vec![],
                        exp: now_unix() + auth.session_ttl.as_secs(),
                    };
                    let jar = session_cookie(auth, &session);
                    return (jar, Redirect::to(&path)).into_response();
                }
                return unauthorized("invalid token");
            }
            unauthorized("missing bearer token")
        }
        AdminAuthMode::Oidc => {
            let Some(oidc) = &auth.oidc else {
                return unauthorized("oidc not configured");
            };
            if let Some(token) = bearer(req.headers()) {
                return match oidc.validate_bearer(token).await {
                    Ok(()) => next.run(req).await,
                    Err(e) => unauthorized(&e),
                };
            }
            if wants_html(req.headers()) {
                let rd = req
                    .uri()
                    .path_and_query()
                    .map(|pq| pq.as_str().to_string())
                    .unwrap_or_else(|| "/".to_string());
                return Redirect::to(&format!("/auth/login?rd={}", urlencode(&rd))).into_response();
            }
            unauthorized("missing bearer token")
        }
        AdminAuthMode::None => unreachable!("handled above"),
    }
}

/// Registers the `/auth/*` routes on the admin router.
pub fn routes() -> Router<AdminState> {
    Router::new()
        .route("/auth/login", get(login))
        .route("/auth/callback", get(callback))
        .route("/auth/logout", get(logout))
}

// ─────────────────────────── OIDC runtime ──────────────────────────

#[derive(Deserialize, Clone)]
struct Discovery {
    authorization_endpoint: String,
    token_endpoint: String,
    jwks_uri: String,
}

struct OidcRuntime {
    cfg: OidcConfig,
    http: reqwest::Client,
    discovery: tokio::sync::OnceCell<Discovery>,
    jwks: RwLock<Option<(jsonwebtoken::jwk::JwkSet, Instant)>>,
}

impl OidcRuntime {
    fn new(cfg: &OidcConfig) -> Self {
        Self {
            cfg: cfg.clone(),
            http: reqwest::Client::new(),
            discovery: tokio::sync::OnceCell::new(),
            jwks: RwLock::new(None),
        }
    }

    async fn discovery(&self) -> Result<&Discovery, String> {
        self.discovery
            .get_or_try_init(|| async {
                let url = format!("{}/.well-known/openid-configuration", self.cfg.issuer);
                self.http
                    .get(&url)
                    .send()
                    .await
                    .map_err(|e| format!("oidc discovery fetch failed: {e}"))?
                    .error_for_status()
                    .map_err(|e| format!("oidc discovery fetch failed: {e}"))?
                    .json::<Discovery>()
                    .await
                    .map_err(|e| format!("oidc discovery parse failed: {e}"))
            })
            .await
    }

    /// Returns the JWKS, refreshing when stale or when `kid` is unknown.
    async fn jwks(&self, kid: Option<&str>) -> Result<jsonwebtoken::jwk::JwkSet, String> {
        {
            let cached = self.jwks.read().await;
            if let Some((set, fetched_at)) = cached.as_ref() {
                let fresh = fetched_at.elapsed() < JWKS_REFRESH_INTERVAL;
                let has_kid = kid.is_none_or(|k| set.find(k).is_some());
                if fresh && has_kid {
                    return Ok(set.clone());
                }
            }
        }
        let uri = self.discovery().await?.jwks_uri.clone();
        let set: jsonwebtoken::jwk::JwkSet = self
            .http
            .get(&uri)
            .send()
            .await
            .map_err(|e| format!("jwks fetch failed: {e}"))?
            .error_for_status()
            .map_err(|e| format!("jwks fetch failed: {e}"))?
            .json()
            .await
            .map_err(|e| format!("jwks parse failed: {e}"))?;
        *self.jwks.write().await = Some((set.clone(), Instant::now()));
        Ok(set)
    }

    /// Validates a JWT's signature (JWKS), issuer, expiry, and — when
    /// configured — audience. Returns the claims.
    async fn validate_jwt(
        &self,
        token: &str,
        expected_aud: Option<&str>,
        expected_nonce: Option<&str>,
    ) -> Result<serde_json::Value, String> {
        let header =
            jsonwebtoken::decode_header(token).map_err(|e| format!("bad jwt header: {e}"))?;
        let jwks = self.jwks(header.kid.as_deref()).await?;
        let jwk = match &header.kid {
            Some(kid) => jwks.find(kid).ok_or("jwt kid not found in jwks")?,
            None => jwks.keys.first().ok_or("empty jwks")?,
        };
        let decoding_key =
            jsonwebtoken::DecodingKey::from_jwk(jwk).map_err(|e| format!("bad jwk: {e}"))?;

        let mut validation = jsonwebtoken::Validation::new(header.alg);
        validation.set_issuer(&[&self.cfg.issuer]);
        match expected_aud {
            Some(aud) => validation.set_audience(&[aud]),
            None => validation.validate_aud = false,
        }

        let data = jsonwebtoken::decode::<serde_json::Value>(token, &decoding_key, &validation)
            .map_err(|e| format!("jwt validation failed: {e}"))?;

        if let Some(nonce) = expected_nonce {
            let claim_nonce = data.claims.get("nonce").and_then(|v| v.as_str());
            if claim_nonce != Some(nonce) {
                return Err("nonce mismatch".into());
            }
        }
        Ok(data.claims)
    }

    /// Bearer path for API clients: signature + issuer + expiry + optional
    /// audience + optional required role.
    async fn validate_bearer(&self, token: &str) -> Result<(), String> {
        let claims = self
            .validate_jwt(token, self.cfg.audience.as_deref(), None)
            .await?;
        self.check_role(&claims)
    }

    fn check_role(&self, claims: &serde_json::Value) -> Result<(), String> {
        let Some(required) = &self.cfg.required_role else {
            return Ok(());
        };
        if extract_roles(claims, self.cfg.role_claim.as_deref()).contains(required) {
            Ok(())
        } else {
            Err(format!("required role '{required}' not present"))
        }
    }
}

/// Extracts roles from claims at a dotted path (e.g. Keycloak's
/// `realm_access.roles`). Falls back to the common top-level `roles` claim.
fn extract_roles(claims: &serde_json::Value, role_claim: Option<&str>) -> Vec<String> {
    let path = role_claim.unwrap_or("roles");
    let mut node = claims;
    for part in path.split('.') {
        match node.get(part) {
            Some(next) => node = next,
            None => return vec![],
        }
    }
    node.as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

// ─────────────────────────── OIDC handlers ─────────────────────────

#[derive(Serialize, Deserialize)]
struct AuthFlow {
    state: String,
    nonce: String,
    verifier: String,
    rd: String,
    exp: u64,
}

fn random_urlsafe(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    rand::thread_rng().fill_bytes(&mut buf);
    URL_SAFE_NO_PAD.encode(buf)
}

fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 3);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Callback URL: explicit config, else derived from Host/X-Forwarded-Proto.
fn redirect_url(cfg: &OidcConfig, headers: &HeaderMap) -> String {
    if let Some(url) = &cfg.redirect_url {
        return url.clone();
    }
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("http");
    let host = headers
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("localhost:9240");
    format!("{scheme}://{host}/auth/callback")
}

#[derive(Deserialize)]
struct LoginParams {
    rd: Option<String>,
}

async fn login(
    State(state): State<AdminState>,
    Query(params): Query<LoginParams>,
    headers: HeaderMap,
) -> Response {
    let auth = &state.auth;
    let Some(oidc) = &auth.oidc else {
        return unauthorized("oidc not configured");
    };
    let discovery = match oidc.discovery().await {
        Ok(d) => d.clone(),
        Err(e) => return (StatusCode::BAD_GATEWAY, e).into_response(),
    };

    // Only ever redirect back to a local path — never an absolute URL.
    let rd = params
        .rd
        .filter(|rd| rd.starts_with('/') && !rd.starts_with("//"))
        .unwrap_or_else(|| "/".to_string());
    let flow = AuthFlow {
        state: random_urlsafe(24),
        nonce: random_urlsafe(24),
        verifier: random_urlsafe(48),
        rd,
        exp: now_unix() + FLOW_TTL_SECS,
    };
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(flow.verifier.as_bytes()));

    let authorize = format!(
        "{}?response_type=code&client_id={}&redirect_uri={}&scope={}&state={}&nonce={}&code_challenge={}&code_challenge_method=S256",
        discovery.authorization_endpoint,
        urlencode(&oidc.cfg.client_id),
        urlencode(&redirect_url(&oidc.cfg, &headers)),
        urlencode(&oidc.cfg.scopes.join(" ")),
        flow.state,
        flow.nonce,
        challenge,
    );

    let jar = SignedCookieJar::new(auth.key.clone()).add(
        Cookie::build((
            FLOW_COOKIE,
            serde_json::to_string(&flow).unwrap_or_default(),
        ))
        .path("/auth")
        .http_only(true)
        .same_site(SameSite::Lax),
    );
    (jar, Redirect::to(&authorize)).into_response()
}

#[derive(Deserialize)]
struct CallbackParams {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
}

#[derive(Deserialize)]
struct TokenResponse {
    id_token: Option<String>,
    access_token: Option<String>,
}

async fn callback(
    State(state): State<AdminState>,
    Query(params): Query<CallbackParams>,
    headers: HeaderMap,
) -> Response {
    let auth = &state.auth;
    let Some(oidc) = &auth.oidc else {
        return unauthorized("oidc not configured");
    };
    if let Some(err) = params.error {
        let detail = params.error_description.unwrap_or_default();
        return unauthorized(&format!("idp error: {err} {detail}"));
    }
    let (Some(code), Some(cb_state)) = (params.code, params.state) else {
        return unauthorized("missing code/state");
    };

    // CSRF check: the state must match the flow cookie we issued.
    let jar = SignedCookieJar::from_headers(&headers, auth.key.clone());
    let Some(flow) = jar
        .get(FLOW_COOKIE)
        .and_then(|c| serde_json::from_str::<AuthFlow>(c.value()).ok())
    else {
        return unauthorized("missing/invalid auth flow cookie");
    };
    if flow.exp < now_unix() {
        return unauthorized("auth flow expired, retry login");
    }
    if flow.state.as_bytes().ct_eq(cb_state.as_bytes()).unwrap_u8() != 1 {
        return unauthorized("state mismatch");
    }

    let discovery = match oidc.discovery().await {
        Ok(d) => d.clone(),
        Err(e) => return (StatusCode::BAD_GATEWAY, e).into_response(),
    };

    // Exchange the code (PKCE verifier always; client secret when present).
    let mut form = vec![
        ("grant_type", "authorization_code".to_string()),
        ("code", code),
        ("redirect_uri", redirect_url(&oidc.cfg, &headers)),
        ("client_id", oidc.cfg.client_id.clone()),
        ("code_verifier", flow.verifier.clone()),
    ];
    if let Some(secret) = &oidc.cfg.client_secret {
        form.push(("client_secret", secret.clone()));
    }
    let token_response = match oidc
        .http
        .post(&discovery.token_endpoint)
        .form(&form)
        .send()
        .await
    {
        Ok(resp) if resp.status().is_success() => match resp.json::<TokenResponse>().await {
            Ok(t) => t,
            Err(e) => return unauthorized(&format!("token response parse failed: {e}")),
        },
        Ok(resp) => {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return unauthorized(&format!("token exchange failed ({status}): {body}"));
        }
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                format!("token exchange failed: {e}"),
            )
                .into_response();
        }
    };
    let Some(id_token) = token_response.id_token else {
        return unauthorized("no id_token in token response");
    };

    // Validate the ID token: signature, issuer, expiry, audience = client id,
    // and the nonce we bound to this flow.
    let claims = match oidc
        .validate_jwt(&id_token, Some(&oidc.cfg.client_id), Some(&flow.nonce))
        .await
    {
        Ok(c) => c,
        Err(e) => return unauthorized(&e),
    };

    // Roles: Keycloak-style IdPs put realm roles in the ACCESS token, not
    // the ID token. Prefer the access token (validated against the same
    // JWKS; audience differs per IdP so it isn't enforced here), fall back
    // to ID-token claims for IdPs that map roles there.
    let mut roles = extract_roles(&claims, oidc.cfg.role_claim.as_deref());
    if roles.is_empty()
        && let Some(access_token) = &token_response.access_token
        && let Ok(access_claims) = oidc.validate_jwt(access_token, None, None).await
    {
        roles = extract_roles(&access_claims, oidc.cfg.role_claim.as_deref());
    }
    if let Some(required) = &oidc.cfg.required_role
        && !roles.contains(required)
    {
        return (
            StatusCode::FORBIDDEN,
            format!("required role '{required}' not present"),
        )
            .into_response();
    }

    let session = Session {
        sub: claims
            .get("sub")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string(),
        name: claims
            .get("preferred_username")
            .or_else(|| claims.get("name"))
            .or_else(|| claims.get("email"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
        roles,
        exp: now_unix() + auth.session_ttl.as_secs(),
    };
    tracing::info!(sub = %session.sub, name = %session.name, "admin console login");

    let jar = session_cookie(auth, &session).remove(Cookie::build((FLOW_COOKIE, "")).path("/auth"));
    (jar, Redirect::to(&flow.rd)).into_response()
}

async fn logout(State(state): State<AdminState>, headers: HeaderMap) -> Response {
    let jar = clear_session(&state.auth, &headers);
    (jar, Redirect::to("/")).into_response()
}

fn query_param(query: Option<&str>, name: &str) -> Option<String> {
    let query = query?;
    for pair in query.split('&') {
        let mut it = pair.splitn(2, '=');
        if it.next() == Some(name) {
            return it.next().map(|v| v.to_string());
        }
    }
    None
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
    fn urlencode_reserved() {
        assert_eq!(urlencode("/a b?c=d"), "%2Fa%20b%3Fc%3Dd");
    }

    #[test]
    fn query_param_extraction() {
        assert_eq!(
            query_param(Some("x=1&access_token=abc"), "access_token").as_deref(),
            Some("abc")
        );
        assert_eq!(query_param(Some("x=1"), "access_token"), None);
        assert_eq!(query_param(None, "access_token"), None);
    }

    #[test]
    fn session_roundtrip_and_expiry() {
        let cfg = AdminAuthConfig {
            mode: AdminAuthMode::Token,
            token: Some("secret".into()),
            oidc: None,
        };
        let auth = AuthRuntime::new(&cfg);

        // The on-wire (signed) cookie only exists in response headers —
        // jar.get() would hand back the already-verified plaintext.
        fn as_request_headers(jar: SignedCookieJar) -> HeaderMap {
            let response = jar.into_response();
            let set_cookie = response
                .headers()
                .get(header::SET_COOKIE)
                .expect("set-cookie present")
                .to_str()
                .unwrap();
            let pair = set_cookie.split(';').next().unwrap().to_string();
            let mut headers = HeaderMap::new();
            headers.insert(header::COOKIE, pair.parse().unwrap());
            headers
        }

        let session = Session {
            sub: "u1".into(),
            name: "user".into(),
            roles: vec!["admin".into()],
            exp: now_unix() + 60,
        };
        let headers = as_request_headers(session_cookie(&auth, &session));
        let read = read_session(&auth, &headers).expect("session read back");
        assert_eq!(read.sub, "u1");

        // Expired sessions are rejected.
        let expired = Session {
            exp: now_unix() - 1,
            ..read
        };
        let headers = as_request_headers(session_cookie(&auth, &expired));
        assert!(read_session(&auth, &headers).is_none());

        // Tampered cookies are rejected by the signature.
        let mut tampered = HeaderMap::new();
        tampered.insert(
            header::COOKIE,
            format!("{SESSION_COOKIE}=forged-value").parse().unwrap(),
        );
        assert!(read_session(&auth, &tampered).is_none());
    }

    #[test]
    fn token_compare_is_exact() {
        let cfg = AdminAuthConfig {
            mode: AdminAuthMode::Token,
            token: Some("correct-horse".into()),
            oidc: None,
        };
        let auth = AuthRuntime::new(&cfg);
        assert!(auth.token_matches("correct-horse"));
        assert!(!auth.token_matches("correct-horsf"));
        assert!(!auth.token_matches("correct-hors"));
        assert!(!auth.token_matches(""));
    }
}
