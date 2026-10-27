use std::collections::BTreeMap;
use std::io::Write as _;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;

use super::config::{GrantConfig, IdentityConfig, IssuerConfig, Role, TokenConfig};
use super::*;

const ISSUER: &str = "https://idp.test";
const APPEND: u16 = kronosdb_protocol::Append::KIND;
const SOURCE: u16 = kronosdb_protocol::Source::KIND;
const VOTE: u16 = kronosdb_protocol::RaftVote::KIND;
/// A kind no request starts with.
const UNKNOWN: u16 = 0x7ffe;

#[derive(Debug)]
struct Denied;

/// Whether `principal` may make a request of frame `kind` on `context` or
/// `bus`, as the native server decides it.
fn authorize(principal: &Principal, kind: u16, context: &str, bus: &str) -> Result<(), Denied> {
    let (access, scope) = policy::demand(kind);
    let target = match scope {
        policy::Scope::Context => context,
        policy::Scope::Bus => bus,
        policy::Scope::Global => "",
    };
    if principal.allows(access, scope, target) {
        Ok(())
    } else {
        Err(Denied)
    }
}

/// An ES256 signing key plus the JWKS file that publishes its public half.
struct TestIdp {
    key: jsonwebtoken::EncodingKey,
    public_raw: Vec<u8>,
    jwks: tempfile::NamedTempFile,
}

impl TestIdp {
    fn new() -> Self {
        let pair = rcgen::KeyPair::generate().unwrap();
        let public_raw = pair.public_key_raw().to_vec();
        // Uncompressed P-256 point: 0x04 || x(32) || y(32).
        assert_eq!(public_raw.len(), 65);
        let mut jwks = tempfile::NamedTempFile::new().unwrap();
        write!(
            jwks,
            r#"{{"keys":[{{"kty":"EC","crv":"P-256","alg":"ES256","use":"sig","kid":"k1","x":"{}","y":"{}"}}]}}"#,
            URL_SAFE_NO_PAD.encode(&public_raw[1..33]),
            URL_SAFE_NO_PAD.encode(&public_raw[33..65]),
        )
        .unwrap();
        Self {
            key: jsonwebtoken::EncodingKey::from_ec_pem(pair.serialize_pem().as_bytes()).unwrap(),
            public_raw,
            jwks,
        }
    }

    fn issuer_config(&self) -> IssuerConfig {
        IssuerConfig {
            name: "test".into(),
            issuer: ISSUER.into(),
            audiences: vec!["kronosdb".into()],
            jwks_uri: None,
            jwks_file: Some(self.jwks.path().to_path_buf()),
            ca_file: None,
            jwks_bearer_token_file: None,
            subject_claim: Some("email".into()),
            role_claim: Some("realm_access.roles".into()),
            role_map: BTreeMap::from([("kronos-admins".to_string(), Role::Admin)]),
        }
    }

    fn sign(&self, claims: serde_json::Value) -> String {
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::ES256);
        header.kid = Some("k1".into());
        jsonwebtoken::encode(&header, &claims, &self.key).unwrap()
    }

    fn claims(&self, email: &str) -> serde_json::Value {
        serde_json::json!({
            "iss": ISSUER,
            "aud": "kronosdb",
            "sub": "u-123",
            "email": email,
            "hd": "example.com",
            "email_verified": true,
            "groups": ["eng", "platform"],
            "exp": now_unix() + 600,
        })
    }
}

fn grant(source: &str, subject: &str, roles: Vec<Role>) -> GrantConfig {
    GrantConfig {
        source: source.into(),
        subject: subject.into(),
        claims: BTreeMap::new(),
        roles,
        contexts: None,
        buses: None,
    }
}

fn token_creds(token: &str) -> Credentials<'_> {
    Credentials {
        token: Some(token),
        client_cert: None,
    }
}

fn client_cert(spiffe: &str) -> Vec<u8> {
    let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    params
        .subject_alt_names
        .push(rcgen::SanType::URI(spiffe.try_into().unwrap()));
    let key = rcgen::KeyPair::generate().unwrap();
    params.self_signed(&key).unwrap().der().to_vec()
}

fn unauthenticated(result: Result<Arc<Principal>, AuthError>) -> String {
    match result {
        Err(AuthError::Unauthenticated(msg)) => msg,
        other => panic!("expected Unauthenticated, got {other:?}"),
    }
}

#[tokio::test]
async fn nothing_configured_is_open_access() {
    let auth = Authenticator::new(&IdentityConfig::default()).unwrap();
    assert!(!auth.enabled());
    let principal = auth.authenticate(Credentials::default()).await.unwrap();
    for kind in [APPEND, SOURCE, VOTE, UNKNOWN] {
        authorize(&principal, kind, "default", "default").unwrap();
    }
    // The admin plane must never mistake "client auth is off" for a credential.
    assert!(auth.authenticate_token("anything").await.is_err());
}

#[tokio::test]
async fn legacy_access_token_is_admin_and_peer() {
    let auth = Authenticator::new(&IdentityConfig {
        access_token: Some("secret123".into()),
        ..Default::default()
    })
    .unwrap();

    let principal = auth.authenticate(token_creds("secret123")).await.unwrap();
    assert!(principal.is_admin());
    authorize(&principal, APPEND, "default", "default").unwrap();
    authorize(&principal, VOTE, "default", "default").unwrap();

    unauthenticated(auth.authenticate(token_creds("secret124")).await);
    unauthenticated(auth.authenticate(token_creds("")).await);
    unauthenticated(auth.authenticate(Credentials::default()).await);
}

#[tokio::test]
async fn named_token_is_scoped() {
    let auth = Authenticator::new(&IdentityConfig {
        tokens: vec![TokenConfig {
            name: "orders-ci".into(),
            token: Some("ci-secret".into()),
            token_file: None,
            roles: vec![Role::Write],
            contexts: Some(vec!["orders-*".into()]),
            buses: Some(vec![]),
        }],
        ..Default::default()
    })
    .unwrap();

    let principal = auth.authenticate(token_creds("ci-secret")).await.unwrap();
    assert_eq!(principal.subject, "orders-ci");
    authorize(&principal, APPEND, "orders-eu", "default").unwrap();
    assert!(matches!(
        authorize(&principal, APPEND, "billing", "default"),
        Err(Denied)
    ));
    assert!(authorize(&principal, VOTE, "orders-eu", "default").is_err());
    assert!(
        authorize(
            &principal,
            kronosdb_protocol::DispatchCommand::KIND,
            "orders-eu",
            "default"
        )
        .is_err()
    );
    assert!(!principal.is_admin());
}

#[tokio::test]
async fn token_file_is_trimmed() {
    let mut file = tempfile::NamedTempFile::new().unwrap();
    writeln!(file, "from-file").unwrap();
    let auth = Authenticator::new(&IdentityConfig {
        tokens: vec![TokenConfig {
            name: "mounted".into(),
            token: None,
            token_file: Some(file.path().to_path_buf()),
            roles: vec![Role::Read],
            contexts: None,
            buses: None,
        }],
        ..Default::default()
    })
    .unwrap();
    auth.authenticate(token_creds("from-file")).await.unwrap();
}

#[tokio::test]
async fn jwt_grants_by_subject_and_claims() {
    let idp = TestIdp::new();
    let mut by_claims = grant("test", "*@example.com", vec![Role::Write]);
    by_claims
        .claims
        .insert("hd".into(), toml::Value::String("example.com".into()));
    by_claims
        .claims
        .insert("email_verified".into(), toml::Value::Boolean(true));
    by_claims
        .claims
        .insert("groups".into(), toml::Value::String("platform".into()));
    by_claims.contexts = Some(vec!["orders".into()]);

    let auth = Authenticator::new(&IdentityConfig {
        issuers: vec![idp.issuer_config()],
        grants: vec![by_claims],
        ..Default::default()
    })
    .unwrap();

    let token = idp.sign(idp.claims("theo@example.com"));
    let principal = auth.authenticate(token_creds(&token)).await.unwrap();
    assert_eq!(principal.subject, "theo@example.com");
    assert_eq!(principal.source, "test");
    authorize(&principal, APPEND, "orders", "default").unwrap();
    assert!(authorize(&principal, APPEND, "billing", "default").is_err());

    // Second call is served from the verified-token cache.
    let again = auth.authenticate(token_creds(&token)).await.unwrap();
    assert!(Arc::ptr_eq(&principal, &again));

    // Verified, but the subject glob doesn't match: no grants at all.
    let outsider = idp.sign(idp.claims("eve@evil.io"));
    let principal = auth.authenticate(token_creds(&outsider)).await.unwrap();
    assert!(principal.grants.is_empty());
    assert!(authorize(&principal, SOURCE, "orders", "default").is_err());

    // Right subject, one claim off.
    let mut claims = idp.claims("mallory@example.com");
    claims["email_verified"] = serde_json::json!(false);
    let principal = auth
        .authenticate(token_creds(&idp.sign(claims)))
        .await
        .unwrap();
    assert!(principal.grants.is_empty());
}

#[tokio::test]
async fn jwt_role_map() {
    let idp = TestIdp::new();
    let auth = Authenticator::new(&IdentityConfig {
        issuers: vec![idp.issuer_config()],
        ..Default::default()
    })
    .unwrap();

    let mut claims = idp.claims("ops@example.com");
    claims["realm_access"] = serde_json::json!({ "roles": ["kronos-admins", "unrelated"] });
    let principal = auth
        .authenticate(token_creds(&idp.sign(claims)))
        .await
        .unwrap();
    assert!(principal.is_admin());
    // admin never implies peer.
    assert!(authorize(&principal, VOTE, "default", "default").is_err());
}

#[tokio::test]
async fn jwt_rejections() {
    let idp = TestIdp::new();
    let auth = Authenticator::new(&IdentityConfig {
        issuers: vec![idp.issuer_config()],
        grants: vec![grant("test", "*", vec![Role::Admin])],
        ..Default::default()
    })
    .unwrap();

    let mut wrong_aud = idp.claims("a@example.com");
    wrong_aud["aud"] = serde_json::json!("some-other-service");
    unauthenticated(auth.authenticate(token_creds(&idp.sign(wrong_aud))).await);

    let mut expired = idp.claims("a@example.com");
    expired["exp"] = serde_json::json!(now_unix() - 3600);
    unauthenticated(auth.authenticate(token_creds(&idp.sign(expired))).await);

    let mut other_issuer = idp.claims("a@example.com");
    other_issuer["iss"] = serde_json::json!("https://evil.test");
    let msg = unauthenticated(
        auth.authenticate(token_creds(&idp.sign(other_issuer)))
            .await,
    );
    assert!(msg.contains("untrusted issuer"), "{msg}");

    // Right issuer and kid, signed by somebody else's key.
    let impostor = TestIdp::new();
    unauthenticated(
        auth.authenticate(token_creds(&impostor.sign(idp.claims("a@example.com"))))
            .await,
    );

    // Algorithm confusion: HS256 keyed with the (public) verification key.
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::HS256);
    header.kid = Some("k1".into());
    let forged = jsonwebtoken::encode(
        &header,
        &idp.claims("a@example.com"),
        &jsonwebtoken::EncodingKey::from_secret(&idp.public_raw),
    )
    .unwrap();
    let msg = unauthenticated(auth.authenticate(token_creds(&forged)).await);
    assert!(msg.contains("symmetric"), "{msg}");

    // Unsigned.
    let body = URL_SAFE_NO_PAD.encode(idp.claims("a@example.com").to_string());
    let none = format!(
        "{}.{body}.",
        URL_SAFE_NO_PAD.encode(r#"{"alg":"none","kid":"k1"}"#)
    );
    unauthenticated(auth.authenticate(token_creds(&none)).await);
}

#[tokio::test]
async fn mtls_identity_and_precedence() {
    let auth = Authenticator::new(&IdentityConfig {
        access_token: Some("secret".into()),
        grants: vec![
            grant("mtls", "spiffe://example.com/kronosdb/*", vec![Role::Peer]),
            grant("mtls", "spiffe://example.com/ns/prod/*", vec![Role::Write]),
        ],
        ..Default::default()
    })
    .unwrap();

    let node = client_cert("spiffe://example.com/kronosdb/node-2");
    let principal = auth
        .authenticate(Credentials {
            token: None,
            client_cert: Some(&node),
        })
        .await
        .unwrap();
    assert_eq!(principal.source, "mtls");
    assert_eq!(principal.subject, "spiffe://example.com/kronosdb/node-2");
    authorize(&principal, VOTE, "default", "default").unwrap();
    assert!(authorize(&principal, APPEND, "default", "default").is_err());

    let workload = client_cert("spiffe://example.com/ns/prod/sa/orders");
    let principal = auth
        .authenticate(Credentials {
            token: None,
            client_cert: Some(&workload),
        })
        .await
        .unwrap();
    authorize(&principal, APPEND, "default", "default").unwrap();
    assert!(authorize(&principal, VOTE, "default", "default").is_err());

    // A valid certificate nobody granted anything to.
    let stranger = client_cert("spiffe://other.org/x");
    let principal = auth
        .authenticate(Credentials {
            token: None,
            client_cert: Some(&stranger),
        })
        .await
        .unwrap();
    assert!(principal.grants.is_empty());

    // A bad token is final: the good certificate doesn't rescue it.
    unauthenticated(
        auth.authenticate(Credentials {
            token: Some("wrong"),
            client_cert: Some(&workload),
        })
        .await,
    );
}

#[test]
fn validation_catches_lockouts() {
    let idp_cfg = IssuerConfig {
        name: "google".into(),
        issuer: "https://accounts.google.com".into(),
        audiences: vec![],
        jwks_uri: None,
        jwks_file: None,
        ca_file: None,
        jwks_bearer_token_file: None,
        subject_claim: None,
        role_claim: None,
        role_map: BTreeMap::new(),
    };

    // Clustered + auth on + no way for peers to authenticate.
    let cfg = IdentityConfig {
        issuers: vec![idp_cfg.clone()],
        ..Default::default()
    };
    assert!(cfg.validate(false, false).is_ok());
    let err = cfg.validate(true, false).unwrap_err();
    assert!(err.contains("peers have no way in"), "{err}");

    // mTLS peer grants are a way in — but only with TLS + CA configured.
    let cfg = IdentityConfig {
        issuers: vec![idp_cfg.clone()],
        grants: vec![grant("mtls", "spiffe://x/*", vec![Role::Peer])],
        ..Default::default()
    };
    assert!(cfg.validate(true, true).is_ok());
    assert!(cfg.validate(true, false).is_err());

    // Grants must reference a declared issuer.
    let cfg = IdentityConfig {
        issuers: vec![idp_cfg],
        grants: vec![grant("gogle", "*", vec![Role::Read])],
        ..Default::default()
    };
    assert!(cfg.validate(false, false).unwrap_err().contains("gogle"));
}

#[test]
fn security_toml_shape() {
    #[derive(serde::Deserialize)]
    struct File {
        security: Security,
    }
    #[derive(serde::Deserialize)]
    struct Security {
        tokens: Vec<TokenConfig>,
        issuers: Vec<IssuerConfig>,
        grants: Vec<GrantConfig>,
    }
    let file: File = toml::from_str(
        r#"
[[security.tokens]]
name = "ci"
token-file = "/var/run/secrets/kronosdb/ci"
roles = ["write"]
contexts = ["orders-*"]

[[security.issuers]]
name = "google"
issuer = "https://accounts.google.com"
subject-claim = "email"

[[security.issuers]]
name = "gke"
issuer = "https://container.googleapis.com/v1/projects/p/locations/l/clusters/c"
audiences = ["kronosdb"]

[[security.issuers]]
name = "keycloak"
issuer = "https://sso.example.com/realms/platform"
audiences = ["kronosdb"]
role-claim = "realm_access.roles"
role-map = { kronosdb-admin = "admin", kronosdb-reader = "read" }

[[security.grants]]
source = "google"
subject = "*@example.com"
claims = { hd = "example.com", email_verified = true }
roles = ["read"]

[[security.grants]]
source = "gke"
subject = "system:serviceaccount:prod:orders"
roles = ["write"]
contexts = ["orders"]
buses = ["default"]

[[security.grants]]
source = "mtls"
subject = "spiffe://example.com/kronosdb/*"
roles = ["peer"]
"#,
    )
    .unwrap();
    assert_eq!(file.security.tokens.len(), 1);
    assert_eq!(file.security.issuers.len(), 3);
    assert_eq!(
        file.security.issuers[2].role_map["kronosdb-admin"],
        Role::Admin
    );
    assert_eq!(file.security.grants[0].subject, "*@example.com");
    assert_eq!(file.security.grants[2].roles, vec![Role::Peer]);

    // Typos fail loudly instead of silently granting nothing.
    assert!(
        toml::from_str::<File>(
            "[[security.tokens]]\nname = \"x\"\ntoken = \"y\"\nrole = [\"read\"]\nroles = []\n\
             [[security.issuers]]\nname=\"a\"\nissuer=\"b\"\n[[security.grants]]\nsource=\"a\"\nroles=[]"
        )
        .is_err()
    );
}
