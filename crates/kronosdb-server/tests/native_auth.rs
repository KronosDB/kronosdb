//! The identity layer over the native protocol: real server processes, real
//! TLS handshakes. A native connection authenticates once, in its Hello, so
//! a bad credential fails the connect and a good one is checked per request.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use kronosdb_client::{Appended, Client, ConnectOptions, Credential, ErrorCode, NewEvent};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

struct Node {
    child: Child,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn work_dir(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("native-auth-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Starts a node listening on `port`.
fn spawn(dir: &Path, port: u16, envs: &[(&str, String)]) -> Node {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_kronosdb-server"));
    cmd.current_dir(dir)
        .env("KRONOSDB_LISTEN", format!("127.0.0.1:{port}"))
        .env(
            "KRONOSDB_ADMIN_LISTEN",
            format!("127.0.0.1:{}", free_port()),
        )
        .env("KRONOSDB_DATA_DIR", dir.join("data"))
        .env("RUST_LOG", "warn");
    for (key, value) in envs {
        cmd.env(key, value);
    }
    if std::env::var_os("KRONOSDB_TEST_LOGS").is_some() {
        cmd.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    } else {
        cmd.stdout(Stdio::null()).stderr(Stdio::null());
    }
    Node {
        child: cmd.spawn().expect("spawn kronosdb-server"),
    }
}

struct Pki {
    ca_pem: String,
    ca: rcgen::Certificate,
    ca_key: rcgen::KeyPair,
}

impl Pki {
    fn new() -> Self {
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "kronosdb-test-ca");
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca = params.self_signed(&ca_key).unwrap();
        Self {
            ca_pem: ca.pem(),
            ca,
            ca_key,
        }
    }

    /// (cert PEM, key PEM)
    fn issue(&self, dns: &[&str], spiffe: Option<&str>) -> (String, String) {
        let mut params =
            rcgen::CertificateParams::new(dns.iter().map(|s| s.to_string()).collect::<Vec<_>>())
                .unwrap();
        if let Some(uri) = spiffe {
            params
                .subject_alt_names
                .push(rcgen::SanType::URI(uri.try_into().unwrap()));
        }
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.signed_by(&key, &self.ca, &self.ca_key).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    /// TLS to `localhost` trusting this CA, optionally presenting a client certificate.
    fn tls(&self, identity: Option<(String, String)>) -> (TlsConnector, ServerName<'static>) {
        let mut roots = RootCertStore::empty();
        roots
            .add(CertificateDer::from_pem_slice(self.ca_pem.as_bytes()).unwrap())
            .unwrap();
        let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
        let builder = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots);
        let config = match identity {
            Some((cert, key)) => builder
                .with_client_auth_cert(
                    vec![CertificateDer::from_pem_slice(cert.as_bytes()).unwrap()],
                    PrivateKeyDer::from_pem_slice(key.as_bytes()).unwrap(),
                )
                .unwrap(),
            None => builder.with_no_client_auth(),
        };
        (
            TlsConnector::from(Arc::new(config)),
            ServerName::try_from("localhost").unwrap(),
        )
    }
}

fn event() -> NewEvent {
    NewEvent {
        identifier: uuid::Uuid::new_v4().to_string(),
        name: "AuthTested".into(),
        version: "1".into(),
        payload: vec![1, 2, 3].into(),
        ..NewEvent::default()
    }
}

/// Connects, waiting for the server to come up. A refused credential is
/// returned, not retried: it is the point.
async fn connect(addr: &str, options: &ConnectOptions) -> Result<Client, kronosdb_client::Error> {
    for _ in 0..150 {
        match Client::connect(addr, options).await {
            Err(e) if e.failure().is_none() => {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            result => return result,
        }
    }
    panic!("server never came up on {addr}");
}

fn options(
    credential: Credential,
    tls: Option<(TlsConnector, ServerName<'static>)>,
) -> ConnectOptions {
    ConnectOptions {
        credential,
        tls,
        ..ConnectOptions::default()
    }
}

fn code(e: &kronosdb_client::Error) -> ErrorCode {
    e.failure()
        .unwrap_or_else(|| panic!("not a server failure: {e}"))
        .code
}

/// Appends, riding out the moments after boot when the node has no leader
/// claim yet. Identity failures are never retried.
async fn append_when_ready(client: &Client) -> Appended {
    let ctx = client.default_context();
    for _ in 0..100 {
        match ctx.append(&[event()], None).await {
            Ok(appended) => return appended,
            Err(e) => {
                let code = code(&e);
                assert!(
                    code != ErrorCode::UNAUTHENTICATED && code != ErrorCode::PERMISSION_DENIED,
                    "{e}"
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("append never succeeded");
}

#[tokio::test(flavor = "multi_thread")]
async fn credentials_over_tls() {
    let dir = work_dir("single");
    let pki = Pki::new();
    let (server_cert, server_key) = pki.issue(&["localhost"], None);
    std::fs::write(dir.join("ca.pem"), &pki.ca_pem).unwrap();
    std::fs::write(dir.join("server.pem"), server_cert).unwrap();
    std::fs::write(dir.join("server.key"), server_key).unwrap();

    // A throwaway identity provider: an ES256 key and its JWKS on disk.
    let idp_key = rcgen::KeyPair::generate().unwrap();
    let raw = idp_key.public_key_raw().to_vec();
    std::fs::write(
        dir.join("jwks.json"),
        format!(
            r#"{{"keys":[{{"kty":"EC","crv":"P-256","alg":"ES256","kid":"k1","x":"{}","y":"{}"}}]}}"#,
            URL_SAFE_NO_PAD.encode(&raw[1..33]),
            URL_SAFE_NO_PAD.encode(&raw[33..65]),
        ),
    )
    .unwrap();
    let jwt = {
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::ES256);
        header.kid = Some("k1".into());
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 600;
        jsonwebtoken::encode(
            &header,
            &serde_json::json!({
                "iss": "https://idp.test",
                "aud": "kronosdb",
                "sub": "system:serviceaccount:prod:orders",
                "exp": exp,
            }),
            &jsonwebtoken::EncodingKey::from_ec_pem(idp_key.serialize_pem().as_bytes()).unwrap(),
        )
        .unwrap()
    };
    std::fs::write(
        dir.join("kronosdb.toml"),
        r#"
[security]
tls-cert = "server.pem"
tls-key = "server.key"
tls-ca = "ca.pem"
tls-client-auth = "optional"

[[security.tokens]]
name = "dashboard"
token = "read-only-secret"
roles = ["read"]

[[security.issuers]]
name = "k8s"
issuer = "https://idp.test"
audiences = ["kronosdb"]
jwks-file = "jwks.json"

[[security.grants]]
source = "k8s"
subject = "system:serviceaccount:prod:*"
roles = ["write"]

[[security.grants]]
source = "mtls"
subject = "spiffe://example.com/ns/prod/sa/billing"
roles = ["write"]
contexts = ["billing"]
"#,
    )
    .unwrap();
    let port = free_port();
    let addr = format!("127.0.0.1:{port}");
    let _node = spawn(&dir, port, &[]);
    let tls = || Some(pki.tls(None));

    // No credential at all: the handshake is refused.
    let refused = connect(&addr, &options(Credential::None, tls()))
        .await
        .err()
        .unwrap();
    assert_eq!(code(&refused), ErrorCode::UNAUTHENTICATED, "{refused}");
    // A wrong token likewise.
    let refused = connect(
        &addr,
        &options(Credential::Token("read-only-secre7".into()), tls()),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(code(&refused), ErrorCode::UNAUTHENTICATED, "{refused}");

    // Read-only token: reads pass, appends don't.
    let reader = connect(
        &addr,
        &options(Credential::Token("read-only-secret".into()), tls()),
    )
    .await
    .unwrap();
    reader
        .default_context()
        .head()
        .await
        .expect("read token may read");
    let denied = reader
        .default_context()
        .append(&[event()], None)
        .await
        .unwrap_err();
    assert_eq!(code(&denied), ErrorCode::PERMISSION_DENIED, "{denied}");
    let me = reader.who_am_i().await.unwrap();
    assert_eq!(
        (me.subject.as_str(), me.source.as_str()),
        ("dashboard", "token")
    );
    assert!(me.authentication_enabled);
    assert_eq!(me.grants.len(), 1);
    assert_eq!(me.grants[0].roles, vec!["read".to_string()]);
    assert!(me.grants[0].contexts.is_none() && me.grants[0].buses.is_none());

    // Workload-identity JWT: granted write.
    let writer = connect(&addr, &options(Credential::Jwt(jwt.clone()), tls()))
        .await
        .unwrap();
    assert!(matches!(
        append_when_ready(&writer).await,
        Appended::Ok { count: 1, .. }
    ));
    // One flipped signature byte.
    let mut tampered = jwt;
    let last = tampered.pop().unwrap();
    tampered.push(if last == 'A' { 'B' } else { 'A' });
    let refused = connect(&addr, &options(Credential::Jwt(tampered), tls()))
        .await
        .err()
        .unwrap();
    assert_eq!(code(&refused), ErrorCode::UNAUTHENTICATED, "{refused}");

    // mTLS: the certificate's SPIFFE ID is the identity; its grant is scoped
    // to the `billing` context.
    let billing_cert = pki.issue(&[], Some("spiffe://example.com/ns/prod/sa/billing"));
    let billing = connect(
        &addr,
        &options(Credential::None, Some(pki.tls(Some(billing_cert)))),
    )
    .await
    .unwrap();
    let denied = billing
        .default_context()
        .append(&[event()], None)
        .await
        .unwrap_err();
    assert_eq!(code(&denied), ErrorCode::PERMISSION_DENIED, "{denied}");
    assert!(
        denied
            .failure()
            .unwrap()
            .message
            .contains("spiffe://example.com"),
        "{denied}"
    );
    // Authorized for `billing`, which simply does not exist on this node.
    let missing = billing.context("billing").await.err().unwrap();
    assert_eq!(code(&missing), ErrorCode::NOT_FOUND, "{missing}");
    // Not authorized for `orders`: told so, without learning whether it exists.
    let hidden = billing.context("orders").await.err().unwrap();
    assert_eq!(code(&hidden), ErrorCode::PERMISSION_DENIED, "{hidden}");
    let me = billing.who_am_i().await.unwrap();
    assert_eq!(me.source, "mtls");
    assert_eq!(me.subject, "spiffe://example.com/ns/prod/sa/billing");
    assert_eq!(me.grants[0].contexts, Some(vec!["billing".to_string()]));

    // A certificate from the right CA that nobody granted anything.
    let stranger_cert = pki.issue(&[], Some("spiffe://example.com/ns/dev/sa/whoever"));
    let stranger = connect(
        &addr,
        &options(Credential::None, Some(pki.tls(Some(stranger_cert)))),
    )
    .await
    .unwrap();
    let denied = stranger.default_context().head().await.unwrap_err();
    assert_eq!(code(&denied), ErrorCode::PERMISSION_DENIED, "{denied}");
    // ...who can still ask why: WhoAmI answers any verified caller.
    let me = stranger
        .who_am_i()
        .await
        .expect("whoami works without grants");
    assert_eq!(me.subject, "spiffe://example.com/ns/dev/sa/whoever");
    assert!(me.grants.is_empty());
}

/// Two voters with an access token: clients without it are refused, and an
/// append commits only if Raft and segment replication got through each
/// other's identity check (quorum is both nodes).
#[tokio::test(flavor = "multi_thread")]
async fn peers_authenticate_with_access_token() {
    let ports = [free_port(), free_port()];
    let peers = format!("1=127.0.0.1:{},2=127.0.0.1:{}", ports[0], ports[1]);
    let _nodes: Vec<Node> = (0..2)
        .map(|i| {
            spawn(
                &work_dir(&format!("cluster-{i}")),
                ports[i],
                &[
                    ("KRONOSDB_CLUSTER_NODE_ID", (i + 1).to_string()),
                    ("KRONOSDB_CLUSTER_PEERS", peers.clone()),
                    ("KRONOSDB_ACCESS_TOKEN", "cluster-secret".into()),
                ],
            )
        })
        .collect();
    let addr = format!("127.0.0.1:{}", ports[0]);

    let refused = connect(&addr, &options(Credential::None, None))
        .await
        .err()
        .expect("no credential is refused");
    assert_eq!(code(&refused), ErrorCode::UNAUTHENTICATED, "{refused}");

    let client = connect(
        &addr,
        &options(Credential::Token("cluster-secret".into()), None),
    )
    .await
    .unwrap();
    assert!(matches!(
        append_when_ready(&client).await,
        Appended::Ok { count: 1, .. }
    ));
}

/// Two voters behind TLS with required client certificates: the nodes reach
/// each other's Raft and replication over TLS with their own identities. An
/// append commits only if they did.
#[tokio::test(flavor = "multi_thread")]
async fn peers_talk_over_tls() {
    let pki = Pki::new();
    let ports = [free_port(), free_port()];
    let peers = format!("1=127.0.0.1:{},2=127.0.0.1:{}", ports[0], ports[1]);
    let _nodes: Vec<Node> = (0..2)
        .map(|i| {
            let dir = work_dir(&format!("tls-cluster-{i}"));
            let (cert, key) = pki.issue(&["localhost", "127.0.0.1"], None);
            std::fs::write(dir.join("ca.pem"), &pki.ca_pem).unwrap();
            std::fs::write(dir.join("node.pem"), cert).unwrap();
            std::fs::write(dir.join("node.key"), key).unwrap();
            std::fs::write(
                dir.join("kronosdb.toml"),
                "[security]\ntls-cert = \"node.pem\"\ntls-key = \"node.key\"\ntls-ca = \"ca.pem\"\n",
            )
            .unwrap();
            spawn(
                &dir,
                ports[i],
                &[
                    ("KRONOSDB_CLUSTER_NODE_ID", (i + 1).to_string()),
                    ("KRONOSDB_CLUSTER_PEERS", peers.clone()),
                    ("KRONOSDB_ACCESS_TOKEN", "cluster-secret".into()),
                ],
            )
        })
        .collect();

    let identity = pki.issue(&["client"], None);
    let client = connect(
        &format!("127.0.0.1:{}", ports[0]),
        &options(
            Credential::Token("cluster-secret".into()),
            Some(pki.tls(Some(identity))),
        ),
    )
    .await
    .unwrap();
    assert!(matches!(
        append_when_ready(&client).await,
        Appended::Ok { count: 1, .. }
    ));
}
