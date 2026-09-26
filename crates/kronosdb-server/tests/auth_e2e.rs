//! Identity layer e2e: real server processes, real TLS handshakes.
//!
//! `credentials_on_one_port` runs a single node with TLS, optional client
//! certificates, a named read-only token, a trusted JWT issuer (keys from a
//! JWKS file), and an mTLS grant — and checks each credential gets exactly
//! what it was granted. `peers_authenticate_with_access_token` forms a
//! two-voter cluster behind an access token: an append only succeeds if the
//! nodes can reach each other's `peer`-guarded services.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command as ProcCommand, Stdio};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tonic::{Code, Request};

// Generated proto code; same allows as src/proto.rs.
#[allow(dead_code, clippy::enum_variant_names, clippy::result_large_err)]
mod pb {
    tonic::include_proto!("kronosdb");
    pub mod eventstore {
        tonic::include_proto!("kronosdb.eventstore");
    }
    pub mod platform {
        tonic::include_proto!("kronosdb.platform");
    }
}

use pb::eventstore::event_store_client::EventStoreClient;
use pb::platform::platform_service_client::PlatformServiceClient;

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
        .join(format!("auth-e2e-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn spawn(dir: &Path, grpc_port: u16, envs: &[(&str, String)]) -> Node {
    let mut cmd = ProcCommand::new(env!("CARGO_BIN_EXE_kronosdb-server"));
    cmd.current_dir(dir)
        .env("KRONOSDB_LISTEN", format!("127.0.0.1:{grpc_port}"))
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

fn with_header<T>(message: T, headers: &[(&'static str, &str)]) -> Request<T> {
    let mut req = Request::new(message);
    for (name, value) in headers {
        req.metadata_mut().insert(*name, value.parse().unwrap());
    }
    req
}

fn one_event() -> pb::eventstore::AppendRequest {
    pb::eventstore::AppendRequest {
        condition: None,
        events: vec![pb::eventstore::TaggedEvent {
            event: Some(pb::eventstore::Event {
                identifier: uuid::Uuid::new_v4().to_string(),
                timestamp: 0,
                name: "AuthTested".into(),
                version: "1".into(),
                payload: vec![1, 2, 3],
                metadata: Default::default(),
            }),
            tags: vec![],
        }],
    }
}

/// Retries until the server answers at all (any gRPC status counts).
async fn wait_until_up(client: &mut EventStoreClient<Channel>, headers: &[(&'static str, &str)]) {
    for _ in 0..150 {
        match client
            .get_head(with_header(pb::eventstore::GetHeadRequest {}, headers))
            .await
        {
            Err(status) if status.code() == Code::Unavailable => {}
            _ => return,
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("server never came up");
}

/// Appends, riding out the moments after boot when the node has no leader
/// claim yet. Identity failures are never retried — they are the point.
async fn append_when_ready(
    client: &mut EventStoreClient<Channel>,
    headers: &[(&'static str, &str)],
) -> pb::eventstore::AppendResponse {
    let mut last = None;
    for _ in 0..100 {
        match client.append(with_header(one_event(), headers)).await {
            Ok(response) => return response.into_inner(),
            Err(status) => {
                assert_ne!(status.code(), Code::Unauthenticated, "{status}");
                assert_ne!(status.code(), Code::PermissionDenied, "{status}");
                last = Some(status);
            }
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    panic!("append never succeeded: {last:?}");
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
}

#[tokio::test(flavor = "multi_thread")]
async fn credentials_on_one_port() {
    let dir = work_dir("single");
    let pki = Pki::new();
    let (server_cert, server_key) = pki.issue(&["localhost"], None);
    std::fs::write(dir.join("ca.pem"), &pki.ca_pem).unwrap();
    std::fs::write(dir.join("server.pem"), server_cert).unwrap();
    std::fs::write(dir.join("server.key"), server_key).unwrap();

    // A throwaway "identity provider": an ES256 key and its JWKS on disk.
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
subject = "spiffe://karma.life/ns/prod/sa/billing"
roles = ["write"]
contexts = ["billing"]
"#,
    )
    .unwrap();

    let port = free_port();
    let _node = spawn(&dir, port, &[]);

    let tls = ClientTlsConfig::new()
        .ca_certificate(Certificate::from_pem(&pki.ca_pem))
        .domain_name("localhost");
    let endpoint = |tls: ClientTlsConfig| {
        Channel::from_shared(format!("https://127.0.0.1:{port}"))
            .unwrap()
            .tls_config(tls)
            .unwrap()
            .connect_lazy()
    };
    let mut client = EventStoreClient::new(endpoint(tls.clone()));
    wait_until_up(&mut client, &[]).await;

    // No credential at all.
    let status = client
        .get_head(pb::eventstore::GetHeadRequest {})
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated, "{status}");

    // Health stays public for probes.
    let mut health = tonic_health::pb::health_client::HealthClient::new(endpoint(tls.clone()));
    health
        .check(tonic_health::pb::HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("health is unauthenticated");

    // Read-only token, both header spellings: reads pass, appends don't.
    for headers in [
        [("kronosdb-token", "read-only-secret")],
        [("authorization", "Bearer read-only-secret")],
    ] {
        client
            .get_head(with_header(pb::eventstore::GetHeadRequest {}, &headers))
            .await
            .expect("read token may read");
        let status = client
            .append(with_header(one_event(), &headers))
            .await
            .unwrap_err();
        assert_eq!(status.code(), Code::PermissionDenied, "{status}");
    }
    let status = client
        .get_head(with_header(
            pb::eventstore::GetHeadRequest {},
            &[("kronosdb-token", "read-only-secre7")],
        ))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated, "{status}");

    // Workload-identity JWT: granted write.
    let bearer = format!("Bearer {jwt}");
    let appended = append_when_ready(&mut client, &[("authorization", &bearer)]).await;
    assert_eq!(appended.count, 1);
    // One flipped signature byte.
    let mut tampered = bearer.clone();
    let last = tampered.pop().unwrap();
    tampered.push(if last == 'A' { 'B' } else { 'A' });
    let status = client
        .append(with_header(one_event(), &[("authorization", &tampered)]))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated, "{status}");

    // mTLS: the certificate's SPIFFE ID is the identity; its grant is
    // scoped to the `billing` context.
    let (cert, key) = pki.issue(&[], Some("spiffe://karma.life/ns/prod/sa/billing"));
    let mtls_client_channel = endpoint(tls.clone().identity(Identity::from_pem(cert, key)));
    let mut mtls_client = EventStoreClient::new(mtls_client_channel.clone());
    let status = mtls_client.append(one_event()).await.unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied, "{status}");
    assert!(status.message().contains("spiffe://karma.life"), "{status}");
    let status = mtls_client
        .append(with_header(one_event(), &[("kronosdb-context", "billing")]))
        .await
        .unwrap_err();
    // Authorized — the context simply doesn't exist on this node.
    assert_ne!(status.code(), Code::PermissionDenied, "{status}");
    assert_ne!(status.code(), Code::Unauthenticated, "{status}");

    // WhoAmI: the server's own account of each caller.
    let mut platform = PlatformServiceClient::new(endpoint(tls.clone()));
    let status = platform
        .who_am_i(pb::platform::WhoAmIRequest {})
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated, "{status}");

    let me = platform
        .who_am_i(with_header(
            pb::platform::WhoAmIRequest {},
            &[("kronosdb-token", "read-only-secret")],
        ))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (me.subject.as_str(), me.source.as_str()),
        ("dashboard", "token")
    );
    assert!(me.authentication_enabled);
    assert_eq!(me.grants.len(), 1);
    assert_eq!(me.grants[0].roles, vec!["read".to_string()]);
    assert!(me.grants[0].all_contexts && me.grants[0].all_buses);

    let me = PlatformServiceClient::new(mtls_client_channel.clone())
        .who_am_i(pb::platform::WhoAmIRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(me.source, "mtls");
    assert_eq!(me.subject, "spiffe://karma.life/ns/prod/sa/billing");
    assert_eq!(me.grants[0].contexts, vec!["billing".to_string()]);
    assert!(!me.grants[0].all_contexts);

    // A certificate from the right CA that nobody granted anything.
    let (cert, key) = pki.issue(&[], Some("spiffe://karma.life/ns/dev/sa/whoever"));
    let stranger_channel = endpoint(tls.clone().identity(Identity::from_pem(cert, key)));
    let mut stranger = EventStoreClient::new(stranger_channel.clone());
    let status = stranger
        .get_head(pb::eventstore::GetHeadRequest {})
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::PermissionDenied, "{status}");
    // ...who can still ask why: WhoAmI answers any verified caller.
    let me = PlatformServiceClient::new(stranger_channel)
        .who_am_i(pb::platform::WhoAmIRequest {})
        .await
        .expect("whoami works without grants")
        .into_inner();
    assert_eq!(me.subject, "spiffe://karma.life/ns/dev/sa/whoever");
    assert!(me.grants.is_empty());
}

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

    let mut client = EventStoreClient::new(
        Channel::from_shared(format!("http://127.0.0.1:{}", ports[0]))
            .unwrap()
            .connect_lazy(),
    );
    let token = [("kronosdb-token", "cluster-secret")];
    wait_until_up(&mut client, &token).await;

    let status = client.append(one_event()).await.unwrap_err();
    assert_eq!(status.code(), Code::Unauthenticated, "{status}");

    // Two voters → quorum is both nodes: this append commits only if Raft
    // and segment replication got through each other's identity layer.
    assert_eq!(append_when_ready(&mut client, &token).await.count, 1);
}
