//! Outbound peer transport: how a node dials the others. Internode traffic
//! (Raft, segment replication, forwarded appends, the messaging fabric)
//! speaks the native protocol through a [`PeerPool`].

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use kronosdb_protocol::Frame;
use kronosdb_protocol::net::client::{self, ConnectOptions, Connection, Credential};
use kronosdb_protocol::net::{NetError, OwnedFrame};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::{self, ClientConfig, RootCertStore};

use crate::error::Error;
use crate::wire;

/// How long a peer has to complete the connection and its handshake.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct PeerTlsConfig {
    pub ca_certificate: Option<Vec<u8>>,
    pub identity_certificate: Vec<u8>,
    pub identity_key: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct PeerTransportConfig {
    pub tls: Option<PeerTlsConfig>,
    /// Shared token every peer connection authenticates with. The server's
    /// identity layer grants it the `peer` role, which the internode
    /// services demand.
    pub access_token: Option<String>,
}

impl PeerTransportConfig {
    fn credential(&self) -> Credential {
        match &self.access_token {
            Some(token) => Credential::Token(token.clone()),
            None => Credential::None,
        }
    }
}

/// The `host:port` a peer advertised, without the scheme membership may carry.
fn strip_scheme(address: &str) -> &str {
    address
        .strip_prefix("http://")
        .or_else(|| address.strip_prefix("https://"))
        .unwrap_or(address)
}

/// One slot per peer address. Dialing holds the slot, so callers that find no
/// connection wait for one dial instead of each starting their own.
type Slot = Arc<tokio::sync::Mutex<Option<Connection>>>;

/// Native connections to peers.
///
/// [`connection`](Self::connection) is one shared connection per address,
/// re-dialed when it has closed, for requests that answer in one frame.
/// [`dedicated`](Self::dedicated) is a connection of its own for a long
/// stream that must not queue behind (or in front of) other traffic: a
/// 60 MiB replication frame would otherwise delay a Raft heartbeat.
pub struct PeerPool {
    transport: PeerTransportConfig,
    tls: OnceLock<Result<TlsConnector, String>>,
    slots: Mutex<HashMap<String, Slot>>,
}

impl std::fmt::Debug for PeerPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerPool").finish_non_exhaustive()
    }
}

impl PeerPool {
    pub fn new(transport: PeerTransportConfig) -> Self {
        Self {
            transport,
            tls: OnceLock::new(),
            slots: Mutex::new(HashMap::new()),
        }
    }

    pub fn transport(&self) -> &PeerTransportConfig {
        &self.transport
    }

    /// The shared connection to the peer advertised at `address`.
    pub async fn connection(&self, address: &str) -> Result<Connection, Error> {
        let slot = {
            let mut slots = self.slots.lock().unwrap();
            Arc::clone(slots.entry(address.to_string()).or_default())
        };
        let mut open = slot.lock().await;
        if let Some(connection) = open.as_ref()
            && !connection.is_closed()
            && !connection.is_draining()
        {
            return Ok(connection.clone());
        }
        let connection = self.dedicated(address).await?;
        *open = Some(connection.clone());
        Ok(connection)
    }

    /// A fresh connection to the peer advertised at `address`. It closes when
    /// the last handle and stream on it are dropped.
    pub async fn dedicated(&self, address: &str) -> Result<Connection, Error> {
        let native = strip_scheme(address);
        let tls = match &self.transport.tls {
            Some(config) => {
                let connector = self
                    .tls
                    .get_or_init(|| tls_connector(config))
                    .clone()
                    .map_err(|message| Error::Rejected {
                        message: format!("peer TLS is misconfigured: {message}"),
                    })?;
                Some((connector, server_name(native)?))
            }
            None => None,
        };
        let options = ConnectOptions {
            client_name: "kronosdb-peer".into(),
            credential: self.transport.credential(),
            tls,
            connect_timeout: CONNECT_TIMEOUT,
            ..ConnectOptions::default()
        };
        client::connect(native, &options)
            .await
            .map_err(|error| Error::Unavailable {
                message: format!("connect to peer {native}: {}", describe(&error)),
            })
    }

    /// Sends a request on the shared connection to `address` and returns the
    /// peer's one answer.
    pub async fn request(&self, address: &str, frame: &Frame<'_>) -> Result<OwnedFrame, Error> {
        self.connection(address)
            .await?
            .request(frame)
            .await
            .map_err(wire::net_error)
    }
}

fn describe(error: &NetError) -> String {
    match error.failure() {
        Some(failure) => failure.message.clone(),
        None => error.to_string(),
    }
}

fn server_name(native_address: &str) -> Result<ServerName<'static>, Error> {
    let host = match native_address.rsplit_once(':') {
        Some((host, _)) => host.trim_start_matches('[').trim_end_matches(']'),
        None => native_address,
    };
    ServerName::try_from(host.to_string()).map_err(|error| Error::Rejected {
        message: format!("peer address {native_address} has no usable TLS name: {error}"),
    })
}

/// A TLS connector that trusts `ca` for the peer's certificate and presents
/// this node's own identity to it.
fn tls_connector(tls: &PeerTlsConfig) -> Result<TlsConnector, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = RootCertStore::empty();
    if let Some(ca) = &tls.ca_certificate {
        for certificate in CertificateDer::pem_slice_iter(ca) {
            let certificate = certificate.map_err(|e| format!("CA certificate: {e}"))?;
            roots
                .add(certificate)
                .map_err(|e| format!("CA certificate: {e}"))?;
        }
    }
    let chain = CertificateDer::pem_slice_iter(&tls.identity_certificate)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("identity certificate: {e}"))?;
    let key = PrivateKeyDer::from_pem_slice(&tls.identity_key)
        .map_err(|e| format!("identity key: {e}"))?;
    let config = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots)
        .with_client_auth_cert(chain, key)
        .map_err(|e| e.to_string())?;
    Ok(TlsConnector::from(Arc::new(config)))
}

#[cfg(test)]
mod tests {
    use kronosdb_protocol::net::Failure;
    use kronosdb_protocol::net::server::{
        self, Accepted, BoxFuture, PeerInfo, ServerConfig, ServerStream, Service,
    };
    use tokio::sync::watch;

    use super::*;

    #[test]
    fn peer_addresses_lose_their_scheme() {
        assert_eq!(strip_scheme("10.0.0.5:50051"), "10.0.0.5:50051");
        assert_eq!(strip_scheme("http://node-1:50051"), "node-1:50051");
        assert_eq!(strip_scheme("https://[::1]:50061"), "[::1]:50061");
    }

    /// A peer that answers every request with a Head.
    struct Answering;

    impl Service for Answering {
        type Session = ();

        fn hello(
            &self,
            _: OwnedFrame,
            _: Arc<PeerInfo>,
        ) -> BoxFuture<Result<Accepted<()>, Failure>> {
            Box::pin(async {
                Ok(Accepted {
                    session: Arc::new(()),
                    principal: String::new(),
                    features: 0,
                    max_frame: None,
                })
            })
        }

        fn request(&self, _: &Arc<()>, stream: ServerStream) -> BoxFuture<()> {
            Box::pin(async move {
                let _ = stream
                    .out
                    .finish(&Frame::Head(kronosdb_protocol::Head { position: 7 }))
                    .await;
            })
        }
    }

    async fn peer_at(addr: &str) -> watch::Sender<bool> {
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let (stop, stopped) = watch::channel(false);
        tokio::spawn(server::serve(
            listener,
            Arc::new(Answering),
            Arc::new(ServerConfig::default()),
            stopped,
        ));
        stop
    }

    fn answered(reply: &OwnedFrame) -> bool {
        matches!(reply.decode(), Ok(Frame::Head(head)) if head.position == 7)
    }

    #[tokio::test]
    async fn a_closed_connection_is_dialed_again() {
        let address = "127.0.0.1:0";
        let listener = tokio::net::TcpListener::bind(address).await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        drop(listener);

        let pool = PeerPool::new(PeerTransportConfig::default());
        let first = peer_at(&addr).await;
        let get_cursors = Frame::GetCursors(kronosdb_protocol::GetCursors {});
        assert!(answered(&pool.request(&addr, &get_cursors).await.unwrap()));
        let connection = pool.connection(&addr).await.unwrap();

        // The peer goes away: its connection closes, and a request while it
        // is down fails as unavailable rather than hanging.
        first.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(2), connection.closed())
            .await
            .expect("the connection closes with its peer");
        let down = pool.request(&addr, &get_cursors).await.unwrap_err();
        assert!(matches!(down, Error::Unavailable { .. }), "{down}");

        // The peer comes back: the next request reconnects by itself.
        let _second = peer_at(&addr).await;
        assert!(answered(&pool.request(&addr, &get_cursors).await.unwrap()));
        assert!(!pool.connection(&addr).await.unwrap().is_closed());
    }

    #[test]
    fn server_names_come_from_the_host() {
        assert!(server_name("node-1.example.com:50052").is_ok());
        assert!(server_name("[::1]:50052").is_ok());
        assert!(server_name("10.1.2.3:50052").is_ok());
    }
}
