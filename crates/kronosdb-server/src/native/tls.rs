//! TLS for the native listener, from the `[security]` PEM files.

use std::path::Path;
use std::sync::Arc;

use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::server::WebPkiClientVerifier;
use tokio_rustls::rustls::{self, RootCertStore, ServerConfig};

fn certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    CertificateDer::pem_file_iter(path)
        .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
        .map_err(|e| format!("failed to read certificates from '{}': {e}", path.display()))
}

/// The acceptor for a listener with `cert`/`key`, verifying client
/// certificates against `ca` when one is given. With `optional_client_certs`
/// a client may connect without one and present a token instead.
pub fn acceptor(
    cert: &Path,
    key: &Path,
    ca: Option<&Path>,
    optional_client_certs: bool,
) -> Result<TlsAcceptor, String> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|e| format!("TLS: {e}"))?;
    let builder = match ca {
        Some(ca) => {
            let mut roots = RootCertStore::empty();
            for c in certs(ca)? {
                roots
                    .add(c)
                    .map_err(|e| format!("invalid CA certificate in '{}': {e}", ca.display()))?;
            }
            let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider);
            let verifier = if optional_client_certs {
                verifier.allow_unauthenticated()
            } else {
                verifier
            };
            builder.with_client_cert_verifier(verifier.build().map_err(|e| format!("TLS: {e}"))?)
        }
        None => builder.with_no_client_auth(),
    };
    let key = PrivateKeyDer::from_pem_file(key)
        .map_err(|e| format!("failed to read TLS key '{}': {e}", key.display()))?;
    let config = builder
        .with_single_cert(certs(cert)?, key)
        .map_err(|e| format!("TLS: {e}"))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}
