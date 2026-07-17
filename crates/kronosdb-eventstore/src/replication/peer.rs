//! Shared outbound peer transport configuration for Raft metadata and native
//! segment replication.

use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

use crate::error::Error;

#[derive(Debug, Clone)]
pub struct PeerTlsConfig {
    pub ca_certificate: Option<Vec<u8>>,
    pub identity_certificate: Vec<u8>,
    pub identity_key: Vec<u8>,
}

#[derive(Debug, Clone, Default)]
pub struct PeerTransportConfig {
    pub tls: Option<PeerTlsConfig>,
    /// Shared authorization token attached to every peer RPC. This matches the
    /// server interceptor so peer-only services cannot bypass configured auth.
    pub access_token: Option<String>,
}

impl PeerTransportConfig {
    pub fn request<T>(&self, message: T) -> Result<Request<T>, Error> {
        let mut request = Request::new(message);
        if let Some(token) = &self.access_token {
            let value = MetadataValue::try_from(token.as_str()).map_err(|error| {
                Error::Io(std::io::Error::other(format!(
                    "access token is not valid gRPC metadata: {error}"
                )))
            })?;
            request.metadata_mut().insert("kronosdb-token", value);
        }
        Ok(request)
    }

    pub async fn connect(&self, address: &str) -> Result<Channel, Error> {
        let has_scheme = address.starts_with("http://") || address.starts_with("https://");
        let endpoint_uri = if has_scheme {
            address.to_string()
        } else if self.tls.is_some() {
            format!("https://{address}")
        } else {
            format!("http://{address}")
        };
        let mut endpoint = Endpoint::from_shared(endpoint_uri.clone()).map_err(|error| {
            Error::Io(std::io::Error::other(format!(
                "invalid peer endpoint {endpoint_uri}: {error}"
            )))
        })?;
        if let Some(tls) = &self.tls {
            let mut config = ClientTlsConfig::new().identity(Identity::from_pem(
                &tls.identity_certificate,
                &tls.identity_key,
            ));
            if let Some(ca) = &tls.ca_certificate {
                config = config.ca_certificate(Certificate::from_pem(ca));
            }
            endpoint = endpoint.tls_config(config).map_err(|error| {
                Error::Io(std::io::Error::other(format!(
                    "configure TLS for peer {endpoint_uri}: {error}"
                )))
            })?;
        }
        endpoint.connect().await.map_err(|error| {
            Error::Io(std::io::Error::other(format!(
                "connect to peer {endpoint_uri}: {error}"
            )))
        })
    }
}
