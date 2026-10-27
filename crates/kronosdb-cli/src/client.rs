//! One connection to a KronosDB node: the native protocol for events,
//! admin HTTP for topology. Both planes get the same bearer token.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use kronosdb_client::{
    Appended, Client, ConnectOptions, Credential, Criterion, ErrorCode, Event, NewEvent, Whoami,
};
use tokio_rustls::TlsConnector;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

use crate::profile::Profile;
use crate::token::TokenSource;

pub struct Connection {
    pub profile_name: String,
    pub profile: Profile,
    pub tokens: TokenSource,
    /// `host:port` the native client dials.
    address: String,
    tls: Option<(TlsConnector, ServerName<'static>)>,
    client: tokio::sync::Mutex<Option<Client>>,
    http: reqwest::Client,
}

/// `http://host:port` or `https://host:port` (the profile's endpoint) as the
/// address to dial and whether to use TLS.
fn parse_endpoint(endpoint: &str) -> Result<(String, String, bool)> {
    let (tls, rest) = match endpoint.split_once("://") {
        Some(("https", rest)) => (true, rest),
        Some(("http", rest)) => (false, rest),
        Some((scheme, _)) => {
            bail!("endpoint {endpoint:?}: unknown scheme {scheme:?}; use http:// or https://")
        }
        None => (false, endpoint),
    };
    let authority = rest.trim_end_matches('/');
    let host = match authority.rsplit_once(':') {
        Some((host, port)) if port.parse::<u16>().is_ok() => host,
        _ => authority,
    };
    let address = if host == authority {
        format!("{authority}:50051")
    } else {
        authority.to_string()
    };
    Ok((address, host.trim_matches(['[', ']']).to_string(), tls))
}

fn tls_connector(profile: &Profile, host: &str) -> Result<(TlsConnector, ServerName<'static>)> {
    let mut roots = RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().certs {
        let _ = roots.add(cert);
    }
    if let Some(path) = &profile.ca_file {
        for cert in CertificateDer::pem_file_iter(path)
            .with_context(|| format!("reading ca-file {}", path.display()))?
        {
            roots
                .add(cert.with_context(|| format!("reading ca-file {}", path.display()))?)
                .with_context(|| format!("invalid certificate in ca-file {}", path.display()))?;
        }
    }
    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());
    let builder = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots);
    let config = match (&profile.cert_file, &profile.key_file) {
        (Some(cert), Some(key)) => {
            let chain = CertificateDer::pem_file_iter(cert)
                .and_then(|certs| certs.collect::<Result<Vec<_>, _>>())
                .with_context(|| format!("reading cert-file {}", cert.display()))?;
            let key = PrivateKeyDer::from_pem_file(key)
                .with_context(|| format!("reading key-file {}", key.display()))?;
            builder.with_client_auth_cert(chain, key)?
        }
        _ => builder.with_no_client_auth(),
    };
    let name = profile
        .tls_domain
        .clone()
        .unwrap_or_else(|| host.to_string());
    let name = ServerName::try_from(name.clone())
        .with_context(|| format!("bad TLS server name {name:?}"))?;
    Ok((TlsConnector::from(Arc::new(config)), name))
}

impl Connection {
    /// Lazy: nothing touches the network until the first call.
    pub fn open(profile_name: String, profile: Profile) -> Result<Self> {
        let (address, host, use_tls) = parse_endpoint(&profile.endpoint)?;
        let mut http = reqwest::Client::builder().timeout(Duration::from_secs(10));
        let tls = if use_tls {
            if let Some(path) = &profile.ca_file {
                let pem = std::fs::read(path)
                    .with_context(|| format!("reading ca-file {}", path.display()))?;
                http = http.add_root_certificate(reqwest::Certificate::from_pem(&pem)?);
            }
            Some(tls_connector(&profile, &host)?)
        } else {
            None
        };
        let tokens = TokenSource::for_profile(&profile_name, &profile);
        Ok(Self {
            profile_name,
            profile,
            tokens,
            address,
            tls,
            client: tokio::sync::Mutex::new(None),
            http: http.build()?,
        })
    }

    /// The live client, connecting (again) if there is none.
    async fn client(&self) -> Result<Client, kronosdb_client::Error> {
        let mut slot = self.client.lock().await;
        if let Some(client) = slot.as_ref().filter(|c| !c.connection().is_closed()) {
            return Ok(client.clone());
        }
        let credential = match self.tokens.token().await.map_err(|e| {
            kronosdb_client::Error::Failed(kronosdb_client::Failure::new(
                ErrorCode::UNAUTHENTICATED,
                e.to_string(),
            ))
        })? {
            Some(token) => Credential::Token(token),
            None => Credential::None,
        };
        let options = ConnectOptions {
            client_name: "kronos".into(),
            client_id: format!("kronos-{}", std::process::id()),
            credential,
            tls: self.tls.clone(),
            connect_timeout: Duration::from_secs(5),
            ..ConnectOptions::default()
        };
        let client = Client::connect(&self.address, &options).await?;
        *slot = Some(client.clone());
        Ok(client)
    }

    /// Runs a call with the current token; when the server rejects a token
    /// that can be re-fetched (file, command), fetches once more.
    async fn call<T, F, Fut>(&self, call: F) -> Result<T>
    where
        F: Fn(Client) -> Fut,
        Fut: Future<Output = Result<T, kronosdb_client::Error>>,
    {
        for attempt in 0..2 {
            let result = match self.client().await {
                Ok(client) => call(client).await,
                Err(e) => Err(e),
            };
            match result {
                Ok(value) => return Ok(value),
                Err(e)
                    if e.failure()
                        .is_some_and(|f| f.code == ErrorCode::UNAUTHENTICATED)
                        && attempt == 0
                        && self.tokens.refreshable() =>
                {
                    self.tokens.invalidate().await;
                    // The credential is the connection's: dial again with the new one.
                    *self.client.lock().await = None;
                }
                Err(e) => return Err(self.explain(e)),
            }
        }
        unreachable!("second attempt always returns")
    }

    /// Says what went wrong in the caller's terms. The server's message is
    /// the explanation; the error code only decides how to frame it.
    fn explain(&self, e: kronosdb_client::Error) -> anyhow::Error {
        let Some(failure) = e.failure() else {
            return anyhow!(
                "cannot reach {} — is the server running and the endpoint right? ({e})",
                self.profile.endpoint
            );
        };
        let message = &failure.message;
        match failure.code {
            ErrorCode::UNAUTHENTICATED => {
                anyhow!("not authenticated ({}): {message}", self.tokens.describe())
            }
            ErrorCode::PERMISSION_DENIED => anyhow!("permission denied: {message}"),
            ErrorCode::INTERNAL | ErrorCode::CORRUPTED => anyhow!("server error: {message}"),
            ErrorCode::PROTOCOL_VIOLATION => anyhow!(
                "the server at {} does not understand this client — one of them is \
                 probably out of date ({message})",
                self.profile.endpoint
            ),
            // The message already says it; the code adds nothing.
            _ => anyhow!("{message}"),
        }
    }

    /// Who the server resolved this credential to, and with which grants.
    pub async fn whoami(&self) -> Result<Whoami> {
        self.call(|client| async move { client.who_am_i().await })
            .await
    }

    pub async fn head(&self, context: &str) -> Result<u64> {
        self.call(|client| async move { client.context(context).await?.head().await })
            .await
    }

    pub async fn tail(&self, context: &str) -> Result<u64> {
        self.call(|client| async move { client.context(context).await?.tail().await })
            .await
    }

    pub async fn tags(&self, context: &str, position: u64) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.call(|client| async move { client.context(context).await?.tags(position).await })
            .await
    }

    /// Reads events from `from` up to the current head, at most `limit`.
    pub async fn source(
        &self,
        context: &str,
        from: u64,
        criteria: &[Criterion],
        limit: usize,
    ) -> Result<Vec<Event>> {
        self.call(|client| async move {
            let mut source = client
                .context(context)
                .await?
                .source(from, criteria, 0)
                .await?;
            let mut events = Vec::new();
            while let Some(batch) = source.next().await? {
                events.extend(batch.to_events());
                if events.len() >= limit {
                    events.truncate(limit);
                    // Dropping the read cancels the rest of it.
                    break;
                }
            }
            Ok(events)
        })
        .await
    }

    pub async fn append(&self, context: &str, events: &[NewEvent]) -> Result<Appended> {
        self.call(|client| async move { client.context(context).await?.append(events, None).await })
            .await
    }

    fn admin_url(&self, path: &str) -> Result<String> {
        let base = self.profile.admin.as_deref().with_context(|| {
            format!(
                "profile {:?} has no admin URL: `kronos profile set {} --admin http://host:9240`",
                self.profile_name, self.profile_name
            )
        })?;
        Ok(format!("{}{path}", base.trim_end_matches('/')))
    }

    async fn admin(
        &self,
        method: reqwest::Method,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let url = self.admin_url(path)?;
        for attempt in 0..2 {
            let mut req = self
                .http
                .request(method.clone(), &url)
                .header("accept", "application/json");
            if let Some(token) = self.tokens.token().await? {
                req = req.bearer_auth(token);
            }
            if let Some(body) = body {
                req = req.json(body);
            }
            let resp = req
                .send()
                .await
                .with_context(|| format!("cannot reach admin API at {url}"))?;
            let status = resp.status();
            if status == reqwest::StatusCode::UNAUTHORIZED
                && attempt == 0
                && self.tokens.refreshable()
            {
                self.tokens.invalidate().await;
                continue;
            }
            let text = resp.text().await.unwrap_or_default();
            if status == reqwest::StatusCode::UNAUTHORIZED {
                bail!(
                    "admin API: not authenticated ({}) — it needs the admin token or an \
                     identity with the admin role: {text}",
                    self.tokens.describe()
                );
            }
            if status == reqwest::StatusCode::NOT_FOUND && path.starts_with("/api/v1/") {
                bail!(
                    "the server at {url} has no {path} — its admin API is older than this \
                     CLI (the JSON API arrived after 0.9.0), or that URL is not a KronosDB \
                     admin port"
                );
            }
            if !status.is_success() {
                let detail = if text.trim().is_empty() {
                    "no details given"
                } else {
                    text.trim()
                };
                bail!("admin API {path} failed ({status}): {detail}");
            }
            return serde_json::from_str(&text)
                .with_context(|| format!("admin API {path} did not return JSON — is {url} a KronosDB admin port with the /api/v1 API?"));
        }
        unreachable!("second attempt always returns")
    }

    pub async fn admin_get(&self, path: &str) -> Result<serde_json::Value> {
        self.admin(reqwest::Method::GET, path, None).await
    }

    pub async fn admin_post(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        self.admin(reqwest::Method::POST, path, Some(body)).await
    }
}
