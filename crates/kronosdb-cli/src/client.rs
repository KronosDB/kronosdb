//! One connection to a KronosDB node: gRPC for events, admin HTTP for
//! topology. Both planes get the same bearer token.

use std::future::Future;
use std::time::Duration;

use anyhow::{Context as _, Result, anyhow, bail};
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Identity};
use tonic::{Code, Request, Status};

use crate::profile::Profile;
use crate::token::TokenSource;

// Generated code refers to sibling packages through `super::`, so the
// module tree has to mirror the proto package tree.
// dead_code: the generated packages carry messages this client never sends.
#[allow(dead_code, clippy::enum_variant_names, clippy::result_large_err)]
pub mod proto {
    tonic::include_proto!("kronosdb");
    pub mod eventstore {
        tonic::include_proto!("kronosdb.eventstore");
    }
    pub mod platform {
        tonic::include_proto!("kronosdb.platform");
    }
}
pub use proto::eventstore as pb;

use pb::event_store_client::EventStoreClient;
use proto::platform::platform_service_client::PlatformServiceClient;

pub struct Connection {
    pub profile_name: String,
    pub profile: Profile,
    pub tokens: TokenSource,
    channel: Channel,
    http: reqwest::Client,
}

impl Connection {
    /// Lazy: nothing touches the network until the first call.
    pub fn open(profile_name: String, profile: Profile) -> Result<Self> {
        let mut endpoint = Channel::from_shared(profile.endpoint.clone())
            .with_context(|| format!("bad endpoint {:?}", profile.endpoint))?
            .connect_timeout(Duration::from_secs(5))
            .tcp_nodelay(true);
        let mut http = reqwest::Client::builder().timeout(Duration::from_secs(10));

        if profile.endpoint.starts_with("https://") {
            let mut tls = ClientTlsConfig::new().with_native_roots();
            if let Some(path) = &profile.ca_file {
                let pem = std::fs::read(path)
                    .with_context(|| format!("reading ca-file {}", path.display()))?;
                http = http.add_root_certificate(reqwest::Certificate::from_pem(&pem)?);
                tls = tls.ca_certificate(Certificate::from_pem(pem));
            }
            if let (Some(cert), Some(key)) = (&profile.cert_file, &profile.key_file) {
                let cert = std::fs::read(cert)
                    .with_context(|| format!("reading cert-file {}", cert.display()))?;
                let key = std::fs::read(key)
                    .with_context(|| format!("reading key-file {}", key.display()))?;
                tls = tls.identity(Identity::from_pem(cert, key));
            }
            if let Some(domain) = &profile.tls_domain {
                tls = tls.domain_name(domain.clone());
            }
            endpoint = endpoint.tls_config(tls)?;
        }

        let tokens = TokenSource::for_profile(&profile_name, &profile);
        Ok(Self {
            profile_name,
            profile,
            tokens,
            channel: endpoint.connect_lazy(),
            http: http.build()?,
        })
    }

    /// Runs a gRPC call with the current token; when the server rejects a
    /// token that can be re-fetched (file, command), fetches once more.
    async fn grpc<T, F, Fut>(&self, context: &str, call: F) -> Result<T>
    where
        F: Fn(Channel, tonic::metadata::MetadataMap) -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        for attempt in 0..2 {
            let mut metadata = tonic::metadata::MetadataMap::new();
            metadata.insert("kronosdb-context", context.parse()?);
            if let Some(token) = self.tokens.token().await? {
                metadata.insert(
                    "authorization",
                    format!("Bearer {token}")
                        .parse()
                        .map_err(|_| anyhow!("token contains characters invalid in a header"))?,
                );
            }
            match call(self.channel.clone(), metadata).await {
                Ok(value) => return Ok(value),
                Err(status)
                    if status.code() == Code::Unauthenticated
                        && attempt == 0
                        && self.tokens.refreshable() =>
                {
                    self.tokens.invalidate().await;
                }
                Err(status) => return Err(self.explain(status)),
            }
        }
        unreachable!("second attempt always returns")
    }

    /// Says what went wrong in the caller's terms. The server's message is
    /// the explanation; the gRPC code only decides how to frame it.
    fn explain(&self, status: Status) -> anyhow::Error {
        // A status with a source never came from the server: tonic built it
        // from a transport failure (refused, DNS, TLS, timeout).
        let transport = std::error::Error::source(&status).is_some();
        let message = status.message();
        match status.code() {
            Code::Unavailable if transport => anyhow!(
                "cannot reach {} — is the server running and the endpoint right? ({message})",
                self.profile.endpoint
            ),
            // The server answered: it is up, just not able to serve this yet.
            Code::Unavailable => anyhow!("{message}"),
            Code::Unauthenticated => {
                anyhow!("not authenticated ({}): {message}", self.tokens.describe())
            }
            Code::PermissionDenied => anyhow!("permission denied: {message}"),
            Code::DeadlineExceeded => anyhow!(
                "{} did not answer in time ({message})",
                self.profile.endpoint
            ),
            Code::Unimplemented => anyhow!(
                "the server at {} does not support this call — it is probably older \
                 than this CLI ({message})",
                self.profile.endpoint
            ),
            Code::Internal | Code::Unknown | Code::DataLoss => {
                anyhow!("server error: {message}")
            }
            // NotFound, AlreadyExists, InvalidArgument, FailedPrecondition,
            // Aborted…: the message already says it; the code adds nothing.
            _ => anyhow!("{message}"),
        }
    }

    /// Who the server resolved this credential to, and with which grants.
    pub async fn whoami(&self) -> Result<proto::platform::WhoAmIResponse> {
        self.grpc("default", |channel, metadata| async move {
            let mut req = Request::new(proto::platform::WhoAmIRequest {});
            *req.metadata_mut() = metadata;
            Ok(PlatformServiceClient::new(channel)
                .who_am_i(req)
                .await?
                .into_inner())
        })
        .await
    }

    pub async fn head(&self, context: &str) -> Result<i64> {
        self.grpc(context, |channel, metadata| async move {
            let mut client = EventStoreClient::new(channel);
            let mut req = Request::new(pb::GetHeadRequest {});
            *req.metadata_mut() = metadata;
            Ok(client.get_head(req).await?.into_inner().sequence)
        })
        .await
    }

    pub async fn tail(&self, context: &str) -> Result<i64> {
        self.grpc(context, |channel, metadata| async move {
            let mut client = EventStoreClient::new(channel);
            let mut req = Request::new(pb::GetTailRequest {});
            *req.metadata_mut() = metadata;
            Ok(client.get_tail(req).await?.into_inner().sequence)
        })
        .await
    }

    pub async fn tags(&self, context: &str, sequence: i64) -> Result<Vec<pb::Tag>> {
        self.grpc(context, |channel, metadata| async move {
            let mut client = EventStoreClient::new(channel);
            let mut req = Request::new(pb::GetTagsRequest { sequence });
            *req.metadata_mut() = metadata;
            Ok(client.get_tags(req).await?.into_inner().tags)
        })
        .await
    }

    /// Reads events from `from` up to the current head, at most `limit`.
    pub async fn source(
        &self,
        context: &str,
        from: i64,
        criteria: &[pb::Criterion],
        limit: usize,
    ) -> Result<Vec<pb::SequencedEvent>> {
        self.grpc(context, |channel, metadata| async move {
            let mut client = EventStoreClient::new(channel);
            let mut req = Request::new(pb::SourceRequest {
                from_sequence: from,
                criteria: criteria.to_vec(),
                batch_size: 0,
            });
            *req.metadata_mut() = metadata;
            let mut stream = client.source(req).await?.into_inner();
            let mut events = Vec::new();
            while let Some(response) = stream.message().await? {
                if let Some(batch) = response.batch {
                    events.extend(batch.events);
                }
                if events.len() >= limit {
                    events.truncate(limit);
                    // Dropping the stream cancels the rest of the read.
                    break;
                }
            }
            Ok(events)
        })
        .await
    }

    pub async fn append(
        &self,
        context: &str,
        request: pb::AppendRequest,
    ) -> Result<pb::AppendResponse> {
        self.grpc(context, |channel, metadata| {
            let request = request.clone();
            async move {
                let mut client = EventStoreClient::new(channel);
                let mut req = Request::new(request);
                *req.metadata_mut() = metadata;
                Ok(client.append(req).await?.into_inner())
            }
        })
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
