//! The tower layer that fronts every gRPC service. A layer rather than a
//! tonic interceptor because interceptors are synchronous and see neither
//! the method path nor a way to await a JWKS refresh.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use tonic::Status;
use tonic::transport::server::{TcpConnectInfo, TlsConnectInfo};
use tower::{Layer, Service};

use super::{AuthError, Authenticator, Credentials};

const TOKEN_HEADER: &str = "kronosdb-token";
const CONTEXT_HEADER: &str = "kronosdb-context";
const BUS_HEADER: &str = "kronosdb-bus";

#[derive(Clone)]
pub struct AuthLayer {
    auth: Arc<Authenticator>,
}

impl AuthLayer {
    pub fn new(auth: Arc<Authenticator>) -> Self {
        Self { auth }
    }
}

impl<S> Layer<S> for AuthLayer {
    type Service = AuthService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        AuthService {
            inner,
            auth: Arc::clone(&self.auth),
        }
    }
}

#[derive(Clone)]
pub struct AuthService<S> {
    inner: S,
    auth: Arc<Authenticator>,
}

fn header<'a, B>(req: &'a http::Request<B>, name: &str) -> Option<&'a str> {
    req.headers().get(name).and_then(|v| v.to_str().ok())
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for AuthService<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
    ResBody: Default,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: http::Request<ReqBody>) -> Self::Future {
        // The clone is the one that was NOT polled ready; swap so the ready
        // instance serves this request.
        let clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, clone);
        let auth = Arc::clone(&self.auth);

        // Everything the decision needs is copied out up front: holding
        // `&req` across an await would demand a `Sync` body.
        let access = super::policy::classify(req.uri().path()).0;
        if access == super::policy::Access::Public || !auth.enabled() {
            if let Some(principal) = auth.open_principal() {
                req.extensions_mut().insert(principal);
            }
            return Box::pin(inner.call(req));
        }
        let token = header(&req, "authorization")
            .and_then(|v| {
                v.strip_prefix("Bearer ")
                    .or_else(|| v.strip_prefix("bearer "))
            })
            .or_else(|| header(&req, TOKEN_HEADER))
            .map(str::to_owned);
        let certs = req
            .extensions()
            .get::<TlsConnectInfo<TcpConnectInfo>>()
            .and_then(|info| info.peer_certs());
        let path = req.uri().path().to_owned();
        let context = header(&req, CONTEXT_HEADER).unwrap_or("default").to_owned();
        let bus = header(&req, BUS_HEADER).unwrap_or("default").to_owned();

        Box::pin(async move {
            let client_cert = certs
                .as_ref()
                .and_then(|chain| chain.first())
                .map(|c| c.as_ref());
            let decision = match auth
                .authenticate(Credentials {
                    token: token.as_deref(),
                    client_cert,
                })
                .await
            {
                Ok(principal) => auth
                    .authorize(&principal, &path, &context, &bus)
                    .map(|()| principal),
                Err(e) => Err(e),
            };

            match decision {
                Ok(principal) => {
                    // Handlers read it as `Arc<Principal>` from the tonic
                    // request's extensions.
                    req.extensions_mut().insert(principal);
                    inner.call(req).await
                }
                Err(AuthError::Unauthenticated(msg)) => {
                    tracing::debug!(path = %path, reason = %msg, "unauthenticated");
                    Ok(Status::unauthenticated(msg).into_http())
                }
                Err(AuthError::PermissionDenied(msg)) => {
                    tracing::warn!(path = %path, reason = %msg, "permission denied");
                    Ok(Status::permission_denied(msg).into_http())
                }
            }
        })
    }
}
