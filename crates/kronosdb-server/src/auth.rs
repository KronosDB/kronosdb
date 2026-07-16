use subtle::ConstantTimeEq;
use tonic::{Request, Status};

/// Creates a tonic interceptor that validates the `kronosdb-token` metadata header.
///
/// If `token` is `None`, all requests are allowed (open access).
/// If `token` is `Some`, every request must carry a matching `kronosdb-token`
/// gRPC metadata value or the call is rejected with `UNAUTHENTICATED`.
pub fn make_auth_interceptor(
    token: Option<String>,
) -> impl Fn(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |req: Request<()>| {
        let Some(ref expected) = token else {
            return Ok(req); // Auth disabled — open access.
        };

        match req.metadata().get("kronosdb-token") {
            Some(t) => {
                // Constant-time compare: a plain `==` short-circuits on the
                // first differing byte, leaking prefix length via timing.
                let presented = t.to_str().unwrap_or("").as_bytes();
                if bool::from(presented.ct_eq(expected.as_bytes())) {
                    Ok(req)
                } else {
                    Err(Status::unauthenticated("invalid access token"))
                }
            }
            None => Err(Status::unauthenticated("missing kronosdb-token header")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataValue;

    fn request_with_token(token: &str) -> Request<()> {
        let mut req = Request::new(());
        req.metadata_mut()
            .insert("kronosdb-token", MetadataValue::try_from(token).unwrap());
        req
    }

    #[test]
    fn no_token_configured_allows_all() {
        let interceptor = make_auth_interceptor(None);
        let req = Request::new(());
        assert!(interceptor(req).is_ok());
    }

    #[test]
    fn valid_token_passes() {
        let interceptor = make_auth_interceptor(Some("secret123".into()));
        let req = request_with_token("secret123");
        assert!(interceptor(req).is_ok());
    }

    #[test]
    fn invalid_token_rejected() {
        let interceptor = make_auth_interceptor(Some("secret123".into()));
        let req = request_with_token("wrong");
        let err = interceptor(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }

    #[test]
    fn missing_token_rejected() {
        let interceptor = make_auth_interceptor(Some("secret123".into()));
        let req = Request::new(());
        let err = interceptor(req).unwrap_err();
        assert_eq!(err.code(), tonic::Code::Unauthenticated);
    }
}
