//! Identity from a verified client certificate. rustls has already checked
//! the chain against `tls-ca` by the time a certificate reaches this code;
//! all that is left is reading the names off the leaf.

use x509_parser::prelude::*;

/// Names a client certificate speaks for, most specific first: URI SANs
/// (SPIFFE IDs — `spiffe://trust-domain/ns/prod/sa/orders`), DNS SANs, then
/// the subject CN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CertIdentity {
    pub names: Vec<String>,
}

impl CertIdentity {
    pub fn from_der(der: &[u8]) -> Result<Self, String> {
        let (_, cert) =
            X509Certificate::from_der(der).map_err(|e| format!("bad client certificate: {e}"))?;

        let mut uris = Vec::new();
        let mut dns = Vec::new();
        if let Ok(Some(san)) = cert.subject_alternative_name() {
            for name in &san.value.general_names {
                match name {
                    GeneralName::URI(uri) => uris.push(uri.to_string()),
                    GeneralName::DNSName(host) => dns.push(host.to_string()),
                    _ => {}
                }
            }
        }
        let cn = cert
            .subject()
            .iter_common_name()
            .filter_map(|cn| cn.as_str().ok().map(String::from));

        let names: Vec<String> = uris.into_iter().chain(dns).chain(cn).collect();
        if names.is_empty() {
            return Err("client certificate carries no URI SAN, DNS SAN, or CN".into());
        }
        Ok(Self { names })
    }

    pub fn primary(&self) -> &str {
        &self.names[0]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_ordered_uri_dns_cn() {
        let mut params =
            rcgen::CertificateParams::new(vec!["orders.prod.svc".to_string()]).expect("params");
        params.subject_alt_names.push(rcgen::SanType::URI(
            "spiffe://karma.life/ns/prod/sa/orders".try_into().unwrap(),
        ));
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "orders");
        let key = rcgen::KeyPair::generate().unwrap();
        let cert = params.self_signed(&key).unwrap();

        let identity = CertIdentity::from_der(cert.der()).unwrap();
        assert_eq!(
            identity.names,
            vec![
                "spiffe://karma.life/ns/prod/sa/orders".to_string(),
                "orders.prod.svc".to_string(),
                "orders".to_string(),
            ]
        );
        assert_eq!(identity.primary(), "spiffe://karma.life/ns/prod/sa/orders");
    }

    #[test]
    fn garbage_is_rejected() {
        assert!(CertIdentity::from_der(b"not a certificate").is_err());
    }
}
