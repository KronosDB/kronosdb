//! Connection profiles — kubeconfig-style named targets in
//! `~/.config/kronosdb/config.toml`. Called *profiles*, not contexts,
//! because a KronosDB context is an event store.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{Context as _, Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    /// gRPC endpoint, `http://` or `https://`.
    pub endpoint: String,
    /// Admin HTTP base URL. Optional: event commands work without it, the
    /// TUI and topology commands need it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub admin: Option<String>,
    /// Default event store context (`kronosdb-context`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,

    // ── credentials: at most one ──
    /// Static token, inline. Fine for localhost; prefer the others.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// Read on every refresh — Kubernetes projected tokens rotate on disk.
    #[serde(rename = "token-file", skip_serializing_if = "Option::is_none")]
    pub token_file: Option<PathBuf>,
    /// Shell command that prints a token, e.g.
    /// `gcloud auth print-identity-token`. The cloud-agnostic hook: any
    /// identity provider with a CLI works without kronos knowing about it.
    #[serde(rename = "token-command", skip_serializing_if = "Option::is_none")]
    pub token_command: Option<String>,

    // ── TLS ──
    /// Extra root CA (PEM). Without it, https uses the system roots.
    #[serde(rename = "ca-file", skip_serializing_if = "Option::is_none")]
    pub ca_file: Option<PathBuf>,
    /// Client certificate + key (PEM) for mTLS.
    #[serde(rename = "cert-file", skip_serializing_if = "Option::is_none")]
    pub cert_file: Option<PathBuf>,
    #[serde(rename = "key-file", skip_serializing_if = "Option::is_none")]
    pub key_file: Option<PathBuf>,
    /// Name to verify the server certificate against, when it differs from
    /// the endpoint host (port-forwards, IP endpoints).
    #[serde(rename = "tls-domain", skip_serializing_if = "Option::is_none")]
    pub tls_domain: Option<String>,
}

impl Profile {
    pub fn local() -> Self {
        Self {
            endpoint: "http://127.0.0.1:50051".into(),
            admin: Some("http://127.0.0.1:9240".into()),
            ..Default::default()
        }
    }

    pub fn validate(&self, name: &str) -> Result<()> {
        if !self.endpoint.starts_with("http://") && !self.endpoint.starts_with("https://") {
            bail!(
                "profile {name:?}: endpoint {:?} must start with http:// or https://",
                self.endpoint
            );
        }
        let credentials = [
            self.token.is_some(),
            self.token_file.is_some(),
            self.token_command.is_some(),
        ];
        if credentials.iter().filter(|set| **set).count() > 1 {
            bail!("profile {name:?}: set only one of token, token-file, token-command");
        }
        if self.cert_file.is_some() != self.key_file.is_some() {
            bail!("profile {name:?}: cert-file and key-file go together");
        }
        Ok(())
    }

    pub fn context(&self) -> &str {
        self.context.as_deref().unwrap_or("default")
    }
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct ConfigFile {
    #[serde(rename = "current-profile", skip_serializing_if = "Option::is_none")]
    pub current_profile: Option<String>,
    #[serde(default)]
    pub profiles: BTreeMap<String, Profile>,
}

pub fn config_path() -> PathBuf {
    if let Some(path) = std::env::var_os("KRONOS_CONFIG") {
        return PathBuf::from(path);
    }
    let base = std::env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))
        .unwrap_or_else(|| PathBuf::from("."));
    base.join("kronosdb").join("config.toml")
}

impl ConfigFile {
    pub fn load() -> Result<Self> {
        let path = config_path();
        match std::fs::read_to_string(&path) {
            Ok(contents) => {
                toml::from_str(&contents).with_context(|| format!("parsing {}", path.display()))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::default()),
            Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
        }
    }

    pub fn save(&self) -> Result<()> {
        let path = config_path();
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        std::fs::write(&path, toml::to_string_pretty(self)?)
            .with_context(|| format!("writing {}", path.display()))?;
        // Profiles may hold inline tokens.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        }
        Ok(())
    }

    /// Picks the profile: `--profile` / `KRONOS_PROFILE`, then
    /// `current-profile`, then a lone configured profile, then localhost.
    pub fn resolve(&self, requested: Option<&str>) -> Result<(String, Profile)> {
        let name = match requested.or(self.current_profile.as_deref()) {
            Some(name) => name.to_string(),
            None if self.profiles.len() == 1 => self.profiles.keys().next().unwrap().clone(),
            None if self.profiles.is_empty() => return Ok(("local".into(), Profile::local())),
            None => bail!(
                "several profiles are configured and none is current: run \
                 `kronos profile use <name>` or pass --profile ({})",
                self.profiles.keys().cloned().collect::<Vec<_>>().join(", ")
            ),
        };
        let profile = self
            .profiles
            .get(&name)
            .cloned()
            .or_else(|| (name == "local").then(Profile::local))
            .with_context(|| {
                format!(
                    "no profile named {name:?} in {} (have: {})",
                    config_path().display(),
                    self.profiles.keys().cloned().collect::<Vec<_>>().join(", ")
                )
            })?;
        profile.validate(&name)?;
        Ok((name, profile))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_order() {
        let mut cfg = ConfigFile::default();
        assert_eq!(cfg.resolve(None).unwrap().0, "local");

        cfg.profiles.insert("prod".into(), Profile::local());
        assert_eq!(cfg.resolve(None).unwrap().0, "prod");

        cfg.profiles.insert("dev".into(), Profile::local());
        assert!(cfg.resolve(None).is_err());
        assert_eq!(cfg.resolve(Some("dev")).unwrap().0, "dev");

        cfg.current_profile = Some("prod".into());
        assert_eq!(cfg.resolve(None).unwrap().0, "prod");
        assert!(cfg.resolve(Some("nope")).is_err());
    }

    #[test]
    fn validation() {
        let mut p = Profile::local();
        p.token = Some("a".into());
        p.token_command = Some("b".into());
        assert!(p.validate("x").is_err());

        let mut p = Profile::local();
        p.endpoint = "localhost:50051".into();
        assert!(p.validate("x").is_err());

        let mut p = Profile::local();
        p.cert_file = Some("c.pem".into());
        assert!(p.validate("x").is_err());
    }

    #[test]
    fn toml_roundtrip() {
        let cfg: ConfigFile = toml::from_str(
            r#"
current-profile = "prod"

[profiles.prod]
endpoint = "http://localhost:50051"
admin = "http://localhost:9240"
context = "orders"
token-command = "gcloud auth print-identity-token"
"#,
        )
        .unwrap();
        let (name, profile) = cfg.resolve(None).unwrap();
        assert_eq!(name, "prod");
        assert_eq!(profile.context(), "orders");
        let again: ConfigFile = toml::from_str(&toml::to_string_pretty(&cfg).unwrap()).unwrap();
        assert_eq!(again.profiles["prod"], profile);
    }
}
