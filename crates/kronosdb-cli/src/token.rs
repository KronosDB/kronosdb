//! Where the bearer token comes from. The server trusts OIDC issuers and
//! knows nothing about clouds; this is the matching client half —
//! `token-command` runs whatever prints a token (`gcloud auth
//! print-identity-token`, `az account get-access-token ...`, `vault read
//! ...`), the way kubectl's exec credential plugins do.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context as _, Result, bail};
use base64::Engine as _;
use tokio::sync::Mutex;

use crate::profile::Profile;

/// Refresh this long before a JWT's `exp`.
const EXPIRY_MARGIN_SECS: u64 = 60;

#[derive(Debug, Clone, PartialEq)]
enum Source {
    None,
    Static(String),
    File(PathBuf),
    Command(String),
}

pub struct TokenSource {
    source: Source,
    /// Disk cache for `token-command` results, so one-shot invocations
    /// don't each pay for the command (gcloud takes about a second).
    cache_path: Option<PathBuf>,
    cached: Mutex<Option<String>>,
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// `exp` of a JWT, read without verifying anything (the server verifies;
/// this only schedules refreshes). `None` for opaque tokens.
pub fn jwt_claims(token: &str) -> Option<serde_json::Value> {
    let mut parts = token.split('.');
    let (_, payload, _) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn is_fresh(token: &str) -> bool {
    match jwt_claims(token).and_then(|c| c.get("exp")?.as_u64()) {
        Some(exp) => exp > now_unix() + EXPIRY_MARGIN_SECS,
        // Opaque token: good until the server says otherwise.
        None => true,
    }
}

impl TokenSource {
    pub fn for_profile(name: &str, profile: &Profile) -> Self {
        // KRONOS_TOKEN beats the profile: handy in CI.
        let source = if let Ok(token) = std::env::var("KRONOS_TOKEN") {
            Source::Static(token)
        } else if let Some(token) = &profile.token {
            Source::Static(token.clone())
        } else if let Some(path) = &profile.token_file {
            Source::File(path.clone())
        } else if let Some(command) = &profile.token_command {
            Source::Command(command.clone())
        } else {
            Source::None
        };
        let cache_path = matches!(source, Source::Command(_)).then(|| cache_dir().join(name));
        Self {
            source,
            cache_path,
            cached: Mutex::new(None),
        }
    }

    /// True when a rejected token can be replaced by asking again.
    pub fn refreshable(&self) -> bool {
        matches!(self.source, Source::File(_) | Source::Command(_))
    }

    pub fn describe(&self) -> String {
        match &self.source {
            Source::None => "none".into(),
            Source::Static(_) => "static token".into(),
            Source::File(path) => format!("token-file {}", path.display()),
            Source::Command(command) => format!("token-command `{command}`"),
        }
    }

    pub async fn token(&self) -> Result<Option<String>> {
        let mut cached = self.cached.lock().await;
        if let Some(token) = cached.as_ref()
            && is_fresh(token)
        {
            return Ok(Some(token.clone()));
        }
        let token = match &self.source {
            Source::None => return Ok(None),
            Source::Static(token) => token.clone(),
            Source::File(path) => tokio::fs::read_to_string(path)
                .await
                .with_context(|| format!("reading token-file {}", path.display()))?
                .trim()
                .to_string(),
            Source::Command(command) => match self.read_disk_cache().await {
                Some(token) => token,
                None => {
                    let token = run_command(command).await?;
                    self.write_disk_cache(&token).await;
                    token
                }
            },
        };
        if token.is_empty() {
            bail!("{} produced an empty token", self.describe());
        }
        *cached = Some(token.clone());
        Ok(Some(token))
    }

    /// Forgets the current token (the server rejected it).
    pub async fn invalidate(&self) {
        *self.cached.lock().await = None;
        if let Some(path) = &self.cache_path {
            let _ = tokio::fs::remove_file(path).await;
        }
    }

    async fn read_disk_cache(&self) -> Option<String> {
        let token = tokio::fs::read_to_string(self.cache_path.as_ref()?)
            .await
            .ok()?;
        let token = token.trim();
        // Only JWTs are cached on disk: an opaque token has no expiry to
        // judge staleness by.
        (jwt_claims(token).is_some() && is_fresh(token)).then(|| token.to_string())
    }

    async fn write_disk_cache(&self, token: &str) {
        let Some(path) = &self.cache_path else { return };
        if jwt_claims(token).is_none() {
            return;
        }
        // Best effort: a read-only home just means no cache.
        let write = async {
            tokio::fs::create_dir_all(path.parent()?).await.ok()?;
            let mut options = tokio::fs::OpenOptions::new();
            options.create(true).write(true).truncate(true);
            #[cfg(unix)]
            options.mode(0o600);
            let mut file = options.open(path).await.ok()?;
            tokio::io::AsyncWriteExt::write_all(&mut file, token.as_bytes())
                .await
                .ok()?;
            // tokio completes file writes on a background thread; without
            // the flush a reader (the next `kronos` run) can see an empty file.
            tokio::io::AsyncWriteExt::flush(&mut file).await.ok()
        };
        let _ = write.await;
    }
}

fn cache_dir() -> PathBuf {
    std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))
        .unwrap_or_else(std::env::temp_dir)
        .join("kronosdb")
        .join("tokens")
}

async fn run_command(command: &str) -> Result<String> {
    let output = tokio::process::Command::new("sh")
        .arg("-c")
        .arg(command)
        .stdin(std::process::Stdio::null())
        .output()
        .await
        .with_context(|| format!("running token-command `{command}`"))?;
    if !output.status.success() {
        bail!(
            "token-command `{command}` failed ({}): {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jwt(exp: u64) -> String {
        let enc = |s: &str| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(s);
        format!(
            "{}.{}.sig",
            enc(r#"{"alg":"ES256"}"#),
            enc(&format!(r#"{{"sub":"me","exp":{exp}}}"#))
        )
    }

    fn source(source: Source) -> TokenSource {
        TokenSource {
            source,
            cache_path: None,
            cached: Mutex::new(None),
        }
    }

    #[test]
    fn freshness() {
        assert!(is_fresh("opaque-secret"));
        assert!(is_fresh(&jwt(now_unix() + 3600)));
        assert!(!is_fresh(&jwt(now_unix() + 10)));
        assert!(!is_fresh(&jwt(now_unix() - 10)));
        assert_eq!(jwt_claims(&jwt(5)).unwrap()["sub"], "me");
        assert!(jwt_claims("a.b").is_none());
    }

    #[tokio::test]
    async fn file_source_rereads_after_invalidate() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), "first\n").unwrap();
        let tokens = source(Source::File(file.path().to_path_buf()));
        assert_eq!(tokens.token().await.unwrap().as_deref(), Some("first"));

        std::fs::write(file.path(), "second\n").unwrap();
        // Opaque tokens stay cached until the server rejects them.
        assert_eq!(tokens.token().await.unwrap().as_deref(), Some("first"));
        tokens.invalidate().await;
        assert_eq!(tokens.token().await.unwrap().as_deref(), Some("second"));
    }

    #[tokio::test]
    async fn command_source_and_expiry() {
        let tokens = source(Source::Command("echo ' from-command '".into()));
        assert_eq!(
            tokens.token().await.unwrap().as_deref(),
            Some("from-command")
        );

        // A cached JWT about to expire is replaced without being asked.
        *tokens.cached.lock().await = Some(jwt(now_unix() + 5));
        assert_eq!(
            tokens.token().await.unwrap().as_deref(),
            Some("from-command")
        );

        let failing = source(Source::Command("echo nope >&2; exit 3".into()));
        let err = failing.token().await.unwrap_err().to_string();
        assert!(err.contains("nope"), "{err}");
    }

    #[tokio::test]
    async fn command_results_are_cached_on_disk_as_jwt_only() {
        let dir = tempfile::tempdir().unwrap();
        let token = jwt(now_unix() + 3600);
        let tokens = TokenSource {
            source: Source::Command(format!("echo {token}")),
            cache_path: Some(dir.path().join("prod")),
            cached: Mutex::new(None),
        };
        assert_eq!(tokens.token().await.unwrap(), Some(token.clone()));
        assert_eq!(
            std::fs::read_to_string(dir.path().join("prod")).unwrap(),
            token
        );

        // A second process picks the cached token up without the command.
        let second = TokenSource {
            source: Source::Command("exit 1".into()),
            cache_path: Some(dir.path().join("prod")),
            cached: Mutex::new(None),
        };
        assert_eq!(second.token().await.unwrap(), Some(token));
        second.invalidate().await;
        assert!(second.token().await.is_err());

        let opaque = TokenSource {
            source: Source::Command("echo opaque".into()),
            cache_path: Some(dir.path().join("opaque")),
            cached: Mutex::new(None),
        };
        opaque.token().await.unwrap();
        assert!(!dir.path().join("opaque").exists());
    }
}
