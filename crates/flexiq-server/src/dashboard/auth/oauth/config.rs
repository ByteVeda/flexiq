//! OAuth configuration, read from the environment.
//!
//! Secrets stay in the process environment and never reach the settings store,
//! which is why this is parsed here rather than exposed through the settings
//! API like the rest of the dashboard's configuration.

use anyhow::{bail, Result};

use crate::config::{flag, value, Env};

/// Slots the built-in providers own.
const RESERVED_SLOTS: [&str; 2] = ["google", "github"];

/// Google's OpenID discovery document.
pub const GOOGLE_DISCOVERY_URL: &str =
    "https://accounts.google.com/.well-known/openid-configuration";

/// Hosts where a plain-http redirect base is tolerated, for local development.
const LOCAL_HOSTS: [&str; 3] = ["localhost", "127.0.0.1", "::1"];

/// Which flow a provider speaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderKind {
    /// Google, over OIDC discovery.
    Google,
    /// GitHub's OAuth2-only flow — no id_token.
    GitHub,
    /// Any other OIDC-compliant issuer.
    Oidc,
}

impl ProviderKind {
    /// Type name the login UI uses to pick an icon.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Google => "google",
            Self::GitHub => "github",
            Self::Oidc => "oidc",
        }
    }
}

/// Where GitHub's OAuth and API endpoints live.
///
/// Overridable so the exchange can be pointed at a stub: GitHub publishes no
/// discovery document, so without this the flow is only reachable against the
/// real github.com and cannot be regression-tested at all.
#[derive(Debug, Clone)]
pub struct GitHubEndpoints {
    /// Where the browser is sent to authenticate.
    pub authorize: String,
    /// Where the code is exchanged.
    pub token: String,
    /// Root of the REST API.
    pub api_base: String,
}

impl Default for GitHubEndpoints {
    fn default() -> Self {
        Self {
            authorize: "https://github.com/login/oauth/authorize".to_string(),
            token: "https://github.com/login/oauth/access_token".to_string(),
            api_base: "https://api.github.com".to_string(),
        }
    }
}

impl GitHubEndpoints {
    /// Point every endpoint at one base, for a stub that serves all of them.
    pub fn rooted_at(base: &str) -> Self {
        let base = base.trim_end_matches('/');
        Self {
            authorize: format!("{base}/login/oauth/authorize"),
            token: format!("{base}/login/oauth/access_token"),
            api_base: base.to_string(),
        }
    }
}

/// One configured provider.
#[derive(Debug, Clone)]
pub struct ProviderConfig {
    /// URL-safe id, used in the callback path.
    pub slot: String,
    /// Button label.
    pub label: String,
    /// Flow this provider speaks.
    pub kind: ProviderKind,
    /// OAuth client id.
    pub client_id: String,
    /// OAuth client secret.
    pub client_secret: String,
    /// Discovery document, for the OIDC kinds.
    pub discovery_url: Option<String>,
    /// Email domains permitted to log in; empty means any.
    pub allowed_domains: Vec<String>,
    /// GitHub orgs permitted to log in; empty means any.
    pub allowed_orgs: Vec<String>,
    /// GitHub's endpoints. Always the real ones outside tests.
    pub github: GitHubEndpoints,
}

/// Top-level OAuth configuration.
#[derive(Debug, Clone)]
pub struct OAuthConfig {
    /// Public origin the dashboard is served at; every callback derives from it.
    pub redirect_base_url: String,
    /// Whether the password form is still offered alongside the providers.
    pub password_auth_enabled: bool,
    /// Emails that get the admin role on first provider login.
    pub admin_emails: Vec<String>,
    /// Providers, in display order.
    pub providers: Vec<ProviderConfig>,
}

impl OAuthConfig {
    /// Callback URL for one slot.
    pub fn callback_url(&self, slot: &str) -> String {
        format!(
            "{}/api/auth/oauth/callback/{slot}",
            self.redirect_base_url.trim_end_matches('/')
        )
    }

    /// The provider registered in `slot`.
    pub fn provider(&self, slot: &str) -> Option<&ProviderConfig> {
        self.providers.iter().find(|provider| provider.slot == slot)
    }
}

/// Parse the OAuth block, or `None` when no provider is configured.
///
/// A half-configured provider is an error rather than a silent skip: an
/// operator who set a client id and forgot the secret must find out at
/// startup, not when the first person tries to log in.
pub fn from_env(env: &Env) -> Result<Option<OAuthConfig>> {
    let base_url = value(env, "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL");
    let mut providers = Vec::new();

    if let Some(client_id) = value(env, "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID") {
        providers.push(ProviderConfig {
            slot: "google".into(),
            label: "Google".into(),
            kind: ProviderKind::Google,
            client_secret: require_secret(env, "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_SECRET")?,
            client_id,
            discovery_url: Some(GOOGLE_DISCOVERY_URL.to_string()),
            allowed_domains: csv(env, "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_ALLOWED_DOMAINS"),
            allowed_orgs: Vec::new(),
            github: GitHubEndpoints::default(),
        });
    }

    if let Some(client_id) = value(env, "FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_ID") {
        providers.push(ProviderConfig {
            slot: "github".into(),
            label: "GitHub".into(),
            kind: ProviderKind::GitHub,
            client_secret: require_secret(env, "FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_SECRET")?,
            client_id,
            discovery_url: None,
            allowed_domains: Vec::new(),
            allowed_orgs: csv(env, "FLEXIQ_DASHBOARD_OAUTH_GITHUB_ALLOWED_ORGS"),
            github: GitHubEndpoints::default(),
        });
    }

    for slot in csv(env, "FLEXIQ_DASHBOARD_OAUTH_OIDC_PROVIDERS") {
        providers.push(oidc_provider(env, &slot, &providers)?);
    }

    if providers.is_empty() {
        return Ok(None);
    }

    let Some(redirect_base_url) = base_url else {
        bail!(
            "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL must be set when any OAuth \
             provider is configured — it is what every callback URL is built from"
        );
    };
    validate_base_url(&redirect_base_url)?;

    let admin_emails = csv(env, "FLEXIQ_DASHBOARD_OAUTH_ADMIN_EMAILS");
    if admin_emails.is_empty() {
        // Provider logins only ever get the viewer role without an allowlist,
        // so an OAuth-only deployment would come up with no admins at all.
        log::warn!(
            "OAuth is configured without FLEXIQ_DASHBOARD_OAUTH_ADMIN_EMAILS: \
             every provider login gets the viewer role"
        );
    }

    Ok(Some(OAuthConfig {
        redirect_base_url,
        password_auth_enabled: flag(env, "FLEXIQ_DASHBOARD_PASSWORD_AUTH_ENABLED", true),
        admin_emails,
        providers,
    }))
}

fn oidc_provider(env: &Env, slot: &str, existing: &[ProviderConfig]) -> Result<ProviderConfig> {
    validate_slot(slot)?;
    if existing.iter().any(|provider| provider.slot == slot) {
        bail!("OIDC slot '{slot}' is listed twice");
    }
    let prefix = format!(
        "FLEXIQ_DASHBOARD_OAUTH_OIDC_{}",
        slot.to_ascii_uppercase().replace('-', "_")
    );
    let Some(client_id) = value(env, &format!("{prefix}_CLIENT_ID")) else {
        bail!("OIDC slot '{slot}': {prefix}_CLIENT_ID is required");
    };
    let Some(discovery_url) = value(env, &format!("{prefix}_DISCOVERY_URL")) else {
        bail!("OIDC slot '{slot}': {prefix}_DISCOVERY_URL is required");
    };
    Ok(ProviderConfig {
        label: value(env, &format!("{prefix}_LABEL")).unwrap_or_else(|| title_case(slot)),
        slot: slot.to_string(),
        kind: ProviderKind::Oidc,
        client_secret: require_secret(env, &format!("{prefix}_CLIENT_SECRET"))?,
        client_id,
        discovery_url: Some(discovery_url),
        allowed_domains: csv(env, &format!("{prefix}_ALLOWED_DOMAINS")),
        allowed_orgs: Vec::new(),
        github: GitHubEndpoints::default(),
    })
}

fn require_secret(env: &Env, key: &str) -> Result<String> {
    value(env, key).ok_or_else(|| anyhow::anyhow!("{key} is required when its client id is set"))
}

fn csv(env: &Env, key: &str) -> Vec<String> {
    value(env, key)
        .map(|raw| {
            raw.split(',')
                .map(str::trim)
                .filter(|part| !part.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn validate_slot(slot: &str) -> Result<()> {
    if RESERVED_SLOTS.contains(&slot) {
        bail!("OIDC slot '{slot}' collides with a built-in provider");
    }
    let valid = slot.len() <= 32
        && slot.starts_with(|c: char| c.is_ascii_lowercase())
        && slot
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
    if !valid {
        bail!("OIDC slot '{slot}' must be lowercase alphanumeric with '-' or '_', starting with a letter");
    }
    Ok(())
}

/// The redirect base is what a provider will send a user back to, so a typo
/// here is a login that lands somewhere else entirely.
fn validate_base_url(url: &str) -> Result<()> {
    let Some((scheme, rest)) = url.split_once("://") else {
        bail!("FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL must be http(s)");
    };
    let scheme = scheme.to_ascii_lowercase();
    if !matches!(scheme.as_str(), "http" | "https") {
        bail!("FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL must be http(s), got '{scheme}'");
    }
    let host = rest
        .split(['/', '?', '#'])
        .next()
        .map(|authority| {
            let host_port = authority.rsplit('@').next().unwrap_or(authority);
            match host_port.strip_prefix('[') {
                Some(v6) => v6.split(']').next().unwrap_or(v6).to_string(),
                None => host_port.split(':').next().unwrap_or(host_port).to_string(),
            }
        })
        .filter(|host| !host.is_empty());
    let Some(host) = host else {
        bail!("FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL must include a hostname");
    };
    if scheme == "http" && !LOCAL_HOSTS.contains(&host.as_str()) {
        bail!(
            "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL must use https for non-local hosts \
             (got http://{host}) — an OAuth code returned over plain http is interceptable"
        );
    }
    Ok(())
}

fn title_case(slot: &str) -> String {
    let mut chars = slot.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    #[test]
    fn no_providers_means_oauth_is_off() {
        assert!(from_env(&env(&[])).expect("valid").is_none());
        // A base URL alone configures nothing.
        assert!(from_env(&env(&[(
            "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL",
            "https://ops.example.com"
        )]))
        .expect("valid")
        .is_none());
    }

    #[test]
    fn a_provider_without_a_base_url_is_an_error() {
        let error = from_env(&env(&[
            ("FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID", "id"),
            ("FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_SECRET", "secret"),
        ]))
        .expect_err("must refuse");
        assert!(error.to_string().contains("REDIRECT_BASE_URL"));
    }

    #[test]
    fn a_client_id_without_its_secret_is_an_error() {
        let error = from_env(&env(&[
            (
                "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL",
                "https://ops.example.com",
            ),
            ("FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_ID", "id"),
        ]))
        .expect_err("must refuse");
        assert!(error.to_string().contains("CLIENT_SECRET"));
    }

    #[test]
    fn google_and_github_parse_with_their_allowlists() {
        let config = from_env(&env(&[
            (
                "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL",
                "https://ops.example.com/",
            ),
            ("FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID", "gid"),
            ("FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_SECRET", "gsecret"),
            (
                "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_ALLOWED_DOMAINS",
                "example.com, other.com",
            ),
            ("FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_ID", "hid"),
            ("FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_SECRET", "hsecret"),
            ("FLEXIQ_DASHBOARD_OAUTH_GITHUB_ALLOWED_ORGS", "byteveda"),
            ("FLEXIQ_DASHBOARD_OAUTH_ADMIN_EMAILS", "ops@example.com"),
        ]))
        .expect("valid")
        .expect("configured");

        assert_eq!(config.providers.len(), 2);
        assert_eq!(config.providers[0].kind, ProviderKind::Google);
        assert_eq!(
            config.providers[0].allowed_domains,
            vec!["example.com".to_string(), "other.com".to_string()]
        );
        assert_eq!(
            config.providers[1].allowed_orgs,
            vec!["byteveda".to_string()]
        );
        assert_eq!(
            config.callback_url("google"),
            "https://ops.example.com/api/auth/oauth/callback/google"
        );
    }

    #[test]
    fn an_oidc_slot_needs_its_own_variables() {
        let base = [
            (
                "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL",
                "https://ops.example.com",
            ),
            ("FLEXIQ_DASHBOARD_OAUTH_OIDC_PROVIDERS", "acme-okta"),
        ];
        assert!(from_env(&env(&base)).is_err());

        let mut full = base.to_vec();
        full.push(("FLEXIQ_DASHBOARD_OAUTH_OIDC_ACME_OKTA_CLIENT_ID", "id"));
        full.push((
            "FLEXIQ_DASHBOARD_OAUTH_OIDC_ACME_OKTA_CLIENT_SECRET",
            "secret",
        ));
        full.push((
            "FLEXIQ_DASHBOARD_OAUTH_OIDC_ACME_OKTA_DISCOVERY_URL",
            "https://acme.okta.com/.well-known/openid-configuration",
        ));
        let config = from_env(&env(&full)).expect("valid").expect("configured");
        assert_eq!(config.providers[0].slot, "acme-okta");
        assert_eq!(config.providers[0].label, "Acme-okta");
    }

    #[test]
    fn reserved_and_malformed_slots_are_rejected() {
        for slot in ["google", "GitHub", "9lives", "has space"] {
            assert!(validate_slot(slot).is_err(), "must reject '{slot}'");
        }
        validate_slot("acme-okta").expect("allowed");
    }

    #[test]
    fn a_non_local_http_base_url_is_refused() {
        assert!(validate_base_url("http://ops.example.com").is_err());
        validate_base_url("http://localhost:8080").expect("local development");
        validate_base_url("https://ops.example.com").expect("production");
        assert!(validate_base_url("ops.example.com").is_err());
    }
}
