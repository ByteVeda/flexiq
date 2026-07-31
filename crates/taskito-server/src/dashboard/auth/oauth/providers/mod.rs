//! Provider implementations and the runtime that owns their HTTP state.

pub mod github;
pub mod oidc;

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use crate::dashboard::auth::oauth::config::{OAuthConfig, ProviderConfig, ProviderKind};

/// How long any provider call may take before it is treated as a failure.
const HTTP_TIMEOUT: Duration = Duration::from_secs(10);

/// Who just logged in, normalised across providers.
#[derive(Debug, Clone)]
pub struct Identity {
    /// Provider slot the identity came from.
    pub slot: String,
    /// The provider's stable id for this account — never the email, which can
    /// be reassigned to someone else.
    pub subject: String,
    /// Email, when the provider gives one.
    pub email: Option<String>,
    /// Whether the provider vouches for that email.
    pub email_verified: bool,
    /// Display name.
    pub name: Option<String>,
}

/// Why a login could not complete.
#[derive(Debug, thiserror::Error)]
pub enum OAuthError {
    /// No provider is registered in the requested slot.
    #[error("OAuth provider '{0}' is not configured")]
    NotConfigured(String),
    /// The callback's state was missing, expired, replayed, or forged.
    #[error("state is invalid, expired, or already used")]
    StateInvalid,
    /// The provider refused, or its response could not be trusted.
    #[error("identity could not be established: {0}")]
    IdentityFetch(String),
    /// The identity is genuine but outside the configured allowlist.
    #[error("access denied: {0}")]
    Denied(String),
}

/// Shared HTTP client plus the caches a provider needs between requests.
///
/// Discovery documents and JWKS change rarely and are fetched on the login
/// path, so caching them keeps a login from making three round trips before
/// the user's browser has even been redirected.
pub struct OAuthRuntime {
    /// Parsed configuration.
    pub config: OAuthConfig,
    http: reqwest::Client,
    discovery: Mutex<HashMap<String, oidc::Discovery>>,
    jwks: Mutex<HashMap<String, jsonwebtoken::jwk::JwkSet>>,
}

impl OAuthRuntime {
    /// Build the runtime for `config`.
    pub fn new(config: OAuthConfig) -> Self {
        Self {
            config,
            http: reqwest::Client::builder()
                .timeout(HTTP_TIMEOUT)
                // A provider that redirects the token exchange somewhere else
                // is not a provider we should be handing a client secret to.
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .unwrap_or_default(),
            discovery: Mutex::new(HashMap::new()),
            jwks: Mutex::new(HashMap::new()),
        }
    }

    /// The provider registered in `slot`.
    pub fn provider(&self, slot: &str) -> Result<&ProviderConfig, OAuthError> {
        self.config
            .provider(slot)
            .ok_or_else(|| OAuthError::NotConfigured(slot.to_string()))
    }

    /// Compact provider list for the login screen. Never includes secrets.
    pub fn listing(&self) -> Vec<serde_json::Value> {
        self.config
            .providers
            .iter()
            .map(|provider| {
                serde_json::json!({
                    "slot": provider.slot,
                    "label": provider.label,
                    "type": provider.kind.as_str(),
                })
            })
            .collect()
    }

    /// Where to send the browser to begin a login.
    pub async fn authorization_url(
        &self,
        provider: &ProviderConfig,
        state: &str,
        nonce: &str,
        code_challenge: &str,
        redirect_uri: &str,
    ) -> Result<String, OAuthError> {
        match provider.kind {
            ProviderKind::GitHub => Ok(github::authorization_url(
                provider,
                state,
                code_challenge,
                redirect_uri,
            )),
            ProviderKind::Google | ProviderKind::Oidc => {
                oidc::authorization_url(self, provider, state, nonce, code_challenge, redirect_uri)
                    .await
            }
        }
    }

    /// Exchange the callback's code for a verified identity.
    pub async fn exchange_code(
        &self,
        provider: &ProviderConfig,
        code: &str,
        code_verifier: &str,
        redirect_uri: &str,
        expected_nonce: &str,
    ) -> Result<Identity, OAuthError> {
        match provider.kind {
            ProviderKind::GitHub => {
                github::exchange_code(self, provider, code, code_verifier, redirect_uri).await
            }
            ProviderKind::Google | ProviderKind::Oidc => {
                oidc::exchange_code(
                    self,
                    provider,
                    code,
                    code_verifier,
                    redirect_uri,
                    expected_nonce,
                )
                .await
            }
        }
    }

    /// Apply the provider's allowlist to an established identity.
    ///
    /// GitHub's org check needs the access token, so it happens during the
    /// exchange; what remains here is the email-domain rule.
    pub fn check_allowlist(
        &self,
        provider: &ProviderConfig,
        identity: &Identity,
    ) -> Result<(), OAuthError> {
        if provider.allowed_domains.is_empty() {
            return Ok(());
        }
        // An unverified email is a claim, not a fact; a domain allowlist that
        // trusted it would be trivially bypassable.
        let Some(email) = identity
            .email
            .as_deref()
            .filter(|_| identity.email_verified)
        else {
            return Err(OAuthError::Denied(
                "a verified email is required by the domain allowlist".into(),
            ));
        };
        let domain = email_domain(email).unwrap_or_default();
        let allowed = provider
            .allowed_domains
            .iter()
            .any(|candidate| candidate.eq_ignore_ascii_case(&domain));
        if allowed {
            Ok(())
        } else {
            Err(OAuthError::Denied(format!(
                "email domain '{domain}' is not in the allowed domains list"
            )))
        }
    }

    /// The role a brand-new provider user gets.
    ///
    /// Admin requires a verified email on the configured allowlist — never
    /// "first login wins", which would hand admin to whoever raced there.
    pub fn role_for_new_user(&self, identity: &Identity) -> crate::dashboard::auth::model::Role {
        use crate::dashboard::auth::model::Role;

        let Some(email) = identity
            .email
            .as_deref()
            .filter(|_| identity.email_verified)
        else {
            return Role::Viewer;
        };
        if self
            .config
            .admin_emails
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(email))
        {
            Role::Admin
        } else {
            Role::Viewer
        }
    }

    fn http(&self) -> &reqwest::Client {
        &self.http
    }
}

fn email_domain(email: &str) -> Option<String> {
    email
        .rsplit_once('@')
        .map(|(_, domain)| domain.to_ascii_lowercase())
}

/// Recover a poisoned cache lock instead of cascading the panic; the state
/// behind it is a plain map that is safe to keep using.
fn recover<T>(poisoned: std::sync::PoisonError<T>) -> T {
    poisoned.into_inner()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dashboard::auth::model::Role;
    use crate::dashboard::auth::oauth::config::ProviderKind;

    fn runtime(admin_emails: &[&str], allowed_domains: &[&str]) -> OAuthRuntime {
        OAuthRuntime::new(OAuthConfig {
            redirect_base_url: "https://ops.example.com".into(),
            password_auth_enabled: true,
            admin_emails: admin_emails.iter().map(|e| e.to_string()).collect(),
            providers: vec![ProviderConfig {
                slot: "google".into(),
                label: "Google".into(),
                kind: ProviderKind::Google,
                client_id: "id".into(),
                client_secret: "secret".into(),
                discovery_url: None,
                allowed_domains: allowed_domains.iter().map(|d| d.to_string()).collect(),
                allowed_orgs: vec![],
                github: crate::dashboard::auth::oauth::config::GitHubEndpoints::default(),
            }],
        })
    }

    fn identity(email: Option<&str>, verified: bool) -> Identity {
        Identity {
            slot: "google".into(),
            subject: "1234".into(),
            email: email.map(str::to_string),
            email_verified: verified,
            name: None,
        }
    }

    #[test]
    fn an_unknown_slot_is_not_configured() {
        let runtime = runtime(&[], &[]);
        assert!(matches!(
            runtime.provider("okta"),
            Err(OAuthError::NotConfigured(_))
        ));
        assert!(runtime.provider("google").is_ok());
    }

    #[test]
    fn the_domain_allowlist_requires_a_verified_email() {
        let runtime = runtime(&[], &["example.com"]);
        let provider = runtime.provider("google").expect("configured");

        runtime
            .check_allowlist(provider, &identity(Some("ops@example.com"), true))
            .expect("allowed domain");
        assert!(matches!(
            runtime.check_allowlist(provider, &identity(Some("ops@example.com"), false)),
            Err(OAuthError::Denied(_))
        ));
        assert!(matches!(
            runtime.check_allowlist(provider, &identity(Some("ops@evil.example"), true)),
            Err(OAuthError::Denied(_))
        ));
        assert!(matches!(
            runtime.check_allowlist(provider, &identity(None, true)),
            Err(OAuthError::Denied(_))
        ));
    }

    #[test]
    fn no_allowlist_admits_everyone() {
        let runtime = runtime(&[], &[]);
        let provider = runtime.provider("google").expect("configured");
        runtime
            .check_allowlist(provider, &identity(None, false))
            .expect("no allowlist configured");
    }

    #[test]
    fn admin_comes_only_from_a_verified_allowlisted_email() {
        let runtime = runtime(&["Ops@Example.com"], &[]);
        assert_eq!(
            runtime.role_for_new_user(&identity(Some("ops@example.com"), true)),
            Role::Admin,
            "the comparison is case-insensitive"
        );
        assert_eq!(
            runtime.role_for_new_user(&identity(Some("ops@example.com"), false)),
            Role::Viewer,
            "an unverified email must never grant admin"
        );
        assert_eq!(
            runtime.role_for_new_user(&identity(Some("someone@example.com"), true)),
            Role::Viewer
        );
        assert_eq!(
            runtime.role_for_new_user(&identity(None, true)),
            Role::Viewer
        );
    }

    #[test]
    fn the_listing_carries_no_secrets() {
        let listing = runtime(&[], &[]).listing();
        assert_eq!(listing.len(), 1);
        let encoded = serde_json::to_string(&listing).expect("serializes");
        assert!(!encoded.contains("secret"));
        assert!(encoded.contains("\"slot\":\"google\""));
    }
}
