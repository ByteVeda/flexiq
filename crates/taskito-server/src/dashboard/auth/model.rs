//! Persisted auth types.
//!
//! Users and sessions live in the settings store under `auth:` keys — the same
//! layout every SDK dashboard uses, so a session minted by one is accepted by
//! all of them. That interoperability is why the field names and the password
//! encoding here are a contract rather than an implementation detail.

use serde::{Deserialize, Serialize};

/// Settings key holding the user map.
pub const USERS_KEY: &str = "auth:users";
/// Settings-key prefix for a session, suffixed by its token.
pub const SESSION_PREFIX: &str = "auth:session:";

/// How long a session stays valid.
pub const SESSION_TTL_SECONDS: i64 = 24 * 60 * 60;

/// Prefix marking a user who authenticates through a provider and has no
/// password at all, so a password attempt can be rejected without hashing.
pub const OAUTH_PASSWORD_MARKER: &str = "oauth:";

const USERNAME_MAX_LEN: usize = 64;
const PASSWORD_MIN_LEN: usize = 8;
const PASSWORD_MAX_LEN: usize = 256;

/// A dashboard user's access level.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    /// Full read/write access.
    Admin,
    /// Read-only, plus their own account.
    Viewer,
}

impl Role {
    /// Wire form, as stored.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Viewer => "viewer",
        }
    }

    /// Parse a stored or submitted role, failing closed on anything else.
    pub fn parse_or_viewer(value: Option<&str>) -> Self {
        match value {
            Some("admin") => Self::Admin,
            _ => Self::Viewer,
        }
    }
}

/// A persisted user. `password_hash` never leaves the store.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    /// Login name; the map key, repeated here for convenience.
    #[serde(skip)]
    pub username: String,
    /// Encoded PBKDF2 hash, or the OAuth marker.
    pub password_hash: String,
    /// Access level.
    #[serde(default = "viewer")]
    pub role: Role,
    /// Unix milliseconds.
    #[serde(default)]
    pub created_at: i64,
    /// Unix milliseconds.
    #[serde(default)]
    pub last_login_at: Option<i64>,
    /// Set for provider logins.
    #[serde(default)]
    pub email: Option<String>,
    /// Set for provider logins.
    #[serde(default)]
    pub display_name: Option<String>,
}

fn viewer() -> Role {
    Role::Viewer
}

impl User {
    /// Whether this user authenticates through a provider.
    pub fn is_oauth(&self) -> bool {
        self.password_hash.starts_with(OAUTH_PASSWORD_MARKER)
    }

    /// The user as the API returns it — never the hash.
    pub fn to_api_json(&self) -> serde_json::Value {
        serde_json::json!({
            "username": self.username,
            "role": self.role.as_str(),
            "created_at": self.created_at,
            "last_login_at": self.last_login_at,
        })
    }
}

/// An active session. The token is the settings-key suffix, so it is not
/// stored inside the document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Session {
    /// Opaque session token, carried in the cookie.
    #[serde(skip)]
    pub token: String,
    /// Who the session belongs to.
    pub username: String,
    /// Their role when the session was created.
    #[serde(default = "viewer")]
    pub role: Role,
    /// Unix **seconds**, matching what the SDK dashboards store.
    #[serde(default)]
    pub created_at: i64,
    /// Unix seconds.
    pub expires_at: i64,
    /// Double-submit CSRF token for this session.
    pub csrf_token: String,
}

impl Session {
    /// Whether the session has expired at `now` (Unix seconds).
    pub fn is_expired(&self, now: i64) -> bool {
        now >= self.expires_at
    }

    /// The session as the API returns it — the token stays in the cookie.
    pub fn to_api_json(&self) -> serde_json::Value {
        serde_json::json!({
            "username": self.username,
            "role": self.role.as_str(),
            "expires_at": self.expires_at,
            "csrf_token": self.csrf_token,
        })
    }
}

/// Reject a username that could not round-trip through a settings key or a log
/// line.
pub fn validate_username(username: &str) -> Result<(), String> {
    if username.is_empty() {
        return Err("username must not be empty".into());
    }
    if username.len() > USERNAME_MAX_LEN {
        return Err(format!("username must be <= {USERNAME_MAX_LEN} chars"));
    }
    if !username
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-')
    {
        return Err("username may only contain letters, digits, '.', '_', or '-'".into());
    }
    Ok(())
}

/// Enforce the password length bounds the SDK dashboards enforce.
pub fn validate_password(password: &str) -> Result<(), String> {
    if password.len() < PASSWORD_MIN_LEN {
        return Err(format!("password must be >= {PASSWORD_MIN_LEN} chars"));
    }
    if password.len() > PASSWORD_MAX_LEN {
        return Err(format!("password must be <= {PASSWORD_MAX_LEN} chars"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roles_fail_closed_to_viewer() {
        assert_eq!(Role::parse_or_viewer(Some("admin")), Role::Admin);
        assert_eq!(Role::parse_or_viewer(Some("superuser")), Role::Viewer);
        assert_eq!(Role::parse_or_viewer(None), Role::Viewer);
    }

    #[test]
    fn roles_serialize_to_their_stored_form() {
        assert_eq!(
            serde_json::to_string(&Role::Admin).expect("serializes"),
            "\"admin\""
        );
    }

    #[test]
    fn usernames_are_restricted_to_safe_characters() {
        validate_username("ops.team_1-a").expect("allowed");
        assert!(validate_username("").is_err());
        assert!(validate_username("has space").is_err());
        assert!(validate_username("bad/slash").is_err());
        assert!(validate_username(&"x".repeat(USERNAME_MAX_LEN + 1)).is_err());
    }

    #[test]
    fn password_bounds_are_enforced() {
        validate_password("longenough").expect("allowed");
        assert!(validate_password("short").is_err());
        assert!(validate_password(&"x".repeat(PASSWORD_MAX_LEN + 1)).is_err());
    }

    #[test]
    fn an_oauth_user_is_recognisable_from_its_hash() {
        let user = User {
            username: "google:1234".into(),
            password_hash: format!("{OAUTH_PASSWORD_MARKER}google"),
            role: Role::Viewer,
            created_at: 0,
            last_login_at: None,
            email: None,
            display_name: None,
        };
        assert!(user.is_oauth());
        assert!(user.to_api_json().get("password_hash").is_none());
    }
}
