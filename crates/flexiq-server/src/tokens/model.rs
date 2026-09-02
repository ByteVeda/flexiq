//! The stored row, the states it can be in, and the rules a mint must satisfy.

use flexiq_core::now_millis;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::scope::ScopeSet;

/// Milliseconds in a day, the unit every lifetime here is expressed in.
const DAY_MS: i64 = 24 * 60 * 60 * 1000;

/// How long a token lives when the operator does not say.
pub const DEFAULT_LIFETIME_DAYS: i64 = 90;

/// The longest life a token may be given.
///
/// A credential with no maximum lifetime is a permanent credential with extra
/// steps, so there is no unlimited option to pick. A year is the cap because it
/// is a rotation period an operator already has a calendar entry for; Temporal
/// Cloud draws the same line at two, and the argument for having a line at all
/// is the same one.
pub const MAX_LIFETIME_DAYS: i64 = 365;

/// Days remaining at which a token in use is warned about, in the order they
/// are crossed.
pub const EXPIRY_WARNING_DAYS: [i64; 3] = [30, 20, 10];

/// The longest a token's name may be.
const MAX_NAME_LEN: usize = 64;

/// One stored token.
///
/// `hash` is in this struct because the store needs it; it is never in the
/// struct's JSON *response* shape — see [`ApiToken::to_api_json`], the one
/// function the routes use.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiToken {
    /// Public identifier, also the settings key's suffix.
    pub id: String,
    /// Operator-chosen label, so a listing says which client holds it.
    pub name: String,
    /// `sha256(secret)`, hex. The token itself is not recoverable from it.
    pub hash: String,
    /// The packages this token may call.
    pub scopes: ScopeSet,
    /// The one namespace every call on this credential is scoped to. Never
    /// empty: the NULL namespace is not addressable over the wire (D11).
    pub namespace: String,
    /// Unix milliseconds.
    pub created_at: i64,
    /// Who minted it — a dashboard username, or the command line.
    #[serde(default)]
    pub created_by: Option<String>,
    /// Unix milliseconds, coalesced: written at most once a minute per token.
    #[serde(default)]
    pub last_used_at: Option<i64>,
    /// Unix milliseconds. Mandatory, by [`MAX_LIFETIME_DAYS`].
    pub expires_at: i64,
    /// Unix milliseconds, set once and never cleared.
    #[serde(default)]
    pub revoked_at: Option<i64>,
}

/// What a token is, right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenStatus {
    /// Usable.
    Active,
    /// Past its expiry.
    Expired,
    /// Revoked by an operator.
    Revoked,
}

impl TokenStatus {
    /// The name the API and the command line print.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Expired => "expired",
            Self::Revoked => "revoked",
        }
    }
}

impl ApiToken {
    /// What this token is at `now`.
    ///
    /// Revocation wins over expiry: an operator who revoked a token wants to see
    /// that they did, not to be told it timed out on its own.
    pub fn status(&self, now: i64) -> TokenStatus {
        if self.revoked_at.is_some() {
            TokenStatus::Revoked
        } else if now >= self.expires_at {
            TokenStatus::Expired
        } else {
            TokenStatus::Active
        }
    }

    /// Whether a call presenting this token may proceed.
    pub fn is_usable(&self, now: i64) -> bool {
        self.status(now) == TokenStatus::Active
    }

    /// Whole days until expiry, negative once past it.
    pub fn days_remaining(&self, now: i64) -> i64 {
        (self.expires_at - now).div_euclid(DAY_MS)
    }

    /// The row as the API returns it.
    ///
    /// The hash is not in it. It is not the token and cannot be turned back into
    /// one, but it is the only stored material an attacker could compare a
    /// guess against, and a listing has no use for it.
    pub fn to_api_json(&self, now: i64) -> Value {
        json!({
            "id": self.id,
            "name": self.name,
            "scopes": self.scopes,
            "namespace": self.namespace,
            "created_at": self.created_at,
            "created_by": self.created_by,
            "last_used_at": self.last_used_at,
            "expires_at": self.expires_at,
            "revoked_at": self.revoked_at,
            "status": self.status(now).as_str(),
        })
    }
}

/// A mint request that has already been checked.
///
/// The store takes one of these rather than the raw fields, so there is no way
/// to reach a write with an unvalidated name, an empty scope set, a lifetime
/// past the cap, or the ambiguous namespace.
#[derive(Debug, Clone)]
pub struct NewToken {
    /// Operator-chosen label.
    pub name: String,
    /// What the token may call. Never empty.
    pub scopes: ScopeSet,
    /// The namespace it is bound to. Never empty.
    pub namespace: String,
    /// Days from now until it expires.
    pub lifetime_days: i64,
    /// Who is minting it.
    pub created_by: Option<String>,
}

impl NewToken {
    /// Check a mint request, or say what is wrong with it.
    ///
    /// The error strings are user-facing on two surfaces — a 400 from the
    /// dashboard and a message on the command line — so each says what to do,
    /// not just what was refused.
    pub fn new(
        name: &str,
        scopes: ScopeSet,
        namespace: &str,
        lifetime_days: Option<i64>,
        created_by: Option<String>,
    ) -> Result<Self, String> {
        let name = name.trim();
        if name.is_empty() {
            return Err("name is required — a token nobody can identify cannot be \
                        revoked with confidence"
                .to_string());
        }
        if name.chars().count() > MAX_NAME_LEN {
            return Err(format!("name must be at most {MAX_NAME_LEN} characters"));
        }
        if name.chars().any(char::is_control) {
            return Err("name must not contain control characters".to_string());
        }
        if scopes.is_empty() {
            return Err(format!(
                "at least one scope is required — a token granting nothing can \
                 open no door. Available: {}",
                super::scope::Scope::names()
            ));
        }
        if namespace.is_empty() {
            return Err("namespace is required".to_string());
        }
        let lifetime_days = lifetime_days.unwrap_or(DEFAULT_LIFETIME_DAYS);
        if lifetime_days < 1 {
            return Err("expires_in_days must be at least 1".to_string());
        }
        if lifetime_days > MAX_LIFETIME_DAYS {
            return Err(format!(
                "expires_in_days must be at most {MAX_LIFETIME_DAYS} — a credential \
                 with no maximum lifetime is a permanent one with extra steps"
            ));
        }
        Ok(Self {
            name: name.to_string(),
            scopes,
            namespace: namespace.to_string(),
            lifetime_days,
            created_by,
        })
    }

    /// The row this request becomes, given the material [`super::secret::mint`]
    /// generated.
    pub fn into_row(self, id: String, hash: String) -> ApiToken {
        let created_at = now_millis();
        ApiToken {
            id,
            name: self.name,
            hash,
            scopes: self.scopes,
            namespace: self.namespace,
            created_at,
            created_by: self.created_by,
            last_used_at: None,
            expires_at: created_at + self.lifetime_days * DAY_MS,
            revoked_at: None,
        }
    }
}

/// The namespace a mint is allowed to bind to.
///
/// Design doc §5.4: one process runs one scheduler on one namespace, so a token
/// minted for a namespace this process does not serve would be a working enqueue
/// path into a queue nothing ever polls — a success response and no work. Both
/// mint surfaces, the dashboard route and the command line, resolve the
/// namespace through here so neither can be the one that forgets.
///
/// `requested` is accepted and validated rather than ignored: the field exists
/// so that multi-namespace credentials are an additive change later (§12), and
/// refusing a mismatch loudly is worth more than silently substituting the right
/// answer.
pub fn mint_namespace(server: Option<&str>, requested: Option<&str>) -> Result<String, String> {
    let server = server.filter(|name| !name.is_empty()).ok_or(
        "this process serves no namespace, so it cannot mint a credential for one. \
         Set FLEXIQ_NAMESPACE to the namespace your producers enqueue into — an \
         unset namespace means 'every namespace' to a read and 'only the \
         unnamespaced rows' to a dequeue, and neither is addressable over gRPC.",
    )?;
    match requested.filter(|name| !name.is_empty()) {
        Some(requested) if requested != server => Err(format!(
            "cannot mint a token for namespace '{requested}': this process serves \
             '{server}' and nothing dequeues '{requested}' here, so work enqueued \
             with it would sit pending forever. Mint it on a server running that \
             namespace."
        )),
        _ => Ok(server.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokens::scope::Scope;

    fn request(lifetime: Option<i64>) -> Result<NewToken, String> {
        NewToken::new(
            "ci",
            ScopeSet::of(&[Scope::Produce]),
            "prod",
            lifetime,
            None,
        )
    }

    fn row() -> ApiToken {
        request(None)
            .expect("valid")
            .into_row("abc".to_string(), "hash".to_string())
    }

    #[test]
    fn a_fresh_token_is_active_and_expires_at_the_default() {
        let token = row();
        assert_eq!(token.status(token.created_at), TokenStatus::Active);
        assert!(token.is_usable(token.created_at));
        assert_eq!(
            token.expires_at - token.created_at,
            DEFAULT_LIFETIME_DAYS * DAY_MS
        );
    }

    #[test]
    fn expiry_is_inclusive_of_the_instant_it_names() {
        let token = row();
        assert!(token.is_usable(token.expires_at - 1));
        assert!(
            !token.is_usable(token.expires_at),
            "a token is not usable at the millisecond it expires"
        );
        assert_eq!(token.status(token.expires_at), TokenStatus::Expired);
    }

    #[test]
    fn revocation_wins_over_expiry() {
        let mut token = row();
        token.revoked_at = Some(token.created_at);
        assert_eq!(
            token.status(token.expires_at + DAY_MS),
            TokenStatus::Revoked
        );
        assert!(!token.is_usable(token.created_at));
    }

    #[test]
    fn days_remaining_counts_down_and_goes_negative() {
        let token = row();
        assert_eq!(
            token.days_remaining(token.created_at),
            DEFAULT_LIFETIME_DAYS
        );
        assert_eq!(
            token.days_remaining(token.expires_at - 10 * DAY_MS),
            10,
            "the thresholds are read off this"
        );
        assert!(token.days_remaining(token.expires_at + DAY_MS) < 0);
    }

    #[test]
    fn the_api_shape_never_carries_the_hash() {
        let token = row();
        let rendered = token.to_api_json(token.created_at);
        assert!(rendered.get("hash").is_none());
        assert!(!rendered.to_string().contains("hash"));
        assert_eq!(rendered["status"], "active");
        assert_eq!(rendered["scopes"], json!(["produce"]));
    }

    #[test]
    fn a_lifetime_past_the_cap_is_refused_rather_than_clamped() {
        let error = request(Some(MAX_LIFETIME_DAYS + 1)).expect_err("must refuse");
        assert!(error.contains(&MAX_LIFETIME_DAYS.to_string()), "{error}");
        assert!(request(Some(MAX_LIFETIME_DAYS)).is_ok());
        assert!(request(Some(0)).is_err());
        assert!(request(Some(-1)).is_err());
    }

    #[test]
    fn a_token_must_be_named_and_must_grant_something() {
        for name in ["", "   ", "with\nnewline"] {
            assert!(
                NewToken::new(name, ScopeSet::ALL, "prod", None, None).is_err(),
                "name: {name:?}"
            );
        }
        assert!(NewToken::new(
            &"x".repeat(MAX_NAME_LEN + 1),
            ScopeSet::ALL,
            "prod",
            None,
            None
        )
        .is_err());
        let error = NewToken::new("ci", ScopeSet::NONE, "prod", None, None)
            .expect_err("a scopeless token opens nothing");
        assert!(
            error.contains("produce"),
            "the error must list what is available: {error}"
        );
    }

    #[test]
    fn a_name_is_stored_trimmed() {
        let token = NewToken::new("  ci  ", ScopeSet::ALL, "prod", None, None).expect("valid");
        assert_eq!(token.name, "ci");
    }

    #[test]
    fn a_mint_binds_the_namespace_the_process_serves() {
        assert_eq!(mint_namespace(Some("prod"), None), Ok("prod".to_string()));
        assert_eq!(
            mint_namespace(Some("prod"), Some("prod")),
            Ok("prod".to_string())
        );
    }

    /// §11: "a token is mintable for a namespace the process does not serve".
    #[test]
    fn a_mint_for_another_namespace_is_refused() {
        let error = mint_namespace(Some("prod"), Some("staging")).expect_err("must refuse");
        assert!(
            error.contains("staging") && error.contains("prod"),
            "{error}"
        );
    }

    /// D11: there is no way to mint against the ambiguous namespace.
    #[test]
    fn a_process_with_no_namespace_cannot_mint_at_all() {
        for server in [None, Some("")] {
            let error = mint_namespace(server, Some("prod")).expect_err("must refuse");
            assert!(error.contains("FLEXIQ_NAMESPACE"), "{error}");
        }
    }
}
