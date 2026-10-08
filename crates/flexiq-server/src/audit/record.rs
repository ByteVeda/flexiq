//! One action, as the records it leaves — whichever surface took it.

use axum::http::StatusCode;
use flexiq_core::job::now_millis;
use flexiq_core::storage::records::{AUDIT_ACCESS_READ, AUDIT_ACCESS_WRITE};
use flexiq_core::AuditRecord;

/// What an audited action does to the namespace. Stored as its
/// [`Self::as_str`] form; reads keep a shorter window (#1018).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// It changes state. Always recorded.
    Write,
    /// It only looks. Recorded when reads are turned on (#993).
    Read,
}

impl Access {
    /// The stored spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Write => AUDIT_ACCESS_WRITE,
            Self::Read => AUDIT_ACCESS_READ,
        }
    }
}

/// Who a record's `token_id` names. Stored as its [`Self::as_str`] form,
/// which is the `principal_kind` a listing filters on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrincipalKind {
    /// A gRPC token, named by its public id.
    Token,
    /// A dashboard user, named by their username.
    User,
    /// `flexiq-server token`, run by whoever can reach the database.
    Cli,
    /// A dashboard with auth off: what changed is known, who is not.
    Anonymous,
}

impl PrincipalKind {
    /// The stored spelling.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Token => "token",
            Self::User => "user",
            Self::Cli => "cli",
            Self::Anonymous => "anonymous",
        }
    }
}

/// Who took an action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Actor {
    /// What `id` is.
    pub kind: PrincipalKind,
    /// The credential's public id; empty for a kind that has none.
    pub id: String,
    /// The credential's name — a token's label, a username.
    pub name: String,
}

impl Actor {
    /// A token, by its public id and name.
    pub fn token(id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            kind: PrincipalKind::Token,
            id: id.into(),
            name: name.into(),
        }
    }

    /// A dashboard user. The username is both the id and the name.
    pub fn user(username: impl Into<String>) -> Self {
        let username = username.into();
        Self {
            kind: PrincipalKind::User,
            id: username.clone(),
            name: username,
        }
    }

    /// The token command line. It has no identity of its own to record: the
    /// process proves only that it could open the database.
    pub fn cli() -> Self {
        Self::unnamed(PrincipalKind::Cli)
    }

    /// A dashboard request with auth off.
    pub fn anonymous() -> Self {
        Self::unnamed(PrincipalKind::Anonymous)
    }

    fn unnamed(kind: PrincipalKind) -> Self {
        Self {
            kind,
            id: String::new(),
            name: kind.as_str().to_string(),
        }
    }
}

/// The records one action leaves: one per `(kind, id)` target, or one with
/// no target when it named none — a refusal before the target was known, or
/// an action on nothing in particular.
pub fn records(
    namespace: &str,
    actor: &Actor,
    access: Access,
    operation: &str,
    targets: Vec<(String, String)>,
    outcome: &str,
) -> Vec<AuditRecord> {
    let at_ms = now_millis();
    let record = |target: Option<(String, String)>| {
        let (target_kind, target) = target.unzip();
        AuditRecord {
            id: uuid::Uuid::now_v7().to_string(),
            namespace: namespace.to_string(),
            at_ms,
            principal_kind: actor.kind.as_str().to_string(),
            token_id: actor.id.clone(),
            principal: actor.name.clone(),
            operation: operation.to_string(),
            target_kind,
            target,
            outcome: outcome.to_string(),
            access: access.as_str().to_string(),
        }
    };
    if targets.is_empty() {
        return vec![record(None)];
    }
    targets.into_iter().map(|t| record(Some(t))).collect()
}

/// The `google.rpc.Code` name an HTTP answer stands for, so a dashboard
/// record's outcome reads like a gRPC one and one filter covers both.
pub fn outcome_of(status: StatusCode) -> &'static str {
    use StatusCode as S;
    match status {
        s if s.is_success() => "OK",
        S::BAD_REQUEST => "INVALID_ARGUMENT",
        S::UNAUTHORIZED => "UNAUTHENTICATED",
        S::FORBIDDEN => "PERMISSION_DENIED",
        S::NOT_FOUND => "NOT_FOUND",
        S::CONFLICT => "ABORTED",
        S::TOO_MANY_REQUESTS => "RESOURCE_EXHAUSTED",
        S::NOT_IMPLEMENTED => "UNIMPLEMENTED",
        S::SERVICE_UNAVAILABLE => "UNAVAILABLE",
        S::GATEWAY_TIMEOUT => "DEADLINE_EXCEEDED",
        s if s.is_client_error() => "FAILED_PRECONDITION",
        s if s.is_server_error() => "INTERNAL",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn one_record_per_target_or_one_without() {
        let actor = Actor::user("alice");
        let targets = vec![
            ("topic".to_string(), "orders".to_string()),
            ("subscription".to_string(), "audit".to_string()),
        ];
        let records = records("prod", &actor, Access::Write, "op", targets, "OK");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].principal_kind, "user");
        assert_eq!(records[0].access, "write");
        assert_eq!(records[0].token_id, "alice");
        assert_eq!(records[0].principal, "alice");
        assert_eq!(records[1].target_kind.as_deref(), Some("subscription"));
        assert_ne!(records[0].id, records[1].id);

        let untargeted = super::records("prod", &actor, Access::Read, "op", Vec::new(), "OK");
        assert_eq!(untargeted.len(), 1);
        assert_eq!(untargeted[0].target_kind, None);
        assert_eq!(untargeted[0].access, "read");
    }

    #[test]
    fn unnamed_actors_record_their_kind_as_the_name() {
        assert_eq!(Actor::cli().name, "cli");
        assert_eq!(Actor::cli().id, "");
        assert_eq!(Actor::anonymous().kind.as_str(), "anonymous");
    }

    #[test]
    fn http_answers_read_as_grpc_code_names() {
        use StatusCode as S;
        for (status, code) in [
            (S::OK, "OK"),
            (S::NO_CONTENT, "OK"),
            (S::BAD_REQUEST, "INVALID_ARGUMENT"),
            (S::UNAUTHORIZED, "UNAUTHENTICATED"),
            (S::FORBIDDEN, "PERMISSION_DENIED"),
            (S::NOT_FOUND, "NOT_FOUND"),
            (S::CONFLICT, "ABORTED"),
            (S::TOO_MANY_REQUESTS, "RESOURCE_EXHAUSTED"),
            (S::SERVICE_UNAVAILABLE, "UNAVAILABLE"),
            (S::PAYLOAD_TOO_LARGE, "FAILED_PRECONDITION"),
            (S::INTERNAL_SERVER_ERROR, "INTERNAL"),
            (S::FOUND, "UNKNOWN"),
        ] {
            assert_eq!(outcome_of(status), code, "{status}");
        }
    }
}
