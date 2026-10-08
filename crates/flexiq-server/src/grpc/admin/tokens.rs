//! `CreateToken`, `GetToken`, `ListTokens` and `RevokeToken`: a credential
//! managing credentials (#851).
//!
//! The dashboard's token routes stay behind a session; this is a second door,
//! for automation, and it is held to rules the session door does not need:
//!
//! 1. Every grant minted must be covered by one of the caller's own
//!    ([`Grants::first_uncovered`]) — a token never mints a wider one.
//! 2. The minted token expires no later than the caller. Refused, not
//!    shortened: otherwise a token could renew itself forever, and a silent
//!    clamp would hand back a credential the caller did not ask for.
//! 3. Everything is the caller's namespace; another's token reads as absent.
//! 4. Revoking needs the target's grants covered too, so a narrow `tokens`
//!    holder cannot revoke a broader token. A token may always revoke itself.
//!
//! The caller's own row is read fresh on every write rather than trusted from
//! the principal: its expiry is not on the principal, and a principal that is
//! not a stored token — no such authenticator exists today — has no row, so it
//! is refused rather than allowed to mint without a ceiling.

use chrono::{DateTime, SecondsFormat};
use flexiq_core::now_millis;
use tonic::{Response, Status};

use super::{convert, require, Scoped};
use crate::grpc::audit::TargetKind;
use crate::grpc::auth::Scope;
use crate::grpc::blocking::on_storage;
use crate::grpc::pb::admin as pb;
use crate::grpc::status::{reason, WireError};
use crate::tokens::model::DAY_MS;
use crate::tokens::{store, ApiToken, Grant, Grants, NewToken};

/// Mint a token in the caller's namespace, within the caller's grants and
/// lifetime.
pub(crate) async fn create(
    scoped: &Scoped,
    request: pb::CreateTokenRequest,
) -> Result<Response<pb::CreateTokenResponse>, Status> {
    require_door(scoped)?;
    let caller = caller(scoped).await?;

    let grants = Grants::parse_all(request.scopes.iter().map(String::as_str))
        .map_err(WireError::invalid_request)?;
    let minted = NewToken::new(
        &request.name,
        grants,
        &scoped.namespace_owned(),
        request.expire_days.map(i64::from),
        Some(format!("token:{}", caller.id)),
    )
    .map_err(WireError::invalid_request)?;
    if let Some(grant) = caller.scopes.first_uncovered(&minted.scopes) {
        return Err(uncovered(&grant, "grant").into());
    }
    let now = now_millis();
    if minted.expires_at(now) > caller.expires_at {
        return Err(outlives(
            &caller,
            minted.lifetime_days,
            request.expire_days.is_some(),
            now,
        )
        .into());
    }

    let (row, secret) = on_storage(scoped.storage(), move |storage| {
        store::create_at(storage, minted, now)
    })
    .await?;
    scoped.audit(TargetKind::Token, row.id.clone());
    Ok(Response::new(pb::CreateTokenResponse {
        token: Some(convert::api_token(row, now)),
        secret,
    }))
}

/// One token of the caller's namespace.
pub(crate) async fn get(
    scoped: &Scoped,
    request: pb::GetTokenRequest,
) -> Result<Response<pb::GetTokenResponse>, Status> {
    require_door(scoped)?;
    let id = require("token_id", request.token_id)?;
    scoped.audit(TargetKind::Token, id.clone());
    let token = find(scoped, &id).await?;
    Ok(Response::new(pb::GetTokenResponse {
        token: Some(convert::api_token(token, now_millis())),
    }))
}

/// Every token of the caller's namespace, newest first.
pub(crate) async fn list(scoped: &Scoped) -> Result<Response<pb::ListTokensResponse>, Status> {
    require_door(scoped)?;
    let namespace = scoped.namespace_owned();
    let tokens = on_storage(scoped.storage(), move |storage| {
        store::list(storage, Some(&namespace))
    })
    .await?;
    let now = now_millis();
    Ok(Response::new(pb::ListTokensResponse {
        tokens: tokens
            .into_iter()
            .map(|token| convert::api_token(token, now))
            .collect(),
    }))
}

/// Revoke a token the caller's grants cover, or the caller itself.
pub(crate) async fn revoke(
    scoped: &Scoped,
    request: pb::RevokeTokenRequest,
) -> Result<Response<pb::RevokeTokenResponse>, Status> {
    require_door(scoped)?;
    let id = require("token_id", request.token_id)?;
    scoped.audit(TargetKind::Token, id.clone());
    let caller = caller(scoped).await?;
    let target = find(scoped, &id).await?;
    // Grants never change after a mint, so checking the row read here holds
    // for the revoke below.
    if target.id != caller.id {
        if let Some(grant) = caller.scopes.first_uncovered(&target.scopes) {
            return Err(uncovered(&grant, "revoke a token carrying").into());
        }
    }

    let namespace = scoped.namespace_owned();
    let lookup = id.clone();
    let revoked = on_storage(scoped.storage(), move |storage| {
        if !store::revoke(storage, &lookup, Some(&namespace))? {
            return Ok(None);
        }
        store::get(storage, &lookup)
    })
    .await?
    // Also reached when the row vanished between the check and the write.
    .ok_or_else(|| not_found(&id))?;
    Ok(Response::new(pb::RevokeTokenResponse {
        token: Some(convert::api_token(revoked, now_millis())),
    }))
}

/// Refuse a call the layer let through on any door but `tokens`.
///
/// The gate already answers this; it is checked again here because these are
/// the methods that mint credentials, and a path misclassified onto `admin`
/// must not be enough to reach them.
fn require_door(scoped: &Scoped) -> Result<(), WireError> {
    if scoped.door() == Scope::Tokens {
        Ok(())
    } else {
        Err(WireError::scope_denied(Scope::Tokens.as_str()))
    }
}

/// The caller's own token row, read now: usable, and in this namespace.
///
/// Anything else is answered as the door answers a bad credential — it is one:
/// revoked or expired since the layer read it, or no stored token at all.
async fn caller(scoped: &Scoped) -> Result<ApiToken, Status> {
    let id = scoped.token_id().to_string();
    let found = on_storage(scoped.storage(), move |storage| store::get(storage, &id)).await?;
    let namespace = scoped.namespace_owned();
    found
        .filter(|token| token.namespace == namespace && token.is_usable(now_millis()))
        .ok_or_else(|| WireError::unauthenticated().into())
}

/// A token of the caller's namespace by id, or `TOKEN_NOT_FOUND` — the same
/// answer for another namespace's id as for one never minted.
async fn find(scoped: &Scoped, id: &str) -> Result<ApiToken, Status> {
    let lookup = id.to_string();
    let found = on_storage(scoped.storage(), move |storage| {
        store::get(storage, &lookup)
    })
    .await?;
    let namespace = scoped.namespace_owned();
    found
        .filter(|token| token.namespace == namespace)
        .ok_or_else(|| not_found(id))
}

fn not_found(id: &str) -> Status {
    WireError::not_found(reason::TOKEN_NOT_FOUND, "token", id).into()
}

/// The refusal for a grant the caller does not hold. `what` reads after
/// "cannot": "grant", or "revoke a token carrying".
fn uncovered(grant: &Grant, what: &str) -> WireError {
    WireError::not_covered(
        grant.scope.as_str(),
        format!(
            "this credential cannot {what} `{grant}`: none of its own grants covers it. \
             A token may only hand on, or take away, grants it holds itself."
        ),
    )
}

/// The refusal for a token that would outlive the caller, naming the latest
/// expiry it could have.
fn outlives(caller: &ApiToken, days: i64, explicit: bool, now: i64) -> WireError {
    let ceiling = rfc3339(caller.expires_at);
    let max_days = (caller.expires_at - now).div_euclid(DAY_MS);
    let advice = if max_days >= 1 {
        format!("ask for at most {max_days} days")
    } else {
        "this credential has less than a day left and can mint nothing; mint from a \
         longer-lived one"
            .to_string()
    };
    // Name the default, not an `expire_days` the caller never sent.
    let asked = if explicit {
        format!("expire_days {days}")
    } else {
        format!("a lifetime of {days} days (the default)")
    };
    WireError::invalid_request(format!(
        "{asked} would outlive this credential, which expires at \
         {ceiling}; a minted token must expire by then — {advice}"
    ))
}

/// Unix milliseconds as RFC 3339, or the number itself if out of range.
fn rfc3339(millis: i64) -> String {
    DateTime::from_timestamp_millis(millis).map_or_else(
        || millis.to_string(),
        |time| time.to_rfc3339_opts(SecondsFormat::Secs, true),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn token(expires_at: i64) -> ApiToken {
        let mut row = NewToken::new(
            "t",
            Grants::parse_all(["read"]).expect("valid"),
            "prod",
            None,
            None,
        )
        .expect("valid")
        .into_row_at("id".to_string(), "hash".to_string(), 0);
        row.expires_at = expires_at;
        row
    }

    #[test]
    fn an_outliving_mint_names_the_ceiling_and_the_days_left() {
        let caller = token(10 * DAY_MS + 5);
        let error = outlives(&caller, 90, true, 0);
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(
            error.message().contains("1970-01-11T00:00:00Z"),
            "{}",
            error.message()
        );
        assert!(
            error.message().contains("at most 10 days"),
            "{}",
            error.message()
        );
    }

    #[test]
    fn an_omitted_expiry_is_reported_as_the_default() {
        let error = outlives(&token(10 * DAY_MS + 5), 90, false, 0);
        assert!(
            error
                .message()
                .contains("a lifetime of 90 days (the default)"),
            "{}",
            error.message()
        );
        assert!(
            !error.message().contains("expire_days"),
            "{}",
            error.message()
        );
    }

    #[test]
    fn a_caller_with_under_a_day_left_is_told_it_can_mint_nothing() {
        let error = outlives(&token(DAY_MS - 1), 1, true, 0);
        assert!(
            error.message().contains("less than a day"),
            "{}",
            error.message()
        );
    }

    #[test]
    fn an_uncovered_grant_is_scope_denied_naming_its_scope() {
        let grant = Grant::parse("produce:queue=billing").expect("valid");
        let error = uncovered(&grant, "grant");
        assert_eq!(error.code(), tonic::Code::PermissionDenied);
        assert_eq!(error.reason(), reason::SCOPE_DENIED);
        assert!(error.message().contains("produce:queue=billing"));
    }
}
