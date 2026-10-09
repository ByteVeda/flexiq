//! `fq tokens` — mint, read and revoke API tokens.
//!
//! The `tokens` scope, which no other scope implies. A mint is bounded by the
//! calling token: the grants must be covered by its own and the expiry may not
//! pass its own, and the server refuses rather than trims.

use std::io::Write;

use anyhow::{Context, Result};

use super::{emit, refused};
use crate::cli::{TokenCreateArgs, TokenIdArgs, TokensCommand};
use crate::connect::AdminClient;
use crate::output;
use crate::output::admin::{
    create_token_json, list_tokens_json, token_envelope_json, token_row, TOKEN_COLUMNS,
};
use crate::pb::admin as pb;
use crate::safe::escape;

/// Dispatch the four verbs.
pub async fn run(client: &mut AdminClient, command: &TokensCommand, json: bool) -> Result<()> {
    match command {
        TokensCommand::Create(args) => create(client, args, json).await,
        TokensCommand::List => list(client, json).await,
        TokensCommand::Show(args) => show(client, args, json).await,
        TokensCommand::Revoke(args) => revoke(client, args, json).await,
    }
}

/// The mint request. Grants go as typed: the server owns the grammar and the
/// covering rule, so a second parser here could only drift from it.
pub fn create_request(args: &TokenCreateArgs) -> pb::CreateTokenRequest {
    pb::CreateTokenRequest {
        name: args.name.clone(),
        scopes: args.scopes.clone(),
        expire_days: args.expire_days,
    }
}

/// `fq tokens create`.
///
/// Without `--json` the secret is the whole of stdout, so
/// `SECRET=$(fq tokens create ...)` captures it, and the summary goes to
/// stderr. The summary is written first and carries the id: if the stdout
/// write fails the token is already stored, and the id is the only way left to
/// revoke a credential nobody got to read.
async fn create(client: &mut AdminClient, args: &TokenCreateArgs, json: bool) -> Result<()> {
    let response = client
        .create_token(create_request(args))
        .await
        .map_err(refused)?
        .into_inner();
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&create_token_json(&response))?
        );
        return Ok(());
    }

    let id = response
        .token
        .as_ref()
        .map_or("?", |token| token.id.as_str());
    let name = response
        .token
        .as_ref()
        .map_or("", |token| token.name.as_str());
    eprintln!(
        "Minted '{}' (id {}). This is the only time the token is shown; \
         only its hash is kept.",
        escape(name),
        escape(id),
    );
    let mut out = std::io::stdout().lock();
    writeln!(out, "{}", response.secret)
        .and_then(|()| out.flush())
        .with_context(|| {
            format!(
                "the token was minted but could not be written to stdout. Revoke it \
                 with `fq tokens revoke {}` and mint another.",
                escape(id)
            )
        })
}

/// `fq tokens list`.
async fn list(client: &mut AdminClient, json: bool) -> Result<()> {
    let response = client
        .list_tokens(pb::ListTokensRequest {})
        .await
        .map_err(refused)?
        .into_inner();
    emit(
        json,
        || list_tokens_json(&response),
        || {
            let rows: Vec<_> = response.tokens.iter().map(token_row).collect();
            output::table(&TOKEN_COLUMNS, &rows)
        },
    )
}

/// `fq tokens show`.
async fn show(client: &mut AdminClient, args: &TokenIdArgs, json: bool) -> Result<()> {
    let response = client
        .get_token(pb::GetTokenRequest {
            token_id: args.id.clone(),
        })
        .await
        .map_err(refused)?
        .into_inner();
    print_token(response.token.as_ref(), json)
}

/// `fq tokens revoke`. Revoking a revoked token answers it unchanged.
async fn revoke(client: &mut AdminClient, args: &TokenIdArgs, json: bool) -> Result<()> {
    let response = client
        .revoke_token(pb::RevokeTokenRequest {
            token_id: args.id.clone(),
        })
        .await
        .map_err(refused)?
        .into_inner();
    print_token(response.token.as_ref(), json)
}

fn print_token(token: Option<&pb::ApiToken>, json: bool) -> Result<()> {
    emit(
        json,
        || token_envelope_json(token),
        || {
            let rows = token
                .map(|token| vec![token_row(token)])
                .unwrap_or_default();
            output::table(&TOKEN_COLUMNS, &rows)
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_flag_reaches_the_wire() {
        let request = create_request(&TokenCreateArgs {
            name: "ci".into(),
            scopes: vec!["read".into(), "produce:queue=mail-*".into()],
            expire_days: Some(30),
        });
        assert_eq!(request.name, "ci");
        assert_eq!(request.scopes, ["read", "produce:queue=mail-*"]);
        assert_eq!(request.expire_days, Some(30));
    }

    /// An omitted `--expire-days` is the server's default, not a zero.
    #[test]
    fn an_omitted_lifetime_is_unset() {
        let request = create_request(&TokenCreateArgs {
            name: "ci".into(),
            scopes: vec!["read".into()],
            expire_days: None,
        });
        assert_eq!(request.expire_days, None);
    }
}
