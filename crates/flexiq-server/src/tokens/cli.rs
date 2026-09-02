//! `flexiq-server token` — minting, listing and revoking from a shell.
//!
//! The dashboard can do all three, but requiring it would mean every gRPC
//! deployment also had to run and expose a dashboard just to provision its first
//! credential. This is the path a `kubectl exec` takes, and it is the same path
//! the operator of a gRPC-only pod already has.
//!
//! It configures itself from the same environment the server reads —
//! `FLEXIQ_DSN`, `FLEXIQ_BACKEND`, `FLEXIQ_NAMESPACE` — but not through
//! [`Config`](crate::config::Config), which requires at least one *role* to be
//! enabled. Provisioning a credential is not a role.

use std::io::Write;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand, ValueEnum};

use flexiq_core::{now_millis, StorageBackend};

use super::model::{mint_namespace, NewToken};
use super::scope::{Scope, ScopeSet};
use super::store;
use crate::config::{flag, value, Env};

/// Managing the credentials the gRPC door accepts.
#[derive(Debug, Args)]
pub struct TokenCommand {
    /// What to do.
    #[command(subcommand)]
    action: Action,
}

/// The three things an operator does to a token.
#[derive(Debug, Subcommand)]
enum Action {
    /// Mint a token and print it once.
    Create {
        /// Label shown in listings, so a credential can be told from another.
        #[arg(long)]
        name: String,
        /// A door this token may open. Repeat for more than one.
        #[arg(long = "scope", value_enum, required = true)]
        scopes: Vec<ScopeArg>,
        /// Days until it expires.
        #[arg(long, default_value_t = super::model::DEFAULT_LIFETIME_DAYS)]
        expires_in_days: i64,
    },
    /// List the tokens this namespace has.
    List {
        /// Include every namespace's tokens, not just this process's.
        #[arg(long)]
        all_namespaces: bool,
    },
    /// Revoke a token by its id. It stops working on the next call.
    Revoke {
        /// The id a listing shows, and the part of the token before the dot.
        id: String,
    },
}

/// A scope, as clap spells it on the command line.
#[derive(Debug, Clone, Copy, ValueEnum)]
enum ScopeArg {
    /// `flexiq.v1` — submit, read and cancel work.
    Produce,
    /// `flexiq.executor.v1` — claim work and report on it.
    Execute,
}

impl From<ScopeArg> for Scope {
    fn from(arg: ScopeArg) -> Self {
        match arg {
            ScopeArg::Produce => Self::Produce,
            ScopeArg::Execute => Self::Execute,
        }
    }
}

/// Run the subcommand against the configured database.
pub fn run(command: TokenCommand) -> Result<()> {
    let env: Env = std::env::vars().collect();
    let namespace = value(&env, "FLEXIQ_NAMESPACE");
    let storage = open(&env, namespace.clone())?;

    match command.action {
        Action::Create {
            name,
            scopes,
            expires_in_days,
        } => create(
            &storage,
            namespace.as_deref(),
            &name,
            &scopes,
            expires_in_days,
        ),
        Action::List { all_namespaces } => {
            list(&storage, namespace.as_deref().filter(|_| !all_namespaces))
        }
        Action::Revoke { id } => revoke(&storage, &id),
    }
}

/// Open the same storage the server would, without requiring a role.
fn open(env: &Env, namespace: Option<String>) -> Result<StorageBackend> {
    let dsn = value(env, "FLEXIQ_DSN").context(
        "FLEXIQ_DSN is required — point it at the same database the gRPC server \
         reads, or the token will be minted somewhere nothing checks it",
    )?;
    Ok(crate::config::backend::open(
        &dsn,
        value(env, "FLEXIQ_BACKEND").as_deref(),
        namespace,
        flag(env, "FLEXIQ_AUTO_MIGRATE", true),
    )?
    .storage)
}

/// Mint one, and print it the only time it can be printed.
fn create(
    storage: &StorageBackend,
    namespace: Option<&str>,
    name: &str,
    scopes: &[ScopeArg],
    expires_in_days: i64,
) -> Result<()> {
    // The namespace comes from the process, never from an argument: a token
    // minted for a namespace this deployment does not schedule would accept
    // enqueues nothing ever dequeues (design doc §5.4).
    let namespace = mint_namespace(namespace, None).map_err(|error| anyhow::anyhow!(error))?;
    let scopes = ScopeSet::of(&scopes.iter().copied().map(Scope::from).collect::<Vec<_>>());
    let request = NewToken::new(
        name,
        scopes,
        &namespace,
        Some(expires_in_days),
        Some("cli".to_string()),
    )
    .map_err(|error| anyhow::anyhow!(error))?;

    let (row, plaintext) = store::create(storage, request)?;

    // The summary goes first, and it carries the id. If the write below fails —
    // a closed pipe, a full disk — the token is already stored, and the id is
    // the only thing that lets the operator revoke a credential they never got
    // to read.
    eprintln!(
        "Minted '{}' (id {}) for namespace '{}', scopes {}, expiring in {expires_in_days} days.\n\
         This is the only time the token is shown. Store it now; only its hash is kept.",
        row.name, row.id, row.namespace, row.scopes,
    );

    // The command's *output*, not a log line: an operator pipes this into a
    // secret manager. Written to the stdout handle rather than through
    // `println!`, which would make it a logging sink and would panic on a
    // closed pipe instead of letting the failure be reported with the id.
    let mut out = std::io::stdout().lock();
    writeln!(out, "{plaintext}")
        .and_then(|()| out.flush())
        .with_context(|| {
            format!(
                "the token was minted but could not be written to stdout. Revoke it \
                 with `flexiq-server token revoke {}` and mint another.",
                row.id
            )
        })?;
    Ok(())
}

/// Print the tokens, one per line.
fn list(storage: &StorageBackend, namespace: Option<&str>) -> Result<()> {
    let now = now_millis();
    let tokens = store::list(storage, namespace)?;
    if tokens.is_empty() {
        eprintln!(
            "No tokens. The gRPC door refuses every call until one exists — \
             mint one with `flexiq-server token create --name <name> --scope produce`."
        );
        return Ok(());
    }
    println!(
        "{:<18} {:<24} {:<10} {:<16} {:<10} SCOPES",
        "ID", "NAME", "STATUS", "NAMESPACE", "EXPIRES"
    );
    for token in tokens {
        let expires = match token.days_remaining(now) {
            days if days < 0 => "expired".to_string(),
            days => format!("{days}d"),
        };
        println!(
            "{:<18} {:<24} {:<10} {:<16} {:<10} {}",
            token.id,
            token.name,
            token.status(now).as_str(),
            token.namespace,
            expires,
            token.scopes,
        );
    }
    Ok(())
}

/// Revoke one, or say there was nothing to revoke.
fn revoke(storage: &StorageBackend, id: &str) -> Result<()> {
    if !store::revoke(storage, id)? {
        bail!("no token with id '{id}' — `flexiq-server token list` shows the ids");
    }
    eprintln!("Revoked '{id}'. It stops working on the next call; no restart is needed.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokens::model::MAX_LIFETIME_DAYS;
    use clap::Parser;

    /// The parser as `main` assembles it, so the tests exercise the real
    /// argument surface rather than a copy of it.
    #[derive(Parser)]
    struct Cli {
        #[command(subcommand)]
        command: Wrapper,
    }

    #[derive(Subcommand)]
    enum Wrapper {
        Token(TokenCommand),
    }

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("flexiq-server").chain(args.iter().copied()))
    }

    #[test]
    fn create_needs_a_name_and_at_least_one_scope() {
        assert!(parse(&["token", "create", "--name", "ci"]).is_err());
        assert!(parse(&["token", "create", "--scope", "produce"]).is_err());
        assert!(parse(&["token", "create", "--name", "ci", "--scope", "produce"]).is_ok());
    }

    #[test]
    fn scopes_repeat_and_are_spelled_as_the_wire_spells_them() {
        let cli = parse(&[
            "token", "create", "--name", "ci", "--scope", "produce", "--scope", "execute",
        ])
        .expect("both scopes");
        let Wrapper::Token(TokenCommand {
            action: Action::Create { scopes, .. },
        }) = cli.command
        else {
            panic!("expected create");
        };
        let set = ScopeSet::of(&scopes.iter().copied().map(Scope::from).collect::<Vec<_>>());
        assert_eq!(set, ScopeSet::ALL);
        // A scope this build does not have must not parse into one it does.
        assert!(parse(&["token", "create", "--name", "ci", "--scope", "admin"]).is_err());
    }

    #[test]
    fn the_default_lifetime_is_the_one_the_model_documents() {
        let cli = parse(&["token", "create", "--name", "ci", "--scope", "produce"])
            .expect("defaults apply");
        let Wrapper::Token(TokenCommand {
            action: Action::Create {
                expires_in_days, ..
            },
        }) = cli.command
        else {
            panic!("expected create");
        };
        assert_eq!(expires_in_days, super::super::model::DEFAULT_LIFETIME_DAYS);
        assert!(expires_in_days <= MAX_LIFETIME_DAYS);
    }

    #[test]
    fn revoke_takes_one_id() {
        assert!(parse(&["token", "revoke"]).is_err());
        assert!(parse(&["token", "revoke", "abc123"]).is_ok());
    }
}
