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
use clap::{Args, Subcommand};

use flexiq_core::scheduler::retention::DEFAULT_NAMESPACE;
use flexiq_core::{now_millis, StorageBackend};

use super::grant::{Grant, Grants};
use super::model::{mint_namespace, NewToken};
use super::store;
use crate::audit::record;
use crate::audit::sink::{record_now, LOG_TARGET};
use crate::audit::{Access, Actor, TargetKind};
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
        /// A door this token may open: produce, read, execute, inspect, admin
        /// or tokens. Narrow produce, read or execute to queues and tasks with
        /// `produce:queue=emails-*,task=send_receipt` (a trailing `*` is a
        /// prefix). Repeat for more than one.
        #[arg(long = "scope", value_parser = Grant::parse, required = true)]
        scopes: Vec<Grant>,
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
        Action::Revoke { id } => revoke(&storage, &id, namespace.as_deref()),
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

/// The parsed `--scope` values, as one token's grants.
fn grants(scopes: &[Grant]) -> Grants {
    let mut grants = Grants::default();
    for grant in scopes {
        grants.insert(grant.clone());
    }
    grants
}

/// Mint one, and print it the only time it can be printed.
fn create(
    storage: &StorageBackend,
    namespace: Option<&str>,
    name: &str,
    scopes: &[Grant],
    expires_in_days: i64,
) -> Result<()> {
    // The namespace comes from the process, never from an argument: a token
    // minted for a namespace this deployment does not schedule would accept
    // enqueues nothing ever dequeues (design doc §5.4).
    let namespace = mint_namespace(namespace, None).map_err(|error| anyhow::anyhow!(error))?;
    let scopes = grants(scopes);
    let request = NewToken::new(
        name,
        scopes,
        &namespace,
        Some(expires_in_days),
        Some("cli".to_string()),
    )
    .map_err(|error| anyhow::anyhow!(error))?;

    let (row, plaintext) = store::create(storage, request)?;
    audit(storage, &row.namespace, "create", &row.id, "OK");

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
///
/// Scoped to the namespace this process serves, like the listing: a token
/// belonging to another namespace reads as absent rather than as a refusal.
fn revoke(storage: &StorageBackend, id: &str, namespace: Option<&str>) -> Result<()> {
    // A scoped revoke is this namespace's action whoever owns the id: filed in
    // the owner's trail, a miss would tell another tenant about the probe and
    // hide it from this one. An unscoped revoke is filed where the token
    // lived, read first because the revoke does not report it back.
    let trail_namespace = match namespace {
        Some(namespace) => namespace.to_string(),
        None => store::get(storage, id)?
            .map(|token| token.namespace)
            .unwrap_or_else(|| DEFAULT_NAMESPACE.to_string()),
    };
    let revoked = store::revoke(storage, id, namespace)?;
    let outcome = if revoked { "OK" } else { "NOT_FOUND" };
    audit(storage, &trail_namespace, "revoke", id, outcome);
    if !revoked {
        bail!("no token with id '{id}' — `flexiq-server token list` shows the ids");
    }
    eprintln!("Revoked '{id}'. It stops working on the next call; no restart is needed.");
    Ok(())
}

/// Record one `flexiq-server token <action>` on token `id` in the audit
/// trail. The credential change has already happened, so a trail that cannot
/// take the record is a warning, never a failure of the command.
fn audit(storage: &StorageBackend, namespace: &str, action: &str, id: &str, outcome: &str) {
    let records = record::records(
        namespace,
        &Actor::cli(),
        Access::Write,
        &format!("cli token {action}"),
        vec![(TargetKind::Token.as_str().to_string(), id.to_string())],
        outcome,
    );
    if !record_now(storage, &records) {
        eprintln!(
            "warning: the audit trail did not take the record of this {action}; \
             it was written to the log under '{LOG_TARGET}' instead."
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tokens::model::MAX_LIFETIME_DAYS;
    use crate::tokens::scope::{Scope, ScopeSet};
    use clap::Parser;
    use flexiq_core::storage::sqlite::SqliteStorage;
    use flexiq_core::{AuditRecord, Storage as _};

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
            "token", "create", "--name", "ci", "--scope", "produce", "--scope", "read", "--scope",
            "execute", "--scope", "inspect", "--scope", "admin", "--scope", "tokens",
        ])
        .expect("every scope");
        let Wrapper::Token(TokenCommand {
            action: Action::Create { scopes, .. },
        }) = cli.command
        else {
            panic!("expected create");
        };
        assert_eq!(grants(&scopes), Grants::from(ScopeSet::ALL));
        // A scope this build does not have must not parse into one it does.
        assert!(parse(&["token", "create", "--name", "ci", "--scope", "teleport"]).is_err());
    }

    /// A narrowed grant is refused at parse, not at mint, when it cannot be
    /// read — so a typo never reaches the store.
    #[test]
    fn a_scope_can_be_narrowed_on_the_command_line() {
        let cli = parse(&[
            "token",
            "create",
            "--name",
            "edge",
            "--scope",
            "produce:queue=emails-*,task=send_receipt",
            "--scope",
            "read:queue=emails",
        ])
        .expect("narrowed grants parse");
        let Wrapper::Token(TokenCommand {
            action: Action::Create { scopes, .. },
        }) = cli.command
        else {
            panic!("expected create");
        };
        assert_eq!(
            grants(&scopes).spelled(),
            [
                "produce:queue=emails-*,task=send_receipt",
                "read:queue=emails"
            ]
        );
        for bad in ["produce:queue=a*b", "admin:queue=", "produce:colour=red"] {
            assert!(
                parse(&["token", "create", "--name", "ci", "--scope", bad]).is_err(),
                "{bad}"
            );
        }
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

    fn trail(storage: &StorageBackend, namespace: &str) -> Vec<AuditRecord> {
        storage
            .list_audit_after(namespace, &Default::default(), 100, None)
            .expect("list")
    }

    /// A mint and a revoke from the shell are the credential lifecycle's most
    /// sensitive events; each leaves one `cli` record naming the token.
    #[test]
    fn create_and_revoke_are_recorded() {
        let storage = StorageBackend::Sqlite(SqliteStorage::in_memory().expect("sqlite"));
        create(
            &storage,
            Some("prod"),
            "ci",
            &[Grant::whole(Scope::Produce)],
            30,
        )
        .expect("mint");
        let id = store::list(&storage, Some("prod")).expect("list")[0]
            .id
            .clone();
        // Unscoped, as a shell with no FLEXIQ_NAMESPACE runs it: still
        // recorded in the token's own namespace.
        revoke(&storage, &id, None).expect("revoke");
        let unknown = "ffffffffffffffff";
        assert!(revoke(&storage, unknown, None).is_err(), "no such token");

        let records = trail(&storage, "prod");
        let summary: Vec<_> = records
            .iter()
            .rev()
            .map(|r| (r.operation.as_str(), r.outcome.as_str()))
            .collect();
        assert_eq!(
            summary,
            [("cli token create", "OK"), ("cli token revoke", "OK"),],
            "{records:?}"
        );
        for record in &records {
            assert_eq!(record.principal_kind, "cli");
            assert_eq!(record.target_kind.as_deref(), Some("token"));
            assert_eq!(record.target.as_deref(), Some(id.as_str()));
        }
        // No token owns the unknown id, so its failed revoke is recorded where
        // the process would put it: no namespace configured, the default one.
        let missed = trail(&storage, DEFAULT_NAMESPACE);
        assert_eq!(missed.len(), 1, "{missed:?}");
        assert_eq!(missed[0].outcome, "NOT_FOUND");
        assert_eq!(missed[0].target.as_deref(), Some(unknown));
    }

    /// A revoke scoped to one namespace that names another's token is that
    /// namespace's miss: recorded in its own trail, never the owner's.
    #[test]
    fn a_scoped_revoke_of_another_namespaces_token_stays_in_its_own_trail() {
        let storage = StorageBackend::Sqlite(SqliteStorage::in_memory().expect("sqlite"));
        create(
            &storage,
            Some("prod"),
            "ci",
            &[Grant::whole(Scope::Produce)],
            30,
        )
        .expect("mint");
        let id = store::list(&storage, Some("prod")).expect("list")[0]
            .id
            .clone();

        assert!(
            revoke(&storage, &id, Some("staging")).is_err(),
            "not theirs"
        );

        let theirs = trail(&storage, "staging");
        assert_eq!(theirs.len(), 1, "{theirs:?}");
        assert_eq!(theirs[0].outcome, "NOT_FOUND");
        let owners = trail(&storage, "prod");
        assert_eq!(owners.len(), 1, "only the mint: {owners:?}");
        assert_eq!(owners[0].operation, "cli token create");
    }
}
