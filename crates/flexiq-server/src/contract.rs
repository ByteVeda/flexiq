//! `flexiq-server contract` — reading and raising the deployment's contract
//! floor from a shell.
//!
//! Every SDK exposes `min_contract` / `set_min_contract` on its queue handle,
//! so an operator running any of them already has the dial. A gRPC-only
//! deployment runs none of them: its producers speak protobuf and its executors
//! attach over a socket, and neither holds storage. Without this command such a
//! deployment can read the floor that refuses its steps but cannot raise it.
//!
//! Configures itself from the same environment the server reads — `FLEXIQ_DSN`,
//! `FLEXIQ_BACKEND`, `FLEXIQ_NAMESPACE` — but not through
//! [`Config`](crate::config::Config), which requires at least one *role*.
//! Turning a dial is not a role, the same reasoning `tokens::cli` follows.

use anyhow::{Context, Result};

use flexiq_core::{StorageBackend, CONTRACT_VERSION, STEPS_CONTRACT_LEVEL};

use crate::config::{flag, value, Env};

/// Reading and raising the level a deployment requires of every process.
#[derive(Debug, clap::Args)]
pub struct ContractCommand {
    /// What to do.
    #[command(subcommand)]
    action: Action,
}

#[derive(Debug, clap::Subcommand)]
enum Action {
    /// Print this build's contract level and the deployment's floor.
    Show,
    /// Raise (or lower) the level every process must speak to join.
    ///
    /// Do this once every process in the deployment has been upgraded: a floor
    /// above what a running peer speaks refuses that peer on its next open.
    SetFloor {
        /// The level to require.
        level: u32,
    },
}

pub fn run(command: ContractCommand) -> Result<()> {
    let storage = open()?;
    match command.action {
        Action::Show => show(&storage),
        Action::SetFloor { level } => set_floor(&storage, level),
    }
}

/// Open the same storage the server would, without requiring a role.
///
/// The twin of `tokens::cli::open` — both read the deployment's environment
/// rather than its configuration, for the same reason and through the same
/// [`crate::config::backend::open`].
fn open() -> Result<StorageBackend> {
    let env: Env = std::env::vars().collect();
    let dsn = value(&env, "FLEXIQ_DSN").context(
        "FLEXIQ_DSN is required — point it at the same database the server reads, \
         or the floor is read from one nothing else joins",
    )?;
    Ok(crate::config::backend::open(
        &dsn,
        value(&env, "FLEXIQ_BACKEND").as_deref(),
        value(&env, "FLEXIQ_NAMESPACE"),
        flag(&env, "FLEXIQ_AUTO_MIGRATE", true),
    )?
    .storage)
}

/// Both numbers, plus what the gap between them means for steps — the question
/// an operator is actually asking when they run this.
fn show(storage: &StorageBackend) -> Result<()> {
    let floor = flexiq_core::min_contract(storage)?;
    println!("this build speaks contract {CONTRACT_VERSION}");
    println!("deployment floor:          {floor}");
    if floor < STEPS_CONTRACT_LEVEL {
        println!(
            "\ndurable steps are refused: they require every process at contract \
             >= {STEPS_CONTRACT_LEVEL}.\nrun `flexiq-server contract set-floor \
             {STEPS_CONTRACT_LEVEL}` once every process is upgraded."
        );
    }
    Ok(())
}

/// `set_min_contract` already refuses a level this build cannot speak and one
/// below the oldest that means anything, so this adds no checks of its own —
/// two implementations of one rule is how they come to disagree.
fn set_floor(storage: &StorageBackend, level: u32) -> Result<()> {
    flexiq_core::set_min_contract(storage, level)?;
    println!("deployment floor is now {level}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::{Parser, Subcommand};
    use flexiq_core::{SqliteStorage, MIN_CONTRACT_VERSION};

    /// The parser as `main` assembles it, so the tests exercise the real
    /// argument surface rather than a copy of it.
    #[derive(Parser)]
    struct Cli {
        #[command(subcommand)]
        command: Wrapper,
    }

    #[derive(Subcommand)]
    enum Wrapper {
        Contract(ContractCommand),
    }

    fn parse(args: &[&str]) -> Result<Cli, clap::Error> {
        Cli::try_parse_from(std::iter::once("flexiq-server").chain(args.iter().copied()))
    }

    fn storage() -> StorageBackend {
        StorageBackend::Sqlite(SqliteStorage::in_memory().expect("in-memory sqlite"))
    }

    #[test]
    fn set_floor_takes_exactly_one_level() {
        assert!(parse(&["contract", "show"]).is_ok());
        assert!(parse(&["contract", "set-floor"]).is_err());
        assert!(parse(&["contract", "set-floor", "2"]).is_ok());
        assert!(parse(&["contract", "set-floor", "two"]).is_err());
    }

    #[test]
    fn set_floor_round_trips_through_storage() {
        let storage = storage();
        set_floor(&storage, MIN_CONTRACT_VERSION).expect("lower");
        assert_eq!(
            flexiq_core::min_contract(&storage).expect("floor"),
            MIN_CONTRACT_VERSION
        );

        set_floor(&storage, STEPS_CONTRACT_LEVEL).expect("raise");
        assert_eq!(
            flexiq_core::min_contract(&storage).expect("floor"),
            STEPS_CONTRACT_LEVEL
        );
    }

    /// The core owns the rule; this command must not grow a second copy of it
    /// that can disagree.
    #[test]
    fn a_level_this_build_cannot_speak_is_refused() {
        let storage = storage();
        let error = set_floor(&storage, CONTRACT_VERSION + 1).expect_err("must refuse");
        assert!(error.to_string().contains("lock it out"), "{error}");
    }

    #[test]
    fn show_reports_a_floor_it_can_read() {
        let storage = storage();
        // A storage this build created is already at its level, which is what
        // `show` has to be able to say without an operator having set anything.
        assert_eq!(
            flexiq_core::min_contract(&storage).expect("floor"),
            CONTRACT_VERSION
        );
        show(&storage).expect("show");
    }
}
