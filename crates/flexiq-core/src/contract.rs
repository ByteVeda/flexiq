//! The contract level a deployment requires, and the floor that enforces it.
//!
//! Every SDK build speaks one [`CONTRACT_VERSION`] — the revision of the shared
//! storage and wire contract it implements. A polyglot deployment can outlive
//! several of those revisions, so the *storage* carries a floor: the lowest
//! contract level a process may speak and still be allowed to join. A build
//! below the floor refuses to open rather than joining and misreading rows its
//! contract never described.
//!
//! The floor is an operator's dial. On an existing deployment it stays where it
//! is and is only raised deliberately, once every process has been upgraded — a
//! floor that rose on its own would lock out the peers still mid-rollout.
//!
//! The single exception is a deployment being created: a migration run that
//! builds the schema from nothing records this build's level, because a database
//! that did not exist a moment ago has no older peer to lock out. That is what
//! keeps [`STEPS_CONTRACT_LEVEL`] from meeting a new user as a refusal.

use crate::error::{QueueError, Result};
use crate::storage::Storage;

/// The storage and wire contract this build implements.
///
/// Bump it in the same change that makes an older build unable to read what a
/// newer one writes. Additive changes — a new column, a new optional field —
/// keep the level, because the expand-only rule keeps them readable by both.
///
/// Level 2 is durable steps: `job_steps` is additive at the schema level, but a
/// build that cannot read it re-runs every committed step of a job it claims,
/// which is the behavioural break the level exists to record.
pub const CONTRACT_VERSION: u32 = 2;

/// The oldest contract level any build still interoperates with, and therefore
/// the floor a deployment starts at when nothing has raised it.
///
/// Deliberately *behind* [`CONTRACT_VERSION`]: a level-1 build still joins and
/// still runs ordinary jobs correctly, so locking it out on sight would break
/// the rolling upgrade the floor exists to allow. What it may not be trusted
/// with is durable steps, and that is gated on the floor rather than here — see
/// [`STEPS_CONTRACT_LEVEL`].
pub const MIN_CONTRACT_VERSION: u32 = 1;

/// The floor durable steps require of *every* process in a deployment.
///
/// Steps are memoized against `job_steps`, and a build below this level cannot
/// read that table: it would claim a job whose steps already committed, find
/// none, and run every one of them a second time. The step API therefore checks
/// the deployment's floor rather than its own level — this build speaking the
/// contract says nothing about the older worker sharing the queue with it.
pub const STEPS_CONTRACT_LEVEL: u32 = 2;

/// Settings key holding the deployment's floor. Under the reserved `contract:`
/// prefix, so a dashboard's generic settings surface can neither read nor spoof
/// it — see [`crate::settings`].
pub const CONTRACT_FLOOR_SETTING: &str = "contract:min_sdk";

/// The floor this storage requires, defaulting to [`MIN_CONTRACT_VERSION`] when
/// nothing has been recorded yet.
///
/// Only an *absent* key takes the default. A present value that will not parse
/// is an error, not a fallback: reading it as permissive would let anything able
/// to write the key neuter a raised floor by storing garbage. Recovery does not
/// need the read — [`set_min_contract`] overwrites the key without consulting
/// it.
pub fn min_contract<S: Storage>(storage: &S) -> Result<u32> {
    let Some(raw) = storage.get_setting(CONTRACT_FLOOR_SETTING)? else {
        return Ok(MIN_CONTRACT_VERSION);
    };
    raw.trim().parse().map_err(|_| {
        QueueError::Config(format!(
            "{CONTRACT_FLOOR_SETTING} holds {raw:?}, which is not a contract level;              set it to a whole number to repair the floor"
        ))
    })
}

/// Raise or lower the floor, within the levels that mean something.
///
/// Refuses a level this build does not itself speak — writing it would lock the
/// writer out of the deployment on its next open, with no process left able to
/// lower it again — and equally one below [`MIN_CONTRACT_VERSION`], which admits
/// nothing a floor of `MIN_CONTRACT_VERSION` does not.
pub fn set_min_contract<S: Storage>(storage: &S, level: u32) -> Result<()> {
    if level > CONTRACT_VERSION {
        return Err(QueueError::Config(format!(
            "cannot require contract {level}: this build speaks contract {CONTRACT_VERSION}, \
             so the write would lock it out of its own storage"
        )));
    }
    if level < MIN_CONTRACT_VERSION {
        return Err(QueueError::Config(format!(
            "cannot require contract {level}: {MIN_CONTRACT_VERSION} is the oldest level any \
             build speaks, so a lower floor admits nothing extra and only obscures the dial"
        )));
    }
    storage.set_setting(CONTRACT_FLOOR_SETTING, &level.to_string())
}

/// Refuse to continue when this build is below the floor. Called once per
/// storage open, by every process that joins a deployment.
///
/// Read-only: an unset floor is the permissive default, so opening never writes
/// and a deployment that never raises the dial carries no row for it. Speaking a
/// *newer* contract than the floor is always fine — the floor is a minimum, not
/// the equality check the worker handshake uses.
pub fn ensure_contract_supported<S: Storage>(storage: &S) -> Result<()> {
    let required = min_contract(storage)?;
    if CONTRACT_VERSION < required {
        return Err(QueueError::ContractTooOld {
            speaks: CONTRACT_VERSION,
            required,
        });
    }
    Ok(())
}

/// Refuse the inline-step API unless the deployment's floor has reached
/// [`STEPS_CONTRACT_LEVEL`].
///
/// The check is the *deployment's*, not this build's. A process running steps
/// is necessarily at the level itself; what it cannot see is the older peer
/// still claiming from the same queue, and the floor is the only statement in
/// the system about what that peer may be.
///
/// [`QueueError::Config`] rather than a retryable error: nothing changes until
/// an operator raises the dial, so replaying the attempt only burns the retry
/// budget. The message names the setting and the condition for raising it,
/// because the operator reading it is being asked to make exactly that call.
pub fn ensure_steps_allowed<S: Storage>(storage: &S) -> Result<()> {
    let floor = min_contract(storage)?;
    if floor < STEPS_CONTRACT_LEVEL {
        return Err(QueueError::Config(format!(
            "inline steps require every worker at contract >= {STEPS_CONTRACT_LEVEL}, and this \
             deployment's floor is {floor}; raise {CONTRACT_FLOOR_SETTING} to \
             {STEPS_CONTRACT_LEVEL} once every process is upgraded"
        )));
    }
    Ok(())
}

/// Record this build's level as the floor of a deployment that did not exist
/// until now.
///
/// Called only where a migration run *created* the schema. That is the one
/// moment the "never raise the floor on someone's behalf" rule does not apply:
/// a database with no tables a moment ago has no older process reading it, so
/// there is nobody to lock out, and seeding is what makes durable steps work
/// for a new deployment without an operator first discovering a dial.
///
/// Absent-only, so re-running migrations against a deployment that has since
/// chosen its own floor leaves that choice alone.
pub fn seed_floor_for_new_deployment<S: Storage>(storage: &S) -> Result<()> {
    if storage.get_setting(CONTRACT_FLOOR_SETTING)?.is_some() {
        return Ok(());
    }
    storage.set_setting(CONTRACT_FLOOR_SETTING, &CONTRACT_VERSION.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sqlite::SqliteStorage;
    use crate::storage::Storage;

    fn storage() -> SqliteStorage {
        SqliteStorage::in_memory().expect("in-memory sqlite")
    }

    /// A storage standing in for a deployment that predates the seed: an
    /// existing database, created by a build that recorded no floor. Most of
    /// the rules below are about that case, because it is the one where the
    /// dial still belongs to the operator.
    fn upgraded_storage() -> SqliteStorage {
        let storage = storage();
        storage
            .delete_setting(CONTRACT_FLOOR_SETTING)
            .expect("clear the seeded floor");
        storage
    }

    #[test]
    fn a_storage_this_build_created_reports_this_build() {
        let storage = storage();
        assert_eq!(min_contract(&storage).expect("floor"), CONTRACT_VERSION);
    }

    #[test]
    fn an_upgraded_storage_reports_the_permissive_default() {
        let storage = upgraded_storage();
        assert_eq!(min_contract(&storage).expect("floor"), MIN_CONTRACT_VERSION);
    }

    #[test]
    fn opening_writes_nothing() {
        let storage = upgraded_storage();
        ensure_contract_supported(&storage).expect("open");
        assert_eq!(
            storage
                .get_setting(CONTRACT_FLOOR_SETTING)
                .expect("setting"),
            None,
            "an unraised floor must not add a row to every deployment"
        );
    }

    #[test]
    fn a_floor_at_this_build_still_opens() {
        let storage = storage();
        set_min_contract(&storage, CONTRACT_VERSION).expect("raise");
        ensure_contract_supported(&storage).expect("open");
        assert_eq!(min_contract(&storage).expect("floor"), CONTRACT_VERSION);
    }

    #[test]
    fn a_floor_above_this_build_is_refused() {
        let storage = storage();
        // Written directly: `set_min_contract` rejects it, which is the point.
        storage
            .set_setting(CONTRACT_FLOOR_SETTING, &(CONTRACT_VERSION + 1).to_string())
            .expect("write");

        let error = ensure_contract_supported(&storage).expect_err("must refuse");
        let message = error.to_string();
        assert!(
            message.contains(&CONTRACT_VERSION.to_string())
                && message.contains(&(CONTRACT_VERSION + 1).to_string()),
            "the error must name both levels: {message}"
        );
    }

    #[test]
    fn a_floor_below_the_oldest_level_is_rejected() {
        let storage = upgraded_storage();
        let error = set_min_contract(&storage, MIN_CONTRACT_VERSION - 1).expect_err("must reject");
        assert!(error.to_string().contains("oldest level"));
        assert_eq!(
            storage
                .get_setting(CONTRACT_FLOOR_SETTING)
                .expect("setting"),
            None,
            "a rejected floor must not write"
        );
    }

    #[test]
    fn a_floor_this_build_cannot_speak_is_rejected() {
        let storage = upgraded_storage();
        let error = set_min_contract(&storage, CONTRACT_VERSION + 1).expect_err("must reject");
        assert!(error.to_string().contains("lock it out"));
        assert_eq!(
            storage
                .get_setting(CONTRACT_FLOOR_SETTING)
                .expect("setting"),
            None,
            "a rejected raise must not write"
        );
    }

    #[test]
    fn migrating_checks_the_floor_it_could_not_read_at_open() {
        use crate::storage::StorageBackend;

        let storage = StorageBackend::Sqlite(SqliteStorage::in_memory().expect("sqlite"));
        // A gated open skips the check because the settings table may not exist
        // yet; `migrate` is where the deferred check lands, so a deployment the
        // build may not join is refused there rather than never.
        storage
            .set_setting(CONTRACT_FLOOR_SETTING, &(CONTRACT_VERSION + 1).to_string())
            .expect("write");

        let error = storage.migrate().expect_err("migrate must refuse");
        assert!(
            matches!(error, QueueError::ContractTooOld { .. }),
            "{error}"
        );
    }

    #[test]
    fn an_unreadable_floor_fails_closed() {
        let storage = storage();
        storage
            .set_setting(CONTRACT_FLOOR_SETTING, "not-a-level")
            .expect("write");

        // Reading it as the permissive default would let anything able to write
        // the key neuter a raised floor by storing garbage.
        let error = min_contract(&storage).expect_err("must not fall back");
        assert!(error.to_string().contains("not a contract level"));
        ensure_contract_supported(&storage).expect_err("open must refuse too");

        // The repair path does not read the broken value.
        set_min_contract(&storage, CONTRACT_VERSION).expect("repair");
        assert_eq!(min_contract(&storage).expect("floor"), CONTRACT_VERSION);
    }

    #[test]
    fn steps_are_refused_below_their_level() {
        let storage = upgraded_storage();
        // The permissive default: a deployment that has never raised the dial
        // may still be running a build that cannot read `job_steps`. Held at
        // compile time — the day the two meet, this gate stops gating.
        const { assert!(MIN_CONTRACT_VERSION < STEPS_CONTRACT_LEVEL) };

        let error = ensure_steps_allowed(&storage).expect_err("must refuse");
        let message = error.to_string();
        assert!(
            message.contains(CONTRACT_FLOOR_SETTING)
                && message.contains(&STEPS_CONTRACT_LEVEL.to_string()),
            "the message must name the dial and the level: {message}"
        );
    }

    #[test]
    fn steps_are_allowed_once_the_floor_is_raised() {
        let storage = storage();
        set_min_contract(&storage, STEPS_CONTRACT_LEVEL).expect("raise");
        ensure_steps_allowed(&storage).expect("steps");
    }

    /// The gate inherits [`min_contract`]'s refusal to read garbage as
    /// permissive: anything able to write the key would otherwise be able to
    /// re-enable steps across a fleet that cannot run them.
    #[test]
    fn an_unreadable_floor_refuses_steps() {
        let storage = storage();
        storage
            .set_setting(CONTRACT_FLOOR_SETTING, "not-a-level")
            .expect("write");

        ensure_steps_allowed(&storage).expect_err("must not fall back");
    }

    #[test]
    fn seeding_records_this_build_on_an_unset_floor() {
        let storage = storage();
        seed_floor_for_new_deployment(&storage).expect("seed");

        assert_eq!(min_contract(&storage).expect("floor"), CONTRACT_VERSION);
        // The whole point of seeding: a deployment created by this build runs
        // steps without an operator first finding the dial.
        ensure_steps_allowed(&storage).expect("steps");
    }

    #[test]
    fn seeding_leaves_a_floor_someone_already_chose() {
        let storage = storage();
        set_min_contract(&storage, MIN_CONTRACT_VERSION).expect("choose");

        seed_floor_for_new_deployment(&storage).expect("seed");

        assert_eq!(
            min_contract(&storage).expect("floor"),
            MIN_CONTRACT_VERSION,
            "re-running migrations must not overwrite a deliberate floor"
        );
    }
}
