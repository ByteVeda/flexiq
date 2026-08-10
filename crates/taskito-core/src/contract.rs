//! The contract level a deployment requires, and the floor that enforces it.
//!
//! Every SDK build speaks one [`CONTRACT_VERSION`] — the revision of the shared
//! storage and wire contract it implements. A polyglot deployment can outlive
//! several of those revisions, so the *storage* carries a floor: the lowest
//! contract level a process may speak and still be allowed to join. A build
//! below the floor refuses to open rather than joining and misreading rows its
//! contract never described.
//!
//! The floor is an operator's dial, not an automatic one. It seeds permissive
//! and is only raised deliberately, once every process in the deployment has
//! been upgraded — a floor that rose on its own would lock out the peers still
//! mid-rollout.

use crate::error::{QueueError, Result};
use crate::storage::Storage;

/// The storage and wire contract this build implements.
///
/// Bump it in the same change that makes an older build unable to read what a
/// newer one writes. Additive changes — a new column, a new optional field —
/// keep the level, because the expand-only rule keeps them readable by both.
pub const CONTRACT_VERSION: u32 = 1;

/// The oldest contract level any build still interoperates with, and therefore
/// the floor a fresh deployment starts at.
pub const MIN_CONTRACT_VERSION: u32 = 1;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::sqlite::SqliteStorage;

    fn storage() -> SqliteStorage {
        SqliteStorage::in_memory().expect("in-memory sqlite")
    }

    #[test]
    fn a_fresh_storage_reports_the_default_floor() {
        let storage = storage();
        assert_eq!(min_contract(&storage).expect("floor"), MIN_CONTRACT_VERSION);
    }

    #[test]
    fn opening_writes_nothing() {
        let storage = storage();
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
        let storage = storage();
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
        let storage = storage();
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
}
