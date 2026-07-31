//! First-admin creation from the environment.

use taskito_core::StorageBackend;

use crate::dashboard::auth::model::{validate_password, validate_username, Role};
use crate::dashboard::auth::store;

/// Create the configured admin if it does not exist yet.
///
/// Idempotent, so it can run on every start: a deployment that keeps the env
/// vars set does not fight with a password the operator changed later. Failures
/// are logged rather than fatal — a bad bootstrap value must not stop the
/// dashboard from coming up and letting someone fix it.
pub fn admin_from_env(storage: &StorageBackend, username: &str, password: &str) {
    if let Err(reason) = validate_username(username).and_then(|_| validate_password(password)) {
        log::warn!("cannot bootstrap the admin user from the environment: {reason}");
        return;
    }

    match store::get_user(storage, username) {
        Ok(Some(_)) => {}
        Ok(None) => match store::create_user(storage, username, password, Role::Admin) {
            Ok(Ok(user)) => log::info!(
                "[taskito] bootstrapped dashboard admin '{}' from the environment",
                user.username
            ),
            Ok(Err(reason)) => log::warn!("cannot bootstrap the admin user: {reason}"),
            Err(error) => log::warn!("cannot bootstrap the admin user: {error}"),
        },
        Err(error) => log::warn!("cannot read the user store to bootstrap an admin: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use taskito_core::SqliteStorage;

    fn storage() -> StorageBackend {
        StorageBackend::Sqlite(SqliteStorage::new(":memory:").expect("in-memory SQLite"))
    }

    #[test]
    fn bootstrapping_twice_creates_one_user() {
        let storage = storage();
        admin_from_env(&storage, "ops", "supersecret");
        admin_from_env(&storage, "ops", "a-different-password");

        assert_eq!(store::count_users(&storage).expect("count"), 1);
        // The second call must not have rewritten the password.
        assert!(store::authenticate(&storage, "ops", "supersecret")
            .expect("storage")
            .is_some());
    }

    #[test]
    fn an_invalid_bootstrap_value_is_ignored_rather_than_fatal() {
        let storage = storage();
        admin_from_env(&storage, "bad user", "supersecret");
        admin_from_env(&storage, "ops", "short");
        assert_eq!(store::count_users(&storage).expect("count"), 0);
    }
}
