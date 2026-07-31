//! Users and sessions, read and written through the settings store.

use std::collections::BTreeMap;

use serde_json::{Map, Value};
use taskito_core::{now_millis, Result, Storage};

use crate::dashboard::auth::model::{
    Role, Session, User, SESSION_PREFIX, SESSION_TTL_SECONDS, USERS_KEY,
};
use crate::dashboard::auth::password;
use crate::dashboard::security::random_token;
use crate::dashboard::stores::kv;

/// Every user, keyed by username.
pub fn list_users(storage: &impl Storage) -> Result<BTreeMap<String, User>> {
    let rows: Map<String, Value> = kv::read(storage, USERS_KEY)?;
    Ok(rows
        .into_iter()
        .filter_map(|(username, row)| {
            let mut user: User = serde_json::from_value(row).ok()?;
            user.username = username.clone();
            Some((username, user))
        })
        .collect())
}

/// How many users exist. Zero is what puts the dashboard in setup mode.
pub fn count_users(storage: &impl Storage) -> Result<usize> {
    Ok(list_users(storage)?.len())
}

/// One user by name.
pub fn get_user(storage: &impl Storage, username: &str) -> Result<Option<User>> {
    Ok(list_users(storage)?.remove(username))
}

/// Create a user with a hashed password.
///
/// Hashing is deliberately slow, so callers must already be on a blocking
/// thread.
pub fn create_user(
    storage: &impl Storage,
    username: &str,
    password: &str,
    role: Role,
) -> Result<std::result::Result<User, String>> {
    let mut users = list_users(storage)?;
    if users.contains_key(username) {
        return Ok(Err(format!("user '{username}' already exists")));
    }
    let now = now_millis();
    let user = User {
        username: username.to_string(),
        password_hash: password::hash(password),
        role,
        created_at: now,
        last_login_at: None,
        email: None,
        display_name: None,
    };
    users.insert(username.to_string(), user.clone());
    save_users(storage, &users)?;
    Ok(Ok(user))
}

/// Replace a user's password.
pub fn update_password(
    storage: &impl Storage,
    username: &str,
    new_password: &str,
) -> Result<std::result::Result<(), String>> {
    let mut users = list_users(storage)?;
    let Some(user) = users.get_mut(username) else {
        return Ok(Err(format!("user '{username}' does not exist")));
    };
    user.password_hash = password::hash(new_password);
    save_users(storage, &users)?;
    Ok(Ok(()))
}

/// Verify credentials, stamping `last_login_at` on success.
///
/// An unknown username still pays for one hash so the response time cannot be
/// used to enumerate accounts.
pub fn authenticate(
    storage: &impl Storage,
    username: &str,
    submitted: &str,
) -> Result<Option<User>> {
    let mut users = list_users(storage)?;
    let Some(user) = users.get_mut(username) else {
        password::verify(submitted, &password::dummy_hash());
        return Ok(None);
    };
    if !password::verify(submitted, &user.password_hash) {
        return Ok(None);
    }
    user.last_login_at = Some(now_millis());
    let authenticated = user.clone();
    save_users(storage, &users)?;
    Ok(Some(authenticated))
}

/// Look up or create the user backing a provider identity.
///
/// The username is `<slot>:<subject>` — the provider's stable id, never the
/// email, which can be reassigned.
pub fn upsert_provider_user(
    storage: &impl Storage,
    slot: &str,
    subject: &str,
    email: Option<&str>,
    display_name: Option<&str>,
    role_on_create: Role,
) -> Result<User> {
    let username = format!("{slot}:{subject}");
    let mut users = list_users(storage)?;
    let now = now_millis();

    let user = match users.get_mut(&username) {
        Some(existing) => {
            // Refresh the profile, but never the role: an allowlist change
            // must not silently demote or promote on the next login.
            if let Some(email) = email {
                existing.email = Some(email.to_string());
            }
            if let Some(name) = display_name {
                existing.display_name = Some(name.to_string());
            }
            existing.last_login_at = Some(now);
            existing.clone()
        }
        None => {
            let created = User {
                username: username.clone(),
                password_hash: format!("{}{slot}", super::model::OAUTH_PASSWORD_MARKER),
                role: role_on_create,
                created_at: now,
                last_login_at: Some(now),
                email: email.map(str::to_string),
                display_name: display_name.map(str::to_string),
            };
            users.insert(username.clone(), created.clone());
            created
        }
    };
    save_users(storage, &users)?;
    Ok(user)
}

/// Start a session for `user`.
pub fn create_session(storage: &impl Storage, user: &User) -> Result<Session> {
    let now = now_seconds();
    let session = Session {
        token: random_token(),
        username: user.username.clone(),
        role: user.role,
        created_at: now,
        expires_at: now + SESSION_TTL_SECONDS,
        csrf_token: random_token(),
    };
    kv::write(
        storage,
        &format!("{SESSION_PREFIX}{}", session.token),
        &session,
    )?;
    Ok(session)
}

/// Load a session by token, deleting it if it has expired.
pub fn get_session(storage: &impl Storage, token: &str) -> Result<Option<Session>> {
    if token.is_empty() {
        return Ok(None);
    }
    let Some(raw) = storage.get_setting(&format!("{SESSION_PREFIX}{token}"))? else {
        return Ok(None);
    };
    let Ok(mut session) = serde_json::from_str::<Session>(&raw) else {
        return Ok(None);
    };
    session.token = token.to_string();
    if session.is_expired(now_seconds()) {
        delete_session(storage, token)?;
        return Ok(None);
    }
    Ok(Some(session))
}

/// Invalidate a session.
pub fn delete_session(storage: &impl Storage, token: &str) -> Result<bool> {
    if token.is_empty() {
        return Ok(false);
    }
    storage.delete_setting(&format!("{SESSION_PREFIX}{token}"))
}

/// Best-effort cleanup of expired sessions. Returns how many were removed.
pub fn prune_expired_sessions(storage: &impl Storage) -> Result<usize> {
    let now = now_seconds();
    let mut removed = 0;
    for (token, raw) in kv::scan_prefix(storage, SESSION_PREFIX)? {
        let expired = serde_json::from_str::<Session>(&raw)
            .map(|session| session.is_expired(now))
            // An unreadable session can never be used, so it is dead weight.
            .unwrap_or(true);
        if expired && delete_session(storage, &token)? {
            removed += 1;
        }
    }
    Ok(removed)
}

fn save_users(storage: &impl Storage, users: &BTreeMap<String, User>) -> Result<()> {
    let rows: Map<String, Value> = users
        .iter()
        .filter_map(|(username, user)| {
            serde_json::to_value(user)
                .ok()
                .map(|value| (username.clone(), value))
        })
        .collect();
    kv::write(storage, USERS_KEY, &rows)
}

/// Sessions store Unix **seconds**, matching the SDK dashboards.
fn now_seconds() -> i64 {
    now_millis() / 1_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use taskito_core::{SqliteStorage, StorageBackend};

    fn storage() -> StorageBackend {
        StorageBackend::Sqlite(SqliteStorage::new(":memory:").expect("in-memory SQLite"))
    }

    #[test]
    fn a_created_user_authenticates_and_records_its_login() {
        let storage = storage();
        create_user(&storage, "ops", "supersecret", Role::Admin)
            .expect("storage")
            .expect("valid user");

        assert_eq!(count_users(&storage).expect("count"), 1);
        assert!(authenticate(&storage, "ops", "wrong")
            .expect("storage")
            .is_none());

        let user = authenticate(&storage, "ops", "supersecret")
            .expect("storage")
            .expect("credentials accepted");
        assert_eq!(user.role, Role::Admin);
        assert!(user.last_login_at.is_some());
    }

    #[test]
    fn a_duplicate_username_is_refused() {
        let storage = storage();
        create_user(&storage, "ops", "supersecret", Role::Admin)
            .expect("storage")
            .expect("first create");
        let refusal = create_user(&storage, "ops", "supersecret", Role::Admin)
            .expect("storage")
            .expect_err("second create");
        assert!(refusal.contains("already exists"));
    }

    #[test]
    fn an_unknown_user_never_authenticates() {
        let storage = storage();
        assert!(authenticate(&storage, "ghost", "whatever")
            .expect("storage")
            .is_none());
    }

    #[test]
    fn sessions_round_trip_and_expire() {
        let storage = storage();
        let user = create_user(&storage, "ops", "supersecret", Role::Viewer)
            .expect("storage")
            .expect("valid user");

        let session = create_session(&storage, &user).expect("create session");
        let loaded = get_session(&storage, &session.token)
            .expect("storage")
            .expect("session is live");
        assert_eq!(loaded.username, "ops");
        assert_eq!(loaded.csrf_token, session.csrf_token);
        assert_eq!(loaded.token, session.token);

        assert!(delete_session(&storage, &session.token).expect("storage"));
        assert!(get_session(&storage, &session.token)
            .expect("storage")
            .is_none());
    }

    #[test]
    fn an_expired_session_is_dropped_on_read() {
        let storage = storage();
        let expired = Session {
            token: "stale-token".into(),
            username: "ops".into(),
            role: Role::Viewer,
            created_at: 0,
            expires_at: 1,
            csrf_token: "csrf".into(),
        };
        kv::write(&storage, &format!("{SESSION_PREFIX}stale-token"), &expired)
            .expect("write session");

        assert!(get_session(&storage, "stale-token")
            .expect("storage")
            .is_none());
        // Reading it also removed it, so the prune has nothing left to do.
        assert_eq!(prune_expired_sessions(&storage).expect("prune"), 0);
    }

    #[test]
    fn a_provider_login_creates_then_refreshes_its_user() {
        let storage = storage();
        let created = upsert_provider_user(
            &storage,
            "google",
            "1234",
            Some("ops@example.com"),
            Some("Ops"),
            Role::Viewer,
        )
        .expect("storage");
        assert_eq!(created.username, "google:1234");
        assert!(created.is_oauth());

        // A later login refreshes the profile but must not change the role.
        let refreshed = upsert_provider_user(
            &storage,
            "google",
            "1234",
            Some("new@example.com"),
            None,
            Role::Admin,
        )
        .expect("storage");
        assert_eq!(refreshed.role, Role::Viewer);
        assert_eq!(refreshed.email.as_deref(), Some("new@example.com"));
        assert_eq!(refreshed.display_name.as_deref(), Some("Ops"));
        assert_eq!(count_users(&storage).expect("count"), 1);
    }

    #[test]
    fn a_provider_user_cannot_log_in_with_a_password() {
        let storage = storage();
        upsert_provider_user(&storage, "google", "1234", None, None, Role::Admin).expect("storage");
        assert!(authenticate(&storage, "google:1234", "oauth:google")
            .expect("storage")
            .is_none());
    }
}
