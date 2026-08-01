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
///
/// A row that cannot be read is skipped and logged. It is never silent: an
/// unreadable row is invisible to every listing, and the operator needs to know
/// the store holds something this build does not understand.
pub fn list_users(storage: &impl Storage) -> Result<BTreeMap<String, User>> {
    Ok(parse_users(&kv::read(storage, USERS_KEY)?))
}

/// How many users the store holds.
///
/// Counts **stored** entries, not parsed ones. Zero is what leaves
/// unauthenticated setup open, so a row this build cannot parse must still keep
/// the door shut.
pub fn count_users(storage: &impl Storage) -> Result<usize> {
    let rows: Map<String, Value> = kv::read(storage, USERS_KEY)?;
    Ok(rows.len())
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
    let now = now_millis();
    // Hashed out here: the closure below re-runs on a lost race, and hashing is
    // the expensive part of this call.
    let user = User {
        username: username.to_string(),
        password_hash: password::hash(password),
        role,
        created_at: now,
        last_login_at: None,
        email: None,
        display_name: None,
    };
    edit_users(storage, |users| {
        if users.contains_key(username) {
            return Err(format!("user '{username}' already exists"));
        }
        users.insert(username.to_string(), user.clone());
        Ok(user.clone())
    })
}

/// Persist a changed user row, keyed by its username.
///
/// The role is deliberately not something a provider login can set; an operator
/// changing it needs a way in that the login path does not have.
pub fn replace_user(storage: &impl Storage, user: &User) -> Result<bool> {
    edit_users(storage, |users| {
        if !users.contains_key(&user.username) {
            return false;
        }
        users.insert(user.username.clone(), user.clone());
        true
    })
}

/// Replace a user's password.
pub fn update_password(
    storage: &impl Storage,
    username: &str,
    new_password: &str,
) -> Result<std::result::Result<(), String>> {
    let hash = password::hash(new_password);
    edit_users(storage, |users| {
        let Some(user) = users.get_mut(username) else {
            return Err(format!("user '{username}' does not exist"));
        };
        user.password_hash = hash.clone();
        Ok(())
    })
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
    // Verified before the write, and once: hashing inside the retry loop would
    // pay for it again on every lost race.
    let Some(user) = list_users(storage)?.remove(username) else {
        password::verify(submitted, &password::dummy_hash());
        return Ok(None);
    };
    if !password::verify(submitted, &user.password_hash) {
        return Ok(None);
    }

    let now = now_millis();
    let stamped = edit_users(storage, |users| {
        let stored = users.get_mut(username)?;
        stored.last_login_at = Some(now);
        Some(stored.clone())
    })?;
    // A row deleted between the check and the stamp does not undo a login that
    // was valid when it was made.
    Ok(Some(stamped.unwrap_or(user)))
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
    let now = now_millis();

    edit_users(storage, |users| match users.get_mut(&username) {
        // Refresh the profile, but never the role: an allowlist change must not
        // silently demote or promote on the next login.
        Some(existing) => {
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
    })
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

/// Invalidate every session belonging to `username`, except `keep_token`.
///
/// The reason to change a password is usually that a credential leaked, so the
/// sessions minted with the old one must not outlive it. The caller's own
/// session is kept so the operator is not logged out of the tab they are using.
pub fn revoke_sessions_for(
    storage: &impl Storage,
    username: &str,
    keep_token: Option<&str>,
) -> Result<usize> {
    let mut revoked = 0;
    for (token, raw) in kv::scan_prefix(storage, SESSION_PREFIX)? {
        if keep_token == Some(token.as_str()) {
            continue;
        }
        let belongs = serde_json::from_str::<Session>(&raw)
            .map(|session| session.username == username)
            .unwrap_or(false);
        if belongs && delete_session(storage, &token)? {
            revoked += 1;
        }
    }
    Ok(revoked)
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

/// Read every user, apply `mutate`, and write the result back only if nobody
/// else touched the document meanwhile.
///
/// `mutate` re-runs on a lost race, so anything expensive — password hashing,
/// above all — belongs outside it.
fn edit_users<R>(
    storage: &impl Storage,
    mut mutate: impl FnMut(&mut BTreeMap<String, User>) -> R,
) -> Result<R> {
    kv::update(storage, USERS_KEY, |rows: &mut Map<String, Value>| {
        let before = parse_users(rows);
        let mut users = before.clone();
        let outcome = mutate(&mut users);

        // Edited per user rather than by replacing the document. A row this
        // build cannot parse never reaches `users`, so writing the whole map
        // back would delete it — and deleting the last row that `count_users`
        // can see reopens the setup flow, which is an authentication bypass.
        for (username, user) in &users {
            if let Ok(value) = serde_json::to_value(user) {
                rows.insert(username.clone(), value);
            }
        }
        // Only rows this build could read are removable; anything it skipped is
        // not this edit's to delete.
        for username in before.keys() {
            if !users.contains_key(username) {
                rows.remove(username);
            }
        }
        outcome
    })
}

/// Parse stored rows into users.
///
/// A row that cannot be read is skipped and logged. It is never silent: an
/// unreadable row is invisible to every listing, and the operator needs to know
/// the store holds something this build does not understand.
fn parse_users(rows: &Map<String, Value>) -> BTreeMap<String, User> {
    rows.iter()
        .filter_map(
            |(username, row)| match serde_json::from_value::<User>(row.clone()) {
                Ok(mut user) => {
                    user.username = username.clone();
                    Some((username.clone(), user))
                }
                Err(error) => {
                    log::warn!(
                        "dashboard user '{username}' is unreadable and was skipped: {error}"
                    );
                    None
                }
            },
        )
        .collect()
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
    fn an_unreadable_row_still_counts_as_a_user() {
        let storage = storage();
        // Shaped like a user but missing the field this build needs. Setup must
        // stay closed even though the row cannot be listed.
        kv::write(
            &storage,
            USERS_KEY,
            &serde_json::json!({ "ops": { "unexpected": true } }),
        )
        .expect("seed a foreign row");

        assert!(list_users(&storage).expect("storage").is_empty());
        assert_eq!(
            count_users(&storage).expect("storage"),
            1,
            "an unreadable row must not reopen setup"
        );
    }

    #[test]
    fn an_edit_leaves_a_row_this_build_cannot_read() {
        // `edit_users` used to replace the whole document with only the rows it
        // could parse, so one create deleted every unreadable neighbour. If the
        // deleted row was the last one `count_users` could see, setup reopened.
        let storage = storage();
        kv::write(
            &storage,
            USERS_KEY,
            &serde_json::json!({ "legacy": { "unexpected": true } }),
        )
        .expect("seed a foreign row");

        create_user(&storage, "ops", "supersecret", Role::Admin)
            .expect("storage")
            .expect("created");

        let rows: Map<String, Value> = kv::read(&storage, USERS_KEY).expect("storage");
        assert!(
            rows.contains_key("legacy"),
            "an edit to a neighbour must not delete an unreadable row: {rows:?}"
        );
        assert_eq!(
            count_users(&storage).expect("storage"),
            2,
            "both rows still count toward keeping setup closed"
        );
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
    fn changing_a_password_can_revoke_every_other_session() {
        let storage = storage();
        let user = create_user(&storage, "ops", "supersecret", Role::Admin)
            .expect("storage")
            .expect("valid user");
        let other = create_user(&storage, "reader", "supersecret", Role::Viewer)
            .expect("storage")
            .expect("valid user");

        let keep = create_session(&storage, &user).expect("session");
        let leaked = create_session(&storage, &user).expect("session");
        let unrelated = create_session(&storage, &other).expect("session");

        let revoked = revoke_sessions_for(&storage, "ops", Some(&keep.token)).expect("storage");
        assert_eq!(revoked, 1);

        assert!(get_session(&storage, &keep.token)
            .expect("storage")
            .is_some());
        assert!(
            get_session(&storage, &leaked.token)
                .expect("storage")
                .is_none(),
            "the sessions minted with the old password must be gone"
        );
        assert!(
            get_session(&storage, &unrelated.token)
                .expect("storage")
                .is_some(),
            "another user's session is not this user's to revoke"
        );
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
