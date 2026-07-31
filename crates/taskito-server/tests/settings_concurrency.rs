//! Concurrent edits to the settings-backed stores.
//!
//! Each of these documents lives in one settings row, so two writers that read
//! the same version and both write would leave only the later edit. The stores
//! write conditionally and retry instead; the assertion is that every edit is
//! still there afterwards.
//!
//! File-backed SQLite, not `:memory:`: the threads need to be looking at one
//! database.

mod support;

use std::sync::Arc;
use std::thread;

use taskito_core::{Storage, StorageBackend};
use taskito_server::dashboard::auth::model::Role;
use taskito_server::dashboard::auth::store as auth_store;
use taskito_server::dashboard::stores::webhooks::WebhookSubscription;
use taskito_server::dashboard::stores::{middleware, webhooks};

use support::temp_storage;

/// Writers per test. Enough to overlap reliably, few enough to stay quick.
const WRITERS: usize = 8;

/// Run `write` on `WRITERS` threads at once, indexed 0..WRITERS.
fn concurrently(storage: &StorageBackend, write: impl Fn(&StorageBackend, usize) + Send + Sync) {
    let storage = Arc::new(storage.clone());
    let write = Arc::new(write);
    thread::scope(|scope| {
        for index in 0..WRITERS {
            let storage = storage.clone();
            let write = write.clone();
            scope.spawn(move || write(&storage, index));
        }
    });
}

/// Provider logins rather than `create_user`: they write the same document
/// without password hashing, so the threads actually overlap on the write.
#[test]
fn concurrent_user_writes_all_survive() {
    let storage = temp_storage("settings-cas-users");

    concurrently(&storage, |storage, index| {
        auth_store::upsert_provider_user(
            storage,
            "github",
            &format!("subject-{index}"),
            None,
            None,
            Role::Viewer,
        )
        .expect("upsert the provider user");
    });

    let users = auth_store::list_users(&*storage).expect("list users");
    assert_eq!(users.len(), WRITERS, "no user may be lost: {users:?}");
    for index in 0..WRITERS {
        assert!(users.contains_key(&format!("github:subject-{index}")));
    }
}

#[test]
fn concurrent_webhook_creates_all_survive() {
    let storage = temp_storage("settings-cas-webhooks");

    concurrently(&storage, |storage, index| {
        let subscription = WebhookSubscription::new(format!("https://example.com/hook/{index}"));
        webhooks::create(storage, &subscription).expect("create the subscription");
    });

    let stored = webhooks::list_all(&*storage).expect("list subscriptions");
    assert_eq!(stored.len(), WRITERS, "no subscription may be lost");
}

#[test]
fn concurrent_middleware_toggles_all_survive() {
    let storage = temp_storage("settings-cas-middleware");

    concurrently(&storage, |storage, index| {
        middleware::set_disabled(storage, "resize", &format!("mw-{index}"), true)
            .expect("toggle the middleware off");
    });

    let disabled = middleware::get_for(&*storage, "resize").expect("read the disable list");
    assert_eq!(
        disabled.len(),
        WRITERS,
        "no toggle may be lost: {disabled:?}"
    );
}

#[test]
fn a_write_that_lost_the_race_is_refused_rather_than_applied() {
    let storage = temp_storage("settings-cas-refusal");
    storage.set_setting("k", "v1").expect("seed the row");

    // What a losing writer holds: the value it read, already replaced.
    storage.set_setting("k", "v2").expect("the other writer");

    assert!(
        !storage
            .set_setting_if("k", Some("v1"), "stale")
            .expect("storage"),
        "a stale expectation must not write"
    );
    assert_eq!(
        storage.get_setting("k").expect("storage").as_deref(),
        Some("v2")
    );
}
