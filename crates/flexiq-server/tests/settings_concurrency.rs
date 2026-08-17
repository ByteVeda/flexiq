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

use flexiq_core::{Storage, StorageBackend};
use flexiq_server::dashboard::auth::model::Role;
use flexiq_server::dashboard::auth::store as auth_store;
use flexiq_server::dashboard::stores::webhooks::WebhookSubscription;
use flexiq_server::dashboard::stores::{middleware, webhooks};

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
fn emptying_the_disable_list_leaves_the_row_behind() {
    // The removal used to delete the row *after* the compare-and-set returned,
    // so an entry added by another writer between the swap and the delete was
    // wiped by it. Asserted as an invariant rather than by racing threads: the
    // window is a few instructions wide, and a test that has to win a race to
    // fail is a test that reports green on a broken build.
    let storage = temp_storage("settings-cas-middleware-empty");

    middleware::set_disabled(&*storage, "resize", "mw-1", true).expect("disable");
    let left = middleware::set_disabled(&*storage, "resize", "mw-1", false).expect("re-enable");
    assert!(left.is_empty(), "the list is empty again: {left:?}");

    assert_eq!(
        storage
            .get_setting("middleware:disabled:resize")
            .expect("read the row"),
        Some("[]".to_string()),
        "an emptied list must leave its row, so no delete can race a concurrent add"
    );
    // Nothing reads the difference: the row parses as "nothing disabled" and
    // never reaches the listing.
    assert!(middleware::get_for(&*storage, "resize")
        .expect("read the disable list")
        .is_empty());
    assert!(middleware::list_all(&*storage)
        .expect("list disables")
        .is_empty());
}

#[test]
fn concurrent_middleware_toggles_survive_both_directions() {
    // Both directions on one task name, guarding the compare-and-set itself.
    let storage = temp_storage("settings-cas-middleware-mixed");

    // Half the writers own an entry and keep it; the other half add theirs and
    // immediately take it away, emptying the list whenever they are last.
    concurrently(&storage, |storage, index| {
        let name = format!("mw-{index}");
        if index % 2 == 0 {
            middleware::set_disabled(storage, "resize", &name, true).expect("disable");
        } else {
            middleware::set_disabled(storage, "resize", &name, true).expect("disable");
            middleware::set_disabled(storage, "resize", &name, false).expect("re-enable");
        }
    });

    let disabled = middleware::get_for(&*storage, "resize").expect("read the disable list");
    for index in (0..WRITERS).step_by(2) {
        let name = format!("mw-{index}");
        assert!(
            disabled.contains(&name),
            "{name} was set and never unset, so it must survive: {disabled:?}"
        );
    }
    for index in (1..WRITERS).step_by(2) {
        let name = format!("mw-{index}");
        assert!(
            !disabled.contains(&name),
            "{name} was unset by its own writer: {disabled:?}"
        );
    }
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
