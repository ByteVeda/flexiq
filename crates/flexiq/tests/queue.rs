//! The handle, driven through a hand-written `Task`.
//!
//! Hand-written on purpose: the macro is ergonomics, and everything below has
//! to work without it. It is also what a caller writing an unusual task does.

use flexiq::{EnqueueOptions, FlexiQ, Outcome, Task, TaskCall};
use flexiq_core::{Job, TaskConfig};

/// A task that takes one name and returns nothing.
struct Greet;

impl Task for Greet {
    const NAME: &'static str = "greet";

    fn config() -> TaskConfig {
        TaskConfig::default()
    }

    fn defaults() -> EnqueueOptions {
        EnqueueOptions::default()
    }

    fn run_encoded(_job: &Job) -> Outcome<Option<Vec<u8>>> {
        Ok(None)
    }
}

/// `Greet::call(name)`, as the macro would generate it.
fn greet(name: &str) -> TaskCall<Greet> {
    TaskCall::from_args(flexiq::__private::encode_args(&[
        flexiq::__private::to_wire(&name).expect("encodable"),
    ]))
}

#[test]
fn an_enqueued_job_carries_the_task_name_and_a_tagged_payload() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(greet("world")).expect("enqueues");

    assert_eq!(job.task_name, "greet");
    assert_eq!(job.payload[0], 0x02, "payload must carry the CBOR tag");
    assert_eq!(job.queue, "default");
    assert_eq!(job.max_retries, 3);
    assert_eq!(job.timeout_ms, 300_000);
}

#[test]
fn per_call_options_reach_the_job() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q
        .enqueue(
            greet("world")
                .queue("billing")
                .priority(9)
                .max_retries(7)
                .timeout_ms(1_234),
        )
        .expect("enqueues");

    assert_eq!(job.queue, "billing");
    assert_eq!(job.priority, 9);
    assert_eq!(job.max_retries, 7);
    assert_eq!(job.timeout_ms, 1_234);
}

#[test]
fn a_delay_moves_the_job_into_the_future() {
    let q = FlexiQ::in_memory().expect("opens");
    let before = flexiq_core::now_millis();
    let job = q
        .enqueue(greet("world").delay_ms(60_000))
        .expect("enqueues");

    assert!(job.scheduled_at >= before + 60_000);
}

#[test]
fn a_unique_key_dedupes_a_second_enqueue() {
    let q = FlexiQ::in_memory().expect("opens");
    let first = q
        .enqueue(greet("world").unique_key("only-once"))
        .expect("enqueues");
    let second = q
        .enqueue(greet("world").unique_key("only-once"))
        .expect("enqueues");

    assert_eq!(
        first.id, second.id,
        "the second call must find the first job"
    );
}

/// The `auto:` key is a cross-language identity, so it is pinned against the
/// digest the other shells compute for the same call — not merely asserted to
/// be stable against itself. Separator is a NUL byte and the digest is
/// truncated to 32 hex characters; getting either wrong still enqueues, and
/// silently stops deduping against a caller in another language.
#[test]
fn idempotent_derives_the_cross_language_auto_key() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(greet("world").idempotent()).expect("enqueues");

    assert_eq!(
        job.unique_key.as_deref(),
        Some("auto:c3abb8ce58c62cb737c17b92beb816be")
    );
}

/// An explicit key wins over the derived one: a caller who names an identity
/// has said something the payload's bytes cannot.
#[test]
fn an_explicit_unique_key_beats_the_derived_one() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q
        .enqueue(greet("world").idempotent().unique_key("mine"))
        .expect("enqueues");

    assert_eq!(job.unique_key.as_deref(), Some("mine"));
}

#[test]
fn a_batch_enqueues_every_call() {
    let q = FlexiQ::in_memory().expect("opens");
    let jobs = q
        .enqueue_batch(vec![greet("a"), greet("b"), greet("c")])
        .expect("enqueues");

    assert_eq!(jobs.len(), 3);
    assert!(jobs.iter().all(|job| job.task_name == "greet"));
}

/// Storage has no batched debounce, and looping it item by item would cost the
/// Diesel backends the atomicity that is the reason to send a batch at all. The
/// server's own door refuses the same combination.
#[test]
fn a_batch_containing_a_debounced_call_is_refused() {
    let q = FlexiQ::in_memory().expect("opens");
    let err = q
        .enqueue_batch(vec![greet("a"), greet("b").debounce_ms("k", 500, 5_000)])
        .expect_err("a debounced item must not ride a batch");

    assert!(err.to_string().contains("debounce"), "message: {err}");
}

/// The window's three values travel together, so the shell takes them
/// together. The other shells have to refuse a partial window at runtime — an
/// absent `max_wait_ms` is an unbounded debounce, which starves the job — and
/// here that call does not compile.
#[test]
fn a_debounced_enqueue_coalesces_on_its_key() {
    let q = FlexiQ::in_memory().expect("opens");
    let first = q
        .enqueue(greet("a").debounce_ms("same-key", 60_000, 300_000))
        .expect("enqueues");
    let second = q
        .enqueue(greet("b").debounce_ms("same-key", 60_000, 300_000))
        .expect("enqueues");

    assert_eq!(first.id, second.id, "the window must coalesce onto one job");
}

#[test]
fn a_job_reads_back_by_id() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(greet("world")).expect("enqueues");

    let read = q.get_job(&job.id).expect("reads").expect("job exists");
    assert_eq!(read.id, job.id);
    assert!(q.get_job("no-such-job").expect("reads").is_none());
}

#[test]
fn cancelling_a_pending_job_reports_true() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(greet("world")).expect("enqueues");

    assert!(q.cancel(&job.id).expect("cancels"));
    assert!(!q.cancel("no-such-job").expect("cancels"));
}

#[test]
fn stats_count_a_pending_job() {
    let q = FlexiQ::in_memory().expect("opens");
    q.enqueue(greet("world")).expect("enqueues");

    let stats = q.stats().expect("reads stats");
    assert_eq!(stats.pending, 1);
}

// ── The macro ────────────────────────────────────────────────────────

/// Charge an order.
#[flexiq::task(max_retries = 5, timeout = "30s", queue = "billing", priority = 3)]
fn charge(order_id: String, cents: i64) -> flexiq::Outcome<i64> {
    let _ = order_id;
    Ok(cents)
}

#[flexiq::task(name = "billing.refund")]
fn refund(cents: i64) -> flexiq::Outcome<()> {
    let _ = cents;
    Ok(())
}

#[flexiq::task]
fn heartbeat() -> flexiq::Outcome<()> {
    Ok(())
}

/// A `cfg` under the attribute has to reach every generated item, or the task
/// stays compiled in under a condition that said not to.
///
/// This file *is* a test binary, so `not(test)` is false here and the
/// `compile_error!` below must never fire. That it does not is the assertion —
/// before the fix, the macro dropped the `cfg` and this body was compiled.
#[cfg(not(test))]
#[flexiq::task]
fn never_compiled(_n: i64) -> flexiq::Outcome<()> {
    compile_error!("the cfg was dropped: this body must never be compiled");
}

/// The mirror case: a `cfg` that holds leaves the task present.
#[cfg(test)]
#[flexiq::task]
fn always_compiled(_n: i64) -> flexiq::Outcome<()> {
    Ok(())
}

/// A doc comment survives onto the generated type, and so does an `allow`.
#[allow(clippy::needless_pass_by_value)]
/// Documented, so `#![deny(missing_docs)]` in a caller's crate is satisfied.
#[flexiq::task]
fn documented(_n: String) -> flexiq::Outcome<()> {
    Ok(())
}

#[test]
fn a_cfg_gate_reaches_every_generated_item() {
    // `never_compiled` is absent entirely; naming it would not compile. That
    // this file builds at all is the assertion.
    assert_eq!(<always_compiled as Task>::NAME, "always_compiled");
    assert_eq!(<documented as Task>::NAME, "documented");
}

#[test]
fn the_macro_carries_its_attributes_into_the_job() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q
        .enqueue(charge::call("ord-1".into(), 4200))
        .expect("enqueues");

    assert_eq!(job.task_name, "charge");
    assert_eq!(job.queue, "billing");
    assert_eq!(job.max_retries, 5);
    assert_eq!(job.timeout_ms, 30_000);
    assert_eq!(job.priority, 3);
}

/// The retry cap reaches both halves: the job row *and* the scheduler's config.
#[test]
fn max_retries_reaches_the_dispatch_config_too() {
    assert_eq!(<charge as Task>::config().retry_policy.max_retries, 5);
}

#[test]
fn an_explicit_name_overrides_the_function_name() {
    assert_eq!(<refund as Task>::NAME, "billing.refund");
}

/// A task with no parameters skips argument decoding entirely — an empty
/// argument array is not a unit value.
#[test]
fn a_task_with_no_parameters_encodes_an_empty_argument_list() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(heartbeat::call()).expect("enqueues");

    assert_eq!(job.task_name, "heartbeat");
    assert_eq!(job.payload, vec![0x02, 0x82, 0x80, 0xa0]);
}

/// The body stays callable, so it can be unit-tested without a queue at all.
#[test]
fn the_original_body_is_still_directly_callable() {
    assert_eq!(charge::run("ord-1".into(), 4200).expect("runs"), 4200);
}

#[test]
fn listing_finds_the_enqueued_job() {
    let q = FlexiQ::in_memory().expect("opens");
    let job = q.enqueue(greet("world")).expect("enqueues");

    let listed = q.list_jobs(10, 0).expect("lists");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, job.id);
}
