//! The `#[task]` attribute for the FlexiQ Rust SDK.
//!
//! This crate is an implementation detail of [`flexiq`](https://docs.rs/flexiq);
//! reach the macro as `flexiq::task`, which re-exports it. Depending on this
//! crate directly gets you a macro whose expansion names `::flexiq::` and will
//! not compile without it.

#![deny(missing_docs)]

use proc_macro::TokenStream;
use syn::{parse_macro_input, ItemFn};

mod attrs;
mod duration;
mod expand;

/// Register a function as a FlexiQ task.
///
/// The function is replaced by a type of the same name carrying three things:
/// `call(..)`, which builds an enqueueable [`flexiq::TaskCall`][tc] with the
/// same argument list; `run(..)`, the original body, still directly callable;
/// and an implementation of `flexiq::Task`, which is what a worker registers.
///
/// [tc]: https://docs.rs/flexiq/latest/flexiq/struct.TaskCall.html
///
/// ```ignore
/// #[flexiq::task(max_retries = 5, timeout = "30s", queue = "billing")]
/// fn charge(order_id: String, cents: i64) -> flexiq::Outcome<i64> {
///     Ok(cents)
/// }
///
/// queue.enqueue(charge::call("ord-1".into(), 4200))?;
/// queue.worker().register::<charge>().spawn()?;
/// ```
///
/// # The task's name
///
/// A task is named after its function — `"charge"` above — unless `name = "..."`
/// says otherwise. Deliberately not the module path: that would embed the crate
/// name, so renaming a binary would change a name a producer in another
/// language has to type.
///
/// # Attributes
///
/// | Attribute | Effect |
/// |---|---|
/// | `name` | The task name a job carries. Defaults to the function's name. |
/// | `queue` | Which queue to enqueue on. |
/// | `priority` | Dispatch priority; higher runs first. |
/// | `max_retries` | Retries before the job dead-letters. |
/// | `retry_backoff_ms`, `retry_max_delay_ms` | The retry policy's base and cap. |
/// | `timeout` | One attempt's execution cap. |
/// | `expires` | How long a pending job stays eligible. |
/// | `result_ttl` | How long the archived result is kept. |
/// | `idempotent` | Derive the dedup key from the name and payload. |
/// | `on_excess` | `"defer"` or `"drop"` when a rate limit turns the job away. |
/// | `max_concurrent` | Cluster-wide cap on concurrent runs. |
/// | `max_in_flight_per_task` | This task's share of one worker's slots. |
/// | `rate_limit`, `retry_budget` | Rates, as `100/s`, `60/m` or `1000/h`. |
/// | `cron`, `timezone` | Register the task as a periodic. |
///
/// Durations accept `500ms`, `30s`, `5m`, `2h`, `1d`, or a bare integer of
/// milliseconds. A rate's count has to be at least one — a bucket that never
/// holds a whole token never releases a job.
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    let parsed = match attrs::TaskAttrs::parse(attr.into()) {
        Ok(parsed) => parsed,
        Err(error) => return error.to_compile_error().into(),
    };
    let item = parse_macro_input!(item as ItemFn);

    match expand::task(parsed, item) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.to_compile_error().into(),
    }
}
