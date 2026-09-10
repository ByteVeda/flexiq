//! Per-enqueue options, and the one place they become a [`NewJob`].

// `DebounceOptions` is not on core's root re-export list, unlike every other
// record beside it. Named through its module rather than adding a re-export in
// this branch; filed as a follow-up.
use flexiq_core::storage::records::DebounceOptions;
use flexiq_core::{now_millis, NewJob};

/// The debounce window, whole.
///
/// A window is three values or none. The other shells accept them as three
/// independent optional arguments and refuse a partial set at runtime, because
/// an absent `max_wait_ms` is an unbounded debounce and starves the job. Here
/// the partial case does not exist.
#[derive(Debug, Clone)]
pub struct Debounce {
    /// The key a burst coalesces on.
    pub key: String,
    /// How far each new arrival slides the deadline.
    pub window_ms: i64,
    /// The ceiling, measured from the first arrival.
    pub max_wait_ms: i64,
    /// Whether a later arrival replaces the pending payload.
    pub replace_payload: bool,
    /// The target queue's admission cap. Only a debounced write carries one:
    /// it is the write whose caller cannot apply the cap itself, because only
    /// storage knows whether the call adds a row or slides an open window.
    pub max_pending: Option<i64>,
}

impl Debounce {
    /// A window with no payload replacement and no admission cap.
    pub fn new(key: impl Into<String>, window_ms: i64, max_wait_ms: i64) -> Self {
        Self {
            key: key.into(),
            window_ms,
            max_wait_ms,
            replace_payload: false,
            max_pending: None,
        }
    }

    /// Core's view of the window, without the key — storage takes that on the
    /// job row itself.
    pub(crate) fn options(&self) -> DebounceOptions {
        DebounceOptions {
            window_ms: self.window_ms,
            max_wait_ms: self.max_wait_ms,
            replace_payload: self.replace_payload,
            max_pending: self.max_pending,
        }
    }
}

/// Everything a caller may set on a single enqueue.
///
/// Core's [`NewJob`] has fifteen public fields, no `Default` and no builder, so
/// every caller today hand-writes all fifteen. This is that builder, with the
/// defaults the other shells already agree on: queue `default`, priority 0,
/// three retries, a five-minute timeout.
#[derive(Debug, Clone)]
pub struct EnqueueOptions {
    pub(crate) queue: String,
    pub(crate) priority: i32,
    pub(crate) delay_ms: Option<i64>,
    pub(crate) max_retries: i32,
    pub(crate) timeout_ms: i64,
    pub(crate) unique_key: Option<String>,
    pub(crate) idempotent: bool,
    pub(crate) metadata: Option<String>,
    pub(crate) notes: Option<String>,
    pub(crate) depends_on: Vec<String>,
    pub(crate) expires_in_ms: Option<i64>,
    pub(crate) result_ttl_ms: Option<i64>,
    pub(crate) namespace: Option<String>,
    pub(crate) debounce: Option<Debounce>,
}

impl Default for EnqueueOptions {
    fn default() -> Self {
        Self {
            queue: "default".into(),
            priority: 0,
            delay_ms: None,
            max_retries: 3,
            timeout_ms: 300_000,
            unique_key: None,
            idempotent: false,
            metadata: None,
            notes: None,
            depends_on: Vec::new(),
            expires_in_ms: None,
            result_ttl_ms: None,
            namespace: None,
            debounce: None,
        }
    }
}

/// The builder surface, emitted into two `impl` blocks so [`EnqueueOptions`]
/// and [`crate::TaskCall`] cannot drift apart.
///
/// The argument is the path from `self` to the options bag — nothing for
/// `EnqueueOptions`, `.options` for `TaskCall`. It has to be a `tt` sequence
/// rather than an `ident` because `.options` is two tokens, and the macro
/// expands *inside* an `impl` rather than emitting one because `TaskCall`'s
/// block is generic and `EnqueueOptions`' is not.
macro_rules! enqueue_setters {
    ($($via:tt)*) => {
            /// Run this job on a named queue.
            pub fn queue(mut self, queue: impl Into<String>) -> Self {
                self $($via)*.queue = queue.into();
                self
            }

            /// Dispatch ahead of lower-priority jobs.
            pub fn priority(mut self, priority: i32) -> Self {
                self $($via)*.priority = priority;
                self
            }

            /// Hold the job for `ms` milliseconds before it becomes eligible.
            pub fn delay_ms(mut self, ms: i64) -> Self {
                self $($via)*.delay_ms = Some(ms);
                self
            }

            /// Cap the retries before the job dead-letters.
            pub fn max_retries(mut self, max_retries: i32) -> Self {
                self $($via)*.max_retries = max_retries;
                self
            }

            /// Cap one attempt's execution time.
            pub fn timeout_ms(mut self, ms: i64) -> Self {
                self $($via)*.timeout_ms = ms;
                self
            }

            /// Deduplicate against any other non-terminal job with this key.
            ///
            /// Not an idempotency key: it releases when the job reaches a
            /// terminal state, so a caller retrying past that point enqueues a
            /// second job.
            pub fn unique_key(mut self, key: impl Into<String>) -> Self {
                self $($via)*.unique_key = Some(key.into());
                self
            }

            /// Derive the dedup key from the task name and payload.
            ///
            /// Loses to an explicit [`unique_key`](Self::unique_key): a caller
            /// who names an identity has said something the bytes cannot.
            pub fn idempotent(mut self) -> Self {
                self $($via)*.idempotent = true;
                self
            }

            /// Attach pre-encoded JSON metadata.
            pub fn metadata(mut self, metadata: impl Into<String>) -> Self {
                self $($via)*.metadata = Some(metadata.into());
                self
            }

            /// Attach structured notes: a JSON object of at most 15 fields.
            pub fn notes(mut self, notes: impl Into<String>) -> Self {
                self $($via)*.notes = Some(notes.into());
                self
            }

            /// Hold the job until these job ids have completed.
            pub fn depends_on(mut self, ids: impl IntoIterator<Item = impl Into<String>>) -> Self {
                self $($via)*.depends_on = ids.into_iter().map(Into::into).collect();
                self
            }

            /// Expire the job if it has not started within `ms` milliseconds.
            pub fn expires_in_ms(mut self, ms: i64) -> Self {
                self $($via)*.expires_in_ms = Some(ms);
                self
            }

            /// Keep the archived result for `ms` milliseconds.
            pub fn result_ttl_ms(mut self, ms: i64) -> Self {
                self $($via)*.result_ttl_ms = Some(ms);
                self
            }

            /// Scope the job to a tenant namespace.
            pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
                self $($via)*.namespace = Some(namespace.into());
                self
            }

            /// Coalesce a burst on `key` into one run.
            pub fn debounce_ms(
                mut self,
                key: impl Into<String>,
                window_ms: i64,
                max_wait_ms: i64,
            ) -> Self {
                self $($via)*.debounce = Some($crate::Debounce::new(key, window_ms, max_wait_ms));
                self
            }

            /// Set the whole debounce window, including payload replacement and
            /// the admission cap.
            pub fn debounce(mut self, debounce: $crate::Debounce) -> Self {
                self $($via)*.debounce = Some(debounce);
                self
            }
    };
}

pub(crate) use enqueue_setters;

impl EnqueueOptions {
    enqueue_setters!();
}

impl EnqueueOptions {
    /// Fill every [`NewJob`] field.
    ///
    /// Relative times resolve against one `now`, read once, so `scheduled_at`
    /// and `expires_at` cannot disagree by the width of the call.
    pub(crate) fn into_new_job(self, task_name: &str, payload: Vec<u8>) -> NewJob {
        let now = now_millis();
        NewJob {
            queue: self.queue,
            task_name: task_name.to_string(),
            payload,
            priority: self.priority,
            scheduled_at: now + self.delay_ms.unwrap_or(0),
            max_retries: self.max_retries,
            timeout_ms: self.timeout_ms,
            unique_key: self.unique_key,
            metadata: self.metadata,
            notes: self.notes,
            depends_on: self.depends_on,
            expires_at: self.expires_in_ms.map(|ms| now + ms),
            result_ttl_ms: self.result_ttl_ms,
            namespace: self.namespace,
            debounce_key: self.debounce.as_ref().map(|d| d.key.clone()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_fill_every_new_job_field() {
        let job = EnqueueOptions::default().into_new_job("charge", vec![0x02]);

        assert_eq!(job.queue, "default");
        assert_eq!(job.task_name, "charge");
        assert_eq!(job.payload, vec![0x02]);
        assert_eq!(job.priority, 0);
        assert_eq!(job.max_retries, 3);
        assert_eq!(job.timeout_ms, 300_000);
        assert!(job.unique_key.is_none());
        assert!(job.depends_on.is_empty());
        assert!(job.namespace.is_none());
        assert!(job.expires_at.is_none());
        assert!(job.debounce_key.is_none());
    }

    #[test]
    fn a_delay_moves_scheduled_at_forward() {
        let before = now_millis();
        let job = EnqueueOptions::default()
            .delay_ms(5_000)
            .into_new_job("charge", Vec::new());

        assert!(job.scheduled_at >= before + 5_000);
    }

    #[test]
    fn a_debounce_key_reaches_the_job_row() {
        let job = EnqueueOptions::default()
            .debounce_ms("burst", 500, 5_000)
            .into_new_job("charge", Vec::new());

        assert_eq!(job.debounce_key.as_deref(), Some("burst"));
    }
}
