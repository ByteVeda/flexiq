use log::{error, warn};

use crate::error::Result;
use crate::job::JobCompletion;
use crate::storage::records::AttemptFence;
use crate::storage::Storage;

use super::{JobResult, ResultOutcome, Scheduler};

/// Dead-letter metadata marking a job the retry budget refused. `ResultOutcome`
/// has no room to say *why* a job was dead-lettered, so this is what tells a
/// budget kill apart from ordinary retry exhaustion when reading the DLQ.
pub const RETRY_BUDGET_EXHAUSTED: &str = "retry_budget_exhausted";

impl Scheduler {
    /// Free a finished job's in-flight slot and decide whether its result still
    /// speaks for the job.
    ///
    /// `false` means superseded: the job is proceeding under another owner —
    /// after a reclaim, or a retry that already bumped the attempt — and the
    /// only correct contribution this attempt can make is none. Dropped with a
    /// warning rather than failed or retried, because failing it would kill a
    /// run proceeding correctly elsewhere.
    ///
    /// A job this scheduler never dispatched has no token to check against — a
    /// duplicate result, or a foreign id — and is left to the transitions' own
    /// guards, exactly as before the fence existed.
    fn authorize_finished(&self, job_id: &str) -> Result<bool> {
        let Some(record) = self.release_in_flight(job_id) else {
            return Ok(true);
        };
        // The fence fails **open**, and deliberately. Propagating the error
        // would drop a result whose dispatch record has already been consumed,
        // leaving a finished job `Running` with nothing to finish it until the
        // stale reaper times it out — a storage blip turned into a phantom
        // timeout. Failing open costs at most what every caller had before the
        // fence existed, and the transitions below carry their own `Running`
        // guards.
        let fence = match self.storage.authorize_attempt(
            job_id,
            &record.owner,
            record.attempt,
            self.namespace.as_deref(),
        ) {
            Ok(fence) => fence,
            Err(e) => {
                error!("could not check the fence for job {job_id}; handling anyway: {e}");
                AttemptFence::Authorized
            }
        };
        if fence == AttemptFence::Superseded {
            warn!(
                "dropping superseded result for job {job_id} from attempt {}",
                record.attempt
            );
            return Ok(false);
        }
        Ok(true)
    }

    /// Handle a completed or failed job result from a worker.
    ///
    /// Returns a [`ResultOutcome`] describing the action taken, so the
    /// caller (the binding) can dispatch its middleware hooks and events.
    pub fn handle_result(&self, result: JobResult) -> Result<ResultOutcome> {
        // A sleep skips the fence, and deliberately. `sleep_job` already left
        // the job `Pending` with no claim — that is what a sleep *is* — so
        // asking whether the attempt still owns it reads a correctly slept job
        // as superseded and drops the one outcome that explains where it went.
        // The write was fenced where it happened; re-fencing the
        // acknowledgement of it is the wrong question. `finalize_sleep` frees
        // the in-flight slot itself.
        let slept = matches!(result, JobResult::Slept { .. });
        // A dispatched job finished — free its in-flight slot and take the
        // token it was dispatched under. `None` for a job this scheduler never
        // dispatched (a duplicate result, or a foreign id), which has nothing to
        // validate against and is left to the transitions' own guards.
        if !slept && !self.authorize_finished(result.job_id())? {
            return Ok(ResultOutcome::Superseded {
                job_id: result.job_id().to_string(),
            });
        }
        match result {
            JobResult::Success {
                job_id,
                result,
                task_name,
                wall_time_ns,
            } => self.finalize_success(&JobCompletion {
                job_id,
                result,
                task_name,
                wall_time_ns,
            }),
            JobResult::Failure {
                job_id,
                error,
                retry_count,
                max_retries,
                task_name,
                wall_time_ns,
                should_retry,
                timed_out,
            } => {
                // The claim is *not* cleared here. Clearing it now and
                // transitioning a dozen statements later leaves the job
                // `Running` with no claim for the whole span in between, and a
                // late write from the failing attempt lands squarely in that
                // window, takes the claim back, and commits a step the retry
                // then replays as a memo hit. Every transition below —
                // `retry`, and the DLQ move's archive — revokes the claim
                // inside its own transaction instead.
                if let Err(e) = self.storage.record_error(
                    &job_id,
                    retry_count,
                    &error,
                    self.namespace.as_deref(),
                ) {
                    log::error!("failed to record error for job {job_id}: {e}");
                }

                if let Err(e) = self.storage.record_metric(
                    &task_name,
                    &job_id,
                    wall_time_ns,
                    0,
                    false,
                    self.namespace.as_deref(),
                ) {
                    log::error!("failed to record metric for job {job_id}: {e}");
                }

                if let Err(e) = self.circuit_breaker.record_failure(&task_name) {
                    log::error!("circuit breaker error for {task_name}: {e}");
                }

                // One fetch serves both the queue-context lookup and any
                // subsequent DLQ move — there's no path that needs two reads.
                let job = self.storage.get_job(&job_id, self.namespace.as_deref())?;
                let queue = job.as_ref().map(|j| j.queue.clone()).unwrap_or_default();

                let move_to_dlq = |job: Option<&crate::job::Job>,
                                   metadata: Option<&str>|
                 -> Result<()> {
                    match job {
                        Some(j) => self.dlq.move_to_dlq(j, &error, metadata),
                        None => {
                            // The one branch that runs no transition, so the
                            // one that has no transaction to revoke the claim
                            // inside. Left alone it would point at a job that
                            // no longer exists until the age sweep collects it.
                            warn!("job {job_id} disappeared before DLQ move");
                            if let Err(e) = self
                                .storage
                                .complete_execution(&job_id, self.namespace.as_deref())
                            {
                                error!("failed to clear the claim of vanished job {job_id}: {e}");
                            }
                            Ok(())
                        }
                    }
                };

                // If should_retry is false (exception filtering), skip straight to DLQ
                if !should_retry {
                    move_to_dlq(job.as_ref(), None)?;
                    return Ok(ResultOutcome::DeadLettered {
                        job_id,
                        task_name,
                        queue,
                        error,
                        timed_out,
                        wall_time_ns,
                    });
                }

                let policy = self
                    .task_configs
                    .get(&task_name)
                    .map(|c| c.retry_policy.clone())
                    .unwrap_or_default();

                // job.max_retries is the budget resolved at enqueue (the queue
                // default or the caller's explicit value), so honor it exactly —
                // treating a stored 0 as "unset" would silently re-run a job the
                // caller marked at-most-once. policy is still used for backoff.
                let effective_max = max_retries;

                if retry_count < effective_max {
                    // Checked here, not before the per-job budget: a job that was
                    // never going to retry must not spend a token, or a task at
                    // its retry ceiling would drain the budget for its siblings.
                    if !self.retry_budget_allows(&task_name) {
                        warn!("retry budget exhausted for {task_name}; dead-lettering {job_id}");
                        move_to_dlq(job.as_ref(), Some(RETRY_BUDGET_EXHAUSTED))?;
                        return Ok(ResultOutcome::DeadLettered {
                            job_id,
                            task_name,
                            queue,
                            error,
                            timed_out,
                            wall_time_ns,
                        });
                    }
                    let next_at = policy.next_retry_at(retry_count);
                    self.storage
                        .retry(&job_id, next_at, self.namespace.as_deref())?;
                    #[cfg(feature = "push-dispatch")]
                    self.signal_scheduled(next_at);
                    Ok(ResultOutcome::Retry {
                        job_id,
                        task_name,
                        queue,
                        error,
                        retry_count,
                        timed_out,
                        wall_time_ns,
                    })
                } else {
                    move_to_dlq(job.as_ref(), None)?;
                    Ok(ResultOutcome::DeadLettered {
                        job_id,
                        task_name,
                        queue,
                        error,
                        timed_out,
                        wall_time_ns,
                    })
                }
            }
            JobResult::Cancelled {
                job_id,
                task_name,
                wall_time_ns,
            } => {
                // `mark_cancelled` archives the job and revokes the claim in
                // one transaction, so there is no window to clear it in first.
                // Mark as cancelled, no retry
                if let Err(e) = self
                    .storage
                    .mark_cancelled(&job_id, self.namespace.as_deref())
                {
                    error!("failed to mark job {job_id} as cancelled: {e}");
                }
                if let Err(e) = self.storage.record_metric(
                    &task_name,
                    &job_id,
                    wall_time_ns,
                    0,
                    false,
                    self.namespace.as_deref(),
                ) {
                    error!("failed to record metric for cancelled job {job_id}: {e}");
                }
                let queue = self
                    .storage
                    .get_job(&job_id, self.namespace.as_deref())?
                    .as_ref()
                    .map(|j| j.queue.clone())
                    .unwrap_or_default();
                Ok(ResultOutcome::Cancelled {
                    job_id,
                    task_name,
                    queue,
                    wall_time_ns,
                })
            }
            JobResult::Slept {
                job_id,
                task_name,
                wake_at,
                wall_time_ns,
            } => self.finalize_sleep(job_id, task_name, wake_at, wall_time_ns),
        }
    }

    /// Handle a batch of results drained from the worker channel in one wake.
    ///
    /// Successful completions are persisted together via a single
    /// [`Storage::complete_batch`] (one transaction / fsync instead of three
    /// writes × N jobs across N transactions); failures and cancellations keep
    /// the branchy per-result path. Returns one outcome per input result, in the
    /// same order, so the caller still dispatches middleware and events exactly
    /// once per job. On a batch-write error the successes fall back to the
    /// proven single-job finalize, so one bad row never drops the whole batch.
    pub fn handle_results(&self, results: Vec<JobResult>) -> Vec<Result<ResultOutcome>> {
        let mut outcomes: Vec<Option<Result<ResultOutcome>>> =
            (0..results.len()).map(|_| None).collect();
        let mut completions: Vec<JobCompletion> = Vec::new();
        let mut success_idx: Vec<usize> = Vec::new();

        for (i, result) in results.into_iter().enumerate() {
            match result {
                JobResult::Success {
                    job_id,
                    result,
                    task_name,
                    wall_time_ns,
                } => {
                    // Each success is a finished job — free its in-flight slot
                    // and fence it on the token it was dispatched under, exactly
                    // as the single-result path does. The batch is the default
                    // drain path, so skipping the fence here would leave it
                    // guarding nothing.
                    match self.authorize_finished(&job_id) {
                        Ok(true) => {
                            success_idx.push(i);
                            completions.push(JobCompletion {
                                job_id,
                                result,
                                task_name,
                                wall_time_ns,
                            });
                        }
                        Ok(false) => outcomes[i] = Some(Ok(ResultOutcome::Superseded { job_id })),
                        Err(e) => outcomes[i] = Some(Err(e)),
                    }
                }
                // Failures and cancellations branch (retry vs DLQ, queue
                // lookups); batching them buys little, so keep the per-result path.
                other => outcomes[i] = Some(self.handle_result(other)),
            }
        }

        if !completions.is_empty() {
            match self
                .storage
                .complete_batch(&completions, self.namespace.as_deref())
            {
                Ok(()) => {
                    for (&idx, c) in success_idx.iter().zip(&completions) {
                        if let Err(e) = self.circuit_breaker.record_success(&c.task_name) {
                            error!("circuit breaker error for {}: {e}", c.task_name);
                        }
                        outcomes[idx] = Some(Ok(ResultOutcome::Success {
                            job_id: c.job_id.clone(),
                            task_name: c.task_name.clone(),
                            wall_time_ns: c.wall_time_ns,
                        }));
                    }
                }
                Err(e) => {
                    warn!("batch complete failed; finalizing successes per job: {e}");
                    for (&idx, c) in success_idx.iter().zip(&completions) {
                        outcomes[idx] = Some(self.finalize_success(c));
                    }
                }
            }
        }

        // Every slot was filled — non-success inline, success in the batch step.
        outcomes
            .into_iter()
            .map(|o| o.expect("every result yields an outcome"))
            .collect()
    }

    /// Release the slot a slept attempt held, and report where its job went.
    ///
    /// Writes nothing. The three writes a sleep needs — the step row, the claim
    /// revocation, the reschedule — were one transaction inside
    /// [`Storage::sleep_job`](crate::storage::Storage::sleep_job); by the time
    /// the result arrives the job is already `Pending` at its deadline.
    ///
    /// And nothing else is touched, because nothing failed: no `retry_count`
    /// (this is a `reschedule`, not a `retry`), no retry-budget token (only the
    /// failure path spends one), no circuit-breaker sample, no `job_errors`
    /// row, and no `task_metrics` row — `succeeded = true` would inflate the
    /// success count and `false` the failure count, and neither happened. The
    /// cost is that per-attempt CPU time is invisible for a job that sleeps
    /// several times; the final success metric still covers the job.
    fn finalize_sleep(
        &self,
        job_id: String,
        task_name: String,
        wake_at: i64,
        wall_time_ns: i64,
    ) -> Result<ResultOutcome> {
        self.release_in_flight(&job_id);
        let queue = self
            .storage
            .get_job(&job_id, self.namespace.as_deref())?
            .as_ref()
            .map(|j| j.queue.clone())
            .unwrap_or_default();
        // The job is scheduled, not running: tell the poller when to come back,
        // exactly as a retry does.
        #[cfg(feature = "push-dispatch")]
        self.signal_scheduled(wake_at);
        Ok(ResultOutcome::Slept {
            job_id,
            task_name,
            queue,
            wake_at,
            wall_time_ns,
        })
    }

    /// Persist one successful completion and return its outcome. Shared by the
    /// single-result path and by [`Self::handle_results`]' per-job fallback so
    /// the success-finalize logic lives in exactly one place.
    fn finalize_success(&self, c: &JobCompletion) -> Result<ResultOutcome> {
        self.storage
            .complete(&c.job_id, c.result.clone(), self.namespace.as_deref())?;

        // Clear execution claim
        if let Err(e) = self
            .storage
            .complete_execution(&c.job_id, self.namespace.as_deref())
        {
            error!("failed to clear execution claim for job {}: {e}", c.job_id);
        }

        if let Err(e) = self.storage.record_metric(
            &c.task_name,
            &c.job_id,
            c.wall_time_ns,
            0,
            true,
            self.namespace.as_deref(),
        ) {
            error!("failed to record metric for job {}: {e}", c.job_id);
        }

        if let Err(e) = self.circuit_breaker.record_success(&c.task_name) {
            error!("circuit breaker error for {}: {e}", c.task_name);
        }

        Ok(ResultOutcome::Success {
            job_id: c.job_id.clone(),
            task_name: c.task_name.clone(),
            wall_time_ns: c.wall_time_ns,
        })
    }
}
