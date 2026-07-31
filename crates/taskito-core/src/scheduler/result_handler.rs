use log::{error, warn};

use crate::error::Result;
use crate::job::JobCompletion;
use crate::storage::Storage;

use super::{JobResult, ResultOutcome, Scheduler};

/// Dead-letter metadata marking a job the retry budget refused. `ResultOutcome`
/// has no room to say *why* a job was dead-lettered, so this is what tells a
/// budget kill apart from ordinary retry exhaustion when reading the DLQ.
pub const RETRY_BUDGET_EXHAUSTED: &str = "retry_budget_exhausted";

impl Scheduler {
    /// Handle a completed or failed job result from a worker.
    ///
    /// Returns a [`ResultOutcome`] describing the action taken, so the
    /// caller (the binding) can dispatch its middleware hooks and events.
    pub fn handle_result(&self, result: JobResult) -> Result<ResultOutcome> {
        // A dispatched job finished — free its in-flight slot (no-op for jobs
        // this scheduler didn't dispatch, e.g. maintenance-recovered orphans).
        self.release_in_flight(result.job_id());
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
                // Clear execution claim so it can be retried
                if let Err(e) = self.storage.complete_execution(&job_id) {
                    log::error!("failed to clear execution claim for job {job_id}: {e}");
                }

                if let Err(e) = self.storage.record_error(&job_id, retry_count, &error) {
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
                let job = self.storage.get_job(&job_id)?;
                let queue = job.as_ref().map(|j| j.queue.clone()).unwrap_or_default();

                let move_to_dlq =
                    |job: Option<&crate::job::Job>, metadata: Option<&str>| -> Result<()> {
                        match job {
                            Some(j) => self.dlq.move_to_dlq(j, &error, metadata),
                            None => {
                                warn!("job {job_id} disappeared before DLQ move");
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
                    self.storage.retry(&job_id, next_at)?;
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
                // Clear execution claim
                if let Err(e) = self.storage.complete_execution(&job_id) {
                    error!("failed to clear execution claim for job {job_id}: {e}");
                }
                // Mark as cancelled, no retry
                if let Err(e) = self.storage.mark_cancelled(&job_id) {
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
                    .get_job(&job_id)?
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
                    success_idx.push(i);
                    completions.push(JobCompletion {
                        job_id,
                        result,
                        task_name,
                        wall_time_ns,
                    });
                }
                // Failures and cancellations branch (retry vs DLQ, queue
                // lookups); batching them buys little, so keep the per-result path.
                other => outcomes[i] = Some(self.handle_result(other)),
            }
        }

        // Each success is a finished job — free its in-flight slot. Non-success
        // results already released theirs via `handle_result` above.
        for c in &completions {
            self.release_in_flight(&c.job_id);
        }

        if !completions.is_empty() {
            match self.storage.complete_batch(&completions) {
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

    /// Persist one successful completion and return its outcome. Shared by the
    /// single-result path and by [`Self::handle_results`]' per-job fallback so
    /// the success-finalize logic lives in exactly one place.
    fn finalize_success(&self, c: &JobCompletion) -> Result<ResultOutcome> {
        self.storage.complete(&c.job_id, c.result.clone())?;

        // Clear execution claim
        if let Err(e) = self.storage.complete_execution(&c.job_id) {
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
