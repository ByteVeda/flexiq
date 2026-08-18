use std::collections::HashMap;
use std::time::Duration;

use log::warn;
use tokio::sync::mpsc::error::TrySendError;

use crate::error::Result;
use crate::job::{now_millis, Job};
use crate::resilience::retry::desync_delay;
use crate::storage::Storage;

use super::{shed, Scheduler};

/// Delay before re-scheduling a circuit-broken job (ms).
const CIRCUIT_BREAKER_RETRY_DELAY_MS: i64 = 5000;

/// Delay before re-scheduling a rate-limited job (ms).
const RATE_LIMIT_RETRY_DELAY_MS: i64 = 1000;

/// Delay before re-scheduling a concurrency-limited job (ms).
const CONCURRENCY_RETRY_DELAY_MS: i64 = 500;

/// Delay before re-scheduling a job whose dispatch was rejected because the
/// worker channel was full or closed. Short enough that the worker pool
/// catches up on the next tick rather than waiting for the stale-job reaper.
const CHANNEL_BACKPRESSURE_RETRY_DELAY_MS: i64 = 100;

/// Delay before retrying a job whose execution claim errored transiently
/// (storage hiccup, pool exhaustion). Short — the job never actually ran, so
/// get it back to `Pending` quickly rather than waiting out the stale reaper.
const CLAIM_ERROR_RETRY_DELAY_MS: i64 = 250;

/// Default execution-claim owner when the binding has not set the worker's id
/// (tests / embedders that don't register a worker). Production bindings call
/// [`Scheduler::set_claim_owner`] with the real `worker_id` so dead-worker
/// recovery can attribute claims.
pub(super) const SCHEDULER_CLAIM_OWNER: &str = "scheduler";

/// Result of attempting to claim a job for dispatch. Each outcome needs
/// distinct handling, so they can't collapse to a bool.
enum ClaimOutcome {
    /// This scheduler now owns the claim — proceed to dispatch.
    Claimed,
    /// Another scheduler already holds the claim — leave the job to its owner.
    AlreadyClaimed,
    /// The claim attempt errored — roll the job back to `Pending`.
    Errored,
}

/// What the pre-claim gates decided about a job. A rejection is not one thing:
/// most gates want the job back later, but a rate limit on a task configured
/// with [`shed::OnExcess::Drop`] wants it gone.
enum GateDecision {
    /// No gate objected — claim and dispatch the job.
    Proceed,
    /// Put the job back with this reschedule delay, in milliseconds.
    Defer(i64),
    /// Dead-letter the job with this reason instead of running it.
    Shed(String),
}

/// The gate decision for a job a rate limiter just turned away.
fn rate_limited(on_excess: shed::OnExcess, scope: &str) -> GateDecision {
    match on_excess {
        shed::OnExcess::Defer => GateDecision::Defer(RATE_LIMIT_RETRY_DELAY_MS),
        shed::OnExcess::Drop => GateDecision::Shed(shed::rate_limit_shed_reason(scope)),
    }
}

/// Per-dispatch-cycle cache of the running-job counts the post-claim
/// concurrency gate consults. A same-task batch used to re-query
/// `count_running_by_task` once per job; here the count is read from storage
/// once, then served from memory for the rest of the batch. A job that is
/// rolled back after being provisionally counted calls [`GateCounts::release`]
/// to free its slot so later jobs in the batch observe it. The single-dispatch
/// path uses a fresh cache per call, so it loads at most once — identical to
/// the old per-job query. The cross-scheduler view can be one batch stale,
/// which is acceptable: the hard exactly-once guarantee is the execution claim,
/// not this best-effort cap.
#[derive(Default)]
struct GateCounts {
    task_running: HashMap<String, i64>,
    queue_running: HashMap<String, i64>,
}

impl GateCounts {
    /// Drop a provisionally-counted job from the cached counts after it was
    /// rolled back, so its slot is available to the rest of the batch. A no-op
    /// for a key not yet loaded — the next load reads the already-updated DB.
    fn release(&mut self, task_name: &str, queue: &str) {
        if let Some(n) = self.task_running.get_mut(task_name) {
            *n -= 1;
        }
        if let Some(n) = self.queue_running.get_mut(queue) {
            *n -= 1;
        }
    }
}

impl Scheduler {
    pub(super) fn try_dispatch(&self, job_tx: &tokio::sync::mpsc::Sender<Job>) -> Result<bool> {
        let now = now_millis();

        let active_queues = self.active_queues();
        if active_queues.is_empty() {
            return Ok(false);
        }

        // Stop dispatching once this scheduler has as many jobs in flight as its
        // workers can run — claiming more would only strand them `Running` and
        // starve peers sharing the DB. A freed slot re-wakes the poll loop.
        if self.in_flight_remaining() == Some(0) {
            return Ok(false);
        }

        // A full channel means every dequeue would claim and then immediately
        // roll back on `try_send` — pure storage churn. Wait for the pool to
        // drain; `try_send` below still guards the race where it fills between
        // this check and the hand-off.
        if job_tx.capacity() == 0 {
            return Ok(false);
        }

        let job = match self.storage.dequeue_from(
            &active_queues,
            now,
            self.namespace.as_deref(),
            &self.dispatch_orders,
        )? {
            Some(j) => j,
            None => return Ok(false),
        };

        // CoDel: shed a stale job before claiming (opt-in per queue). The job is
        // still Pending here, so the DLQ move is uniform across backends.
        let job = match self.codel_admit(vec![job], now)?.pop() {
            Some(j) => j,
            None => return Ok(false),
        };

        // A fresh cache for a single job loads each count at most once — the
        // same one query the old code issued.
        let mut counts = GateCounts::default();
        self.gate_and_dispatch(job, now, &mut counts, job_tx)
    }

    /// Batch variant of [`try_dispatch`]. Dequeues up to `batch_size` jobs,
    /// claims the whole batch in one round-trip, then runs the shared post-claim
    /// gate/concurrency/dispatch tail per job. The pre-sized batch is advisory —
    /// the hard per-task and per-queue caps are still enforced per job after the
    /// claim, rolling back any job that exceeds a limit.
    pub(super) fn try_dispatch_batch(
        &self,
        job_tx: &tokio::sync::mpsc::Sender<Job>,
    ) -> Result<bool> {
        let now = now_millis();

        let active_queues = self.active_queues();
        if active_queues.is_empty() {
            return Ok(false);
        }

        // Size the batch to the worker pool's free capacity so we never claim
        // more jobs than the channel can immediately accept. `try_send` still
        // guards each hand-off, but pre-sizing avoids needless claim/rollback
        // churn — including the full-channel case, where any claim would
        // immediately roll back.
        let capacity = job_tx.capacity();
        if capacity == 0 {
            return Ok(false);
        }
        let mut budget = self.config.batch_size.min(capacity);

        // Never claim past the in-flight cap: a drained batch that outran the
        // workers would mark the surplus `Running` and starve peer schedulers.
        if let Some(remaining) = self.in_flight_remaining() {
            if remaining == 0 {
                return Ok(false);
            }
            budget = budget.min(remaining);
        }

        let jobs = self.storage.dequeue_batch_from(
            &active_queues,
            now,
            self.namespace.as_deref(),
            budget,
            &self.dispatch_orders,
        )?;
        if jobs.is_empty() {
            return Ok(false);
        }

        // CoDel: shed stale jobs before claiming (opt-in per queue). Dropped
        // jobs are still Pending, so the DLQ move is uniform across backends and
        // leaves no execution claim to unwind.
        let jobs = self.codel_admit(jobs, now)?;
        if jobs.is_empty() {
            return Ok(false);
        }

        // Claim the entire batch in one round-trip instead of one per job. A
        // storage error leaves the batch claim atomic-nothing (failed statement
        // / rolled-back txn), so degrade to the proven single-job path where
        // each job claims and error-handles itself.
        let ids: Vec<&str> = jobs.iter().map(|job| job.id.as_str()).collect();
        let claimed = match self.storage.claim_execution_batch(&ids, &self.claim_owner) {
            Ok(flags) => flags,
            Err(e) => {
                warn!("batch claim failed; dispatching per job: {e}");
                let mut counts = GateCounts::default();
                let mut dispatched_any = false;
                for job in jobs {
                    match self.gate_and_dispatch(job, now, &mut counts, job_tx) {
                        Ok(true) => dispatched_any = true,
                        Ok(false) => {}
                        Err(e) => warn!("batch dispatch failed for a claimed job: {e}"),
                    }
                }
                return Ok(dispatched_any);
            }
        };

        // One shared cache spans the whole batch so a same-task run queries its
        // concurrency count once. Isolate per-job failures so one error doesn't
        // strand the rest of the batch in Running until the reaper times it out.
        let mut counts = GateCounts::default();
        let mut dispatched_any = false;
        for (job, won) in jobs.into_iter().zip(claimed) {
            // A lost claim means another scheduler owns the job; leave it to
            // that owner, exactly like the single path's `AlreadyClaimed`. The
            // job stays legitimately Running, so its concurrency slot stands.
            if !won {
                continue;
            }
            match self.dispatch_claimed(job, now, &mut counts, job_tx) {
                Ok(true) => dispatched_any = true,
                Ok(false) => {}
                Err(e) => warn!("batch dispatch failed for a claimed job: {e}"),
            }
        }
        Ok(dispatched_any)
    }

    /// CoDel admission: drop stale jobs before they are claimed (opt-in per
    /// queue). Returns the jobs that survive; shed jobs are dead-lettered with a
    /// reserved `codel:` reason so the auto-retry sweep leaves them alone.
    ///
    /// A job's sojourn is measured from `scheduled_at` (time waiting *past* its
    /// eligibility), so an intentional delay is never counted as staleness. The
    /// empty-map short-circuit keeps this free for queues that never opted in.
    pub(super) fn codel_admit(&self, jobs: Vec<Job>, now: i64) -> Result<Vec<Job>> {
        if self.codel_configs.is_empty() {
            return Ok(jobs);
        }
        let mut states = self.codel_states.lock().unwrap();
        let mut kept = Vec::with_capacity(jobs.len());
        for job in jobs {
            let cfg = match self.codel_configs.get(&job.queue).copied() {
                Some(cfg) => cfg,
                None => {
                    kept.push(job);
                    continue;
                }
            };
            let sojourn = now.saturating_sub(job.scheduled_at).max(0);
            let state = states.entry(job.queue.clone()).or_default();
            if state.should_drop(sojourn, now, &cfg) {
                let reason = format!(
                    "{} sojourn {sojourn}ms exceeded target {}ms under sustained overload",
                    shed::CODEL_REASON_PREFIX,
                    cfg.target_ms
                );
                self.storage
                    .shed_to_dlq(&job, &reason, Some("{\"codel\":true}"))?;
                warn!(
                    "codel shed {} on queue '{}' (sojourn {sojourn}ms)",
                    job.id, job.queue
                );
            } else {
                kept.push(job);
            }
        }
        Ok(kept)
    }

    /// Run the post-dequeue pipeline for a single job: soft pre-claim gates,
    /// exactly-once claim, then the shared post-claim tail. Returns `Ok(true)`
    /// when the job made progress — dispatched, or shed to the DLQ — and
    /// `Ok(false)` when it went back on the queue. Used by the single-dispatch
    /// path and as the batch path's per-job fallback.
    fn gate_and_dispatch(
        &self,
        job: Job,
        now: i64,
        counts: &mut GateCounts,
        job_tx: &tokio::sync::mpsc::Sender<Job>,
    ) -> Result<bool> {
        // Pre-claim soft gates: rate limits and circuit breaker.
        //
        // These don't need to be atomic with the claim — if two schedulers
        // both pass these gates, the gate semantics still hold (each
        // consumes its own token / observes the breaker independently). No
        // claim row exists yet, so a rejection just reschedules.
        match self.check_pre_claim_gates(&job)? {
            GateDecision::Proceed => {}
            GateDecision::Defer(delay_ms) => {
                self.storage
                    .reschedule(&job.id, now + desync_delay(delay_ms))?;
                counts.release(&job.task_name, &job.queue);
                return Ok(false);
            }
            // Progress, not a rejection: the job is terminal and gone from the
            // queue. Reporting it as idle would stop the caller's drain loop
            // and shed only one job per poll wake — against a backoff that
            // doubles because it saw no progress.
            GateDecision::Shed(reason) => {
                self.shed_rate_limited(&job, &reason, false)?;
                counts.release(&job.task_name, &job.queue);
                return Ok(true);
            }
        }

        // Claim exactly-once execution. After this point, the job is reserved
        // for this scheduler instance.
        match self.claim_for_dispatch(&job) {
            ClaimOutcome::Claimed => {}
            // Another scheduler already owns the claim; it will dispatch the
            // job. Leave it alone — rolling back here would clear the owner's
            // claim and race its dispatch.
            ClaimOutcome::AlreadyClaimed => return Ok(false),
            // The claim errored, but the dequeue already flipped the job to
            // `Running` with no claim row written — so it's invisible to the
            // fast orphan-recovery path and would sit stuck until the slow
            // timeout sweep. Return it to `Pending` now.
            ClaimOutcome::Errored => {
                counts.release(&job.task_name, &job.queue);
                self.rollback_claim_and_reschedule(
                    &job.id,
                    now + desync_delay(CLAIM_ERROR_RETRY_DELAY_MS),
                )?;
                return Ok(false);
            }
        }

        self.finish_dispatch(job, now, counts, job_tx)
    }

    /// Dispatch a job this scheduler has *already* claimed (its claim row is
    /// written — the batch path claims the whole batch up front). Runs the same
    /// soft gates and post-claim tail as [`Self::gate_and_dispatch`] but skips
    /// the claim step; because a claim already exists, every rejection clears it
    /// rather than merely rescheduling. Same `Ok(true)` = progress contract.
    fn dispatch_claimed(
        &self,
        job: Job,
        now: i64,
        counts: &mut GateCounts,
        job_tx: &tokio::sync::mpsc::Sender<Job>,
    ) -> Result<bool> {
        match self.check_pre_claim_gates(&job)? {
            GateDecision::Proceed => {}
            GateDecision::Defer(delay_ms) => {
                counts.release(&job.task_name, &job.queue);
                self.rollback_claim_and_reschedule(&job.id, now + desync_delay(delay_ms))?;
                return Ok(false);
            }
            // Progress, like the single path's shed — see there for why.
            GateDecision::Shed(reason) => {
                counts.release(&job.task_name, &job.queue);
                self.shed_rate_limited(&job, &reason, true)?;
                return Ok(true);
            }
        }

        self.finish_dispatch(job, now, counts, job_tx)
    }

    /// Post-claim tail shared by the single and batch paths: enforce the hard
    /// concurrency cap on the already-claimed job, then hand it to the worker
    /// pool. Either rejection clears the claim and reschedules so the job never
    /// sits stranded in `Running`.
    fn finish_dispatch(
        &self,
        job: Job,
        now: i64,
        counts: &mut GateCounts,
        job_tx: &tokio::sync::mpsc::Sender<Job>,
    ) -> Result<bool> {
        // Concurrency caps must be checked AFTER the claim so two schedulers
        // cannot both pass the cap. Status was already transitioned to
        // `Running` by the dequeue, so the running-count includes this job —
        // the cache's strict `>` allows exactly `max_concurrent` jobs.
        if !self.check_post_claim_concurrency(&job, counts)? {
            counts.release(&job.task_name, &job.queue);
            self.rollback_claim_and_reschedule(
                &job.id,
                now + desync_delay(CONCURRENCY_RETRY_DELAY_MS),
            )?;
            return Ok(false);
        }

        // Hand the job to the worker pool. If the channel is unavailable we
        // must reverse the claim — otherwise the job sits in `Running` until
        // the stale-reaper times it out, which surfaces as a *timeout* in
        // metrics and middleware (wrong outcome for a job that never ran).
        //
        // Track BEFORE the send: once the job is in the channel its result can
        // arrive on the drain thread at any moment, and a release that races
        // ahead of the insert would leak the slot forever. Tracking first means
        // the entry always exists by the time a result can be handled; the
        // failure arms untrack what never left.
        let job_id = job.id.clone();
        let task_name = job.task_name.clone();
        self.track_in_flight(&job_id, &task_name);
        match job_tx.try_send(job) {
            Ok(()) => Ok(true),
            Err(TrySendError::Full(job)) => {
                warn!("worker channel full; rescheduling job {job_id} (worker pool is behind)",);
                self.untrack_in_flight(&job_id);
                counts.release(&job.task_name, &job.queue);
                self.rollback_claim_and_reschedule(
                    &job_id,
                    now + desync_delay(CHANNEL_BACKPRESSURE_RETRY_DELAY_MS),
                )?;
                Ok(false)
            }
            Err(TrySendError::Closed(job)) => {
                warn!(
                    "worker channel closed; rescheduling job {job_id} (worker pool shutting down)",
                );
                self.untrack_in_flight(&job_id);
                counts.release(&job.task_name, &job.queue);
                self.rollback_claim_and_reschedule(
                    &job_id,
                    now + desync_delay(CHANNEL_BACKPRESSURE_RETRY_DELAY_MS),
                )?;
                Ok(false)
            }
        }
    }

    /// Snapshot the queue list with paused queues filtered out. The paused
    /// list is cached for 1s to avoid hammering storage on every tick. Borrows
    /// the queue list directly in the common case (nothing paused), allocating
    /// only when a filtered copy is actually needed.
    pub(super) fn active_queues(&self) -> std::borrow::Cow<'_, [String]> {
        let mut cache = self.paused_cache.lock().unwrap_or_else(|poisoned| {
            warn!("paused_cache mutex was poisoned, recovering");
            poisoned.into_inner()
        });
        if cache.1.elapsed() > Duration::from_secs(1) {
            cache.0 = self
                .storage
                .list_paused_queues()
                .unwrap_or_default()
                .into_iter()
                .collect();
            cache.1 = std::time::Instant::now();
        }
        if cache.0.is_empty() {
            std::borrow::Cow::Borrowed(&self.queues)
        } else {
            std::borrow::Cow::Owned(
                self.queues
                    .iter()
                    .filter(|q| !cache.0.contains(*q))
                    .cloned()
                    .collect(),
            )
        }
    }

    /// Evaluate the pre-claim soft gates (queue rate limit, task circuit
    /// breaker, task rate limit) without side effects, naming what the caller
    /// should do with the job. The caller performs the reschedule or the shed —
    /// and, when a claim already exists (batch path), also clears it.
    fn check_pre_claim_gates(&self, job: &Job) -> Result<GateDecision> {
        // Read once: the queue gate below needs the task's shed preference, and
        // this runs on every dispatch.
        let task_config = self.task_configs.get(&job.task_name);
        let on_excess = task_config
            .map(|config| config.on_excess)
            .unwrap_or_default();

        if let Some(qcfg) = self.queue_configs.get(&job.queue) {
            if let Some(ref rl_config) = qcfg.rate_limit {
                let key = format!("queue:{}", job.queue);
                if !self.rate_limiter.try_acquire(&key, rl_config)? {
                    return Ok(rate_limited(on_excess, &format!("queue '{}'", job.queue)));
                }
            }
        }

        if let Some(config) = task_config {
            // A tripped breaker is not excess load — the job is fine, its
            // dependency is not — so shedding here would throw away work the
            // caller never asked to shed. Always defer.
            if config.circuit_breaker.is_some() && !self.circuit_breaker.allow(&job.task_name)? {
                return Ok(GateDecision::Defer(CIRCUIT_BREAKER_RETRY_DELAY_MS));
            }

            if let Some(ref rl_config) = config.rate_limit {
                if !self.rate_limiter.try_acquire(&job.task_name, rl_config)? {
                    return Ok(rate_limited(
                        on_excess,
                        &format!("task '{}'", job.task_name),
                    ));
                }
            }
        }

        Ok(GateDecision::Proceed)
    }

    /// Dead-letter a job the rate limiter shed, clearing its execution claim
    /// first when one exists so the dead-worker reaper never sees an orphan
    /// pointing at an archived job.
    fn shed_rate_limited(&self, job: &Job, reason: &str, claimed: bool) -> Result<()> {
        if claimed {
            if let Err(e) = self
                .storage
                .complete_execution(&job.id, self.namespace.as_deref())
            {
                warn!(
                    "failed to clear execution claim while shedding job {}: {e}",
                    job.id
                );
            }
        }
        self.storage
            .shed_to_dlq(job, reason, Some(shed::RATE_LIMIT_SHED_METADATA))?;
        warn!(
            "rate-limit shed {} on queue '{}' (task '{}')",
            job.id, job.queue, job.task_name
        );
        Ok(())
    }

    /// Try to claim exactly-once execution. The three outcomes are handled
    /// differently by the caller — an error must roll the job back to
    /// `Pending`, whereas an already-claimed job must be left for its owner.
    fn claim_for_dispatch(&self, job: &Job) -> ClaimOutcome {
        match self.storage.claim_execution(&job.id, &self.claim_owner) {
            Ok(true) => ClaimOutcome::Claimed,
            Ok(false) => ClaimOutcome::AlreadyClaimed,
            Err(e) => {
                // Dispatching anyway would double-execute (no claim row guards
                // the job); the caller instead rolls it back to Pending.
                warn!(
                    "claim_execution error for job {}; rolling back to Pending: {e}",
                    job.id
                );
                ClaimOutcome::Errored
            }
        }
    }

    /// Apply the post-claim hard gates (per-queue and per-task concurrency
    /// caps). Returns `Ok(true)` if the job may proceed to dispatch,
    /// `Ok(false)` if the cap is exceeded — caller is responsible for
    /// rolling back the claim.
    fn check_post_claim_concurrency(&self, job: &Job, counts: &mut GateCounts) -> Result<bool> {
        if let Some(qcfg) = self.queue_configs.get(&job.queue) {
            if let Some(max_conc) = qcfg.max_concurrent {
                if self.cached_queue_running(counts, &job.queue)? > max_conc as i64 {
                    return Ok(false);
                }
            }
        }

        if let Some(config) = self.task_configs.get(&job.task_name) {
            if let Some(max_conc) = config.max_concurrent {
                if self.cached_task_running(counts, &job.task_name)? > max_conc as i64 {
                    return Ok(false);
                }
            }

            // In-process bulkhead: keep one task from taking every slot in this
            // worker's pool. Unlike the running-count above, the in-flight map does
            // not yet include this job — it's only tracked once the send succeeds —
            // so this compares with `>=` where that one uses a strict `>`.
            if let Some(max_in_flight) = config.max_in_flight_per_task {
                if self.task_in_flight(&job.task_name) >= max_in_flight {
                    return Ok(false);
                }
            }
        }

        Ok(true)
    }

    /// Running-job count for a task, read from storage once per dispatch cycle
    /// then served from [`GateCounts`]. The stored count already includes every
    /// job the current batch flipped to `Running`, matching the strict `>`
    /// comparison the caller uses.
    fn cached_task_running(&self, counts: &mut GateCounts, task_name: &str) -> Result<i64> {
        if let Some(&n) = counts.task_running.get(task_name) {
            return Ok(n);
        }
        let n = self
            .storage
            .count_running_by_task(task_name, self.namespace.as_deref())?;
        counts.task_running.insert(task_name.to_string(), n);
        Ok(n)
    }

    /// Running-job count for a queue, cached exactly like [`Self::cached_task_running`].
    ///
    /// Counted within this scheduler's namespace: it only ever dequeues its own,
    /// so another tenant's running jobs must not spend this one's budget.
    fn cached_queue_running(&self, counts: &mut GateCounts, queue: &str) -> Result<i64> {
        if let Some(&n) = counts.queue_running.get(queue) {
            return Ok(n);
        }
        let n = self
            .storage
            .stats_by_queue(queue, self.namespace.as_deref())?
            .running;
        counts.queue_running.insert(queue.to_string(), n);
        Ok(n)
    }

    /// Reverse a successful `claim_execution` and reschedule the job. Used
    /// when a post-claim gate rejects the job after the claim row has
    /// already been written. Uses `reschedule` (not `retry`) so a job that
    /// never executed does not lose retry budget.
    fn rollback_claim_and_reschedule(&self, job_id: &str, next_at: i64) -> Result<()> {
        if let Err(e) = self
            .storage
            .complete_execution(job_id, self.namespace.as_deref())
        {
            warn!("failed to clear execution claim during rollback for job {job_id}: {e}");
        }
        self.storage.reschedule(job_id, next_at)
    }
}
