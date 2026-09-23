use log::{error, info, warn};

use crate::error::Result;
use crate::job::now_millis;
use crate::periodic::{next_run, periodic_job};
use crate::scheduler::retention::{
    publish_effective_retention, EffectiveRetention, RetentionConfig, DEFAULT_NAMESPACE,
};
use crate::storage::records::{SettleClaimant, SettleGrant};
use crate::storage::{
    dead_worker_cutoff, try_lead, Storage, RETENTION_LOCK, RETENTION_LOCK_TTL_MS,
};

use super::{JobResult, Scheduler};

/// Prefix of the error a dispatch accepted out of band and never settled is
/// recorded under, so an operator can grep for it.
///
/// Defined here rather than beside the push-dispatch wire constants it started
/// out with, because the reaper is what writes it now and the reaper is not
/// behind the `http-target` feature. One definition, re-exported from
/// `worker::http_target` so its original path still resolves — two constants
/// carrying one string would be free to disagree.
pub const ACCEPTED_NOT_SETTLED: &str = "push.accepted_not_settled";

/// Max log messages compacted per retention tick, bounding each sweep.
const TOPIC_MESSAGE_PURGE_LIMIT: i64 = 10_000;

/// Dead-letter entries considered per auto-retry sweep. The bound is applied by
/// the query, so anything the sweep would skip has to be excluded there too —
/// see `Storage::list_dead_for_retry`.
pub(super) const DLQ_RETRY_CANDIDATES: i64 = 50;

/// A retention window in ms as a compact human duration (`7d`, `12h`, `30m`).
fn humanize_ms(ms: i64) -> String {
    const S: i64 = 1000;
    const M: i64 = 60 * S;
    const H: i64 = 60 * M;
    const D: i64 = 24 * H;
    match ms {
        _ if ms % D == 0 => format!("{}d", ms / D),
        _ if ms % H == 0 => format!("{}h", ms / H),
        _ if ms % M == 0 => format!("{}m", ms / M),
        _ if ms % S == 0 => format!("{}s", ms / S),
        _ => format!("{ms}ms"),
    }
}

/// An epoch-ms cutoff as RFC 3339, falling back to raw ms if out of range.
fn iso_cutoff(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| format!("{ms}ms"))
}

/// Warn, once, that retention is deleting history on the recommended defaults —
/// a policy the operator did not set. Names the queue and every active window
/// (duration + resolved cutoff) and how to opt out.
fn announce_default_retention(namespace: &str, windows: &RetentionConfig, now: i64) {
    // `namespace`, not a queue name: the purge predicates are not queue-scoped,
    // so the windows below apply cluster-wide within this namespace.
    let mut msg = format!(
        "retention is ON by default for namespace '{namespace}': history is auto-deleted on the \
         recommended windows below. Set explicit windows, or pass an empty retention config to \
         disable."
    );
    for (table, ttl) in [
        ("archived_jobs", windows.archived_jobs_ttl_ms),
        ("dead_letter", windows.dead_letter_ttl_ms),
        ("task_metrics", windows.task_metrics_ttl_ms),
        ("job_errors", windows.job_errors_ttl_ms),
        ("task_logs", windows.task_logs_ttl_ms),
    ] {
        if let Some(ttl) = ttl {
            msg.push_str(&format!(
                "\n  {table}: delete after {} (cutoff {})",
                humanize_ms(ttl),
                iso_cutoff(now.saturating_sub(ttl)),
            ));
        }
    }
    warn!("{msg}");
}

/// Run one retention sweep, logging and continuing on failure.
///
/// The five purges are independent, so one table's error must never abort the
/// rest — that would leave every table after it unpurged for as long as the
/// failure persists, silently disabling retention where it was configured.
fn sweep(label: &str, purge: impl FnOnce() -> Result<u64>) -> u64 {
    purge().unwrap_or_else(|e| {
        warn!("{label} failed: {e}");
        0
    })
}

impl Scheduler {
    pub(super) fn reap_stale(&self) -> Result<()> {
        let now = now_millis();
        // Expire pending jobs that passed their TTL
        if let Err(e) = self.storage.expire_pending_jobs(now) {
            warn!("expire_pending_jobs error: {e}");
        }
        // Reap expired distributed locks
        if let Err(e) = self.storage.reap_expired_locks(now) {
            warn!("reap_expired_locks error: {e}");
        }
        // Purge stale execution claims (older than 1 hour)
        let claim_cutoff = now.saturating_sub(3_600_000);
        if let Err(e) = self.storage.purge_execution_claims(claim_cutoff) {
            warn!("purge_execution_claims error: {e}");
        }
        let stale_jobs = self
            .storage
            .reap_stale_jobs(now, self.namespace.as_deref())?;

        for stale in stale_jobs {
            let job = stale.job;
            let error = if stale.awaiting_settle {
                // Consume the marker before failing the job. The reaper is one
                // of three racers for an accepted dispatch and has no more
                // authority than the others: if a `Settle` took the marker
                // first, this attempt is already settled and a timeout on top
                // of it would be a second outcome for one dispatch.
                match self.storage.claim_settle(
                    &job.id,
                    SettleClaimant::Expired { now },
                    self.namespace.as_deref(),
                ) {
                    Ok(SettleGrant::Granted) => {}
                    Ok(SettleGrant::Refused) => continue,
                    Err(e) => {
                        // Fail closed, unlike the rest of this sweep: a storage
                        // blip here would otherwise let the reaper settle a job
                        // it did not win. The job stays `Running` and the next
                        // tick tries again, which is the bounded cost.
                        warn!("claim_settle for {} error: {e}", job.id);
                        continue;
                    }
                }
                format!(
                    "{ACCEPTED_NOT_SETTLED}: the target accepted this job and never settled it \
                     within {}ms",
                    job.timeout_ms
                )
            } else {
                format!("job timed out after {}ms", job.timeout_ms)
            };
            let _ = self.handle_result(JobResult::Failure {
                job_id: job.id.clone(),
                error,
                retry_count: job.retry_count,
                max_retries: job.max_retries,
                task_name: job.task_name.clone(),
                wall_time_ns: 0,
                should_retry: true,
                timed_out: true,
            })?;
        }

        // Fast-path recovery: requeue jobs whose worker died, without waiting
        // for their full timeout. Log-and-continue so a failure here never
        // aborts the rest of the sweep.
        if let Err(e) = self.recover_orphaned_jobs(now) {
            warn!("recover_orphaned_jobs error: {e}");
        }

        Ok(())
    }

    /// Requeue `Running` jobs whose claiming worker is no longer alive (crashed
    /// without finishing). A surviving scheduler detects the dead owner via the
    /// heartbeat table, atomically reclaims the orphaned claim — so exactly one
    /// survivor rescues each job — then routes it through the normal
    /// retry/dead-letter path.
    pub(super) fn recover_orphaned_jobs(&self, now: i64) -> Result<()> {
        // Live owners = workers with a fresh heartbeat, plus self: a scheduler
        // must never orphan its own in-flight jobs (covers the startup window
        // before its first heartbeat row is written).
        let mut live = self.storage.list_live_worker_ids(dead_worker_cutoff(now))?;
        live.push(self.claim_owner.clone());

        for (job, dead_owner) in
            self.storage
                .reap_orphaned_jobs(&live, now, self.namespace.as_deref())?
        {
            // Atomic election: only the survivor that wins the claim transfer
            // requeues the job, so concurrent schedulers can't double-retry it.
            match self
                .storage
                .reclaim_execution(&job.id, &dead_owner, &self.claim_owner)
            {
                Ok(Some(epoch)) => {
                    // The reclaim *is* the authorization: it returns an epoch
                    // only for the survivor that won the transfer. Writing the
                    // dispatch record from it lets the synthesised result
                    // validate like any other — without it the fence would drop
                    // the one result that exists to unstick the job, and after a
                    // restart there is no record to consult at all.
                    //
                    // The epoch is the transfer's, not the dead owner's: the
                    // rescue is a new claim, so the dead owner's executor —
                    // which may still be on its way to reporting — is fenced
                    // out by the same value that authorizes this result.
                    self.track_in_flight(&job.id, &job.task_name, job.retry_count, Some(epoch));
                    let error = format!("worker {dead_owner} died; recovering in-flight job");
                    if let Err(e) = self.handle_result(JobResult::Failure {
                        job_id: job.id.clone(),
                        error,
                        retry_count: job.retry_count,
                        max_retries: job.max_retries,
                        task_name: job.task_name.clone(),
                        wall_time_ns: 0,
                        should_retry: true,
                        timed_out: false,
                    }) {
                        warn!(
                            "recover_orphaned_jobs: handle_result failed for {}: {e}",
                            job.id
                        );
                    }
                }
                Ok(None) => {} // another survivor won the race
                Err(e) => warn!("reclaim_execution failed for {}: {e}", job.id),
            }
        }

        Ok(())
    }

    /// Record the windows this leader applies under its namespace, so a
    /// dashboard in any process can echo the live policy rather than guess at
    /// the defaults. Rewritten every sweep: `reported_at` then doubles as proof
    /// a leader is still enforcing it.
    fn publish_retention(&self, windows: &RetentionConfig, now: i64) -> Result<()> {
        let namespace = self.namespace.as_deref().unwrap_or(DEFAULT_NAMESPACE);
        let snapshot = EffectiveRetention {
            enabled: !windows.is_empty(),
            defaulted: self.config.retention_is_defaulted(),
            namespace: namespace.to_string(),
            reported_at: now,
            windows: windows.clone(),
        };
        publish_effective_retention(&self.storage, self.namespace.as_deref(), &snapshot)
    }

    /// Purge expired rows in each history table per its retention window. The
    /// per-entry TTL sweeps inside the archived/dead purges run every tick — a
    /// job or DLQ entry can carry its own `result_ttl` even when its table has no
    /// window. A table with no window only runs that per-entry sweep; the side
    /// tables (no per-entry TTL) are skipped entirely.
    pub(super) fn auto_cleanup(&self) -> Result<()> {
        // Elect a single cleaner: every worker runs this tick, but the purge
        // sweeps are cluster-wide, so N workers draining the same tables is
        // wasted work — and on a retention-default upgrade, N simultaneous
        // multi-million-row backfills. A separate lock from the reaper so a long
        // sweep can't starve worker-death detection.
        let leading = try_lead(
            &self.storage,
            RETENTION_LOCK,
            &self.claim_owner,
            RETENTION_LOCK_TTL_MS,
        )
        .unwrap_or_else(|e| {
            // A backend error is not the same as losing the election — surface it
            // so a storage outage that stalls retention is diagnosable, not silent.
            warn!("retention election failed: {e}");
            false
        });
        if !leading {
            return Ok(());
        }

        let now = now_millis();
        let windows = self.config.effective_retention();

        // The one moment we tell an operator their history will be auto-deleted
        // on a policy they did not choose. Before the first delete, once per
        // process (the election means that's the leader). Skipped for an explicit
        // or legacy config — that is the operator's own choice.
        if self.config.retention_is_defaulted() {
            let namespace = self.namespace.as_deref().unwrap_or(DEFAULT_NAMESPACE);
            self.retention_announced
                .call_once(|| announce_default_retention(namespace, &windows, now));
        }

        // Publish before deleting: a dashboard that shows the windows only after
        // the rows are gone explains nothing. Log-and-continue — an unreported
        // policy must never stop the sweeps.
        if let Err(e) = self.publish_retention(&windows, now) {
            warn!("publishing the retention windows failed: {e}");
        }

        // A window `ttl` becomes the cutoff `now - ttl`; an absent window is a
        // `None` cutoff, which the fused purges read as "per-entry only".
        let cutoff = |ttl: Option<i64>| ttl.map(|t| now.saturating_sub(t));

        let completed = sweep("purge_completed_with_ttl", || {
            self.storage
                .purge_completed_with_ttl(cutoff(windows.archived_jobs_ttl_ms))
        });
        let dead = sweep("purge_dead_with_ttl", || {
            self.storage
                .purge_dead_with_ttl(cutoff(windows.dead_letter_ttl_ms))
        });

        let errors = match cutoff(windows.job_errors_ttl_ms) {
            Some(c) => sweep("purge_job_errors", || self.storage.purge_job_errors(c)),
            None => 0,
        };
        let metrics = match cutoff(windows.task_metrics_ttl_ms) {
            Some(c) => sweep("purge_metrics", || self.storage.purge_metrics(c)),
            None => 0,
        };
        let logs = match cutoff(windows.task_logs_ttl_ms) {
            Some(c) => sweep("purge_task_logs", || self.storage.purge_task_logs(c)),
            None => 0,
        };
        // Log topics compact themselves: drop messages every subscriber has
        // acked past, plus any expired. Bounded per tick like the other sweeps.
        let topic_msgs = sweep("purge_topic_messages", || {
            self.storage
                .purge_topic_messages(now, TOPIC_MESSAGE_PURGE_LIMIT)
        });

        if completed + dead + errors + metrics + logs + topic_msgs > 0 {
            info!(
                "auto-cleanup: purged {completed} completed, {dead} dead, {errors} errors, {metrics} metrics, {logs} logs, {topic_msgs} topic messages"
            );
        }

        Ok(())
    }

    /// Check for periodic tasks that are due and enqueue them.
    ///
    /// A scheduler with no namespace serves the whole cluster, so it reads
    /// every namespace's due rows and each job it mints inherits the *row's*
    /// namespace rather than the scheduler's (#918). `unique_key` is namespace
    /// scoped since `m0017`, so two tenants whose schedules share a name no
    /// longer deduplicate against each other.
    pub(super) fn check_periodic(&self) -> Result<()> {
        let now = now_millis();
        let due_tasks = self
            .storage
            .get_due_periodic(now, self.namespace.as_deref())?;

        for task in due_tasks {
            let unique_key = Some(format!("periodic:{}:{}", task.name, now));
            if let Err(e) = self
                .storage
                .enqueue_unique(periodic_job(&task, now, unique_key))
            {
                error!("failed to enqueue periodic task '{}': {e}", task.name);
                continue;
            }

            let next_run = match next_run(&task.cron_expr, task.timezone.as_deref(), now) {
                Ok(t) => t,
                Err(e) => {
                    error!("failed to compute next run for '{}': {e}", task.name);
                    continue;
                }
            };

            if let Err(e) = self.storage.update_periodic_schedule(
                &task.name,
                now,
                next_run,
                task.namespace.as_deref(),
            ) {
                error!("failed to update schedule for '{}': {e}", task.name);
            }
        }

        Ok(())
    }

    /// Auto-retry eligible DLQ entries that have aged past the configured delay.
    pub(super) fn auto_retry_dlq(&self) -> Result<()> {
        let delay = match self.config.dlq_auto_retry_delay_ms {
            Some(d) => d,
            None => return Ok(()),
        };

        let now = now_millis();
        let cutoff = now.saturating_sub(delay);
        let max_retries = self.config.dlq_auto_retry_max;

        // Only retry entries this worker actually serves (its namespace + active,
        // non-paused queues), mirroring the poller's dequeue scoping.
        let active_queues = self.active_queues();
        let candidates = self.storage.list_dead_for_retry(
            cutoff,
            max_retries,
            self.namespace.as_deref(),
            &active_queues,
            DLQ_RETRY_CANDIDATES,
        )?;

        if candidates.is_empty() {
            return Ok(());
        }

        let mut retried = 0u64;
        for entry in &candidates {
            // Belt and braces. Storage already excludes shed entries by their
            // `shed` flag — it must, or a DLQ full of them would fill every
            // bounded page and starve genuine failures — but rows written
            // before that column existed carry only the reason prefix.
            if super::shed::is_shed_reason(entry.error.as_deref()) {
                continue;
            }
            match self
                .storage
                .retry_dead(&entry.id, self.namespace.as_deref())
            {
                Ok(new_id) => {
                    info!(
                        "dlq auto-retry: {} -> {} (attempt {}/{})",
                        entry.id,
                        new_id,
                        entry.dlq_retry_count + 1,
                        max_retries
                    );
                    retried += 1;
                }
                Err(e) => {
                    warn!("dlq auto-retry failed for {}: {e}", entry.id);
                }
            }
        }

        if retried > 0 {
            info!("dlq auto-retry: retried {retried} entries");
        }

        Ok(())
    }
}
