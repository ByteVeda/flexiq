# Push: overrides and more than one target

Follow-up to the Cloud Tasks migration guide (#864, #974), which found two gaps
by writing the migration down.

## 1. `flexiq-server` ignored task and queue overrides

`SchedulerSupervisor::build_worker` registered no `task_config` or
`queue_config`, so the server's scheduler — attach or push — retried on
`RetryPolicy::default()` and enforced no override's rate limit or cap. For push
that was every task, since nothing else in the process knows the tasks.

Fix: `runtime::overrides::apply` reads the namespace's task and queue overrides
when a worker is built, the moment an SDK worker reads them, and registers the
scheduler-side fields: task `retry_backoff` (seconds → `base_delay_ms`),
`rate_limit`, `max_concurrent`; queue `rate_limit`, `max_concurrent`.
`timeout`, `priority` and task `paused` are enqueue-side; queue `paused` is
already read from storage per claim. A field that does not parse is logged and
skipped — under attach this runs on the first attach, where a refusal would
leave a scheduler that never starts.

Not done: the backoff cap (`max_delay_ms`) is not an override field in any
surface, so it stays five minutes.

## 2. One push target URL per process

Named targets: `FLEXIQ_PUSH_TARGETS=a,b`, each with `FLEXIQ_PUSH_<NAME>_URL`
and `_QUEUES`, any other setting per target, falling back to
`FLEXIQ_PUSH_TARGET_*`. Parsed by the existing single-target parser over a
merged env view, so validation is identical and errors carry the name.

Runtime: one `Lane` (path + settings) per target, one `Worker` each.
Rejected alternative: one scheduler feeding a routing dispatcher — it sizes
`max_in_flight` from total slots, so it would claim jobs for a full target
while another idles, and a claimed job waits out its deadline in a buffer.

- Lanes start all-or-none; shut down concurrently (grace is sized for one
  drain, `2 × DRAIN`).
- Retention runs on the first lane only.
- The executor door routes `Settle`/`ExtendLease`/progress/log by
  `HttpDispatchTarget::holds(job_id)` (accepted or recently ended), else the
  first target, whose "not here" is the same any would give.
- Refused at boot: `FLEXIQ_PUSH_TARGET_URL` beside the list; a queue on two
  targets; `FLEXIQ_QUEUES` / `FLEXIQ_WORKERS` beside named targets; names with
  `_` (two names could spell one variable) or `target`/`targets`.
- Named targets' `TOKEN` / `HMAC_SECRET` are scrubbed from the environment.

Not done: Helm values for named targets.

## Verified

- Queue override `max_concurrent=1` bounds a capacity-4 target to 1 in flight;
  without the fix the same test sees 4.
- Two lanes each receive only their own queue's jobs.
- A settle through the door reaches the second target when the first is
  listed first; without routing it is refused as "not on this replica".
