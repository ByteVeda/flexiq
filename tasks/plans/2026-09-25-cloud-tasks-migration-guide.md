# Cloud Tasks and EventBridge migration guide (#864)

## Where it goes

`docs/content/docs/server/migrate-cloud-tasks.mdx`, in the Server & wire tier
next to `limits`. The reader runs no SDK worker today, so the page belongs with
the server rather than in one SDK's `operate/migration`. Push dispatch and
triggers link to it; nothing moves, so no redirect.

## What the page must answer

1. The topology: what stays (handlers, platform), what is added (one
   long-lived `flexiq-server`, a database), what changes (who dispatches)
2. A Cloud Tasks queue → `FLEXIQ_QUEUES` + one push process per target URL
3. The handler shim: envelope in, `x-flexiq-outcome` out
4. `httpRequest.url` + `oidcToken` → target URL + `FLEXIQ_PUSH_TARGET_AUTH`
5. `retryConfig` → `max_retries` + the fixed backoff, differences spelled out
6. `rateLimits` → `FLEXIQ_PUSH_TARGET_CAPACITY`
7. `scheduleTime` → `scheduledAt`; `dispatchDeadline` → `timeout`,
   `FLEXIQ_PUSH_TARGET_TIMEOUT`, and `Settle`
8. Task name dedup → `uniqueKey`; `DeleteTask` → `CancelJob`
9. EventBridge rules / targets / input transformers / scheduled rules →
   triggers, push, periodic tasks
10. What they gain, and where each gain stops
11. Running it locally
12. Checklist

## Verified in code

- `flexiq-server` builds its `Worker` with no `task_config` or `queue_config`
  (`runtime/scheduler.rs::build_worker`), so under push every task retries on
  `RetryPolicy::default()` — Full Jitter, 1 s base, 5 min cap — and task/queue
  overrides (backoff, rate limit, concurrency) are not applied. The e2e test
  `push_dispatch_e2e.rs` pins "no push-specific backoff".
- The gRPC door's `max_retries` is proto3 `int32`: unset is `0`, no retries
  (`grpc/producer/convert.rs`). Default `timeout` is 300 s.
- `expires_at` cancels a pending job at dequeue, retries waiting included.
- Push is one URL per process; `FLEXIQ_QUEUES` picks the queues it serves.
- A 2xx without `x-flexiq-outcome` is `MissingOutcome`, not retried; a 4xx
  other than 408/425/429 is not retried.
- `google` OIDC source reads the metadata server only.
- The push client is `reqwest` with `rustls-tls` (bundled webpki roots), and
  loopback is refused unconditionally — a local target needs a publicly
  trusted `https` name.
- Durable steps are attach-only; push refuses `slept`.

## Decisions

- **Honest over tidy.** Three of the issue's five gains need a qualifier under
  push (durable steps need attach, cancel stops the result not the work, the
  push hop needs a public cert locally). The page says so where it claims them.
- The design gaps the guide surfaced are called out on the page as current
  limits, not papered over: no per-task/queue backoff or dispatch rate under
  push, and one target URL per process.
