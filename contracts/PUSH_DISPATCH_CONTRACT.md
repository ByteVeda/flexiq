# FlexiQ — push dispatch contract

What a target must do to receive a job over push dispatch, and what it may
rely on in return. It is the contract for GitHub issues #843 and #844: instead
of an executor attaching to `flexiq-server`, the scheduler POSTs a claimed job
straight to an operator-configured URL. Read this if you are writing that
target — in any language, with no FlexiQ SDK.

Everything here is normative. **MUST**, **MUST NOT**, **SHOULD** and **MAY**
carry their RFC 2119 meanings; a claim without one of them is background.
Every header name, status code and literal below is taken from
`crates/flexiq-core/src/worker/http_target/contract.rs`, the source of truth
this document restates rather than duplicates independently — where the two
ever disagree, the code wins.

## Two contracts, and this is neither of the other two

`crates/flexiq-core/BINDING_CONTRACT.md` is what a language shell implements
against `flexiq-core` in the same process. `contracts/REMOTE_SDK_CONTRACT.md`
is what a client dials **in** to `flexiq-server` through, over gRPC or its
JSON facade. Push dispatch is the third shape: `flexiq-server` dials **out**,
so nothing an executor or a producer client does applies here — a push target
holds no database credential and attaches to no stream.

**One exception, and it is opt-in.** A target that cannot finish inside the
platform's request deadline answers `202 Accepted` and reports the outcome
afterwards, over gRPC, through the executor door. That path is described in
[Work that outlives the request](#work-that-outlives-the-request); everything
else in this document is plain HTTP and stays that way.

The payload format is shared with both. `Job.payload` and a success result are
the same tagged wire envelope `REMOTE_SDK_CONTRACT.md`'s
[Serialization](REMOTE_SDK_CONTRACT.md#serialization) section defines byte for
byte — one tag byte, `0x02` for CBOR by default, then the body that codec
produced. Push dispatch sends and reads that envelope unchanged; it does not
define it a second time here.

## The request

`POST` to the configured URL, once per attempt.

- **Transport**: `https`. The headers below carry a lease, an idempotency key
  and — under `bearer`, `oidc` and `sigv4` — credential material, and the body
  is the job's payload; `hmac` signs a request without encrypting it, so it is
  no exception. A target configured with an `http` URL is refused at
  construction. The one relaxation is a **loopback** host — an address in
  `127.0.0.0/8` or `::1`, or the name `localhost` — and only when the embedder
  has set `allow_loopback`, which is what a same-host sidecar and a local
  development server use. `flexiq-server` never sets it, so a server
  deployment has no cleartext path at all.
- **Body**: the job's payload, verbatim — the same bytes an attached executor
  would receive as a `job` frame's payload. `Content-Type` names the envelope,
  not JSON: `application/vnd.flexiq.envelope`.
- Everything a `job` frame would otherwise carry as a field travels as a
  header instead, because HTTP has no frame to put it in.

| Header | Always sent | Value |
|---|---|---|
| `content-type` | yes | `application/vnd.flexiq.envelope` |
| `user-agent` | yes | `flexiq-core/<version>` |
| `x-flexiq-protocol-version` | yes | The worker frame protocol version this dispatch speaks, decimal (`1` as of this writing) |
| `x-flexiq-job-id` | yes | The job's id |
| `x-flexiq-attempt` | yes | Retries already attempted **before** this dispatch, decimal — `0` on a job's first try |
| `x-flexiq-max-attempts` | yes | The job's retry cap, decimal |
| `x-flexiq-task` | yes | The task name to run |
| `x-flexiq-queue` | yes | The queue the job came from |
| `x-flexiq-namespace` | only for a non-default namespace | The job's namespace |
| `x-flexiq-lease` | only when the scheduler held a lease | An opaque capability token (the lease's base64url bytes, as text) — never inspect or construct one, it exists only to appear in [the idempotency key](#the-idempotency-key) |
| `x-flexiq-idempotency-key` | yes | `<job id>.<attempt>.<lease>`, or `<job id>.<attempt>` with no lease — see [The idempotency key](#the-idempotency-key) |
| `x-flexiq-deadline-ms` | yes | Milliseconds this dispatch will wait for an answer, decimal — an upper bound, not an exact window |
| `x-flexiq-disabled-middleware` | only when non-empty | Comma-separated middleware names the operator has disabled for this task |
| `x-flexiq-metadata` | only when the job carries metadata and it fits | The job's metadata blob, base64url-encoded (no padding) |

Two headers carry a bearer-grade secret and are marked sensitive in the
scheduler's own logs: `x-flexiq-lease` and `x-flexiq-idempotency-key` (which
ends in the lease token whenever one exists). A target's own logs should treat
them the same way — see the operator page's note on header logging.

`x-flexiq-deadline-ms` is not the job's raw remaining timeout. It is the
scheduler's own request budget — the job's execution deadline, less the
reaper's safety margin, capped by the operator's configured request ceiling —
and the scheduler waits **at most** that long, not exactly that long. The
budget starts running before the request is built and signed, so whatever
that costs comes out of it: sub-millisecond usually, but hundreds of
milliseconds when an auth scheme has to fetch an identity token it has not
cached. Read it as a ceiling on the time a target has. An answer that arrives
after the window is already fenced out, so there is no value in a
target racing past it.

`x-flexiq-metadata` is dropped, silently as far as the target is concerned,
when the encoded blob exceeds an internal 8 KiB cap — the job still dispatches
with every other header intact. A target that depends on metadata should not
assume its absence means the job carried none.

Whatever else four auth schemes add on top of these headers is
[below](#the-four-auth-schemes).

## The idempotency key

```
<job id>.<attempt>.<lease>      # a lease was held
<job id>.<attempt>              # no lease — an embedder with no claim book
```

Two requests carrying the **same** key are the same attempt of the same job
under the same claim: a target that memoizes on this key and returns its
memoized answer for a repeat is correct, because nothing about the job has
changed underneath it.

Two requests differing **only** in the lease are two claims of one attempt —
exactly what a requeue produces, whether from a retry, a timeout, or an
operator action. They are deliberately distinct keys, because the scheduler
will accept at most one of their responses; the other's is fenced out on
arrival by the claim it no longer holds. A target's dedupe store is therefore
per-key, not per-`(job id, attempt)`.

`owner` is deliberately not part of the key — no dispatch frame carries it
either, because an owner a peer holds is an owner it could forge. The lease
alone is what makes the second case above two keys instead of one.

## The response

The target answers with an HTTP status and, on success, an
`x-flexiq-outcome` header.

| Status | `x-flexiq-outcome` | Result | Retried? |
|---|---|---|---|
| 2xx (not 202) | `success` | Job succeeds. Body is the result — the tagged envelope: `0x02` then a bare CBOR value, no array wrapper — or empty for no result | — |
| 2xx (not 202) | `failure` | Job fails. Body becomes `Job.error` (lossy UTF-8 decode if the body is not valid UTF-8) — verbatim, except that a missing or unparseable `x-flexiq-retry` appends a line saying it retried for want of the header | `x-flexiq-retry` decides — see below |
| 2xx (not 202) | `cancelled` | Job settles cancelled. Body is ignored | no |
| 2xx (not 202) | `slept` | **Refused.** A push dispatch has no step session to resume; treated as a failure | no |
| 2xx (not 202) | absent | **Refused**: `MissingOutcome` | no |
| 2xx (not 202) | unrecognized value | **Refused**: `UnknownOutcome`, the value echoed back (truncated past 64 characters) | no |
| `202` | — | **Accepted**, when settle callbacks are on: the job stays `Running` and the scheduler waits for a `Settle` until the settle deadline. Body ignored. **Refused**: `Accepted202` when they are off | on the deadline, as a timeout |
| `3xx` | — | **Refused**: `Redirect` — never followed | no |
| `4xx` | — | **Refused**: `ClientError` | only `408`, `425`, `429` |
| `5xx`, or a status outside `1xx`–`5xx` | — | **Refused**: `ServerError` | yes |

A refusal is not an outcome: it settles the job as a failure whose message
names the target and the status or cap it is complaining about, and whose
retry decision is the "Retried?" column above. None of it reaches
`x-flexiq-outcome` — the header is read only once the status has already
cleared the `202` and non-2xx cases.

On a `failure` outcome, `x-flexiq-retry` says whether to retry: `true`/`1` or
`false`/`0`, case-insensitive and trimmed. Absent or unparseable reads as "no
decision", which the scheduler resolves to **retry** — the message it stores
notes that it retried for want of the header, so an operator sees why. A
target that wants to say "do not retry this" **MUST** send `x-flexiq-retry:
false` explicitly.

A conforming target answers within the window `x-flexiq-deadline-ms`
promised; a slower answer is fenced out and the attempt already re-dispatched
elsewhere. A response body over the operator's configured ceiling is refused
as too large rather than read in truncated form — a partial body is not what
the target said, and it does not become a job result.

## Work that outlives the request

A job that cannot finish inside the platform's request deadline — 60 minutes
on Cloud Run, 15 on Lambda — cannot report its result on the connection that
started it. A target in that position answers `202 Accepted` and reports
afterwards:

```
scheduler → POST target                 job, attempt, lease
target    → 202 Accepted                "I have it, I will call back"
   …work runs past the deadline; the connection is gone…
target    → ExtendLease(job, lease)     optional, repeatable
target    → Settle(job, lease, outcome) exactly once
```

**This is off by default.** The operator turns it on with
`FLEXIQ_PUSH_TARGET_SETTLE=grpc`; while it is off a `202` is refused as
`Accepted202`, exactly as before. A target **MUST NOT** answer `202` unless
it is going to call back — a framework that returns `202` by default is the
bug the mandatory `x-flexiq-outcome` header exists to catch, and under settle
callbacks it costs the job its whole deadline instead of one fast failure.

### Reporting

`Settle`, `ExtendLease`, `ReportProgress` and `WriteTaskLog` are RPCs on
`flexiq.executor.v1.ExecutorService`, the executor door. So a target that uses
this path is, for these four calls only, a gRPC client — and the deployment it
POSTs from **MUST** also run the gRPC listener. There is no HTTP form of these
calls and there will not be one; `REMOTE_SDK_CONTRACT.md` and the JSON facade
both serve `flexiq.v1` and never the executor package.

A target **MUST** present two things, and neither substitutes for the other:

- **An executor-scoped bearer token**, as `authorization: Bearer <token>`. It
  authenticates the caller. This is the same credential an attached executor
  uses and is issued the same way.
- **The lease**, from the `x-flexiq-lease` request header, echoed into the
  frame's `lease` field. It fences the call: it says which dispatch of which
  attempt is being reported on.

### What the fence refuses

A `Settle` is **single-use for the attempt it names**. The scheduler records
one marker per accepted dispatch and removes it in the same statement that
tests it, so exactly one of a `Settle` and the scheduler giving the dispatch up
— its deadline passing, a cancel, or a shutdown — can win.

A `Settle` **MUST** reach the scheduler replica that dispatched the job: the
attempt waiting for it lives in that process. One that lands elsewhere is
refused with a message saying so, and consumes nothing. Run a single scheduler
replica, or route these calls to the one that dispatched.

A call that arrives in the moment between the `202` and the scheduler
recording its marker is `UNAVAILABLE`, and a target **SHOULD** retry it
shortly: nothing has been decided, and that is the only status here that means
so.

A call that lost the race is `FAILED_PRECONDITION`, and a target **MUST NOT**
retry it. Losing the fence means the attempt was already settled — by a
retry that ran elsewhere, by an operator requeue, or by the deadline — and
resending would be the double execution the fence exists to refuse. The same
code answers a lease that is absent, undecodable, or not the one this claim
was won under.

This is the normal failure mode of the design rather than an edge case: a
target that runs long **will** eventually lose a race to the deadline, and
`FAILED_PRECONDITION` is how it finds out its work was thrown away.

### The deadline, and asking for longer

An accepted dispatch is waited on until a deadline that starts at the job's
own `timeout_ms`, measured from when the attempt started — the same deadline
that governs a non-202 dispatch. A target that needs longer **MUST** call
`ExtendLease` before it passes, or the job is reaped and retried under it.

`extend_by` is measured from now, not from the current deadline, because a
target knows how long it still needs and does not know what deadline the
scheduler is holding. It is **clamped to one hour per call**, not refused, and
the response carries the deadline that was actually stored — a target
**MUST** plan against that value and not against what it asked for.

The ceiling is per call and not in total: a target that needs six hours asks
six times, and its asking is what tells the scheduler it is still alive. A
target that stops asking is one the deadline collects.

### Progress and task logs

A target with an accepted dispatch **MAY** call `ReportProgress` and
`WriteTaskLog`. Both are fire and forget: an empty response means the frame
was taken, not that a row was written, and a task that only wanted to report
progress **MUST NOT** block on either.

Both are refused silently if the dispatch is not the one this scheduler is
holding open — including when the call reaches a replica other than the one
that dispatched. That costs a progress update and nothing else, which is why
they are not fenced as strictly as `Settle`.

### What a 202 does not buy

- **No durable steps.** `slept` has no arm on `Settle`, for the same reason
  `x-flexiq-outcome: slept` is refused on the request path: there is no step
  session here to resume.
- **No second result.** A target that answers `202` and *also* returns a body
  has not settled anything; the body is ignored.
- **No escape from cancel's semantics.** See below.

## A worked example

The floor for a conforming target — no framework, no dedupe store, in-memory
idempotency for illustration only:

```python
import json

import cbor2
from flask import Flask, request, Response

app = Flask(__name__)
seen = {}  # idempotency key -> (status, outcome, body) — a real target persists this

@app.route("/hook", methods=["POST"])
def handle():
    key = request.headers["x-flexiq-idempotency-key"]
    if key in seen:
        status, outcome, body = seen[key]
        return Response(body, status=status, headers={"x-flexiq-outcome": outcome})

    tag, body = request.data[0], request.data[1:]
    args, kwargs = cbor2.loads(body) if tag == 0x02 else (None, None)

    try:
        result = run_task(request.headers["x-flexiq-task"], args, kwargs)
        # Tagged, like the request was: a result is 0x02 then a bare CBOR
        # value. An untagged body is not an envelope, and a reader either
        # refuses it or reads it as same-SDK legacy — never as your CBOR.
        response_body = b"\x02" + cbor2.dumps(result)
        seen[key] = (200, "success", response_body)
        return Response(response_body, status=200, headers={"x-flexiq-outcome": "success"})
    except Exception as exc:
        error = {"errtype": type(exc).__name__, "message": str(exc), "traceback": []}
        response_body = json.dumps(error).encode()
        seen[key] = (200, "failure", response_body)
        return Response(
            response_body, status=200,
            headers={"x-flexiq-outcome": "failure", "x-flexiq-retry": "true"},
        )
```

A target that skips the dedupe entirely is still conforming — nothing here
requires memoization — but it will double-run every retried attempt's side
effects, which is exactly the case `x-flexiq-idempotency-key` exists to let a
target avoid.

## The four auth schemes

`x-flexiq-*` above travels regardless of scheme. What follows is added on top,
selected by the operator's `FLEXIQ_PUSH_TARGET_AUTH`.

### `none`

No authentication headers at all. Only sensible when the target is otherwise
unreachable — a mesh with mTLS, a socket on the same host.

### `bearer`

`authorization: Bearer <token>`, sent unchanged on every dispatch. Proves the
caller holds a secret the operator configured, nothing more: it covers no
part of the request, so a captured request replays forever. This is the
scheme HMAC exists beside.

### `hmac` — HMAC-SHA256, replay-resistant

Four headers, none of them `authorization`:

| Header | Carries |
|---|---|
| `x-flexiq-dispatch-signature` | `v1=<64 lowercase hex characters>` |
| `x-flexiq-dispatch-timestamp` | The signed timestamp, Unix seconds, canonical decimal (no leading `+`, no leading zero unless the value is exactly `0`) |
| `x-flexiq-dispatch-nonce` | The signed nonce, lowercase hex, 16 random bytes |
| `x-flexiq-dispatch-key-id` | Present only when the operator configured one; names which secret signed the request, for a receiver mid-rotation |

**Deliberately not wire-compatible with the shipped `x-flexiq-signature`
webhook scheme** (`crates/flexiq-server/src/dashboard/webhook_sender.rs`),
which means "HMAC over the body, no replay defence" everywhere it already
exists. Reusing that name here with different signed bytes would let a
webhook verifier accept an unbounded replay by mistake — a new scheme gets new
header names.

The signature is HMAC-SHA256 over a **string to sign**, hex-encoded lowercase.
The string is exactly six fields, joined with `\n`, with **no trailing
newline**:

```
FLEXIQ-HMAC-SHA256
<unix_seconds>
<nonce, lowercase hex>
<HTTP method, uppercase>
<request-target: url path, plus "?" and the query string if there is one>
<sha256(body), lowercase hex>
```

Concretely, for the vector `crates/flexiq-core/src/http/auth/hmac.rs` pins in
its own tests — secret `pinned-test-secret-do-not-rotate`, a 16-byte nonce of
`0x01` bytes, and body `{"job_id":"42","attempt":1}`:

```
FLEXIQ-HMAC-SHA256
1735689600
01010101010101010101010101010101
POST
/dispatch/42?attempt=1
270c575859e3bc6f2fd9b0bb7348b36c9af9542adb5e56807654144cfe3e9b77
```

which signs to `x-flexiq-dispatch-signature: v1=ae8e04b171f581a8d602ac9b2c074c06993423f7ebf8932c70bd5af2bdc30933`.

`<request-target>` is the origin-form target a client actually sends: the
URL's path, and `?` plus the query string only when there is one. The host is
deliberately **not** signed — a per-target secret already binds target
identity, and signing the host would make failures behind a proxy that
rewrites `Host` look like key failures instead.

**This scheme signs no `x-flexiq-*` header.** Its coverage is exactly the six
fields above, so "replay-resistant" binds the body, the method, the
request-target, the timestamp and the nonce — not *which job* the request says
it is. `x-flexiq-job-id`, `x-flexiq-task`, `x-flexiq-lease` and
`x-flexiq-idempotency-key` are unauthenticated under HMAC: an on-path attacker
who cannot forge a signature can still rewrite them and the signature still
verifies. Dispatch over TLS, and do not read those headers as if the signature
covered them. SigV4 is the one scheme that does cover them — it canonicalises
every header present when it signs, so the whole `x-flexiq-*` set appears in
its `SignedHeaders`.

A reference verifier ships in `flexiq-core`, at
`flexiq_core::http::auth::verify` — read it for the exact skew check, the
`v1=` version guard, and the constant-time comparison a target implementing
this scheme in another language needs to match. `string_to_sign`,
`HmacRejection`, `DEFAULT_MAX_SKEW` and the four header constants above are
re-exported beside it. Its default skew window is five minutes.

### `oidc` — a signed identity token

`authorization: Bearer <id-token>`, an OIDC identity token refreshed before it
expires. `flexiq-server`'s environment surface picks the source —
`FLEXIQ_PUSH_TARGET_OIDC_SOURCE` is `google`, `azure-imds` or
`azure-app-service` — but a target never sees which one; it reads the same
header either way and verifies the token against its own platform. Cloud Run
and Azure Functions' native answer to "prove who is calling".

The library API carries two further sources that the environment surface does
not, one of which an embedder has to allowlist: **`OAuth2ClientCredentials`
dials a token endpoint the operator configured, and that endpoint goes through
the same egress guard as the dispatch target.** Same transport rule — `https`,
or `http` only to a loopback host *and* only with the loopback relaxation
enabled — and its host has to be named on the same allowlist. Both are enforced
when the signer is built, not on the first dispatch, so a token URL the
allowlist does not name stops the process at boot.

The remaining sources are not allowlisted, because none of them can be pointed
at a host an operator chose: `GoogleMetadata` and `AzureImds` reach a
compile-time constant, `AzureAppService` reads a platform-supplied endpoint
that is vetted separately as loopback-or-link-local, and `File` reaches no host
at all — it re-reads a projected token off disk.

### `sigv4` — AWS Signature Version 4

`authorization: AWS4-HMAC-SHA256 Credential=...`, plus `x-amz-date`,
`x-amz-content-sha256`, and `x-amz-security-token` when the credential source
carries a session token. Standard SigV4, verified by AWS itself at the door —
Lambda function URLs' and API Gateway's native answer.

## What this contract does not promise

- **No progress and no task-log equivalent over HTTP.** An attached
  executor's `progress` and `task_log` frames have no counterpart on the
  request path, and never will: the POST is one round trip and there is
  nowhere to put them. A target that has answered `202` may report both
  through the executor door — see
  [Work that outlives the request](#work-that-outlives-the-request) — and a
  target that has not must use its own observability.
- **No durable steps.** `job_steps` and `step_ack` exist only inside an
  attached executor's stream. A push target that answers `x-flexiq-outcome:
  slept` is refused outright — there is no step session here to resume, and
  `Settle` has no `slept` arm for the same reason.
- **`cancel()` does not stop the target's work.** It abandons the request,
  settles the attempt `Cancelled`, and fences the target's eventual answer out
  on arrival — the target's process keeps running and its side effects still
  happen. An accepted dispatch inherits this unchanged: a cancel that ends
  the attempt leaves the target's later `Settle` to be refused on the fence,
  and the work it did still ran. See the per-topology table in
  [Custom executors](https://docs.byteveda.org/flexiq/python/custom-executors)
  and issue #846, which is the follow-up that changes this.
