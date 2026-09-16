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
holds no database credential, attaches to no stream, and speaks nothing but
plain HTTP.

The payload format is shared with both. `Job.payload` and a success result are
the same tagged wire envelope `REMOTE_SDK_CONTRACT.md`'s
[Serialization](REMOTE_SDK_CONTRACT.md#serialization) section defines byte for
byte — one tag byte, `0x02` for CBOR by default, then the body that codec
produced. Push dispatch sends and reads that envelope unchanged; it does not
define it a second time here.

## The request

`POST` to the configured URL, once per attempt.

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
| `x-flexiq-deadline-ms` | yes | Milliseconds this dispatch will wait for an answer, decimal |
| `x-flexiq-disabled-middleware` | only when non-empty | Comma-separated middleware names the operator has disabled for this task |
| `x-flexiq-metadata` | only when the job carries metadata and it fits | The job's metadata blob, base64url-encoded (no padding) |

Two headers carry a bearer-grade secret and are marked sensitive in the
scheduler's own logs: `x-flexiq-lease` and `x-flexiq-idempotency-key` (which
ends in the lease token whenever one exists). A target's own logs should treat
them the same way — see the operator page's note on header logging.

`x-flexiq-deadline-ms` is not the job's raw remaining timeout. It is the
scheduler's own request budget — the job's execution deadline, less the
reaper's safety margin, capped by the operator's configured request ceiling.
An answer that arrives after this window has already been fenced out, so
there is no value in a target racing past it.

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
| `202` | — | **Refused**: `Accepted202` — push dispatch has nothing further to wait on for a job the target says it hasn't settled yet (see #845) | no |
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

A conforming target answers within the response `x-flexiq-deadline-ms`
promised; a slower answer is fenced out and the attempt already re-dispatched
elsewhere. A response body over the operator's configured ceiling is refused
as too large rather than read in truncated form — a partial body is not what
the target said, and it does not become a job result.

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

### `sigv4` — AWS Signature Version 4

`authorization: AWS4-HMAC-SHA256 Credential=...`, plus `x-amz-date`,
`x-amz-content-sha256`, and `x-amz-security-token` when the credential source
carries a session token. Standard SigV4, verified by AWS itself at the door —
Lambda function URLs' and API Gateway's native answer.

## What this contract does not promise

- **No progress and no task-log equivalent.** An attached executor's
  `progress` and `task_log` frames have no push-dispatch counterpart; a push
  target that wants to report progress has to do it through its own
  observability, not through FlexiQ.
- **No durable steps.** `job_steps` and `step_ack` exist only inside an
  attached executor's stream. A push target that answers `x-flexiq-outcome:
  slept` is refused outright — there is no step session here to resume.
- **`cancel()` does not stop the target's work.** It abandons the request,
  settles the attempt `Cancelled`, and fences the target's eventual answer out
  on arrival — the target's process keeps running and its side effects still
  happen. See the per-topology table in
  [Custom executors](https://docs.byteveda.org/flexiq/python/custom-executors)
  and issue #846, which is the follow-up that changes this.
