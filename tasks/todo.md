# #828 — `REMOTE_SDK_CONTRACT.md`, the contract the proto does not write down

Branch `docs/remote-sdk-contract` off `master` at `0950d0dc`. One new file, one
release-packaging line, four pointers. No Rust, no proto, no schema.

## What is actually missing

The tier is not undocumented. `docs/content/docs/server/` is ~1800 lines across
seven pages and already carries the tag table, the retryability table, the scope
rule, the "no contract-level field on the wire" answer and a capability-delta
page (`limits.mdx`) that reads almost exactly like the issue's sixth bullet.

Three things are genuinely absent, and they are the reason the file exists:

1. **Nothing states what makes a client conformant.** No page anywhere says
   which RPCs a program must implement to call itself a FlexiQ client and which
   it may skip. The issue asks for that split first, and it is new text.
2. **Nothing is normative.** The docs are a guide — "assert against it too, it
   is the cheapest conformance test you will write". A contract has to be
   testable prose: MUST, MUST NOT, SHOULD, closed lists.
3. **The docs are a website.** A client author downloads
   `flexiq-proto-2.0.0.tar.gz`, gets `proto/` and `wire-vectors.json`, and has
   no specification in the tarball. The seventh bullet — "make it loadable from
   outside this repository" — is half done already
   (`publish-server.yml:339` puts the vectors in the tarball); what is missing
   is the document that says passing them is the bar, in the same tarball.

So this is not a fourth copy of `limits.mdx`. It is the normative source those
pages narrate, and it ships with the artifacts it specifies.

## Placement

`contracts/REMOTE_SDK_CONTRACT.md`, beside `proto/`, `wire-vectors.json`,
`descriptor.binpb` and `BUF_VERSION` — the other half of the same tarball, and
a one-argument change to the `tar` line rather than a `cp` before it.

`crates/flexiq-core/BINDING_CONTRACT.md` stays where it is and keeps its
audience: a language shell compiled against the core, in one process. The two
sit at the same level and point at each other.

## The plan

- [x] `contracts/REMOTE_SDK_CONTRACT.md` — the document. Sections below.
- [x] `.github/workflows/publish-server.yml:339` — add it to the tarball, and
      amend the comment above the `tar` so the *why* survives the next edit.
- [x] `contracts/wire-vectors.json` `$comment` — name the new file as the
      contract the vectors are the conformance bar for. Hex is untouched: a
      diff to a hex string is a wire-format change.
- [x] `README.md` / `ARCHITECTURE.md` — both already link `BINDING_CONTRACT.md`
      in one sentence; the remote contract goes in the same sentence.
- [x] `crates/flexiq-core/BINDING_CONTRACT.md` — one line, at the top, saying
      which of the two contracts the reader wants.
- [x] `docs/content/docs/server/{contract,clients,custom-executors}.mdx` — a
      link to the normative file. The pages keep their prose; they stop being
      the only place the rules exist.

## What the document says

Nine sections, in `BINDING_CONTRACT.md`'s register — bolded claim, then why,
then the mechanical consequence; a table for anything enumerable; a byte-exact
vector for every hard rule.

1. **Two contracts, and which one you are reading.** The §3.1 table from
   `tasks/specs/2026-09-01-flexiq-v1-proto-design.md`, plus which wins where.
2. **Conformance.** The bar is `contracts/wire-vectors.json`: decode all 12,
   encode every one of the 9 `encode` cases the client's call API can express,
   re-encode the two `round_trip_only` cases to the same bytes. Three routes to
   the file without a checkout — release asset, raw URL at a tag, reflection
   for the proto half.
3. **Surface.** Mandatory vs optional, per door.
   - Producer: `Enqueue` and `GetJob` MUST — there is no completion
     notification, so a client that cannot read back cannot observe an outcome
     at all. `CancelJob` SHOULD. `EnqueueBatch`, `ListJobs`, `QueueStats`,
     `SubmitWorkflow`, `GetWorkflowRun` MAY.
   - Executor: `Attach` MUST, `Heartbeat` SHOULD — verified: `idle_ms` feeds
     the dashboard and nothing evicts on it (`worker/remote.rs:476`,
     `dashboard/routes/executors.rs:38`), so the stream is what liveness is.
     Frames: `hello` and exactly one settling frame per job MUST; `progress`,
     `task_log`, `step_commit`, `slept` are gated on the capability that was
     advertised, and MUST NOT be sent without it.
4. **Serialization.** Tag byte, `[args, kwargs]`, bare-value result, the two
   encoder rules (definite-length, shortest-form) and why they are not style —
   the `auto:` idempotency key hashes these bytes. The `structured` arm and the
   four things it refuses rather than rounds.
5. **Errors.** Two different structured errors, which the docs never separate in
   one place: `google.rpc.Status` + `ErrorInfo` for a failed *request*, and the
   `TaskError` JSON inside `Job.error` for a failed *job*. The closed reason
   list, the metadata keys and their encodings, and the two unparseable-payload
   rules (a bad metadata value is absent, not fatal; a `Job.error` that does not
   parse is surfaced verbatim).
6. **Auth.** `authorization: Bearer`, `fqt_<id>.<secret>`, mandatory expiry
   (90 d default, 365 d cap), revocation effective on the next call, two
   package-level scopes that are not a hierarchy.
7. **Namespace.** There is no field. It is the credential's. A token for
   another namespace is `UNAUTHENTICATED`, never `PERMISSION_DENIED` — the
   other answer is an existence oracle.
8. **Compatibility.** Three numbers a reader will collide: the package version
   (`v1`, permanent), `CONTRACT_VERSION`/`MIN_CONTRACT_VERSION` (both `2`,
   storage-only, **no field on either package carries it**), and the executor
   `protocol_version` (`1`, equality-checked, the only version a remote peer
   declares). The floor answer the issue asks for: a remote client cannot
   violate or observe it, `ensure_contract_supported` runs once at storage open
   in the server process, and `CONTRACT_TOO_OLD`'s `speaks` is the *server's*
   level — it is on the closed list because the list is closed, not because a
   client can cause it.
9. **The delta from embedded.** The absent surfaces, stated as absent: settings
   CAS, migrations, retention/election, topic pub/sub, dead-letter operations,
   worker-registry CRUD, middleware, direct storage, and durable steps as a
   unary call. Plus the four that hold for an SDK too but bite a network caller
   first — no ordering, at-least-once, non-atomic batches, impermanent ids.

## Rules for the writing

- **Cross-link, never re-narrate.** `limits.mdx` and `custom-executors.mdx`
  already carry a near-verbatim "an executor cannot enqueue" paragraph each. A
  third copy is how three copies become three different rules.
- **Every hard claim is checked against code, not against the design spec.**
  The spec is a decision record and predates two amendments.
- Self-contained on the issue's seven bullets: a reader with the tarball and no
  network can implement from it. Links are for narrative, never for a rule.

## Verify

- [x] `pnpm --dir docs typecheck` / `lint` / `build` — all green, build exit 0
- [x] The `tar` line reproduced by hand; `actionlint` is not installed here, so
      the YAML was parsed and the step's `run` block read back instead
- [x] `json.load` on `wire-vectors.json`, and the counts the document states
      re-derived from it: 9 `encode`, 3 `decode_only`, 2 `round_trip_only`,
      2 `encode` cases with a non-empty `kwargs`
- [x] Every constant in the document re-grepped after the draft. Four claims
      did not survive it — see below.

---

# Review

Five commits, authored as `kartikeya`. No Rust, no proto, no schema: the only
executable change is a `tar` argument.

## The four claims the draft got wrong

Each was written from a design document or a docs page and corrected against
code. This is the argument for the re-grep pass being separate from the writing.

1. **`EnqueueBatch` fails in two shapes, not one.** The draft said a client
   reads results per item. It does — *when the batch could partially apply*.
   Where it could not, the RPC itself fails and carries the failing item's
   `index`, because returning earlier items as enqueued would report jobs that
   do not exist (`producer_service.proto:145-149`). A client that only reads
   `results` never sees that case.
2. **`GetJob` does not return the payload by default.** `include_payload` is
   opt-in (`producer_service.proto:163-165`). Nothing on the docs site says so,
   and a client author would have found it by getting an empty field.
3. **The docs URL.** `flexiq.byteveda.org` does not exist; the site is
   `docs.byteveda.org/flexiq`.
4. **`RetryInfo` is not `QUEUE_FULL`'s.** It is attached to *every*
   `RESOURCE_EXHAUSTED` — `retry_after: (code == Code::ResourceExhausted)`,
   `grpc/status/mod.rs:77` — so `RATE_LIMITED` carries one too. The design spec
   names only `RESOURCE_EXHAUSTED`, which reads as the one reason if you come to
   it through `QUEUE_FULL`'s metadata row.

## Two things resolved by reading rather than by asserting

- **`Heartbeat` is SHOULD, not MUST.** The obvious reading is that an executor
  that stops heartbeating gets reaped. It does not: `idle_ms` is derived from
  `last_seen_ms` (`worker/remote.rs:476`), *every* frame updates that
  (`remote.rs:1439,1449`), and its only consumer is the dashboard's executor
  list (`dashboard/routes/executors.rs:38`). Nothing evicts on it. The stream is
  what liveness is; the heartbeat carries `free_slots` and an operator's view.
- **`CONTRACT_TOO_OLD` is never about the client.** `ensure_contract_supported`
  is called from exactly one place (`contract.rs:94`) and every caller invokes
  it at storage open — including `flexiq-server` itself, before it serves. There
  is no request path that raises it, and its `speaks` is the *server's* level.
  The reason is on the closed list because the list is closed. That is the
  sharpest available answer to the issue's fifth bullet.

## What was deliberately not done

- **No fourth copy of `limits.mdx`.** The delta section states the absent
  surfaces as a table and drops the rationale prose; `limits.mdx` and
  `custom-executors.mdx` already carry a near-verbatim "an executor cannot
  enqueue" paragraph each, and a third would be how three copies become three
  rules. The docs pages now name the normative file instead.
- **No SDK packaging change.** `wire-vectors.json` is in no wheel, npm tarball
  or jar, and adding it to three would be three new packaging surfaces to keep
  in step. The release asset already carries it, and now carries the document
  that says passing it is the bar — which is the issue's seventh bullet.

## One bug found and not fixed, because it is not this issue

`crates/flexiq-core/tests/rust/wire_vector_tests.rs` loads the vectors with
`include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../contracts/wire-vectors.json"))`
— a path **outside the crate root**. `flexiq-core` is published to crates.io
(`publish-crates.yml:40`) and `cargo package` copies nothing above the crate
directory, so `cargo test -p flexiq-core` against the published crate fails to
compile on a missing file. CI never catches it: `cargo package --locked` builds
lib and bin targets, not test targets. Latent, because nobody runs the vendored
crate's tests. Worth its own issue.
