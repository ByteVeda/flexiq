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

- [ ] `contracts/REMOTE_SDK_CONTRACT.md` — the document. Sections below.
- [ ] `.github/workflows/publish-server.yml:339` — add it to the tarball, and
      amend the comment above the `tar` so the *why* survives the next edit.
- [ ] `contracts/wire-vectors.json` `$comment` — name the new file as the
      contract the vectors are the conformance bar for. Hex is untouched: a
      diff to a hex string is a wire-format change.
- [ ] `README.md` / `ARCHITECTURE.md` — both already link `BINDING_CONTRACT.md`
      in one sentence; the remote contract goes in the same sentence.
- [ ] `crates/flexiq-core/BINDING_CONTRACT.md` — one line, at the top, saying
      which of the two contracts the reader wants.
- [ ] `docs/content/docs/server/{contract,clients,custom-executors}.mdx` — a
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

- [ ] `pnpm --dir docs typecheck` / `lint` / `build` (mdx touched)
- [ ] `actionlint` on the workflow, and the `tar` line reproduced by hand
- [ ] `python -c json.load` on `wire-vectors.json` — the `$comment` edit must
      not break the file every SDK's suite parses
- [ ] Every file:line and every constant in the document re-grepped after the
      draft, not while writing it
