# #712 — buf lint, breaking checks and a committed descriptor

## Problem
#711 settled the wire contract on paper. D3 makes `flexiq.v1` permanent and D4
freezes field *names* as well as numbers, and nothing enforces either. #714 is
about to spend field numbers, and a number is spent forever the moment it
merges — so the guard has to exist first.

## Deliverable
The buf module, the CI gate, and a self-test that proves the gate catches what
§11 of `tasks/specs/2026-09-01-flexiq-v1-proto-design.md` says it must.

## Scope calls
- **The seed proto is the `JobStatus` enum and nothing else.** buf refuses a
  module with zero `.proto` files, so #712 must ship one; §7.3 pins `JobStatus`
  verbatim, so transcribing it spends no field number §1.4 hands to #714.
- **No `buf.gen.yaml` yet.** prost/tonic are not workspace deps and
  `flexiq-server` has no `grpc` feature until #713. `contracts/descriptor.binpb`
  is the only artifact §1.1 names; codegen config lands with the crate that
  consumes it.

## Plan
- [x] `contracts/proto/buf.yaml` (v2) — lint `STANDARD`, breaking `WIRE_JSON`.
- [x] `contracts/proto/flexiq/v1/job.proto` — `JobStatus`, the §2.2 unstable
      notice, and §2.1's rules as comments, because a client reads the `.proto`
      and not the design doc.
- [x] `contracts/BUF_VERSION` — one pin, read by CI and by the local script.
- [x] `contracts/descriptor.binpb` — committed, `--as-file-descriptor-set`.
- [x] `scripts/proto-check.sh` — format, lint, descriptor drift. `--fix` to
      regenerate. The same file CI runs.
- [x] `contracts/proto-guard/` + `scripts/proto-guard.sh` — eight fixtures run
      against the production `buf.yaml`.
- [x] `.github/workflows/ci-proto.yml` + the `proto` suite in `ci.yml`.
- [x] A `proto-check` pre-commit hook, so the descriptor cannot go stale locally.

## Review

### Why a self-test rather than a demonstration
The acceptance bullets ("a renumbered field fails CI") cannot be shown on the
real module — the seed has an enum and no message fields — and a break
demonstrated once in a PR description stops being true the moment someone edits
`buf.yaml`. `scripts/proto-guard.sh` stages each fixture beside a copy of
`contracts/proto/buf.yaml` **itself**, so it tests the configuration rather than
buf. Proven by relaxing the config to `BASIC`/`WIRE` and watching four cases go
red, then restoring it.

Two of the eight cases expect a **pass** — `removed-field-reserved` and
`added-field`. Without them a gate tuned until everything is breaking would
still look green.

### `WIRE_JSON`, not `WIRE`
Verified rather than assumed: removing a field while reserving only its number
passes at `WIRE` and fails at `WIRE_JSON`. That is exactly D4 — the JSON facade
(#718) publishes field names to clients that have no `.proto`, and a rename is
invisible to binary protobuf and fatal to them.

### Three things that only showed up by running it
1. **buf errors on a module with no `.proto` files**, which is what forced the
   seed decision above.
2. **`buf breaking --against` a ref whose subdir has no protos errors**, so the
   PR that introduces the module cannot go green without a bootstrap guard. The
   workflow checks `git cat-file -e origin/master:contracts/proto/buf.yaml`
   first; it self-disarms after this merges.
3. **The descriptor is byte-stable only for a fixed buf version**, which is the
   second reason for the pin — the first being that a lint release should not
   red a branch nobody touched.

### Left for whoever needs it
The `proto-breaking` label does not exist in the repo yet. `contains()` on an
absent label is `false`, so the workflow is correct before it is created:
`gh label create proto-breaking -d "Sanctioned pre-release proto break (§2.2)"`.
