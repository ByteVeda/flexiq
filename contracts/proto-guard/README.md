# `proto-guard` — a self-test for the proto CI gate

These are not FlexiQ protos. They are fixtures that prove
`.github/workflows/ci-proto.yml` catches the changes
`tasks/specs/2026-09-01-flexiq-v1-proto-design.md` §11 says it must.

They exist because the real module cannot demonstrate it. `contracts/proto`
carries an enum and no message fields today, so "a renumbered field fails CI"
has nothing to renumber — and a break demonstrated by hand once, in a PR
description, stops being true the moment someone edits `buf.yaml`.

## What makes it load-bearing

`scripts/proto-guard.sh` stages each case into a temp directory next to a copy
of **`contracts/proto/buf.yaml` itself** — the production config, not a second
copy of its rules. Relax `lint.use` from `STANDARD` to `BASIC`, or
`breaking.use` from `WIRE_JSON` to `WIRE`, and the guard goes red. That is the
point: the fixtures test the config, not buf.

`contracts/proto-guard` sits outside the module root (`contracts/proto`), so
nothing here is ever linted, formatted or built as a shipped proto.

## The cases

`baseline/` is the "before" image. Each `cases/<name>/` is a whole copy of it
with one mutation, so a reviewer reads a file rather than a patch.

| Case | Check | Expect | Pins |
|---|---|---|---|
| `renumbered-field` | breaking | fail | §2.1 rule 1 |
| `removed-field-bare` | breaking | fail | §2.1 rule 3 |
| `removed-field-number-only` | breaking | fail | D4 — `WIRE` would accept this |
| `removed-field-reserved` | breaking | **pass** | the sanctioned removal |
| `renamed-field` | breaking | fail | D4 — invisible to binary protobuf |
| `added-field` | breaking | **pass** | §2.1 rule 4 — additive stays free |
| `service-suffix` | lint | fail | §1.2 `SERVICE_SUFFIX`, absent from `BASIC` |
| `enum-value-prefix` | lint | fail | §1.2 `ENUM_VALUE_PREFIX`, absent from `BASIC` |

The two "pass" rows matter as much as the failures. Without them a gate tuned
until everything is breaking would still look green.

## Running it

```bash
scripts/proto-guard.sh
```

Needs the pinned `buf` on `PATH` — see `contracts/BUF_VERSION`.
