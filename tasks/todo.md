# #779 — generate the API reference from the type stubs and gate its coverage

Part of #777. Branch: `docs/generated-api-reference`.

## Problem

Every reference page is hand-written prose with no link back to the symbol it
documents. A method added to the core reaches three shells and zero reference
pages, and nothing notices.

## Decisions (the seam, written down once)

1. **Inventory source per SDK** = the one declaration file that enumerates that
   SDK's queue surface *under the names a user calls*:
   - python `sdks/python/flexiq/_flexiq.pyi` — the shell's mixins forward these
     names verbatim (`cancel_job`, `dead_letters`, `dry_run_retention`, …).
   - node `sdks/node/native/index.d.ts` — same, camelCase.
   - java `org/byteveda/flexiq/*.java` (the public root package) — **not** the
     JNI facade: `QueueBackend`'s `getJobJson` names are native-shaped and are
     not what a Java user calls. A glob, not three named files, so a new public
     interface is picked up rather than silently skipped.
2. **The seam is a pre-build sync step that writes MDX**, not a remark plugin
   and not a virtual module. The search index, `/llms-full.txt` and every parity
   check read *raw MDX text* (`app/lib/mdx-extract.ts`), so anything expanded at
   compile time is invisible to all of them — the symbols would be generated and
   still unsearchable. `sync:changelog` is the existing precedent.
3. **Committed JSON snapshots** under `docs/content/api/` so CI never depends on
   `native/index.d.ts`, which is gitignored and absent on a clean checkout.
   `ci-node.yml` re-checks that snapshot after `build:native`.
4. **No generated parameter tables.** Names, types and defaults are already in
   the signature; the column that would make a table worth reading is
   Description, and that is prose the hand-written page owns.
5. **Inline blocks are adopted per page, Java first.** The Python and Node
   shells re-type most returns (`PyJob` → `JobResult`), so inlining a native
   signature there would make a page *less* accurate.

## Steps

- [x] Read #779 / #777, the parity gate, the search corpus, the reference tree.
- [x] Decide the seam and the per-SDK inventory source.
- [x] `docs/scripts/api/extract/{python,node,java}.mjs` — dependency-free
      parsers producing one symbol shape.
- [x] `inventory.mjs` (sources, groups, snapshot IO) · `render.mjs` (symbol →
      MDX) · `generate.mjs` + `plan.mjs` (one plan, two consumers) ·
      `coverage.mjs` (the measure) · `sync.mjs` (`--check`, `--backlog`, `--sdk`).
- [x] Generated `{sdk}/api-reference/symbols/**` + nav entry (16 files).
- [x] In-page blocks `{/* api:Owner.name */}`, adopted on the Java jobs page.
- [x] `scripts/parity/checks/api-coverage.mjs` + `api-coverage.json`.
- [x] Wire: `package.json`, `parity/index.mjs`, `docs/README.md`, `docs.yml`
      `paths:`, `ci-node.yml` node-only `--check`.
- [x] Search gate asserts a generated-only symbol resolves.
- [x] Verify: typecheck, lint, check:parity, check:diagrams, check:search, build.
- [x] Seven focused commits as kartikeya-27. Not pushed.

## Review

**Coverage, measured (a symbol counts when its name appears in *code* on a
hand-written reference page; the generated section is excluded):**

| SDK | documented | of | allowlisted |
|---|---|---|---|
| python | 36 | 120 | 31 |
| node | 39 | 110 | 15 |
| java | 139 | 167 | 0 |

Python's 36 reproduces the figure in #779 exactly, from an independent path —
good evidence the extractor sees the same surface the issue counted.

**What now fails the build:** a declaration change without `pnpm sync:api`; a
generated page edited by hand; an inline marker naming a symbol that no longer
exists; a declaring type with no page assigned; a stale allowlist entry; and
coverage moving in either direction without the ratchet being updated.

**Drift report:** 24 → 23 flagged topics (`api-reference/queue/jobs` dropped off
at 2.4x). The rest of the reference drift closes as pages adopt inline blocks;
that is the follow-up, not this issue.

**Search:** the index grew to 306 pages / 241 KB gzip (budget 320 KB).
`purge_queue`, `acquire_lock`, `openStepSession` and every other declared symbol
now resolve; before this they returned nothing.

**Known limitation, documented in `docs/README.md`:** for Python and Node the
inventory is the native surface, and the shells rename a handful of methods
(`replay_job` → `replay`, `get_replay_history` → `replay_history`,
`archive_old_jobs` → `archive`). Those read as undocumented in the backlog. The
fix is to extract the shell surface for those two SDKs as Java already does —
worth its own issue.
