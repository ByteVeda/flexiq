# docs: a Rust tier on the site (#920)

Second half of #830. The crate shipped in #916 (`e4340ba7`); the site's switcher
offers three languages and Rust is not one of them.

Branch `docs/rust-tier`, worktree `../taskito-rust-docs`, off `master`. Not pushed.

## The shape of the problem

`"rust"` in `SDK_IDS` is not additive. The moment it joins:

- `shared/**` fans out to `/rust/**` (`mountsForRelPath`) — 41 files, **160
  `<CodeTabs>` blocks**, every one of which `checks/code-tabs.mjs` requires to
  carry a `<Tab sdk="rust">`, no grandfathering.
- `checks/section-shape.mjs` demands all 14 `SECTION_SKELETON` dirs under
  `rust/`, title-exact, `meta.pages` a **subsequence** of the skeleton's list.
- `checks/links.mjs` resolves every unscoped `<SdkLink>`/`<Card to>` once per
  SDK — a shared page linking `operate/cli` now needs `/rust/operate/cli`.
- `scripts/api/inventory.mjs` `SDKS = [...SDK_IDS]`, so `SOURCES.rust` and
  `content/api/rust.json` must exist or `sync:api` throws.
- `CodeTabs` renders `SDK_PROFILES[variant].label` — a `<Tab sdk="rust">` before
  the registry row exists **crashes the prerender**. Registry lands first.

## Decisions

- **Page tree is the honest subset (~45 pages), not 78-page Java parity.**
  `meta.pages` may be a subsequence, so an SDK omits a page rather than
  documenting a surface it does not ship. No workflows builder, DI, canvas,
  saga, events, webhooks, framework integrations, async tasks, prefork or
  streaming pages — the shell has none of them.
- **Every one of the 160 shared blocks gets a `rust` tab.** Real code where the
  shell (or the `queue.storage()` / `flexiq_core` escape hatch) supports it;
  a short honest note where it does not. Never `data-parity-exempt` — an exempt
  block renders an **empty panel** under `data-sdk="rust"`.
- **Accuracy floor: every Rust snippet is grounded in `crates/flexiq`.** The
  README's "durable steps" and "periodic tasks" examples are ```` rust,ignore ````
  and do not compile — `crates/flexiq/tests/{steps,periodic,worker,queue}.rs`
  and `examples/quickstart.rs` are the known-good sources.

## Tasks

### Phase 0 — register the SDK (code)
- [ ] `app/lib/highlight-lite.ts` — `RUST_KW` + `highlightRust()`
- [ ] `app/lib/sdk-registry.ts` — `SDK_IDS` + `SDK_PROFILES.rust` (+ `navSections`)
- [ ] `app/styles/sdk.css` — 4th `[data-sdk="rust"]` pair (not compile-forced)
- [ ] `app/lib/redirects.ts` — its own `SDKS` literal, not derived
- [ ] `app/lib/landing-content.ts` — `lang: "rs"` + a Rust hero pane
- [ ] `app/components/landing/hero.tsx` — `HIGHLIGHT.rs`
- [ ] `app/components/ui/site-nav.tsx` — `TIER_ICONS.rust`
- [ ] `app/components/landing/sections.tsx` — the `python · node · java` string
- [ ] `app/components/diagrams/{arch-stack,worker-fork}.tsx` — `rust=` on all 7
      `<SdkSwap>` (`Partial<Record<Sdk,…>>`: a missing prop silently renders
      Python's copy)

### Phase 1 — the generated API reference
- [ ] `scripts/api/extract/rust.mjs` — text parser over `crates/flexiq/src/*.rs`
      (`cargo doc` is not available to the docs CI job)
- [ ] `scripts/api/inventory.mjs` — `SOURCES.rust` + `OWNERS` rows for the
      shell's declaring types
- [ ] `scripts/api/render.mjs` — `rustParam` + a Rust arm in `callForm`
      (`-> T`, not `: T`)
- [ ] `pnpm sync:api` → commit `content/api/rust.json` + the generated
      `rust/api-reference/symbols/**`
- [ ] `scripts/parity/api-coverage.json` — `allowlist.rust` + `documented.rust`

### Phase 2 — the Rust content tree (`content/docs/rust/**`)
- [ ] 14 skeleton `meta.json` + tier root + `more/`
- [ ] getting-started: `installation`
- [ ] guides: `index`; core: `index`, `execution-model`, `enqueue-options`,
      `cancellation`, `batch-enqueue`, `unique-tasks`, `dependencies`
- [ ] reliability: `index`, `timeouts`, `dead-letter`, `rate-limiting`, `concurrency`
- [ ] extend: `index`, `logging`, `notes`
- [ ] modules: `workflows/index`, `dashboard/index`
- [ ] operate: `index`, `backends`, `inspection`, `cli`, `security`
- [ ] api-reference: `index`, `overview`, `task`, `worker`, `result`, `errors`;
      `queue/{index,jobs,queues,workers}`
- [ ] more/examples: `index`, `notifications`, `data-pipeline`

### Phase 3 — the shared tree
- [ ] 160 `<Tab sdk="rust">` panels across 34 files
- [ ] `rust=` on the 64 `<SdkSwap>` in 16 content files

### Phase 4 — green
- [ ] `pnpm check:parity` (links.mjs names every missing `/rust/` page — that
      list, not a guess, decides the last few pages)
- [ ] `pnpm typecheck`, `pnpm lint`, `check:diagrams`, `check:search`
- [ ] `NODE_OPTIONS=--max-old-space-size=8192 pnpm build`

### Phase 5 — commits
- [ ] Focused commits, stromanni identity, no AI attribution, no push

## Review

All nine parity checks, `typecheck`, `lint`, `check:diagrams`, `check:search`
and the full prerender build are green. The tier is **51 mdx + 17 meta.json**,
of which `api-reference/symbols/` (4 mdx + meta) is generated.

### What the shape of the work turned out to be

The 160 `<CodeTabs>` blocks were the advertised cost. Two things were not:

- **`<SdkOnly>` has no parity check.** 32 shared files carried ~180 blocks with
  no `rust` sibling — parameter tables and whole sections that render as a
  heading with nothing under it, exactly the failure `data-parity-exempt` would
  have caused in a tab. Found by scanning, not by a gate. Now filled; a scan for
  `rust < max(python, node, java)` per file comes back clean.
- **Prose between the tabs.** "the three SDKs", "flexiq ships a Sentry
  middleware", "a dedup key is rejected" — all false for a fourth SDK, none of
  it checkable. `architecture/overview.mdx` rendered "a hybrid **Rust/Rust**
  system" off `<SdkLang />`.

### Two checks changed, both because a fourth SDK exposed them

- `links.mjs` resolved an unscoped `<SdkLink>` for *every* SDK regardless of
  which tree the file lived in, so a `/rust/` page could not link its own pages
  whenever a sibling tier omitted one. A page under `<sdk>/` is only ever read
  under that SDK (`forcedSdkForPath`), so it now scopes to its own tier.
- `api-coverage.mjs` compared against `undefined` when an SDK had no
  `documented.<sdk>` baseline. Both comparisons are false, so the ratchet
  silently never fired. It is now an error.

### Grounding

Every Rust snippet was written against `crates/flexiq` at `e4340ba7`, and most
sections were compile-checked by assembling their fences into a throwaway
`examples/` target and running `cargo check`. That caught real bugs before they
shipped — notably that `let extract = queue.enqueue(extract::call(…))` parses as
a *struct pattern*, because `#[flexiq::task]` emits a unit struct named after
the function.

### Findings worth their own issues

- **`enqueue` silently prefers debounce over a dedup key.** `queue.rs` matches
  `(Some(window), _) => enqueue_debounced` first, so `.idempotent()` is never
  applied; another shell raises on the combination (`app.py:923`). A shipped
  behaviour divergence, not a docs bug.
- **No public typed result read.** `decode_result` is `pub(crate)`; `job.result`
  is `Option<Vec<u8>>` and the crate's own example prints its length.
- **`queue.storage()` is namespace-blind** — a scoped handle silently loses its
  scope through the escape hatch.
- `WorkerBuilder` exposes neither `on_outcome` nor `queue_config`, and reaching
  them through `flexiq_core::Worker` costs the durable-step fence.
- `crates/flexiq` declares `log` as a dependency and never uses it.
- `/api/scaler` is not in `PUBLIC_PATHS`, so `FLEXIQ_DASHBOARD_AUTH=session`
  returns 401 to a KEDA `metrics-api` trigger.
- Mesh `request_shutdown()` notifies one of two waiting loops; the `Leave`
  broadcast only runs in the gossip loop's arm.
