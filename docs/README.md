# FlexiQ docs

The FlexiQ documentation site — a polyglot (Python + Node.js) docs site built with
[React Router v7](https://reactrouter.com) in framework mode, statically prerendered
to HTML for GitHub Pages.

## Stack

- **React Router v7** (framework mode, `ssr: false` + `prerender`) — static export to
  `build/client/`.
- **MDX** via `@mdx-js/rollup` — the content under `content/docs/**/*.mdx` is the source,
  compiled at build and rendered through the component map in `app/components/mdx/`.
- **Shiki** for build-time code highlighting; **Mermaid** (client-side) for diagrams.
- **Tailwind v4** + a ported design system in `app/app.css` (dark + warm-light themes).
- **MiniSearch** for the client-side ⌘K search index.

## Develop

```bash
pnpm install
pnpm dev          # local dev server
pnpm typecheck    # react-router typegen + tsc
pnpm lint         # biome
pnpm build        # static prerender → build/client/
pnpm start        # serve the built output
```

Set `DOCS_BASE_PATH=/flexiq` to build under the GitHub Pages base path (CI does this).

## Layout

```
content/docs/**/*.mdx     the documentation source (+ meta.json nav)
app/
  root.tsx                document shell, fonts, no-flash theme
  routes.ts               route config (landing, llms.txt, docs catch-all)
  routes/                 home, docs-layout, docs.$ (MDX page), llms[.txt]
  components/
    landing/              polyglot landing sections
    docs/                 sidebar, SDK switch, TOC, prev/next, search modal
    mdx/                  Callout/Tabs/Card shims + MDX component map
    ui/                   nav, theme toggle, RawHtml
    animated/             diagram components (reused in MDX)
  lib/                    content glob, nav tree, search index, theme, highlighters
```

Adding a page: drop an `.mdx` file under `content/docs/`, add it to the directory's
`meta.json`. It is picked up, prerendered, indexed for search, and slotted into the
sidebar automatically.

The nav shape itself is committed in `scripts/parity/section-skeleton.mjs` and gated
by `pnpm check:parity`: the three SDK trees carry the same sections with the same
titles, and a `meta.json` may only list pages the skeleton names, in the skeleton's
order. An SDK that lacks a topic omits the page — it never invents a group for it.
Moving a page means adding its old URL to `app/lib/redirects.ts` and repointing
every link, both of which the same gate checks.

## Shared content (one file, one URL per SDK)

A file under `content/docs/shared/` mounts at the same path in **every** SDK tree:
`shared/modules/mesh.mdx` serves `/python/modules/mesh`,
`/node/...`, and `/java/...` from one source. Slug + fan-out logic lives in
`app/lib/doc-slugs.ts` — the runtime loader, prerender walk, manifest plugin, and
parity checks all resolve through it. Non-default-SDK mounts carry a
`<link rel="canonical">` to the default-SDK URL, and `llms.txt` lists each shared
page once.

Authoring rules for shared pages:

- **Prose is SDK-neutral.** Use `<SdkName/>` / `<SdkSwap python=… node=… java=…/>`
  for language-specific words; wrap SDK-specific paragraphs in `<SdkOnly sdk="…">`.
- **Code goes in `<CodeTabs>`** with one `<Tab sdk="…">` per SDK. CI fails a
  shared page whose CodeTabs misses an SDK — add `data-parity-exempt` to the tag
  only when a feature genuinely doesn't exist there (prefer `SdkOnly` instead).
- **Frontmatter is shared** across all mounts — keep title/description
  SDK-neutral.
- **No `meta.json` under `shared/`.** List the page name in each SDK section's
  `meta.json` (that's also how an SDK opts out of a topic).
- **Collisions fail the build.** A per-SDK file at the same path as a shared file
  is an error, never a silent override — delete the per-SDK file when migrating.
- **Accuracy first.** Verify every per-SDK claim against that SDK's source before
  writing it; a missing tab is better than a fabricated API.

`pnpm check:parity` runs the CI gate (`scripts/parity/`): CodeTabs SDK coverage,
slug collisions, redirect shadowing, API reference coverage, plus an
informational drift report ranking per-SDK topic pairs by word-count ratio —
that report is the migration queue. Genuinely SDK-specific pages
(Django/Flask/FastAPI, Express/Fastify/Nest, Spring/GraalVM, `postgres`,
`dashboard-api`, `upgrading-0.15`, …) stay per-SDK.

## Generated API reference (`pnpm sync:api`)

Signatures, parameters, defaults and return types are generated from each SDK's
own declarations, so they cannot contradict the code. Prose, examples and
gotchas stay hand-written beside them.

```bash
pnpm sync:api              # regenerate; the fix for every failure below
pnpm sync:api --check      # what check:parity asserts
pnpm sync:api --backlog    # symbols with no hand-written entry yet
```

**Where the surface comes from.** One rule: the single declaration file that
enumerates that SDK's queue surface *under the names a user calls*.

| SDK | Source | Why |
|---|---|---|
| Python | `sdks/python/flexiq/_flexiq.pyi` | the mixins forward these names verbatim |
| Node | `sdks/node/native/index.d.ts` | same, camelCase |
| Java | `org/byteveda/flexiq/*.java` (the public root package) | **not** the JNI facade — nobody calls `getJobJson` |

Extraction lands in `content/api/<sdk>.json`, which is **committed**. That is
not a cache: `native/index.d.ts` is generated by `pnpm build:native` and
gitignored, so a clean checkout has no Node source and the gate would otherwise
be unrunnable in the docs CI job. `ci-node.yml` re-checks that snapshot after
building the addon — the one job where the real file exists.

**The seam is a pre-build step that writes MDX**, not a remark plugin and not a
virtual module. The search index, `/llms-full.txt` and every parity check read
raw MDX text through `app/lib/mdx-extract.ts`; anything expanded at compile time
is invisible to all of them, so the symbols would be generated and still
unsearchable. `sync:changelog` is the same shape.

Two things are generated:

- **`{sdk}/api-reference/symbols/`** — the complete declared surface, one page
  per group. Entirely generated; don't hand-edit.
- **In-page blocks** on hand-written pages:

  ```mdx
  ### `flexiq.getJob()`

  {/* api:FlexiQ.getJob */}
  {/* /api */}

  A job snapshot — status, progress, timestamps.
  ```

  `pnpm sync:api` fills the block; the marker is the page's link back to the
  symbol, and a marker naming a symbol that no longer exists fails the build.
  Adopt a block **only where the shell's signature is the declared one** — the
  Python and Node shells re-type most returns (`PyJob` → `JobResult`), so
  inlining a native signature there would make the page less accurate, not more.

**The gate** (`scripts/parity/checks/api-coverage.mjs`) blocks on two things:
the generated half disagreeing with the declarations (run `pnpm sync:api`), and
hand-written coverage falling below the ratchet in
`scripts/parity/api-coverage.json`. That file also holds the allowlist — every
deliberately-undocumented symbol is a line with a reason, and a stale exemption
is an error, so it can't become a place gaps hide. The ratchet is exact in both
directions: writing a new reference entry fails the build until the number is
raised.

Future work: extracted code snippets (region-marked files compiled/tested in CI,
inlined by a remark plugin — the `remarkPlugins` array in `vite.config.ts` is the
seam) so examples can't rot.
