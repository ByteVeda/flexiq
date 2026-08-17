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

## Shared content (one file, one URL per SDK)

A file under `content/docs/shared/` mounts at the same path in **every** SDK tree:
`shared/guides/operations/mesh.mdx` serves `/python/guides/operations/mesh`,
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
slug collisions, redirect shadowing, plus an informational drift report ranking
per-SDK topic pairs by word-count ratio — that report is the migration queue.
Genuinely SDK-specific pages (Django/Flask/FastAPI, Express/Fastify/Nest,
Spring/GraalVM, `postgres`, `dashboard-api`, `upgrading-0.15`, …) stay per-SDK.

Future work: extracted code snippets (region-marked files compiled/tested in CI,
inlined by a remark plugin — the `remarkPlugins` array in `vite.config.ts` is the
seam) so examples can't rot.
