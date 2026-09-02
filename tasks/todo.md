# Docs: beginner track (#781) + two nav bugs

## 1. Bug — article legible through the sticky top bar
`.nav` is 78% opaque + `backdrop-filter: blur(16px)`. Pixel-sampling the report
screenshot: text under the bar sits at luminance 197 over a 245 background —
exactly `0.78 * 245 + 0.22 * 25`, with crisp letterforms. The alpha applies; the
blur does not. Legibility must not depend on a filter the browser may skip.

- [x] `app/styles/atoms.css` — `.nav` opaque (`var(--bg)`), drop `backdrop-filter`.

## 2. Bug — top-nav "Modules" jumps to Workflows
`site-nav.tsx` links `modules/workflows` because the section has no index page.

- [x] `content/docs/shared/modules/index.mdx` — Modules landing (one file, 3 SDKs).
- [x] `section-skeleton.mjs` + 3 `modules/meta.json` — list `index`.
- [x] `redirects.ts` — drop `/{sdk}/modules` (a redirect source can't be a page).
- [x] `site-nav.tsx` — link `modules`.

## 3. #781 — a beginner track that ends with something running
Getting Started is four pages and then a cliff into 77 guides. Build the linear
track inside the section #780 settled on, one file per page under `shared/`.

- [x] Retitle the section **Start here** (skeleton + 3 meta.json + nav label).
      URLs unchanged — no redirect churn.
- [x] `shared/getting-started/index.mdx` — the track, what each page adds.
- [x] `shared/getting-started/concepts.mdx` — task, job, queue, worker, result,
      namespace, execution model. Prose + diagrams, no prior queue experience
      assumed. Replaces the three per-SDK copies.
- [x] `shared/getting-started/first-app.mdx` — a real app (signup pipeline),
      ends with a worker draining jobs the reader enqueued.
- [x] `shared/getting-started/reliability.mdx` — retries → error handling →
      timeouts → dead letters, in that order.
- [x] `shared/getting-started/production.mdx` — backend, workers, monitoring,
      deploy checklist. Links into Operate.
- [x] `shared/getting-started/next-steps.mdx` — deliberate links into Modules,
      Guides and Architecture.
- [x] One hub, not two: `capabilities` leaves the learning path for
      `about/capabilities` (evaluation), three per-SDK pages become one.
- [x] Redirects for every retired slug; fix inbound links.

## Verify
- [x] `pnpm check:parity` `check:diagrams` `check:search` `typecheck` `lint` `build`
- [x] Headless-Chrome screenshot of a scrolled docs page for the nav fix.

## Review

**Nav bar.** `.nav` is now `background: var(--bg)` with no `backdrop-filter`.
The blur was doing the legibility work and `@supports` cannot help — it reports
the property as supported on browsers that then skip it — so the only fix that
holds everywhere is an opaque bar. Verified by sampling the rendered pixels: the
band is a flat `#08080c` across the article column, varying only where the nav's
own links are.

**Modules.** `shared/modules/index.mdx` mounts at `/{sdk}/modules`, so the
top-nav entry and the sidebar group header both point at the section instead of
its first child. `Card` gained a `to=` prop (SDK-relative, checked by the link
gate the same way `SdkLink` is) so a shared page can card-link into an SDK tree.

**The track.** Eight pages, `index → installation → quickstart → concepts →
first-app → reliability → production → next-steps`, retitled "Start here".
`concepts` became one shared page covering all seven terms including namespace
and the execution model, replacing three per-SDK copies that had drifted.
`capabilities` moved to `/about/capabilities` as one SDK-scoped page — it is an
evaluation page, and two card walls at the same depth was the actual complaint.

**Not done here.** The generated API reference coverage ratchet is untouched;
`operate/cli` and `operate/migration` still exist for only two of three SDKs, so
the track links around them rather than papering over the gap.
