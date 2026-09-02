# #780 — promote the big modules out of guides, one shape across three SDKs

Branch: `docs/promote-modules-out-of-guides`. Part of #777. Placement, naming and
redirects only — **no new content**.

## Target sidebar (identical in `python/`, `node/`, `java/`)

```
Getting Started        {sdk}/getting-started      unchanged
Guides                 {sdk}/guides               3 thin groups
  Tasks & execution      guides/core
  Reliability            guides/reliability
  Extend & integrate     guides/extend            (new: extensibility+observability+integrations+testing)
Modules                {sdk}/modules              NEW
  Workflows              modules/workflows
  Durable steps          modules/steps            (single shared page)
  Dashboard              modules/dashboard
  Dependency Injection   modules/injection
  Mesh Scheduling        modules/mesh             (single shared page)
  Attached Executors     modules/executor         (single shared page)
  Autoscaling            modules/autoscaling      (index + keda)
Operate                {sdk}/operate              NEW
API Reference          {sdk}/api-reference        unchanged path
Architecture           architecture               moves BELOW reference
Examples               {sdk}/more/examples        unchanged
About                  about                      renamed from `resources`
```

`Server` (#721) is deliberately absent — a slot comment in the skeleton, no stub page.

## Page moves

### guides/core — "Tasks & execution"
Stays: tasks, discovery, queues, workers, execution-model, enqueue-options,
scheduling, cancellation, predicates, batching, pubsub, streaming (node/java).
Arrives: python `advanced-execution/{prefork,async-tasks,dependencies,batch-enqueue,unique-tasks}`,
python `advanced-execution/streaming` → `core/streaming`.
Leaves: `core/steps` → `modules/steps`; python `core/workflows` (13-line pointer stub) → deleted.

### guides/reliability — unchanged (12 pages)

### guides/extend — "Extend & integrate" (new dir)
`extensibility/{events,middleware,webhooks,serializers}` + `observability/{monitoring,logging,notes}`
+ `operations/testing` + `integrations/*`.

### modules/
| new | from |
| --- | --- |
| `modules/workflows/*` | `guides/workflows/*` |
| `modules/steps` | `shared/guides/core/steps` |
| `modules/dashboard/{index,authentication,sso,task-overrides,rest-api}` | py `guides/dashboard/*`; node/java `guides/operations/{dashboard,sso,dashboard-api}` |
| `modules/injection/*` | `guides/resources/*` |
| `modules/mesh` | `shared/guides/operations/mesh` |
| `modules/executor` | `shared/guides/operations/executor` |
| `modules/autoscaling/{index,keda}` | `guides/operations/autoscaling`, `shared/guides/operations/keda` |

### operate/
backends (py `operations/postgres`), inspection (py `operations/job-management`),
cli, deployment, security, troubleshooting, migration, upgrading-0.15, graalvm.

### about/
`content/docs/resources/` → `content/docs/about/`. `scripts/sync-changelog.mjs` target follows.

## Label de-duplication
Only group titles and leaf titles show in the sidebar (an `index` page's own title never does).

- Reference leaves get an `API`/`reference` suffix: Workflow API, Canvas API, Saga API,
  Batching API, Serializer API, Testing API, Worker API, Jobs API, CLI reference,
  java `api-reference/resources` → Resource API. `overview` → Reference overview /
  Architecture overview.
- Generated symbol pages (`scripts/api/inventory.mjs` `GROUPS`) → `… signatures`, so
  "Durable steps" and "Workflows" stop colliding with the Modules entries.
  Description template in `render.mjs` adjusted to still read; `pnpm sync:api` regenerates.
- Guides/modules leaves: Testing tasks, Job management, CLI usage, Injecting resources,
  Resource proxies, Resource observability, Testing resources.

## Mechanics
- [ ] `app/lib/sdk-registry.ts` — new `navSections` order for all three SDKs
- [ ] `meta.json` for every new/changed section, one shape per SDK
- [ ] `app/lib/redirects.ts` — a move table fanned over the SDKs; a stub for every moved slug
- [ ] `scripts/parity/checks/section-shape.mjs` — **blocking**: one skeleton, titles equal,
      each SDK's `pages` a subsequence of it, no orphan MDX, no unknown page
- [ ] `scripts/parity/checks/links.mjs` — **blocking**: every internal link resolves to a
      real slug and is not a redirect source (this is what proves the link rewrite is done)
- [ ] `scripts/sync-changelog.mjs` — `resources/` → `about/`
- [ ] App links: `landing/footer.tsx`, `ui/site-nav.tsx`, `landing/sections.tsx`,
      `landing/scenario-finder.tsx`, `lib/landing-content.ts`
- [ ] `scripts/parity/checks/drift.mjs` — `NAME_EQUIVALENTS` for the normalized names
- [ ] Rewrite internal links in prose — 381 (file, prefix) edits over 169 MDX files,
      **via Edit/Write only**, verified by the new links check
- [ ] Rewrite the card walls that survive (`guides/index`, `guides/core/index`,
      `guides/reliability/index`, `guides/extend/index`, module indexes); delete the
      card walls whose group is gone (`observability`, `integrations`,
      `advanced-execution`, `operations`) and redirect them

## Verification
`pnpm --dir docs check:parity` · `check:search` · `check:diagrams` · `lint` · `typecheck` ·
`NODE_OPTIONS=--max-old-space-size=8192 pnpm --dir docs build`

## Review

Done. 306 content files, 563 prerendered URLs (real pages + redirect stubs).

- **Nav**: 8 sections per SDK, identical in all three trees. Architecture sits
  below API Reference; `resources/` is `about/`; `Modules` and `Operate` are new.
- **Guides** kept `core/` and `reliability/` as directory names — the labels
  ("Tasks & execution", "Reliability") are honest and ~330 links did not have to
  move. Only `extend/` is new.
- **Labels**: 0 duplicated sidebar entries in python (140), node (131) and
  java (128). Was 7 in python, including three "Workflows" and two "Resources".
  The generated symbol pages now read "… signatures" so they cannot collide with
  the Modules entries; `render.mjs`'s description template moved with them.
- **Links**: 1376 internal links resolve, none to a redirect source. The new
  `links.mjs` is `SdkOnly`/`Tab`-aware — a shared page that links a java-only
  page from inside `<SdkOnly sdk="java">` is correct, and treating it as broken
  would have taught the next reader to weaken the check.
- **Redirects**: 257 stubs, generated from a move table rather than listed by
  hand. Chains were flattened (`/python/guides/workflows/sagas` now points at
  `/python/modules/workflows/saga`, not at another stub).

### Gaps found while moving (not fixed here)

- Java has no "migrating from a brokered queue" guide. `about/comparison.mdx`
  linked one for all three SDKs; it is now per-SDK and java gets no link.
- `operate` and `modules` have no landing page, so their sidebar headers are
  labels rather than links and a bare hit relies on a section-landing redirect.

### Verified

`check:parity` (7 checks) · `check:search` (295 pages, 238 KB gzip) ·
`check:diagrams` (50 charts) · `biome check` · `typecheck` · `build`.
