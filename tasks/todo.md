# Dark-theme parity, Node 24, Lighthouse

Three asks, two PRs off `master` at `49728c2b`. The theme fix is independent.
Node 24 and Lighthouse ship together because the Lighthouse workflow reads the
`.nvmrc` the Node commit introduces, and both touch the same two `package.json`
files and lockfiles — separating them would only create a merge-order trap.

## 1. Dark theme: the dashboard adopts the docs' midnight palette

`fix/dashboard-dark-theme-matches-docs` — one file, `dashboard/src/globals.css`.

The two apps keep hand-synced palettes with no shared source; the only token
name they share is `--bg`. Light mode was already paired on purpose
(`tokens.css` says "matches the FlexiQ console"), dark mode never was:

| role | dashboard was | docs |
|---|---|---|
| page bg | `#16120f` L .185 **h58 warm** | `#08080c` L .137 **h285 cool** |
| panel | `#211b17` L .227 | `#0f0f17` L .172 |
| body text | `#f2f0eb` h82 | `#ececf4` h286 |
| brand green | `#5eca91` L .76 | `#1f9d54` L .61 |

Docs is the reference: its dark palette also drives `mermaid-theme.ts` (hexes
mirrored by hand), `shiki.css` and the landing gradients, so moving docs would
cost four files and a re-tune. The dashboard's dark block is one file, and no
component uses a Tailwind `dark:` utility — everything reads `var()`.

The three docs greens map onto the three accent roles by how each is consumed,
which is not what their names suggest:

- `--accent-strong` fills the primary button (`ui/button.tsx`) → `--indigo-dk`
  `#15803d`, with `--accent-fg` white, the pairing docs' own `.btn.pri` uses.
- `--accent-ink` is accent *text* → `--indigo-br` `#3bbf72`.
- `--accent` is borders, tints and the ring → `--indigo` `#1f9d54`.

- [x] Retune `html.dark` to the docs ladder, each value naming its source token
- [x] Keep `--info` blue — docs has no blue, and its `--cyan` reads as the
      success green; the existing blue already sits in the docs' status band
- [x] Rename `--shadow-warm` → `--shadow-tint`; nothing outside this file used it
- [x] Verify every text pair against WCAG AA

Verified in the built CSS, not just the source: `#08080c`, `#0f0f17`, `#ececf4`,
`#15803d`, `#ffb86b`, `#ff6b6b` come out byte-identical to the docs tokens; the
rest land within one unit of the oklch round-trip.

| pair | before | after |
|---|---|---|
| `--fg` on `--bg` | 16.35:1 | 17.01:1 |
| `--fg-muted` on `--surface` | 7.38:1 | 10.89:1 |
| `--fg-subtle` on `--bg` | 4.71:1 | 6.09:1 |
| `--accent-fg` on `--accent-strong` (button) | 5.27:1 | 4.96:1 |
| `--accent-ink` on `--surface` | 9.60:1 | 8.07:1 |
| focus ring on `--bg` | 2.76:1 ✗ | 3.63:1 ✓ |

The button and accent-text pairs give back a little headroom because they now
sit on the docs' exact greens; both still clear AA, and the ring crosses 3:1 for
the first time. Lighthouse scores the result 100 on accessibility.

Not in scope: docs defaults to dark with no `prefers-color-scheme` fallback and
offers no "system" option, where the dashboard has all three. That is a
behaviour difference, not a palette one.

## 2. Node 24 for the docs and dashboard toolchain

`chore/node-24-and-lighthouse`, first commit.

- [x] `.github/actions/dashboard-build/action.yml` `"22"` → `dashboard/.nvmrc`
- [x] `.github/actions/setup-node/action.yml` default `"22"` → `"24"`
- [x] `docker/scheduler.Dockerfile` `node:22-alpine` → `node:24-alpine`
- [x] `docs.yml` `node-version: 24` → `node-version-file: docs/.nvmrc`
- [x] New `docs/.nvmrc`, `dashboard/.nvmrc`
- [x] `engines.node: ">=24"` on both `package.json`s
- [x] `docs/package.json` `@types/node` `^25` → `^26`, matching the dashboard
- [x] Drop `ci-node.yml`'s second Node install — it existed only to reach 24 for
      the docs API-sync script while the job ran on 22

Deliberately untouched, because they are the Node SDK's *library support floor*
and not the build toolchain: `sdks/node/package.json` `engines.node: ">=20"`,
and the `[20, 22, 24]` matrices in `ci-node.yml` and `ci-polyglot.yml`.

**`engine-strict` was tried and reverted.** pnpm only warns on `engines` by
default, so making the floor bite needs `engine-strict=true` — but that also
enforces every *transitive* dependency's range, and `jsdom@30.0.1` wants
`^24.15.0`. On Node 24.12 the docs install hard-fails locally while CI, which
gets the latest 24.x, passes. A gate that breaks developers and not CI is worse
than none. The `.nvmrc` files are the real pin: CI installs from them, so the
toolchain is fixed regardless.

## 3. Lighthouse for both

`chore/node-24-and-lighthouse`, second commit. Nothing measured performance,
accessibility or anything Lighthouse-shaped anywhere in the repo before this.

- [x] `@lhci/cli` + `lighthouserc.yml` + a `lighthouse` script per project
- [x] Audit the real production builds, not a dev server
- [x] `.github/workflows/lighthouse.yml`, path-filtered, two jobs
- [x] Floors calibrated to measured medians, so the gate starts green
- [x] Upload the HTML reports and write a score table to the step summary

Measured (median of 3 runs, desktop preset):

| | perf | a11y | best-practices | seo |
|---|---:|---:|---:|---:|
| dashboard `/` | 100 | 100 | 100 | 82 |
| docs `/` | 98 | 97 | 100 | 100 |
| docs `/python/getting-started/quickstart` | 99 | 94 | 96 | 90 |
| docs `/architecture` | 99 | 94 | 96 | 90 |

Performance asserts at 0.90 against measured 98-100, which absorbs a slower
runner. The other three are deterministic and pinned at what they measure, so
any drop fails. The dashboard's SEO is ungated: the console is behind auth and
deliberately not indexable, so it has no meta description to give.

YAML rather than JSON for the rc files so the calibration can carry the comment
explaining what would raise each floor. Biome does not read `.gitignore` here,
so `.lighthouseci` needed an explicit exclude in `docs/biome.json` alongside the
existing `!build`.

## Review

### Found on the way, not fixed

Three pre-existing docs issues surfaced by the first Lighthouse run. All predate
this work; none are caused by it, and each is someone's decision to make:

- **Every contrast failure is one token.** `--dim: #5c5c70` at 2.92-3.06:1, on
  sidebar group labels, footer headings, the search placeholder, the SDK label
  and the copyright line — 10 to 17 nodes per page. Lifting that single value
  clears the category and lets the a11y floor go to 1.0.
- **React error #418** on doc pages but not the landing page: a hydration
  mismatch under the docs layout. It is what holds best-practices at 0.96.
- **No meta description** on doc pages, holding SEO at 0.90.

### Worth knowing

- The root cause behind item 1 is untouched: two palettes, hand-synced, sharing
  exactly one token name. They will drift again. A shared token source needs a
  pnpm workspace, and there isn't one — `docs/` and `dashboard/` are separate
  projects with separate lockfiles.
- `docker/scheduler.Dockerfile` is the one change not verified locally: building
  the image needs docker, which this machine does not use. It is a one-line base
  image bump on a stage that only runs `pnpm install` and `vite build`.
