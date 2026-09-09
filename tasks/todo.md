# #823 — `flexiq-server` as a fourth hero tab

Branch `docs/hero-server-tab` off `master` at `bbc4ca0b`. One file of data, one
component. #825 (`1788a838`) already built the seam this needs: `Tier = Sdk |
"server"` in `app/lib/tier-registry.ts`, with `SDK_IDS` deliberately still three
because its value is the persisted `<html data-sdk>` one.

## The two things the issue asks to settle

**1. The tab must not corrupt `useActiveSdk`.** Writing `server` into the SDK
store lands it in `<html data-sdk>`, matches no `<SdkOnly>`/`<CodeTabs>` variant
and blanks every shared page — and every SDK-relative link on the site resolves
through that store's derived value. So the server tab is a *pane* and nothing
more: it is held in the hero's own state and never reaches `setSdk`. This is the
same rule `TierSelect` follows in `site-nav.tsx`, where picking the server tier
navigates instead of setting the SDK.

The hero's own two buttons stop being built out of the store (`/${sdk}/modules`)
and come off the pane instead — the server tab has no `/python/modules` under
it, and composing one from the store is exactly the prefix corruption the tier
split exists to prevent.

**2. `p.sdk === sdk` assumes tab identity is SDK identity.** A pane is keyed by
`tier`, and the branch is `isSdk(next)` — the registry predicate — not a string
compare against `"server"`. A future non-SDK tier needs no new branch.

## Content

The snippet is the door, not a language: the env-var invocation plus the
`grpcurl` call that submits a job. Both halves already exist verified in-repo —
the invocation is the server fold's first transcript (extracted to one const, so
the page holds one copy of a command it shows twice), and the `grpcurl` shape is
`examples/polyglot/grpc_producer.sh`, the working bash producer `clients.mdx`
points at. It enqueues the same `add(2, 3)` the other three tabs define, so the
four tabs tell one story.

`grpcurl`, not `curl`, on purpose: the server fold lower on the same page is the
HTTP/JSON binding, and printing the same request twice on one page teaches less
than printing both doors once.

## Commits

- [x] 1 — a hero pane is a tier, not an SDK (mechanical; still three tabs)
  - [x] `LangPane` → `HeroPane`, `sdk: Sdk` → `tier: Tier`
  - [x] `docHref` → `primary`/`secondary` CTAs on the pane; drop the
        `/${sdk}/modules` template
  - [x] drop `install` and `docLabel` — dead since the marketing sections left
  - [x] highlighter chain → a `Record<lang, fn>` lookup (a fourth dialect is
        coming and the ternary chain is already two deep)
  - [x] tab label from `tierProfile`, so one registry names every tab
- [x] 2 — the fourth tab
  - [x] `SERVER_START` extracted; server `HeroPane` added with `lang: "sh"`
  - [x] `runtag` per pane — the server snippet starts no worker, so the title
        bar cannot claim `worker · live`
  - [x] `pinnedTier` state: a tier the SDK store cannot hold, `isSdk`-routed
  - [x] hero sub copy stops promising the tabs are all languages

## Verify

- [x] `pnpm typecheck`
- [x] `pnpm lint`
- [x] `pnpm check:parity` — `/server` and `/server/clients` are real slugs
- [x] `pnpm build` — the hero prerenders, so the snippet is highlighted at build
- [x] rendered `/` from the build: four tabs, server pane selected, and
      `<html data-sdk>` still a language

## Review

Two commits on `docs/hero-server-tab`, not pushed. `typecheck`, `lint`,
`check:parity` and `build` all green.

The prerendered `/` carries four `.langtab` buttons — the fourth labelled
`flexiq-server` out of `tierProfile`, so one registry names every tab — with
Python still active and `<html data-sdk="python">`. Rendering the server pane
directly (a throwaway `pinnedTier` default, reverted) confirmed the rest: the
title bar reads `door · live`, the two buttons resolve to `/server` and
`/server/clients`, and `highlightShell` tokenises the snippet cleanly — the
`sqlite:///tmp` and `localhost:50051` colons and the JSON body all survive,
which is the case that pass exists for. `data-sdk` stayed `python` with the
server tab active, which is the invariant the issue asked for.

The `[2, 3]` in the snippet is scalars where `grpc_producer.sh` passes an
object: checked against `crates/flexiq-server/src/grpc/producer/structured.rs`,
which converts any `google.protobuf.Value` and refuses only an integer past
2^53.

Worth keeping: `check:parity`'s `links.mjs` resolves `to=` literals out of
`app/`, so moving the hero CTAs onto the pane put two hrefs under the gate that
were previously composed at runtime from the SDK store and checked by nothing.

---

# #823 follow-up — the server tier shows only server content

Same branch. The tier switcher reads `flexiq-server` and the sidebar is the
server tier's, but two things on the page still resolve through the *stored
SDK* and walk a reader back out of the tier.

## 1. The top nav bar

`site-nav.tsx` builds `Concepts` / `API` / `Examples` as `` `/${sdk}/${href}` ``
off `useActiveSdk()`. On `/server` that is the stored language, so the bar
under a `flexiq-server` switcher links into `/python/…`. Same failure mode as
the sidebar/prev-next trap #825 caught before shipping — one more consumer that
was keyed to the SDK when it meant the tier.

Fix: `useActiveTier()`, and each tier owns its three links, tier-relative so
there is one prefixing rule. The server tier's are the two doors as a caller
meets them, then running it — it has no `getting-started/concepts` to point at.

- [x] `SDK_LINKS` / `SERVER_LINKS` / `SHARED_LINKS` + `navLinks(tier)`
- [x] `SiteNav` reads `useActiveTier`; the landing is unaffected
      (`tierForPath("/")` is null, so it still resolves to the stored SDK)

## 2. ⌘K search

`docs-layout.tsx` passes `useActiveSdk()`, and `search.ts` scopes on
`forcedSdkForPath`, which returns **null** for `/server/*` — so server pages
read as *shared* and every tier sees every other tier's pages. On `/server` the
palette lists the Python tree.

Fix, symmetric (user's call): a tier's search offers what its sidebar offers.

```
inTier(slug, tier) = tierForPath(slug) === null || tierForPath(slug) === tier
```

- [x] `inSdk` → `inTier`; `mountFor` swaps the prefix only when the tier is an
      SDK; `hitInScope`'s per-SDK shared page is in scope for any SDK tier and
      no other kind
- [x] `SearchModal` prop `sdk` → `tier`; both callers pass `useActiveTier()`
- [x] `SECTION_ORDER` gains `Server` before `Architecture` — the browse list is
      sidebar-ordered, and without it a tier's own pages sort below shared ones

Not touched: `Architecture` and `About` stay in the server sidebar and stay
in scope. They are tier-neutral by design (#825) — the engine and the project
are the same ones whichever door you came through. `<SdkLink>` inside a server
page (`modules/executor` in the local-mode column) also stays SDK-resolved: it
is talking about the SDK feature on purpose.

## Verify

- [x] `typecheck`, `lint`, `check:parity`, `check:search`, `build`
- [x] prerendered `/server`: nav links are `/server/*`, none `/python/*`
- [x] prerendered `/python/*`: nav links unchanged

## Review

Two more commits, still unpushed. All five gates green (`typecheck`, `lint`,
`check:parity`, `check:search`, `build`).

Nav, read out of the prerendered HTML: `/server` and `/server/operate/tokens`
carry `/server/clients`, `/server/custom-executors`, `/server/operate`; every
SDK page is unchanged; `/architecture/*` and `/` still fall back to the stored
SDK, which is right — those pages belong to no tier.

Search, checked by replaying `inTier` over every prerendered mount (443 of
them) rather than by reading the diff:

```
python  in-scope 246  (own 225, tier-neutral 21)  leaked 0
node    in-scope 201  (own 180, tier-neutral 21)  leaked 0
java    in-scope 199  (own 178, tier-neutral 21)  leaked 0
server  in-scope  30  (own   9, tier-neutral 21)  leaked 0
```

Python pages visible from the server tier: **0**, was every one of them. The 21
tier-neutral are `/about/*`, `/architecture/*` and `/resources/*` — unchanged,
and the reason the server tier still has 30 pages to search rather than 9.

---

# Follow-up 2 — the tier survives a tier-neutral page

Reported: pick `flexiq-server`, click **Architecture** in its sidebar, land in
Node.js. The sidebar is right to offer `/architecture/*` — those pages are
tier-neutral by design — but the URL names no tier, so `useActiveTier` fell
through to the *stored SDK*, and the switcher, nav bar, sidebar and search scope
all flipped to whatever language was last picked. Every shared page was an exit
from the server tier.

The SDK store cannot hold the answer: its value is the `<html data-sdk>` one.
So a second, tiny store holds the one thing it cannot.

- [x] `app/lib/tier-store.ts` — `pinned: Tier | null`, null meaning "follow the
      SDK". `set()` routes on `isSdk`: a language goes to `sdkStore` and clears
      the pin, anything else is the pin
- [x] In memory, not persisted — it carries a choice across a navigation, and a
      fresh load of a shared page has no such choice to honour. Keeps the
      no-flash boot script about `data-sdk` alone
- [x] `useActiveTier` = `tierForPath(path) ?? pinned ?? sdk`
- [x] `docs-layout`'s sticky effect becomes `tierForPath` + `tierStore.set`,
      which subsumes the old `forcedSdkForPath` + `setSdk`
- [x] `TierSelect.select` writes through the same store
- [x] Hero drops its local `pinnedTier` for the shared one — one tier state, and
      picking the server tab now also gives `/` the server nav bar and search
      scope, which is what "only server content" meant there

## Verify

- [x] `typecheck`, `lint`, `check:parity`, `check:search`, `build`
- [x] seed the pin to `server` and prerender: `/architecture` and
      `/about/changelog` render the server sidebar, nav and switcher
- [x] restore: those pages are Python again, `/server` unchanged, `/` unchanged

## Review

Two commits, `c5997fab` + the hero one. All five gates green.

Verified by seeding the pin to `server` (both the live value and the server
snapshot), building, and reading the prerendered HTML — the same trick as the
hero pane, and the only way to see client state in a static build:

```
/architecture     switcher flexiq-server · nav /server/* · sidebar Server & wire | Operate | Architecture | About
/about/changelog  switcher flexiq-server · nav /server/*
```

Restored, the same two pages are Python again and `/server`, `/python/*` and `/`
are untouched. That the *sidebar* followed is the real proof: it reads
`useActiveTier` independently of the nav bar, so both consumers agreed off one
store.

Worth keeping: the fix is one predicate in one place because `tierStore.set`
routes on `isSdk` rather than on the tier's name — the same shape the hero used
locally, which is why folding the hero into it removed state rather than adding
any.

---

# Follow-up 3 — the serverdoor demo's row stayed `pending`

`server-door-demo.tsx` drew the `jobs` row from a constant whose `status` was
the literal `"pending"`. The trace does not stop at the 200: two stages follow
it — a worker claims the job, then the result is written back — and neither
touched the row. So the demo ended showing a finished job as pending, on the
one page that argues the door writes the same row an SDK does.

- [x] `Stage.rowStatus?: RowStatus`, typed `"pending" | "running" | "complete"`
      — `JobStatus::as_str` in `flexiq-core`, so a typo is a type error
- [x] `running` on the claim stage, `complete` on the result stage; `ROW` keeps
      `pending` as the value the *insert* writes, and `status` falls back to it
- [x] derived latest-wins like the client's status line, so scrubbing backwards
      walks the row's status back too
- [x] both stage details now say what moves it, rather than leaving the row to
      contradict the prose

## Verify

- [x] `typecheck`, `lint`, `build`
- [x] prerendered the demo at four playhead positions (temporary `const play`,
      reverted): `0 → —`, `4000 → pending`, `6000 → running`, `DUR → complete`

---

# Follow-up 4 — the working box breathes

`.sd-box.on` said *where* the current stage was, and nothing said it was still
happening. Added `live` beside it: the ring breathes on the active box while the
playhead is short of the end, in the stage's own tone (`--c`), so a refusal
pulses red without a second rule.

- [x] `Box` takes `live`; the class is only added alongside `on`
- [x] `@keyframes sdlive` in `demos.css` — a breath, not a hard blink: this sits
      mid-article, and an on/off blink at that size reads as an error state
- [x] added to the file's existing `prefers-reduced-motion` query, which is
      where every other demo animation is switched off
- [x] verified by prerender: `play=0` → `sd-box on live`, `play=DUR` → `on`
      alone, and both the keyframes and the reduced-motion rule ship in the CSS

# Follow-up 5 — the finder's two panes are one height

`.finder` was `align-items: start`, so the question list (a fixed seven options)
and the answer card ended at different heights on nearly every scenario, and the
pair resized whenever a different scenario was picked.

- [x] `align-items: stretch`, and `.fd-answer` is a column flex with
      `.fd-card { flex: 1 }` so the card fills its side too — otherwise the
      shorter scenarios still stopped early inside a stretched column
- [x] `.fd-foot` gets `margin-top: auto`: the spare space belongs under the
      options, not under the footer line
- [x] dropped `position: sticky` from `.fd-ask` and its mobile `static`
      override — an item as tall as its row has nothing to slide past
- [x] `.fd-code { min-height: calc(7lh + 30px) }` — the snippets run 3 to 7
      lines and that spread was most of the resizing. `lh` is the block's own
      line box, so it follows the font rather than restating it; confirmed it
      survives Lightning CSS into the shipped bundle
