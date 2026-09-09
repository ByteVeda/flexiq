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
