# #778 — docs: the search index carries no body text

Epic #777, stage 1 (no URL churn, unblocks the rest).
**Fails if:** the index still holds frontmatter only · code fences contribute no
tokens · a query of `apply_async` misses `queue.apply_async` · a shared page is
stored once per SDK mount · `/llms-full.txt` and the index grow separate
extraction paths · the corpus lands in the eagerly-loaded chunk.

Branch: `docs/search-body-index`, worktree off `master`. Commit as kartik.
**Do not push.**

## The shape

`app/lib/mdx-extract.ts` is the single extraction path; the search index and
`/llms-full.txt` both read through it, so neither can drift from what a page
says. Fences come out **first** — Java and Python samples open with `import`
lines and carry `List<String>` generics, so stripping MDX before that eats code.

One index entry per content **file**, keyed by canonical slug: that is how the
index stays SDK-scoped without three copies of a shared body. The manifest's
existing `canonical` field is already the "this page is shared" marker, so a hit
resolves to the active SDK's mount by swapping the prefix.

Measured: the corpus is 1.04 MB prose + 464 KB code. Shipped raw it is 484 KB
gzip plus 1-2 s of main-thread `addAll`; serialised at build time it is 229 KB
gzip and free to load. Hence a prebuilt index behind a dynamic import, and a
`virtual:docs-corpus` that is emitted **only into the SSR build** so the prose
never reaches `build/client`.

## Tasks

### 1. Extraction
- [x] `app/lib/mdx-extract.ts`: `extractDocText` → `{ text, code, headings }`.
- [x] Fences and inline code out before any MDX/markdown stripping.
- [x] `_` and `.` survive the markdown pass — stripping them shreds
      `max_in_flight` and `queue.apply_async`.
- [x] `app/lib/search-schema.ts`: `IndexedDoc`, fields, boosts, tokenizer,
      shared by the builder and `loadJSON` (they must agree or terms don't line up).
- [x] `app/lib/search-corpus.ts`: one walk, `toCorpus`, `buildSearchIndex`.
- [x] Plugin emits `virtual:docs-search-index` + `virtual:docs-corpus`;
      `virtual:docs-manifest` unchanged so the eager chunk doesn't grow.

### 2. Index and ranking
- [x] Tokenizer splits `.` and `_`, emitting whole **and** parts.
- [x] `code` weighted above `text`; `title` 8x, `headings` 4x, `code` 3x.
- [x] `search.ts`: sync `browseDocs`, async `searchDocs`, `prefetchSearchIndex`.
- [x] Hits resolve canonical → the active SDK's mount.
- [x] Modal: results as state, stale-query guard, "Searching…" while loading.

### 3. `/llms-full.txt`
- [x] Emits real bodies from `virtual:docs-corpus`, still deduplicated to the
      canonical mount.
- [x] Code samples included — a full-text corpus without them is not one.

### 4. Gate
- [x] `scripts/check-search-index.mjs` + `pnpm check:search`, wired into
      `docs.yml` beside `check:diagrams`.
- [x] Asserts the four symbols from the issue resolve, that the tokenizer still
      splits, that no page extracts to nothing, and a gzip budget.

## Verification

- [x] `pnpm typecheck` · `pnpm lint` · `pnpm check:parity` · `pnpm check:search`
- [x] `pnpm build`
- [x] Eager-chunk sizes before/after — nothing moved into the critical path
- [x] `build/client` carries no copy of the prose corpus
