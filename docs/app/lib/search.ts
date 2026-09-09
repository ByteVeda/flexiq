import type MiniSearch from "minisearch";
import { DOC_METAS, docMeta } from "./manifest";
import { DEFAULT_SDK, isSdk } from "./sdk-registry";
import {
  type IndexedDoc,
  SEARCH_INDEX_OPTIONS,
  SEARCH_QUERY_OPTIONS,
} from "./search-schema";
import { type Tier, tierForPath } from "./tier-registry";

// Two paths, deliberately different in cost.
//
// Browsing (the palette's empty-query list) reads the eagerly-loaded manifest,
// so opening ⌘K fetches nothing. Searching needs the full-text index, which is
// ~250 kB gzipped — far too large for the chunk every page loads — so it is
// built at build time, code-split behind a dynamic import, and fetched once on
// first use. `prefetchSearchIndex` starts that fetch when the palette opens, so
// it is usually in flight before the first keystroke lands.

export interface SearchHit {
  /** URL to navigate to: the mount under the active SDK. */
  id: string;
  title: string;
  section: string;
  description: string;
}

function sectionOf(slug: string): string {
  const top = slug.split("/")[1] ?? "";
  return top
    ? top.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase())
    : "Home";
}

// Browse-mode section order (mirrors the sidebar); unknown sections sort last.
// `Server` sits above the tier-neutral sections for the same reason the SDK
// sections do: a tier's own pages come before the ones it shares.
const SECTION_ORDER = [
  "Getting Started",
  "Guides",
  "Server",
  "Architecture",
  "Api Reference",
  "More",
  "Node",
];
const sectionRank = (s: string) => {
  const i = SECTION_ORDER.indexOf(s);
  return i === -1 ? SECTION_ORDER.length : i;
};

/**
 * A page is in scope when it is tier-neutral (`/architecture/*`, `/about/*`) or
 * belongs to the active tier.
 *
 * Scoped on the tier and not on the SDK, symmetrically: what a tier's search
 * offers is what its sidebar offers, and nothing a reader would have to switch
 * tiers to open. `forcedSdkForPath` — the old test — returns null for
 * `/server/*`, which read the server tier as *shared* and put the Python tree
 * in the palette of a `/server` page and the server tree in every SDK's.
 */
function inTier(slug: string, tier?: Tier): boolean {
  if (!tier) {
    return true;
  }
  const pageTier = tierForPath(slug);
  return pageTier === null || pageTier === tier;
}

/** The index stores one entry per content file, at its canonical URL, so a
 *  shared page is indexed once instead of once per SDK. That entry stands for
 *  every SDK's copy: swap the prefix to reach the active SDK's mount. A tier
 *  that is not an SDK carries no such copy, so there is nothing to swap to. */
function mountFor(canonical: string, tier?: Tier): string {
  const meta = docMeta(canonical);
  if (!meta?.canonical || !tier || !isSdk(tier)) {
    return canonical;
  }
  return `/${tier}${canonical.slice(`/${DEFAULT_SDK}`.length)}`;
}

/** A page that fans out per SDK has a mount under every SDK tier and under no
 *  other kind; anything else is in scope only if it is in the active tier. */
function hitInScope(canonical: string, tier?: Tier): boolean {
  if (!docMeta(canonical)?.canonical) {
    return inTier(canonical, tier);
  }
  return tier === undefined || isSdk(tier);
}

function toHit(canonical: string, tier?: Tier): SearchHit | null {
  const meta = docMeta(canonical);
  if (!meta) {
    return null;
  }
  const id = mountFor(canonical, tier);
  return {
    id,
    title: meta.title,
    section: sectionOf(id),
    description: meta.description,
  };
}

/** The full page list for the active tier, sidebar-ordered. No index needed. */
export function browseDocs(tier?: Tier): SearchHit[] {
  return DOC_METAS.filter((d) => inTier(d.slug, tier))
    .map((d) => ({
      id: d.slug,
      title: d.title,
      section: sectionOf(d.slug),
      description: d.description,
    }))
    .sort((a, b) => sectionRank(a.section) - sectionRank(b.section));
}

let indexPromise: Promise<MiniSearch<IndexedDoc>> | null = null;

async function loadIndex(): Promise<MiniSearch<IndexedDoc>> {
  const [{ default: MiniSearchCtor }, { SEARCH_INDEX }] = await Promise.all([
    import("minisearch"),
    import("virtual:docs-search-index"),
  ]);
  // loadJSON must be handed the options that built the index — same fields,
  // same tokenizer — or the stored terms and the query's don't line up.
  return MiniSearchCtor.loadJSON<IndexedDoc>(
    SEARCH_INDEX,
    SEARCH_INDEX_OPTIONS,
  );
}

function index(): Promise<MiniSearch<IndexedDoc>> {
  indexPromise ??= loadIndex().catch((err) => {
    indexPromise = null; // a dropped chunk shouldn't kill search for the session
    throw err;
  });
  return indexPromise;
}

/** Start fetching the index without waiting for it — call when the palette opens. */
export function prefetchSearchIndex(): void {
  index().catch(() => {});
}

/** Ranked matches for a non-empty query, scoped to the active tier. */
export async function searchDocs(
  query: string,
  tier?: Tier,
): Promise<SearchHit[]> {
  const q = query.trim();
  if (!q) {
    return browseDocs(tier);
  }
  const results = (await index()).search(q, SEARCH_QUERY_OPTIONS);
  const hits: SearchHit[] = [];
  for (const result of results) {
    const canonical = String(result.id);
    if (!hitInScope(canonical, tier)) {
      continue;
    }
    const hit = toHit(canonical, tier);
    if (hit) {
      hits.push(hit);
    }
    if (hits.length === 20) {
      break;
    }
  }
  return hits;
}
