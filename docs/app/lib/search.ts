import type MiniSearch from "minisearch";
import { DOC_METAS, docMeta } from "./manifest";
import { DEFAULT_SDK, isSdk } from "./sdk-registry";
import {
  type IndexedDoc,
  SEARCH_INDEX_OPTIONS,
  SEARCH_QUERY_OPTIONS,
} from "./search-schema";
import { SERVER_TIER, type Tier, tierForPath } from "./tier-registry";

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
 * belongs to one of the tiers on offer.
 *
 * Scoped on the tier and not on the SDK: what a docs page's search offers is
 * what its sidebar offers, and nothing a reader would have to switch tiers to
 * open. `forcedSdkForPath` — the old test — returns null for `/server/*`, which
 * read the server tier as *shared* and put the Python tree in the palette of a
 * `/server` page and the server tree in every SDK's.
 *
 * A *list* rather than one tier because the landing has no sidebar to agree
 * with: see `landingTiers`.
 */
function inTiers(slug: string, tiers?: readonly Tier[]): boolean {
  if (!tiers) {
    return true;
  }
  const pageTier = tierForPath(slug);
  return pageTier === null || tiers.includes(pageTier);
}

/**
 * What the landing page's palette offers: the tier the hero is showing, and the
 * server tier beside it.
 *
 * A reader at the front door has not picked a door yet, and the one that needs
 * no SDK at all is exactly the one they cannot know to search for. On a docs
 * page the sidebar is the contract and the scope stays the single tier; here
 * there is no sidebar to contradict.
 *
 * `tiers[0]` stays the hero's tier, so a shared page still mounts under the
 * language the reader is looking at. The `Set` collapses the pair when the hero
 * is already showing the server tab.
 */
export function landingTiers(tier: Tier): Tier[] {
  return [...new Set<Tier>([tier, SERVER_TIER])];
}

/** The index stores one entry per content file, at its canonical URL, so a
 *  shared page is indexed once instead of once per SDK. That entry stands for
 *  every SDK's copy: swap the prefix to reach the first tier's mount. A tier
 *  that is not an SDK carries no such copy, so there is nothing to swap to. */
function mountFor(canonical: string, tiers?: readonly Tier[]): string {
  const meta = docMeta(canonical);
  const primary = tiers?.[0];
  if (!meta?.canonical || !primary || !isSdk(primary)) {
    return canonical;
  }
  return `/${primary}${canonical.slice(`/${DEFAULT_SDK}`.length)}`;
}

/** A page that fans out per SDK has a mount under every SDK tier and under no
 *  other kind, so it is offered only when there is an SDK to mount it under —
 *  the same tier `mountFor` would swap to. Anything else is in scope if its
 *  tier is on offer. */
function hitInScope(canonical: string, tiers?: readonly Tier[]): boolean {
  if (!docMeta(canonical)?.canonical) {
    return inTiers(canonical, tiers);
  }
  return tiers === undefined || isSdk(tiers[0]);
}

function toHit(canonical: string, tiers?: readonly Tier[]): SearchHit | null {
  const meta = docMeta(canonical);
  if (!meta) {
    return null;
  }
  const id = mountFor(canonical, tiers);
  return {
    id,
    title: meta.title,
    section: sectionOf(id),
    description: meta.description,
  };
}

/** The full page list for the tiers on offer, sidebar-ordered. No index needed. */
export function browseDocs(tiers?: readonly Tier[]): SearchHit[] {
  return DOC_METAS.filter((d) => inTiers(d.slug, tiers))
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

/** Ranked matches for a non-empty query, scoped to the tiers on offer. */
export async function searchDocs(
  query: string,
  tiers?: readonly Tier[],
): Promise<SearchHit[]> {
  const q = query.trim();
  if (!q) {
    return browseDocs(tiers);
  }
  const results = (await index()).search(q, SEARCH_QUERY_OPTIONS);
  const hits: SearchHit[] = [];
  for (const result of results) {
    const canonical = String(result.id);
    if (!hitInScope(canonical, tiers)) {
      continue;
    }
    const hit = toHit(canonical, tiers);
    if (hit) {
      hits.push(hit);
    }
    if (hits.length === 20) {
      break;
    }
  }
  return hits;
}
