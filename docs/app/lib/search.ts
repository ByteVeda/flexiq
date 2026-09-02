import type MiniSearch from "minisearch";
import { DOC_METAS, docMeta } from "./manifest";
import { forcedSdkForPath } from "./nav";
import { DEFAULT_SDK } from "./sdk-registry";
import type { Sdk } from "./sdk-store";
import {
  type IndexedDoc,
  SEARCH_INDEX_OPTIONS,
  SEARCH_QUERY_OPTIONS,
} from "./search-schema";

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
const SECTION_ORDER = [
  "Getting Started",
  "Guides",
  "Architecture",
  "Api Reference",
  "More",
  "Node",
];
const sectionRank = (s: string) => {
  const i = SECTION_ORDER.indexOf(s);
  return i === -1 ? SECTION_ORDER.length : i;
};

// A page is in scope when it's shared (no SDK prefix) or matches the active SDK.
function inSdk(slug: string, sdk?: Sdk): boolean {
  if (!sdk) {
    return true;
  }
  const pageSdk = forcedSdkForPath(slug);
  return pageSdk === null || pageSdk === sdk;
}

/** The index stores one entry per content file, at its canonical URL, so a
 *  shared page is indexed once instead of once per SDK. That entry stands for
 *  every SDK's copy: swap the prefix to reach the active SDK's mount. */
function mountFor(canonical: string, sdk?: Sdk): string {
  const meta = docMeta(canonical);
  if (!meta?.canonical || !sdk) {
    return canonical;
  }
  return `/${sdk}${canonical.slice(`/${DEFAULT_SDK}`.length)}`;
}

/** A shared page always has a mount under the active SDK; anything else is in
 *  scope only if it isn't another SDK's page. */
function hitInScope(canonical: string, sdk?: Sdk): boolean {
  return docMeta(canonical)?.canonical ? true : inSdk(canonical, sdk);
}

function toHit(canonical: string, sdk?: Sdk): SearchHit | null {
  const meta = docMeta(canonical);
  if (!meta) {
    return null;
  }
  const id = mountFor(canonical, sdk);
  return {
    id,
    title: meta.title,
    section: sectionOf(id),
    description: meta.description,
  };
}

/** The full page list for the active SDK, sidebar-ordered. No index needed. */
export function browseDocs(sdk?: Sdk): SearchHit[] {
  return DOC_METAS.filter((d) => inSdk(d.slug, sdk))
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

/** Ranked matches for a non-empty query, scoped to the active SDK. */
export async function searchDocs(
  query: string,
  sdk?: Sdk,
): Promise<SearchHit[]> {
  const q = query.trim();
  if (!q) {
    return browseDocs(sdk);
  }
  const results = (await index()).search(q, SEARCH_QUERY_OPTIONS);
  const hits: SearchHit[] = [];
  for (const result of results) {
    const canonical = String(result.id);
    if (!hitInScope(canonical, sdk)) {
      continue;
    }
    const hit = toHit(canonical, sdk);
    if (hit) {
      hits.push(hit);
    }
    if (hits.length === 20) {
      break;
    }
  }
  return hits;
}
