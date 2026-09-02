// The MiniSearch contract shared across the build/query boundary. The index is
// serialised at build time and revived with `loadJSON` in the browser, and
// `loadJSON` only agrees with the stored data if it is handed the same fields,
// tokenizer and term processor that built it — so both sides import these,
// never their own copy.
//
// Isomorphic: no node imports, so the vite plugin, the CI gate and the client
// chunk can all pull it in.

/** The document shape the index is built from; the keys match SEARCH_FIELDS.
 *  Declared here rather than beside the build-time reader so the browser can
 *  type the revived index without importing anything that touches `node:fs`. */
export interface IndexedDoc {
  /** Canonical URL — the default-SDK mount for a shared page, else the only
   *  one. Doubles as the index's id, so a hit is already a link. */
  slug: string;
  title: string;
  description: string;
  headings: string;
  code: string;
  text: string;
}

/** Indexed fields, in weight order. Split so a symbol used in an example can
 *  outrank a page that only mentions it in a sentence. */
export const SEARCH_FIELDS = [
  "title",
  "headings",
  "code",
  "description",
  "text",
] as const;

/** Per-field query weights. `text` is the 1x baseline and stays unlisted. */
export const SEARCH_BOOST: Record<string, number> = {
  title: 8,
  headings: 4,
  code: 3,
  description: 2,
};

/** Anything that cannot be part of an identifier ends a token. `_` and `.` are
 *  kept in, then split back out below — they are the two characters that carry
 *  meaning in a symbol name. */
const BOUNDARY = /[^\p{L}\p{N}_.]+/u;
/** Trailing punctuation from prose ("call `queue.enqueue`.") and leading dots. */
const EDGE_PUNCT = /^[._]+|[._]+$/g;
/** Terms longer than this are hashes, base64 blobs or minified noise. */
const MAX_TERM = 40;

/** A symbol yields its whole name *and* its parts, so `queue.apply_async` is
 *  found by `apply_async`, by `apply`, and by itself. Because queries run
 *  through the same expansion, a page containing the literal symbol matches
 *  every one of its terms while a page with the words scattered through prose
 *  matches fewer — which is the ranking difference we want. */
function expand(token: string, into: Set<string>): void {
  into.add(token);
  for (const dotted of token.split(".")) {
    if (!dotted) {
      continue;
    }
    into.add(dotted);
    for (const part of dotted.split("_")) {
      if (part) {
        into.add(part);
      }
    }
  }
}

export function tokenizeSymbols(text: string): string[] {
  const terms = new Set<string>();
  for (const raw of text.split(BOUNDARY)) {
    const token = raw.replace(EDGE_PUNCT, "");
    if (token) {
      expand(token, terms);
    }
  }
  return [...terms];
}

function processTerm(term: string): string | null {
  return term.length > MAX_TERM ? null : term.toLowerCase();
}

/** Options for both `new MiniSearch(...)` at build time and `loadJSON(...)` in
 *  the browser. `storeFields` is empty on purpose: titles and descriptions are
 *  already in the eagerly-loaded manifest, so storing them here would ship a
 *  second copy inside the much larger index. */
export const SEARCH_INDEX_OPTIONS = {
  /** Documents are keyed by canonical slug, so a hit is already a URL. */
  idField: "slug",
  fields: [...SEARCH_FIELDS],
  storeFields: [] as string[],
  tokenize: tokenizeSymbols,
  processTerm,
};

/** Query-time options. `AND` is safe despite the expansion above: every term a
 *  symbol expands to comes from that one symbol, so requiring all of them
 *  narrows to real matches instead of every page containing "apply" or "async". */
export const SEARCH_QUERY_OPTIONS = {
  boost: SEARCH_BOOST,
  prefix: true,
  fuzzy: 0.2,
  combineWith: "AND" as const,
};
