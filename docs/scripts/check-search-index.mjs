#!/usr/bin/env node
import { gzipSync } from "node:zlib";
import MiniSearch from "minisearch";
import {
  buildSearchIndex,
  readDocFiles,
  toCorpus,
} from "../app/lib/search-corpus.ts";
import {
  SEARCH_INDEX_OPTIONS,
  SEARCH_QUERY_OPTIONS,
} from "../app/lib/search-schema.ts";

// Search-index gate (run: pnpm check:search).
//
// The index is built from extracted MDX bodies, and every part of that is
// silent when it breaks: a stripping regex that eats code fences, a tokenizer
// that stops splitting `_`, a field that drops out of the schema. The build
// still succeeds and search still returns *something*. These assertions are
// what notices.
//
// Imports the same builder the vite plugin uses, so a green run is a statement
// about the shipped index and not about a copy of it.

/** Gzipped index budget. It ships as a lazy chunk, but a full-text index is
 *  large by nature and a regression here is a real cost to every first search. */
const MAX_INDEX_GZIP_KB = 320;

const corpus = toCorpus(readDocFiles());
const json = buildSearchIndex(corpus);
const index = MiniSearch.loadJSON(json, SEARCH_INDEX_OPTIONS);
const bySlug = new Map(corpus.map((doc) => [doc.slug, doc]));

const errors = [];
const report = [];

function search(query) {
  return index.search(query, SEARCH_QUERY_OPTIONS).map((r) => String(r.id));
}

// (a) Body text is reachable at all. Each of these is a symbol from #778 that
// returned nothing when the index held frontmatter only.
const SYMBOLS = [
  "apply_async",
  "RateLimitExceededError",
  "max_in_flight",
  "retry_budget",
];
for (const symbol of SYMBOLS) {
  const hits = search(symbol);
  if (hits.length === 0) {
    errors.push(`"${symbol}" returns no pages — body text is not indexed`);
  } else {
    report.push(`  ${symbol} → ${hits.length} page(s), top: ${hits[0]}`);
  }
}

// (b) The tokenizer still splits on `_` and `.`, which is what lets a query of
// `apply_async` reach `queue.apply_async` in a code sample.
const DOTTED = [
  ["queue.apply_async", "apply_async"],
  ["queue.enqueue", "enqueue"],
];
for (const [whole, part] of DOTTED) {
  const terms = SEARCH_INDEX_OPTIONS.tokenize(whole);
  if (!terms.includes(part) || !terms.includes(whole)) {
    errors.push(
      `tokenizer lost "${part}" or "${whole}" from "${whole}" (got ${terms.join(", ")})`,
    );
  }
}

// (c) Code is weighted above prose: a page whose examples call the symbol must
// beat one that only names it in a sentence. `headings` is its own field with a
// weight above `code`, so a heading match is a symbol match too — classifying it
// as prose would fail a page that is ranked exactly as intended.
function fieldOf(slug, symbol) {
  const doc = bySlug.get(slug);
  if (!doc) {
    return "missing";
  }
  const has = (value) =>
    SEARCH_INDEX_OPTIONS.tokenize(value).includes(symbol.toLowerCase()) ||
    value.toLowerCase().includes(symbol.toLowerCase());
  if (has(doc.title)) {
    return "title";
  }
  if (has(doc.headings)) {
    return "headings";
  }
  return has(doc.code) ? "code" : "text";
}
for (const symbol of SYMBOLS) {
  // Over the whole result set, not a truncated head: a prose-only page that
  // outranks a code page at position six is the same defect, and capping the
  // scan at five would let it through.
  const hits = search(symbol);
  const firstProse = hits.findIndex((slug) => fieldOf(slug, symbol) === "text");
  const firstSymbol = hits.findIndex(
    (slug) => fieldOf(slug, symbol) !== "text",
  );
  if (firstProse !== -1 && firstSymbol !== -1 && firstProse < firstSymbol) {
    errors.push(
      `"${symbol}": prose-only page ${hits[firstProse]} (#${firstProse + 1}) outranks ${hits[firstSymbol]} (#${firstSymbol + 1}), which has it in ${fieldOf(hits[firstSymbol], symbol)}`,
    );
  }
}

// (d) Every page contributed something. A file extracting to nothing means the
// stripper ate it.
const empty = corpus.filter((doc) => !doc.text && !doc.code && !doc.headings);
if (empty.length > 0) {
  errors.push(
    `${empty.length} page(s) extracted to empty text: ${empty
      .slice(0, 5)
      .map((d) => d.slug)
      .join(", ")}`,
  );
}

// (e) Size budget.
const gzipKb = gzipSync(json).length / 1024;
report.push(
  `  index: ${corpus.length} pages, ${(json.length / 1024).toFixed(0)} KB raw, ${gzipKb.toFixed(0)} KB gzip`,
);
if (gzipKb > MAX_INDEX_GZIP_KB) {
  errors.push(
    `index is ${gzipKb.toFixed(0)} KB gzipped, over the ${MAX_INDEX_GZIP_KB} KB budget`,
  );
}

console.log(`\n== Search index: ${errors.length > 0 ? "FAIL" : "ok"}`);
for (const line of report) {
  console.log(line);
}
for (const error of errors) {
  console.error(`ERROR ${error}`);
}
process.exit(errors.length > 0 ? 1 : 0);
