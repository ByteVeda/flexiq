// Build-time reader for the doc corpus: walks the content tree once, extracts
// each page's body, and serialises the MiniSearch index the browser revives.
//
// Node-only (fs), like doc-paths.ts. Nothing in the client graph imports it —
// the vite plugin and the CI gate do, which is what keeps the shipped index and
// the gate's assertions built from the same code.

import { readdirSync, readFileSync } from "node:fs";
import { join, relative, sep } from "node:path";
import { fileURLToPath } from "node:url";
import MiniSearch from "minisearch";
import { mountsForRelPath } from "./doc-slugs.ts";
import { extractDocText } from "./mdx-extract.ts";
import { type IndexedDoc, SEARCH_INDEX_OPTIONS } from "./search-schema.ts";

export const CONTENT_DIR = fileURLToPath(
  new URL("../../content/docs", import.meta.url),
);

/** One content file: posix content-relative path plus raw source. */
export interface DocFile {
  rel: string;
  raw: string;
}

function walk(dir: string, out: string[]): void {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      walk(full, out);
    } else if (entry.name.endsWith(".mdx")) {
      out.push(full);
    }
  }
}

/** Every MDX file under `content/docs`, read once for all build-time consumers. */
export function readDocFiles(): DocFile[] {
  const files: string[] = [];
  walk(CONTENT_DIR, files);
  return files.map((file) => ({
    rel: relative(CONTENT_DIR, file).split(sep).join("/"),
    raw: readFileSync(file, "utf8"),
  }));
}

export function parseFrontmatter(raw: string): {
  title: string;
  description: string;
} {
  const block = raw.match(/^---\n([\s\S]*?)\n---/)?.[1] ?? "";
  const field = (name: string) =>
    block.match(new RegExp(`^${name}:\\s*"?(.+?)"?\\s*$`, "m"))?.[1] ?? "";
  return { title: field("title"), description: field("description") };
}

/** One entry per content file, at its canonical URL — so a shared page that
 *  mounts under three SDKs is stored once, not three times. */
export function toCorpus(files: DocFile[]): IndexedDoc[] {
  return files
    .map(({ rel, raw }) => {
      const { title, description } = parseFrontmatter(raw);
      const mounts = mountsForRelPath(rel);
      const slug = mounts[0].canonical ?? mounts[0].slug;
      return {
        slug,
        title: title || slug,
        description,
        ...extractDocText(raw),
      };
    })
    .sort((a, b) => a.slug.localeCompare(b.slug));
}

/** The serialised index, as JSON text. Kept a string rather than an object
 *  literal: at this size `JSON.parse` beats the JS parser by a wide margin, and
 *  `MiniSearch.loadJSON` wants the string anyway. */
export function buildSearchIndex(entries: IndexedDoc[]): string {
  const index = new MiniSearch<IndexedDoc>(SEARCH_INDEX_OPTIONS);
  index.addAll(entries);
  return JSON.stringify(index);
}
