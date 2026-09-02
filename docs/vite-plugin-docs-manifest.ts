import type { Plugin } from "vite";
import { mountsForRelPath } from "./app/lib/doc-slugs.ts";
import {
  buildSearchIndex,
  type DocFile,
  parseFrontmatter,
  readDocFiles,
  toCorpus,
} from "./app/lib/search-corpus.ts";
import type { IndexedDoc } from "./app/lib/search-schema.ts";

// Build-time virtual modules derived from the content tree, in one walk.
//
//   virtual:docs-manifest      { slug, title, description, canonical? } only —
//                              NO compiled components and NO body text. nav and
//                              the browse list import this eagerly, so it must
//                              stay small; pulling in the shiki-inflated MDX
//                              modules or the full corpus would inflate the
//                              chunk every page loads.
//   virtual:docs-search-index  the serialised MiniSearch index, imported
//                              dynamically so it lands in its own chunk and is
//                              fetched on first search, not on first paint.
//   virtual:docs-corpus        raw page bodies for /llms-full.txt. Emitted only
//                              into the SSR build, where the prerender runs;
//                              the client gets an empty array so ~1 MB of prose
//                              never reaches build/client.
//
// Shared files emit one manifest entry per SDK mount (same frontmatter) but a
// single index/corpus entry, at the canonical mount.

const VIRTUAL_MANIFEST = "virtual:docs-manifest";
const VIRTUAL_SEARCH_INDEX = "virtual:docs-search-index";
const VIRTUAL_CORPUS = "virtual:docs-corpus";
const VIRTUAL_IDS = [VIRTUAL_MANIFEST, VIRTUAL_SEARCH_INDEX, VIRTUAL_CORPUS];
const resolved = (id: string) => `\0${id}`;

export interface DocMeta {
  slug: string;
  title: string;
  description: string;
  /** Default-SDK URL of a shared page; present only on fan-out mounts. */
  canonical?: string;
}

function buildManifest(files: DocFile[]): DocMeta[] {
  const sources = new Map<string, string>(); // slug → content-relative source file
  const metas: DocMeta[] = [];
  for (const { rel, raw } of files) {
    const { title, description } = parseFrontmatter(raw);
    for (const { slug, canonical } of mountsForRelPath(rel)) {
      const existing = sources.get(slug);
      if (existing) {
        // A shared file and a per-SDK file at the same URL would silently
        // shadow each other and reintroduce drift — fail the build instead.
        throw new Error(
          `docs-manifest: slug ${slug} is produced by both ${existing} and ${rel}`,
        );
      }
      sources.set(slug, rel);
      metas.push({ slug, title: title || slug, description, canonical });
    }
  }
  return metas.sort((a, b) => a.slug.localeCompare(b.slug));
}

/** Only the fields /llms-full.txt emits — headings and the search-only split
 *  would just bloat the SSR chunk. */
function toLlmsCorpus(entries: IndexedDoc[]) {
  return entries.map(({ slug, title, text, code }) => ({
    slug,
    title,
    text,
    code,
  }));
}

export function docsManifest(): Plugin {
  // One read of the tree per build, shared by all three modules.
  let files: DocFile[] | null = null;
  const load = () => {
    files ??= readDocFiles();
    return files;
  };

  return {
    name: "docs-manifest",
    buildStart() {
      files = null; // pick up content edits across rebuilds
    },
    resolveId(id) {
      if (VIRTUAL_IDS.includes(id)) {
        return resolved(id);
      }
    },
    load(id, options) {
      if (id === resolved(VIRTUAL_MANIFEST)) {
        return `export const DOCS = ${JSON.stringify(buildManifest(load()))};`;
      }
      if (id === resolved(VIRTUAL_SEARCH_INDEX)) {
        const json = buildSearchIndex(toCorpus(load()));
        return `export const SEARCH_INDEX = ${JSON.stringify(json)};`;
      }
      if (id === resolved(VIRTUAL_CORPUS)) {
        const corpus = options?.ssr ? toLlmsCorpus(toCorpus(load())) : [];
        return `export const CORPUS = ${JSON.stringify(corpus)};`;
      }
    },
  };
}
