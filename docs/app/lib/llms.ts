import { CORPUS } from "virtual:docs-corpus";
import { DOC_METAS } from "./manifest";

// Prefix the deploy base (`/flexiq` on Pages, empty locally) so emitted links
// resolve under the project path, not the domain root.
const BASE = import.meta.env.BASE_URL.replace(/\/$/, "");
const docUrl = (slug: string): string => `${BASE}${slug}`;

// Shared pages mount at one URL per SDK; list each only once, at its
// canonical (default-SDK) URL, so the corpus carries no duplicates. The corpus
// module is keyed that way already — this is the equivalent filter for the
// per-mount manifest.
function uniqueMetas() {
  return DOC_METAS.filter((d) => !d.canonical || d.canonical === d.slug).sort(
    (a, b) => a.slug.localeCompare(b.slug),
  );
}

/** Index of every doc page (title + URL) — the `/llms.txt` body. */
export function llmsIndex(): string {
  const lines = ["# FlexiQ documentation", ""];
  for (const meta of uniqueMetas()) {
    lines.push(`- [${meta.title}](${docUrl(meta.slug)})`);
  }
  return `${lines.join("\n")}\n`;
}

/** Full corpus — the `/llms-full.txt` body. One block per page: prose with the
 *  MDX machinery stripped, then that page's code samples. Both come from the
 *  same extraction the search index is built from, so the file and the index
 *  can never disagree about what a page says. */
export function llmsFull(): string {
  const blocks = CORPUS.map((doc) => {
    const head = `## ${doc.title}\nURL: ${docUrl(doc.slug)}\n\n${doc.text}\n`;
    return doc.code ? `${head}\n\`\`\`\n${doc.code}\n\`\`\`\n` : head;
  });
  return `# FlexiQ documentation (full text)\n\n${blocks.join("\n---\n\n")}`;
}
