// Build-time MDX → plain text. The search index and /llms-full.txt read their
// corpus through this one module so the two can never grow separate extraction
// paths and disagree about what a page says.
//
// Isomorphic on purpose (no node imports, explicit .ts specifiers at the call
// sites): the vite manifest plugin and the CI gate both import it, the latter
// under plain Node type stripping.

/** The searchable text of one MDX file, split into independently weighted fields. */
export interface DocText {
  /** Prose, with MDX machinery and markdown syntax removed. */
  text: string;
  /** Code, verbatim. A reference lookup is a symbol lookup, so this is signal. */
  code: string;
  /** Heading text, space-joined. */
  headings: string;
}

const FRONTMATTER = /^---\n[\s\S]*?\n---\n?/;
/** Fenced code. Non-greedy so an indented closing fence still terminates it. */
const FENCE = /```[^\n]*\n([\s\S]*?)```/g;
/** Inline code. Kept in the prose too — `apply_async` in a sentence is a mention. */
const INLINE_CODE = /`([^`\n]+)`/g;
/** `<Mermaid chart={`…`}/>`, the only form the docs use. Diagram syntax, not prose. */
const MERMAID = /<Mermaid[\s\S]*?\/>/g;
/** MDX ESM at the top level, including the multi-line `import {\n A,\n B\n }
 *  from "m";` form. Only safe once fences are gone — Java, TypeScript and
 *  Python samples open with `import`/`export` lines of their own. Anchored on
 *  the closing `";` so a sentence that happens to start with "import" survives. */
const MDX_ESM = /^import\s+(?:[\s\S]*?\sfrom\s+)?["'][^"'\n]*["'];[ \t]*$/gm;
/** `icon={<Compass />}` — a decorative element passed as an attribute. It has to
 *  go before tags are matched: the `>` inside it would otherwise end the match
 *  early and spill the rest of the tag's attributes into the prose. */
const ICON_ATTR = /\s[\w-]+=\{<[A-Za-z][^{}]*?\/>\}/g;
const JSX_COMMENT = /\{\/\*[\s\S]*?\*\/\}/g;
const ATX_HEADING = /^#{1,6}[ \t]+(.+?)[ \t]*#*$/gm;
/** Attributes that carry prose rather than wiring — a `<Card>`'s title and
 *  description are the only text that card contributes. */
const PROSE_ATTR = /\b(?:title|label|alt|description)="([^"]*)"/g;
/** A JSX tag. `<` must be followed by a name character, so "x < y" survives. */
const JSX_TAG = /<\/?[A-Za-z][^>]*>/g;
/** JSX fragment delimiters, including the `{<>` / `</>}` attribute-child form. */
const JSX_FRAGMENT = /<\/?>/g;
const MD_IMAGE = /!\[([^\]]*)\]\([^)]*\)/g;
const MD_LINK = /\[([^\]]*)\]\([^)]*\)/g;
/** Emphasis, table pipes, blockquotes, rules. Deliberately excludes `_` and `.`:
 *  stripping those would shred `max_in_flight` and `queue.apply_async`. */
const MD_PUNCT = /[*~|>]|^[ \t]*[-=]{3,}[ \t]*$/gm;

function collect(source: string, pattern: RegExp): string[] {
  return [...source.matchAll(pattern)].map((m) => m[1]);
}

function squash(value: string): string {
  return value.replace(/\s+/g, " ").trim();
}

/** Code keeps its newlines and indentation. Tokenizing ignores whitespace, so
 *  this costs the index nothing — but `/llms-full.txt` republishes this field as
 *  a fenced block, and a Python sample flattened onto one line is not a sample. */
function joinCode(fences: string[], inline: string[]): string {
  const blocks = fences.map((fence) => fence.replace(/\s+$/, ""));
  // Inline spans are single symbols; one trailing line keeps them out of the way.
  const symbols = inline.join(" ").trim();
  return [...blocks, symbols].filter(Boolean).join("\n\n");
}

/** Split an MDX source into the fields the search index weights separately. */
export function extractDocText(raw: string): DocText {
  const body = raw.replace(FRONTMATTER, "");

  // Fences first: everything below rewrites MDX syntax, and code contains
  // plenty of text that looks like it.
  const fences = collect(body, FENCE);
  // Mermaid charts are template literals, so they must go before inline code is
  // collected or a one-line chart's backticks read as a code span.
  const prose = body
    .replace(FENCE, " ")
    .replace(MERMAID, " ")
    .replace(MDX_ESM, " ");

  const headings = collect(prose, ATX_HEADING);
  const inline = collect(prose, INLINE_CODE);

  const text = prose
    .replace(JSX_COMMENT, " ")
    .replace(ICON_ATTR, " ")
    // Lift prose-bearing attributes out before the tag itself is dropped.
    .replace(JSX_TAG, (tag) => ` ${collect(tag, PROSE_ATTR).join(" ")} `)
    .replace(JSX_FRAGMENT, " ")
    .replace(/[{}]/g, " ")
    .replace(MD_IMAGE, "$1")
    .replace(MD_LINK, "$1")
    .replace(ATX_HEADING, "$1")
    .replace(INLINE_CODE, "$1")
    .replace(MD_PUNCT, " ");

  return {
    text: squash(text),
    code: joinCode(fences, inline),
    headings: squash(headings.join(" ")),
  };
}
