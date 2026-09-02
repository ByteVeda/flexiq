// Splitting helpers shared by the three extractors.
//
// Every language's parameter list is "commas, except the ones inside brackets"
// — `list[int | None] | None = None`, `Promise<Array<JsJob>>`, `Map<String,
// List<Job>>`. One depth-aware splitter keeps the three parsers from each
// growing their own subtly different bracket counter.

const OPEN = { "(": ")", "[": "]", "{": "}", "<": ">" };
const CLOSE = new Set([")", "]", "}", ">"]);

/**
 * Split `text` on top-level occurrences of the single-character `separator`.
 * Angle brackets count only for `<`/`>` pairs, which is safe here: none of the
 * three declaration syntaxes uses a bare comparison operator.
 */
export function splitTopLevel(text, separator) {
  const parts = [];
  let depth = 0;
  let current = "";
  let quote = null;
  for (const char of text) {
    if (quote) {
      current += char;
      if (char === quote) {
        quote = null;
      }
      continue;
    }
    if (char === '"' || char === "'") {
      quote = char;
      current += char;
      continue;
    }
    if (OPEN[char]) {
      depth += 1;
    } else if (CLOSE.has(char)) {
      depth -= 1;
    }
    if (char === separator && depth === 0) {
      parts.push(current);
      current = "";
      continue;
    }
    current += char;
  }
  parts.push(current);
  return parts;
}

/** Index of the `)` matching the `(` at `open`, or -1. */
export function matchParen(text, open) {
  let depth = 0;
  for (let i = open; i < text.length; i += 1) {
    if (text[i] === "(") {
      depth += 1;
    } else if (text[i] === ")") {
      depth -= 1;
      if (depth === 0) {
        return i;
      }
    }
  }
  return -1;
}
