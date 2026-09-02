// Splitting helpers shared by the three extractors.
//
// Every language's parameter list is "commas, except the ones inside brackets"
// — `list[int | None] | None = None`, `Promise<Array<JsJob>>`, `Map<String,
// List<Job>>`. One depth-aware splitter keeps the three parsers from each
// growing their own subtly different bracket counter.
//
// Three things it has to get right, each of which produced a wrong inventory
// before it did: the `>` of a TypeScript `=>` is not a closing bracket, a
// delimiter inside a string literal is not a delimiter, and a backslash inside
// a string literal escapes the quote that would otherwise end it.

const OPEN = new Set(["(", "[", "{", "<"]);
const CLOSE = new Set([")", "]", "}", ">"]);

/** Scanner state shared by both walkers: bracket depth, quote, escape. */
function createScanner() {
  return { depth: 0, quote: null, escaped: false, previous: "" };
}

/**
 * Feed one character. Returns true when it is structural — outside any string
 * literal, so the caller may treat it as a delimiter.
 */
function step(scanner, char) {
  if (scanner.escaped) {
    scanner.escaped = false;
    scanner.previous = char;
    return false;
  }
  if (scanner.quote) {
    if (char === "\\") {
      scanner.escaped = true;
    } else if (char === scanner.quote) {
      scanner.quote = null;
    }
    scanner.previous = char;
    return false;
  }
  if (char === '"' || char === "'" || char === "`") {
    scanner.quote = char;
    scanner.previous = char;
    return false;
  }
  // `=>` is an arrow, not a closing angle bracket. Counting it would drop the
  // depth below zero and hide every later comma — which is how `startExecutor`
  // swallowed its `options` parameter into the callback's type.
  const arrow = char === ">" && scanner.previous === "=";
  if (OPEN.has(char)) {
    scanner.depth += 1;
  } else if (CLOSE.has(char) && !arrow) {
    scanner.depth -= 1;
  }
  scanner.previous = char;
  return true;
}

/** Split `text` on top-level occurrences of the single-character `separator`. */
export function splitTopLevel(text, separator) {
  const parts = [];
  const scanner = createScanner();
  let current = "";
  for (const char of text) {
    const structural = step(scanner, char);
    if (structural && char === separator && scanner.depth === 0) {
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
  const scanner = createScanner();
  let depth = 0;
  for (let i = open; i < text.length; i += 1) {
    const structural = step(scanner, text[i]);
    if (!structural) {
      continue;
    }
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
