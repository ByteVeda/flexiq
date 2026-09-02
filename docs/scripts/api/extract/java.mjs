// Java inventory: parse the public interfaces of `org.byteveda.flexiq`.
//
// Deliberately NOT the JNI facade (`spi/QueueBackend`): its method names are
// native-shaped (`getJobJson`, `statsAllQueuesJson`) and no Java user ever
// types one, so gating the reference on them would measure the wrong surface.
// `FlexiQ`, `Queue` and `JobContext` are what the reference documents and what
// javadoc publishes.
//
// A parser rather than the javadoc tool for the same reason as the other two:
// the docs CI job has Node and nothing else.

import { splitTopLevel } from "./shared.mjs";

const STRING_LITERAL = /"(?:[^"\\\n]|\\.)*"|'(?:[^'\\\n]|\\.)'/g;
const BLOCK_COMMENT = /\/\*[\s\S]*?\*\//g;
const LINE_COMMENT = /\/\/[^\n]*/g;

const TYPE_DECL =
  /^\s*(?:(?:public|protected|private|final|static|abstract|sealed|non-sealed)\s+)*(class|interface|enum|record)\s+(\w+)/;
const MODIFIERS =
  /^(?:(?:@\w+(?:\([^)]*\))?|public|protected|private|static|final|default|abstract|synchronized|native)\s+)*/;
const METHOD_DECL =
  /^([\w.$<>[\],?@\s]+?)\s+(\w+)\s*\(([\s\S]*)\)\s*(?:throws\s+[\w.,\s]+?)?\s*[;{]$/;
/** Statements that look like a declaration but open a block. */
const KEYWORDS = new Set(["if", "for", "while", "switch", "catch", "return"]);

function isPublic(modifiers, insideInterface) {
  if (/\bprivate\b|\bprotected\b/.test(modifiers)) {
    return false;
  }
  // Interface members are public without saying so; class members are not.
  return insideInterface || /\bpublic\b/.test(modifiers);
}

function parseParams(inside) {
  return splitTopLevel(inside, ",")
    .map((part) => part.trim().replace(/\s+/g, " "))
    .filter(Boolean)
    .map((part) => {
      const cut = part.lastIndexOf(" ");
      return cut === -1
        ? { name: part, type: null, default: null }
        : {
            name: part.slice(cut + 1),
            type: part.slice(0, cut).replace(/^final\s+/, ""),
            default: null,
          };
    });
}

/** Count `{`/`}` in a line that has already had comments and strings removed. */
function braceDelta(line) {
  let delta = 0;
  for (const char of line) {
    if (char === "{") {
      delta += 1;
    } else if (char === "}") {
      delta -= 1;
    }
  }
  return delta;
}

function balanced(text) {
  let depth = 0;
  for (const char of text) {
    if (char === "(") {
      depth += 1;
    } else if (char === ")") {
      depth -= 1;
    }
  }
  return depth <= 0;
}

export function extractJava(source) {
  const clean = source
    .replace(STRING_LITERAL, '""')
    .replace(BLOCK_COMMENT, "")
    .replace(LINE_COMMENT, "");
  const lines = clean.split("\n");
  const symbols = [];
  const stack = [];
  let depth = 0;

  for (let i = 0; i < lines.length; i += 1) {
    let line = lines[i];
    if (!line.trim()) {
      continue;
    }
    const top = stack[stack.length - 1];
    const isMemberPosition = top && depth === top.bodyDepth;

    const asType = line.match(TYPE_DECL);
    if (asType && (!top || isMemberPosition)) {
      const qualified = top ? `${top.name}.${asType[2]}` : asType[2];
      const published = isPublic(
        line.slice(0, line.indexOf(asType[1])),
        top?.kind === "interface",
      );
      depth += braceDelta(line);
      stack.push({
        name: qualified,
        kind: asType[1],
        published: published && (top?.published ?? true),
        bodyDepth: depth,
      });
      continue;
    }

    if (isMemberPosition && top.published) {
      // Join a declaration that wraps across lines before matching it.
      let joined = line.trim();
      let end = i;
      while (!balanced(joined) && end + 1 < lines.length) {
        end += 1;
        joined = `${joined} ${lines[end].trim()}`;
      }
      const modifiers = joined.match(MODIFIERS)?.[0] ?? "";
      const rest = joined.slice(modifiers.length);
      const asMethod = rest.match(METHOD_DECL);
      if (asMethod && !KEYWORDS.has(asMethod[2])) {
        const returns = asMethod[1].replace(/\s+/g, " ").trim();
        const params = parseParams(asMethod[3]);
        if (isPublic(modifiers, top.kind === "interface")) {
          symbols.push({
            owner: top.name,
            name: asMethod[2],
            kind: /\bstatic\b/.test(modifiers) ? "static" : "method",
            signature: `${returns} ${asMethod[2]}(${params
              .map((param) => `${param.type} ${param.name}`)
              .join(", ")})`,
            params,
            returns,
          });
        }
        for (let skip = i; skip <= end; skip += 1) {
          depth += braceDelta(lines[skip]);
        }
        i = end;
        continue;
      }
      line = joined;
      i = end;
    }

    depth += braceDelta(line);
    while (stack.length > 0 && depth < stack[stack.length - 1].bodyDepth) {
      stack.pop();
    }
  }
  return symbols;
}
