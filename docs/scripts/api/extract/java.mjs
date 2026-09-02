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
/** A line carrying only annotations, or the marker left by a `@hidden` doc. */
const ANNOTATION_ONLY = /^(?:@[\w.]+(?:\([^)]*\))?\s*)+$/;
/** Survives comment stripping so a javadoc `@hidden` reaches its declaration. */
const HIDDEN = "@__hidden__";

/**
 * Comments and string literals out, line numbering intact.
 *
 * Line count is preserved so a wrapped declaration still joins correctly, and a
 * javadoc block carrying `@hidden` leaves a marker behind — the flag has to
 * outlive the comment it was written in.
 */
function declutter(source) {
  return source
    .replace(STRING_LITERAL, '""')
    .replace(BLOCK_COMMENT, (comment) => {
      const blanks = "\n".repeat((comment.match(/\n/g) ?? []).length);
      return comment.includes("@hidden") ? `${HIDDEN}${blanks}` : blanks;
    })
    .replace(LINE_COMMENT, "");
}

/** `<T> String` → `{ typeParams: "<T>", returns: "String" }`. */
function splitTypeParams(declared) {
  if (!declared.startsWith("<")) {
    return { typeParams: null, returns: declared };
  }
  let depth = 0;
  for (let i = 0; i < declared.length; i += 1) {
    if (declared[i] === "<") {
      depth += 1;
    } else if (declared[i] === ">") {
      depth -= 1;
      if (depth === 0) {
        return {
          typeParams: declared.slice(0, i + 1),
          returns: declared.slice(i + 1).trim(),
        };
      }
    }
  }
  return { typeParams: null, returns: declared };
}

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
  const lines = declutter(source).split("\n");
  const symbols = [];
  const stack = [];
  let depth = 0;
  let pending = "";

  for (let i = 0; i < lines.length; i += 1) {
    let line = lines[i];
    if (!line.trim()) {
      continue;
    }
    const top = stack[stack.length - 1];

    // A type whose `{` is on the next line is not open yet, so its members are
    // still a depth below where they will be. Wait for the brace rather than
    // recording the pre-body depth and skipping every member of that type.
    if (top && !top.open) {
      depth += braceDelta(line);
      if (depth > top.enclosingDepth) {
        top.bodyDepth = depth;
        top.open = true;
      }
      continue;
    }
    const isMemberPosition = top && depth === top.bodyDepth;

    // Annotations and the `@hidden` marker sit on their own lines above the
    // declaration they qualify; carry them forward to it.
    if (isMemberPosition && ANNOTATION_ONLY.test(line.trim())) {
      pending += ` ${line.trim()}`;
      continue;
    }

    const asType = line.match(TYPE_DECL);
    if (asType && (!top || isMemberPosition)) {
      const qualified = top ? `${top.name}.${asType[2]}` : asType[2];
      const published = isPublic(
        line.slice(0, line.indexOf(asType[1])),
        top?.kind === "interface",
      );
      const enclosingDepth = depth;
      depth += braceDelta(line);
      stack.push({
        name: qualified,
        kind: asType[1],
        published: published && (top?.published ?? true),
        enclosingDepth,
        bodyDepth: depth,
        open: depth > enclosingDepth,
      });
      pending = "";
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
      const declaredModifiers = joined.match(MODIFIERS)?.[0] ?? "";
      const modifiers = `${pending} ${declaredModifiers}`;
      const rest = joined.slice(declaredModifiers.length);
      const asMethod = rest.match(METHOD_DECL);
      if (asMethod && !KEYWORDS.has(asMethod[2])) {
        const declared = asMethod[1].replace(/\s+/g, " ").trim();
        const { typeParams, returns } = splitTypeParams(declared);
        const params = parseParams(asMethod[3]);
        // `@hidden` is javadoc's own "not published API" marker, and the SDK
        // uses it for the context-binding lifecycle. Honour it the way javadoc
        // does rather than presenting `JobContext.enter()` as a supported call.
        const hidden = modifiers.includes(HIDDEN);
        if (isPublic(modifiers, top.kind === "interface") && !hidden) {
          const deprecated = /@Deprecated\b/.test(modifiers);
          symbols.push({
            owner: top.name,
            name: asMethod[2],
            kind: /\bstatic\b/.test(modifiers) ? "static" : "method",
            signature: `${declared} ${asMethod[2]}(${params
              .map((param) => `${param.type} ${param.name}`)
              .join(", ")})`,
            params,
            returns,
            ...(typeParams ? { typeParams } : {}),
            ...(deprecated ? { deprecated } : {}),
          });
        }
        for (let skip = i; skip <= end; skip += 1) {
          depth += braceDelta(lines[skip]);
        }
        i = end;
        pending = "";
        continue;
      }
      line = joined;
      i = end;
    }

    pending = "";
    depth += braceDelta(line);
    while (stack.length > 0 && depth < stack[stack.length - 1].bodyDepth) {
      stack.pop();
    }
  }
  return symbols;
}
