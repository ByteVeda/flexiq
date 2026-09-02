// Node inventory: parse `sdks/node/native/index.d.ts`.
//
// napi-rs generates it, so the shape is predictable: one member per line, JSDoc
// above, no overloads. Interfaces are skipped — they are option and record
// *types*, not surface a reader looks up by method name; the classes are what
// the shell's `Queue`/`Worker` wrappers forward to under the same names.
//
// The file is gitignored (a build artifact of `pnpm build:native`), which is
// why the snapshot beside this extractor is committed: the docs CI job has no
// Rust toolchain and must still be able to run the gate.

import { matchParen, splitTopLevel } from "./shared.mjs";

const CLASS = /^export declare class (\w+)/;
const FUNCTION = /^export declare function (\w+)\s*\(/;
const MEMBER = /^ {2}(?:(static|get|set)\s+)?(\w+)\s*(\(|:)/;
const DOC_LINE = /^\s*(\/\*\*|\*|\*\/)/;

function parseParams(inside) {
  return splitTopLevel(inside, ",")
    .map((part) => part.trim())
    .filter(Boolean)
    .map((part) => {
      const [head, ...rest] = splitTopLevel(part, ":");
      const name = head.trim();
      return {
        name: name.replace(/\?$/, ""),
        type: rest.join(":").trim() || null,
        // TypeScript spells "has a default" as `?`; the value itself is not in
        // the declaration, so record the optionality rather than invent one.
        default: name.endsWith("?") ? "optional" : null,
      };
    });
}

/** `name(a: T, b?: U): R` → structured parts, or null when it isn't callable. */
function parseCallable(text, name) {
  const open = text.indexOf("(", text.indexOf(name) + name.length - 1);
  if (open === -1) {
    return null;
  }
  const close = matchParen(text, open);
  if (close === -1) {
    return null;
  }
  const returns = text
    .slice(close + 1)
    .replace(/^\s*:\s*/, "")
    .trim();
  return { params: parseParams(text.slice(open + 1, close)), returns };
}

export function extractNode(source) {
  const symbols = [];
  let owner = null;

  for (const line of source.split("\n")) {
    if (DOC_LINE.test(line)) {
      continue;
    }
    const asClass = line.match(CLASS);
    if (asClass) {
      owner = asClass[1];
      continue;
    }
    const asFunction = line.match(FUNCTION);
    if (asFunction) {
      owner = null;
      const parts = parseCallable(line, asFunction[1]);
      if (parts) {
        symbols.push({
          owner: null,
          name: asFunction[1],
          kind: "function",
          signature: line.replace(/^export declare function\s+/, "").trim(),
          params: parts.params,
          returns: parts.returns || null,
        });
      }
      continue;
    }
    if (/^export /.test(line)) {
      // An interface, enum or type alias — everything indented below it belongs
      // to a type, not to the last class we saw.
      owner = null;
      continue;
    }
    const asMember = owner && line.match(MEMBER);
    if (!asMember) {
      continue;
    }
    const [, modifier, name, delimiter] = asMember;
    if (delimiter === ":") {
      // A declared field. napi emits these for `#[napi(getter)]`-free structs
      // only, which are interfaces; on a class it is not callable surface.
      continue;
    }
    const parts = parseCallable(line, name);
    if (!parts) {
      continue;
    }
    const kind =
      modifier === "get" || modifier === "set"
        ? "property"
        : modifier === "static"
          ? "static"
          : "method";
    symbols.push({
      owner,
      name,
      kind,
      signature: line.trim().replace(/^(static|get|set)\s+/, ""),
      params: parts.params,
      returns: parts.returns || null,
    });
  }
  return symbols;
}
