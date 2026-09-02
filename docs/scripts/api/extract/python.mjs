// Python inventory: parse `sdks/python/flexiq/_flexiq.pyi`.
//
// The stub is the one file that enumerates the whole native surface, and the
// shell's mixins forward those names verbatim (`cancel_job`, `dead_letters`,
// `list_jobs_after`), so a name here is a name a user calls. Callables and
// `@property` only — plain annotated attributes are data on a record, not API
// a reader looks up by name.
//
// Hand-rolled rather than pulled from a Python AST: the gate has to run in the
// docs CI job, which has Node and no Python toolchain.

import { splitTopLevel } from "./shared.mjs";

const CLASS = /^class\s+(\w+)/;
const DEF = /^ {4}(?:async\s+)?def\s+(\w+)\s*\(/;
const DECORATOR = /^ {4}@(\w+)/;

/** Params the caller never passes. */
const IMPLICIT = new Set(["self", "cls"]);

/** `name: type = default` — annotation and default are both optional. */
function parseParam(text) {
  const marker = text.match(/^(\*{0,2})\s*([\w]+)?/);
  const stars = marker?.[1] ?? "";
  const name = marker?.[2];
  if (!name) {
    // Bare `*` or `/`: positional/keyword markers, not parameters.
    return null;
  }
  const rest = text.slice(stars.length + name.length);
  const eq = splitTopLevel(rest, "=");
  const annotation = eq[0].replace(/^\s*:\s*/, "").trim();
  return {
    name: stars + name,
    type: annotation || null,
    default: eq.length > 1 ? eq.slice(1).join("=").trim() : null,
  };
}

/** Join a `def` that spans lines into one declaration, ignoring `...` bodies. */
function readDeclaration(lines, start) {
  let text = "";
  let depth = 0;
  for (let i = start; i < lines.length; i += 1) {
    const line = lines[i].trim();
    text += (text ? " " : "") + line;
    for (const char of line) {
      if (char === "(" || char === "[") {
        depth += 1;
      } else if (char === ")" || char === "]") {
        depth -= 1;
      }
    }
    // The declaration ends at the `:` that closes it, once parens balance.
    if (depth <= 0 && /:\s*(\.\.\.)?$/.test(text)) {
      return { text, end: i };
    }
  }
  return { text, end: lines.length - 1 };
}

export function extractPython(source) {
  const lines = source.split("\n");
  const symbols = [];
  let owner = null;
  let decorators = [];

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    const asClass = line.match(CLASS);
    if (asClass) {
      owner = asClass[1];
      decorators = [];
      continue;
    }
    const asDecorator = line.match(DECORATOR);
    if (asDecorator) {
      decorators.push(asDecorator[1]);
      continue;
    }
    const asDef = line.match(DEF);
    if (!asDef || !owner) {
      if (line.trim() && !line.startsWith(" ")) {
        owner = null;
      }
      continue;
    }

    const { text, end } = readDeclaration(lines, i);
    i = end;
    const open = text.indexOf("(");
    const close = text.lastIndexOf(")");
    const inside = text.slice(open + 1, close);
    const tail = text.slice(close + 1);
    const returns =
      tail.match(/->\s*(.+?)\s*:\s*(?:\.\.\.)?$/)?.[1]?.trim() ?? null;

    const params = splitTopLevel(inside, ",")
      .map((part) => part.trim())
      .filter(Boolean)
      .map(parseParam)
      .filter((param) => param && !IMPLICIT.has(param.name));

    const kind = decorators.includes("property")
      ? "property"
      : decorators.includes("staticmethod") ||
          decorators.includes("classmethod")
        ? "static"
        : "method";
    const rendered = params
      .map(
        (param) =>
          param.name +
          (param.type ? `: ${param.type}` : "") +
          (param.default === null ? "" : ` = ${param.default}`),
      )
      .join(", ");

    symbols.push({
      owner,
      name: asDef[1],
      kind,
      signature:
        kind === "property"
          ? `${asDef[1]}: ${returns ?? "Any"}`
          : `${asDef[1]}(${rendered})${returns ? ` -> ${returns}` : ""}`,
      params,
      returns,
    });
    decorators = [];
  }
  return symbols;
}
