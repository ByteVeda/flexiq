// The Rust SDK's public surface, read out of `crates/flexiq/src/`.
//
// A text parser rather than `cargo doc --output-format json`, for the reason
// `extract/java.mjs` gives: the docs CI job has Node and nothing else. It is
// also the only SDK whose surface is not one declaration file — Python and Node
// have a native stub and Java a single public interface, while a Rust crate
// declares its API wherever the `impl` block lives. `SOURCES.rust.files` names
// the modules that are publicly reachable and leaves out the ones that are not
// (`encode`/`decode` are private modules re-exported only under `__private`).
//
// Three Rust-specific things this has to get right, each of which silently
// empties part of the inventory otherwise:
//
//   - **`macro_rules!`.** The fourteen enqueue-option setters are written once
//     in `enqueue_setters!` and expanded into both `impl EnqueueOptions` and
//     `impl<T: Task> TaskCall<T>`. A parser that only reads `impl` bodies finds
//     none of them, and the two biggest types in the reference come out empty.
//   - **An apostrophe is a lifetime, not a quote.** `&'static str` opens a
//     string literal that never closes, which freezes the bracket depth and
//     hides every parameter after it. That is why the splitting here is local
//     rather than `shared.mjs`'s — its scanner is right for three languages and
//     wrong for this one.
//   - **`{}` inside a `format!`.** Brace depth is the whole state machine, so
//     string literals have to be neutralised before it counts anything.

/** `pub`, but not `pub(crate)` / `pub(super)` — those are not the surface. */
const PUBLIC = /^pub(?:\s|$)/;

/** A type-defining header we may be inside of. */
const IMPL_DECL = /^impl(?:\s*<.*?>)?\s+(.+?)\s*\{?\s*$/;
const TRAIT_DECL = /^pub\s+trait\s+([A-Za-z_]\w*)/;
const MACRO_DECL = /^macro_rules!\s+([A-Za-z_]\w*)/;
const MACRO_CALL = /^([A-Za-z_]\w*)!\s*\(/;
const FN_DECL = /\bfn\s+([A-Za-z_]\w*)/;

const OPEN = new Set(["(", "[", "{", "<"]);
const CLOSE = new Set([")", "]", "}", ">"]);

/**
 * Blank out comments and string literals, preserving line count and every
 * brace that is really a brace.
 *
 * Char literals are deliberately not handled: in a signature a `'` is a
 * lifetime every time, and treating `'static` as an open quote is exactly the
 * failure described above.
 */
function declutter(source) {
  let out = "";
  let mode = "code";
  let hashes = 0;
  for (let i = 0; i < source.length; i += 1) {
    const char = source[i];
    const next = source[i + 1];
    if (mode === "code") {
      if (char === "/" && next === "/") {
        mode = "line";
        i += 1;
        continue;
      }
      if (char === "/" && next === "*") {
        mode = "block";
        i += 1;
        continue;
      }
      if (char === "r" && (next === '"' || next === "#")) {
        const raw = /^r(#*)"/.exec(source.slice(i));
        if (raw) {
          hashes = raw[1].length;
          mode = "raw";
          i += raw[0].length - 1;
          continue;
        }
      }
      if (char === '"') {
        mode = "string";
        continue;
      }
      out += char;
      continue;
    }
    if (mode === "line") {
      if (char === "\n") {
        mode = "code";
        out += char;
      }
      continue;
    }
    if (mode === "block") {
      if (char === "*" && next === "/") {
        mode = "code";
        i += 1;
      } else if (char === "\n") {
        out += char;
      }
      continue;
    }
    if (mode === "string") {
      if (char === "\\") {
        i += 1;
      } else if (char === '"') {
        mode = "code";
      } else if (char === "\n") {
        out += char;
      }
      continue;
    }
    // raw
    if (
      char === '"' &&
      source.slice(i + 1, i + 1 + hashes) === "#".repeat(hashes)
    ) {
      mode = "code";
      i += hashes;
    } else if (char === "\n") {
      out += char;
    }
  }
  return out;
}

/** Net brace change on one decluttered line. */
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

/** Split on top-level `separator`, counting brackets and nothing else. */
function splitTopLevel(text, separator) {
  const parts = [];
  let depth = 0;
  let current = "";
  for (const char of text) {
    if (OPEN.has(char)) {
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
function matchParen(text, open) {
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

/** True once a declaration has as many `)` as `(` — it may span lines. */
function balanced(text) {
  const open = (text.match(/\(/g) ?? []).length;
  return open > 0 && open === (text.match(/\)/g) ?? []).length;
}

const SELF = /^&?\s*(?:'\w+\s*)?(?:mut\s+)?self$/;

/** `name: Type` pairs, with the receiver dropped. Rust has no defaults. */
function parseParams(inner) {
  return splitTopLevel(inner, ",")
    .map((part) => part.trim())
    .filter((part) => part.length > 0 && !SELF.test(part))
    .map((part) => {
      const at = splitTopLevel(part, ":");
      return at.length > 1
        ? {
            name: at[0].trim(),
            type: at.slice(1).join(":").trim(),
            default: null,
          }
        : { name: part, type: "", default: null };
    });
}

/** Does this declaration take a receiver? That is method vs associated fn. */
function takesSelf(inner) {
  return splitTopLevel(inner, ",").some((part) => SELF.test(part.trim()));
}

/** One `fn` declaration → a symbol, or null when it is not one we document. */
function toSymbol(declaration, owner, { hidden, requirePub }) {
  if (hidden) {
    return null;
  }
  const trimmed = declaration.trim();
  if (requirePub && !PUBLIC.test(trimmed)) {
    return null;
  }
  const named = FN_DECL.exec(trimmed);
  if (!named) {
    return null;
  }
  const name = named[1];
  const afterName = trimmed.slice(named.index + named[0].length);
  const paren = afterName.indexOf("(");
  if (paren === -1) {
    return null;
  }
  const typeParams = afterName.slice(0, paren).trim() || null;
  const close = matchParen(afterName, paren);
  if (close === -1) {
    return null;
  }
  const inner = afterName.slice(paren + 1, close);
  const params = parseParams(inner);
  // Stop at the body, the semicolon of a trait requirement, or a `where` — all
  // three end the return type, and only the first is always present.
  const tail = afterName.slice(close + 1);
  const end = tail.search(/\{|;|\bwhere\b/);
  const returned = (end === -1 ? tail : tail.slice(0, end)).trim();
  const returns = returned.startsWith("->")
    ? returned.slice(2).trim() || null
    : null;
  const rendered = params
    .map((param) => (param.type ? `${param.name}: ${param.type}` : param.name))
    .join(", ");
  return {
    owner,
    name,
    kind: takesSelf(inner) ? "method" : "static",
    signature: `fn ${name}${typeParams ?? ""}(${rendered})${
      returns ? ` -> ${returns}` : ""
    }`,
    params,
    returns,
    ...(typeParams ? { typeParams } : {}),
  };
}

/**
 * Every `fn` declaration in a `macro_rules!` body, at any depth.
 *
 * Depth is meaningless here — the body is wrapped in the matcher's own braces
 * and expands *into* an `impl` rather than emitting one. The bodies this reads
 * hold method declarations and nothing that could be mistaken for one.
 */
function macroMembers(body, owner) {
  // `$crate` is how a macro names its defining crate from inside a caller's.
  // Printed raw it is noise; what a reader types is the crate's real name, and
  // this extractor reads exactly one crate.
  const body_ = body.replace(/\$crate::/g, "flexiq::");
  const symbols = [];
  let buffer = null;
  let hidden = false;
  for (const raw of body_.split("\n")) {
    const line = raw.trim();
    if (buffer !== null) {
      buffer += ` ${line}`;
      if (balanced(buffer)) {
        const symbol = toSymbol(buffer, owner, { hidden, requirePub: true });
        if (symbol) {
          symbols.push(symbol);
        }
        buffer = null;
        hidden = false;
      }
      continue;
    }
    if (line.startsWith("#[")) {
      hidden = hidden || line.includes("doc(hidden)");
      continue;
    }
    if (!FN_DECL.test(line) || !PUBLIC.test(line)) {
      continue;
    }
    buffer = line;
    if (balanced(buffer)) {
      const symbol = toSymbol(buffer, owner, { hidden, requirePub: true });
      if (symbol) {
        symbols.push(symbol);
      }
      buffer = null;
      hidden = false;
    }
  }
  return symbols;
}

/** The type an `impl` header declares on, or null for a trait impl. */
function implOwner(header) {
  // `impl Trait for Type` documents the trait, not the type — and every one of
  // them here is a std trait (`Default`, `From`) whose methods a reader looks up
  // in std, not in these docs.
  if (/\bfor\b/.test(header)) {
    return null;
  }
  const path = header.replace(/\s*\{\s*$/, "").trim();
  const bare = path.split("<")[0].trim();
  const segments = bare.split("::");
  return segments[segments.length - 1] || null;
}

/**
 * Declared macros, kept across files.
 *
 * `enqueue_setters!` is defined in `options.rs` and expanded in `call.rs`, so a
 * per-file registry loses all fifteen of `TaskCall`'s setters — the extractor
 * sees an invocation naming a macro it has never read. The `files` list in
 * `inventory.mjs` therefore puts `options.rs` first, and that order is
 * load-bearing. It does not reach the snapshot: `extractFromSource` sorts by
 * owner and name before writing.
 */
const MACROS = new Map();

/**
 * `source` is one `.rs` file. Returns the symbols it declares publicly.
 */
export function extractRust(source) {
  const text = declutter(source);
  const macros = MACROS;
  const symbols = [];

  let depth = 0;
  let scope = null; // { owner, bodyDepth, requirePub }
  let macro = null; // { name, startDepth, lines }
  let buffer = null;
  let hidden = false;

  for (const raw of text.split("\n")) {
    const line = raw.trim();

    if (macro) {
      const after = depth + braceDelta(raw);
      if (after <= macro.startDepth) {
        macros.set(macro.name, macro.lines.join("\n"));
        macro = null;
      } else {
        macro.lines.push(raw);
      }
      depth = after;
      continue;
    }

    if (buffer !== null) {
      buffer += ` ${line}`;
      if (balanced(buffer)) {
        const symbol = toSymbol(buffer, scope?.owner ?? null, {
          hidden,
          requirePub: scope?.requirePub ?? true,
        });
        if (symbol) {
          symbols.push(symbol);
        }
        buffer = null;
        hidden = false;
      }
      depth += braceDelta(raw);
      continue;
    }

    if (line.startsWith("#[")) {
      hidden = hidden || line.includes("doc(hidden)");
      depth += braceDelta(raw);
      continue;
    }

    const asMacro = MACRO_DECL.exec(line);
    if (asMacro) {
      macro = { name: asMacro[1], startDepth: depth, lines: [] };
      depth += braceDelta(raw);
      continue;
    }

    if (scope && depth === scope.bodyDepth) {
      const called = MACRO_CALL.exec(line);
      if (called && macros.has(called[1])) {
        symbols.push(...macroMembers(macros.get(called[1]), scope.owner));
        depth += braceDelta(raw);
        continue;
      }
    }

    if (depth === 0) {
      const asTrait = TRAIT_DECL.exec(line);
      const asImpl = asTrait ? null : IMPL_DECL.exec(line);
      if (asTrait) {
        // A trait's items carry no `pub` — the trait's own visibility is theirs,
        // the same rule `extract/java.mjs` applies to interface members.
        scope = { owner: asTrait[1], bodyDepth: 1, requirePub: false };
        hidden = false;
        depth += braceDelta(raw);
        continue;
      }
      if (asImpl) {
        const owner = implOwner(asImpl[1]);
        scope = owner ? { owner, bodyDepth: 1, requirePub: true } : null;
        hidden = false;
        depth += braceDelta(raw);
        continue;
      }
    }

    const documentable =
      FN_DECL.test(line) &&
      (scope ? depth === scope.bodyDepth : depth === 0) &&
      (scope?.requirePub === false || PUBLIC.test(line));
    if (documentable) {
      buffer = line;
      if (balanced(buffer)) {
        const symbol = toSymbol(buffer, scope?.owner ?? null, {
          hidden,
          requirePub: scope?.requirePub ?? true,
        });
        if (symbol) {
          symbols.push(symbol);
        }
        buffer = null;
        hidden = false;
      }
      depth += braceDelta(raw);
      continue;
    }

    if (line.length > 0 && !line.startsWith("//")) {
      hidden = false;
    }
    depth += braceDelta(raw);
    if (scope && depth < scope.bodyDepth) {
      scope = null;
    }
  }

  return symbols;
}
