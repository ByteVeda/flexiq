// Lightweight regex syntax highlighter ported from the prototype's landing.js.
// Deterministic + synchronous, so it runs during prerender (HTML baked in) — no
// async Shiki on the landing. Emits the same .kw/.str/.fn/.num/.cmt/.def token
// classes the design system styles. Build-time Shiki still powers MDX code blocks.

function escapeHtml(code: string): string {
  return code
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

const PY_KW = new Set([
  "from",
  "import",
  "def",
  "return",
  "class",
  "async",
  "await",
  "try",
  "except",
  "raise",
  "if",
  "else",
  "for",
  "in",
  "with",
  "as",
  "None",
  "True",
  "False",
  "self",
]);

const TS_KW = new Set([
  "import",
  "from",
  "export",
  "const",
  "let",
  "var",
  "function",
  "return",
  "await",
  "async",
  "new",
  "class",
  "extends",
  "if",
  "else",
  "for",
  "of",
  "in",
  "try",
  "catch",
  "throw",
  "typeof",
  "number",
  "string",
  "boolean",
  "void",
  "null",
  "undefined",
  "true",
  "false",
]);

function tokenize(code: string, kw: Set<string>): string {
  const escaped = escapeHtml(code);
  const re =
    /(\/\/[^\n]*|#[^\n]*)|(`(?:[^`\\]|\\.)*`|"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')|(@[A-Za-z_][\w.]*)|(\b\d+(?:\.\d+)?\b)|([A-Za-z_$]\w*)/g;
  return escaped.replace(
    re,
    (m, cmt, str, dec, num, ident, offset: number, full: string) => {
      if (cmt != null) return `<span class="cmt">${cmt}</span>`;
      if (str != null) return `<span class="str">${str}</span>`;
      if (dec != null) return `<span class="fn">${dec}</span>`;
      if (num != null) return `<span class="num">${num}</span>`;
      if (ident != null) {
        if (kw.has(ident)) return `<span class="kw">${ident}</span>`;
        if (/^\s*\(/.test(full.slice(offset + ident.length)))
          return `<span class="def">${ident}</span>`;
        return ident;
      }
      return m;
    },
  );
}

export function highlightPython(code: string): string {
  return tokenize(code, PY_KW);
}

export function highlightTs(code: string): string {
  return tokenize(code, TS_KW);
}

const JAVA_KW = new Set([
  "import",
  "package",
  "public",
  "private",
  "final",
  "static",
  "class",
  "interface",
  "record",
  "return",
  "new",
  "try",
  "catch",
  "throw",
  "throws",
  "if",
  "else",
  "for",
  "var",
  "int",
  "long",
  "double",
  "boolean",
  "void",
  "null",
  "true",
  "false",
]);

export function highlightJava(code: string): string {
  return tokenize(code, JAVA_KW);
}

// `Ok`/`Err`/`Some`/`None` are constructors rather than keywords, but they read
// as control flow in a hero snippet and every Rust theme colours them.
const RUST_KW = new Set([
  "use",
  "pub",
  "fn",
  "let",
  "mut",
  "const",
  "static",
  "struct",
  "enum",
  "impl",
  "trait",
  "for",
  "in",
  "if",
  "else",
  "match",
  "loop",
  "while",
  "return",
  "move",
  "async",
  "await",
  "dyn",
  "where",
  "as",
  "ref",
  "Ok",
  "Err",
  "Some",
  "None",
  "true",
  "false",
]);

export function highlightRust(code: string): string {
  return tokenize(code, RUST_KW);
}

// Shell gets its own pass rather than a keyword set: `tokenize` treats `//` as a
// comment, which would swallow the rest of `http://localhost:50051/v1/jobs` and
// of `sqlite:///tmp/flexiq.db`. Only `#` opens a comment here, and a token's
// role comes from its position — leading word, `-flag`, `NAME=` — not a list.
const SH_RE =
  /(#[^\n]*)|('[^']*'|"[^"]*")|(\$\{?\w+\}?)|(?<=^|\s)(-{1,2}[A-Za-z][\w-]*)|(?<=^|\s)([A-Z][A-Z0-9_]*)(?==)|(?<=^|\s)([a-z][\w-]*)(?=\s|$)/gm;

export function highlightShell(code: string): string {
  return escapeHtml(code).replace(
    SH_RE,
    (match, comment, str, variable, flag, envName, command) => {
      if (comment != null) return `<span class="cmt">${comment}</span>`;
      if (str != null) return `<span class="str">${str}</span>`;
      if (variable != null) return `<span class="num">${variable}</span>`;
      if (flag != null) return `<span class="kw">${flag}</span>`;
      if (envName != null) return `<span class="kw">${envName}</span>`;
      if (command != null) return `<span class="def">${command}</span>`;
      return match;
    },
  );
}
