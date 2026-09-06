import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const DOCS_ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "../../..",
);
const SEARCH_ROOTS = ["app", "content/docs"];
const TEXT_EXTENSIONS = new Set([
  ".css",
  ".json",
  ".md",
  ".mdx",
  ".ts",
  ".tsx",
]);
const HISTORICAL_PATHS = new Set([
  "app/lib/redirects.ts",
  "content/docs/about/changelog.mdx",
  "content/docs/about/migrating-to-flexiq.mdx",
]);

function* walk(directory) {
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const absolute = path.join(directory, entry.name);
    if (entry.isDirectory()) {
      yield* walk(absolute);
    } else if (TEXT_EXTENSIONS.has(path.extname(entry.name))) {
      yield absolute;
    }
  }
}

export function checkLegacyBranding() {
  const errors = [];
  let scanned = 0;

  for (const root of SEARCH_ROOTS) {
    for (const absolute of walk(path.join(DOCS_ROOT, root))) {
      // Normalize to POSIX separators so the historical allowlist below
      // still matches on Windows, where path.relative yields backslashes.
      const relative = path
        .relative(DOCS_ROOT, absolute)
        .split(path.sep)
        .join("/");
      if (HISTORICAL_PATHS.has(relative)) continue;
      scanned += 1;

      // Match both plain text and JSX/HTML-split branding such as
      // `taski<span>to</span>`, which a literal search misses.
      if (/taski(?:\s|<[^>]*>)*to/i.test(fs.readFileSync(absolute, "utf8"))) {
        errors.push(`${relative} contains non-historical Taskito branding`);
      }
    }
  }

  return {
    name: "Legacy Taskito branding",
    errors,
    report: [`  ${scanned} live docs source files scanned`],
  };
}
