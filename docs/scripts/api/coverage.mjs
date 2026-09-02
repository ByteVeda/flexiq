// How much of each SDK's declared surface the hand-written reference actually
// documents.
//
// A symbol counts as documented when its name appears *in code* on a
// hand-written reference page — a fence or a code span, not a sentence that
// happens to use the word "stats". The generated section is excluded on
// purpose: generating a page is not documenting it, and a measure that counted
// it would read 100% the moment `pnpm sync:api` ran.
//
// Shared by the parity gate (which turns the numbers into a ratchet) and
// `pnpm sync:api --backlog` (which prints what is still missing).

import { readFileSync } from "node:fs";
import { extractDocText } from "../../app/lib/mdx-extract.ts";
import { SECTION } from "./generate.mjs";
import { documentable, qualify, SDKS } from "./inventory.mjs";

export const CONFIG = JSON.parse(
  readFileSync(new URL("../parity/api-coverage.json", import.meta.url), "utf8"),
);

/** `PyQueue.set_workflow_*` → a regex over qualified names. */
function toPattern(entry) {
  const escaped = entry.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  return new RegExp(`^${escaped.replace(/\\\*/g, "[\\w.$]*")}$`);
}

/** Code from every hand-written reference page of one SDK, concatenated. */
function referenceCode(files, sdk) {
  const generated = `${sdk}/${SECTION}/`;
  return files
    .filter(
      (file) =>
        file.rel.endsWith(".mdx") &&
        !file.rel.startsWith(generated) &&
        (file.rel.startsWith(`${sdk}/api-reference/`) ||
          file.rel.startsWith("shared/api-reference/")),
    )
    .map((file) => extractDocText(file.raw).code)
    .join("\n");
}

function mentions(code, name) {
  return new RegExp(`(?<![\\w])${name.replace(/\$/g, "\\$")}(?![\\w])`).test(
    code,
  );
}

/**
 * Per-SDK coverage. `expected` excludes allowlisted symbols; `unusedAllowlist`
 * names exemptions that no longer match anything, so a stale one is an error
 * rather than a silent hole.
 */
export function computeCoverage(files, inventories) {
  const bySdk = new Map();
  for (const sdk of SDKS) {
    const inventory = inventories.get(sdk);
    if (!inventory) {
      continue;
    }
    const allowlist = Object.keys(CONFIG.allowlist[sdk] ?? {}).map((entry) => ({
      entry,
      pattern: toPattern(entry),
    }));
    const used = new Set();
    const expected = [];
    const exempt = [];
    for (const symbol of documentable(inventory.symbols)) {
      const hit = allowlist.find(({ pattern }) =>
        pattern.test(qualify(symbol)),
      );
      if (hit) {
        used.add(hit.entry);
        exempt.push(symbol);
      } else {
        expected.push(symbol);
      }
    }
    const code = referenceCode(files, sdk);
    const undocumented = expected.filter(
      (symbol) => !mentions(code, symbol.name),
    );
    bySdk.set(sdk, {
      expected,
      exempt,
      undocumented,
      documented: expected.length - undocumented.length,
      baseline: CONFIG.documented[sdk],
      unusedAllowlist: allowlist
        .filter(({ entry }) => !used.has(entry))
        .map(({ entry }) => entry),
    });
  }
  return bySdk;
}
