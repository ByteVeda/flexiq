// What the generated half of the reference should contain, given the
// inventories. Pure: it plans file contents, it does not write them — so
// `pnpm sync:api` and the parity gate reach the same answer from the same code
// instead of a writer and a checker drifting apart.

import { existsSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { CONTENT_DIR, loadContentFiles } from "../parity/content.mjs";
import { documentable, GROUPS, placementOf, qualify } from "./inventory.mjs";
import {
  entriesOf,
  renderEntry,
  renderGroupPage,
  renderIndexPage,
  renderMeta,
} from "./render.mjs";

/** Section the generated pages live in, under each SDK's `api-reference/`. */
export const SECTION = "api-reference/symbols";

/** Opening marker of an in-page generated block, e.g. `api:PyQueue.enqueue`. */
export const INLINE_OPEN = "{/* api:";

/** Opening marker, symbol name, body, closing marker. */
const INLINE_BLOCK =
  /(\{\/\* api:([\w.$]+) \*\/\}\n)[\s\S]*?(\n\{\/\* \/api \*\/\})/g;

/** Drop generated blocks, markers and all — what a page says in its own words. */
export function stripInlineBlocks(raw) {
  return raw.replace(INLINE_BLOCK, "");
}

function sectionDir(sdk) {
  return join(CONTENT_DIR, sdk, SECTION);
}

/** Group one SDK's symbols by generated page, in nav order. */
export function groupSymbols(inventory) {
  const byGroup = new Map(Object.keys(GROUPS).map((group) => [group, []]));
  const unmapped = [];
  for (const symbol of documentable(inventory.symbols)) {
    const { group } = placementOf(symbol);
    if (byGroup.has(group)) {
      byGroup.get(group).push(symbol);
    } else {
      unmapped.push(qualify(symbol));
    }
  }
  return { byGroup, unmapped };
}

/**
 * The generated section for one SDK: absolute path → file contents, plus any
 * symbol whose declaring type has no page assigned.
 */
export function planSection(inventory) {
  const { byGroup, unmapped } = groupSymbols(inventory);
  const filled = [...byGroup].filter(([, symbols]) => symbols.length > 0);
  const dir = sectionDir(inventory.sdk);
  const files = new Map([
    [join(dir, "meta.json"), renderMeta(filled.map(([group]) => group))],
    [join(dir, "index.mdx"), renderIndexPage(inventory.sdk, byGroup)],
    ...filled.map(([group, symbols]) => [
      join(dir, `${group}.mdx`),
      renderGroupPage(inventory.sdk, group, symbols),
    ]),
  ]);
  return { files, unmapped };
}

/** Files sitting in a generated section that the plan no longer contains. */
export function staleFiles(planned) {
  const dirs = new Set([...planned.keys()].map((file) => join(file, "..")));
  const stale = [];
  for (const dir of dirs) {
    if (!existsSync(dir)) {
      continue;
    }
    for (const entry of readdirSync(dir)) {
      const full = join(dir, entry);
      if (!planned.has(full)) {
        stale.push(full);
      }
    }
  }
  return stale;
}

/** Every overload of a qualified name, or null when the inventory has none. */
function lookup(inventory, name) {
  const matches = inventory.symbols.filter(
    (symbol) => qualify(symbol) === name,
  );
  return matches.length > 0 ? matches : null;
}

/**
 * Rewrite the in-page generated blocks of the hand-written reference pages.
 * Returns absolute path → new contents for every page carrying a block, plus
 * the markers naming a symbol no inventory has.
 */
export function planInline(inventories) {
  const files = new Map();
  const missing = [];
  for (const file of loadContentFiles()) {
    if (!file.rel.endsWith(".mdx") || !file.raw.includes(INLINE_OPEN)) {
      continue;
    }
    // `replace` only ever sees complete blocks, so an unterminated marker would
    // leave stale content in place and report nothing — the one shape of this
    // failure the gate could not otherwise see.
    const opened = file.raw.split(INLINE_OPEN).length - 1;
    const complete = [...file.raw.matchAll(INLINE_BLOCK)].length;
    if (opened !== complete) {
      missing.push(
        `${file.rel}: ${opened - complete} api block(s) never closed with {/* /api */}`,
      );
      continue;
    }
    const sdk = file.rel.split("/")[0];
    const inventory = inventories.get(sdk);
    const next = file.raw.replace(INLINE_BLOCK, (whole, open, name, close) => {
      const symbols = inventory && lookup(inventory, name);
      if (!symbols) {
        missing.push(`${file.rel}: no ${sdk} symbol named ${name}`);
        return whole;
      }
      return `${open}${renderEntry(sdk, entriesOf(symbols)[0])}${close}`;
    });
    files.set(join(CONTENT_DIR, file.rel), next);
  }
  return { files, missing };
}
