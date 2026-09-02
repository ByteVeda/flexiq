// One plan, two consumers: `pnpm sync:api` writes it, `pnpm check:parity`
// asserts the tree already matches it. Anything that decides *what* the
// generated reference should be lives here, so the writer and the gate can
// never answer that question differently.

import { existsSync, readFileSync } from "node:fs";
import { planInline, planSection, staleFiles } from "./generate.mjs";
import { SOURCES, snapshotPath } from "./inventory.mjs";

function onDisk(path) {
  return existsSync(path) ? readFileSync(path, "utf8") : null;
}

/** Snapshot JSON exactly as `writeSnapshot` would produce it. */
function serialize(inventory) {
  return `${JSON.stringify(inventory, null, 2)}\n`;
}

/**
 * @param collected  output of `collectInventories()`
 * @param sdks       the SDKs whose snapshots to compare against their sources
 */
export function runApiSyncPlan(collected, sdks) {
  const inventories = new Map();
  const snapshotDrift = [];
  const errors = [];
  const notes = [];

  for (const [sdk, { extracted, current }] of collected) {
    if (!current) {
      errors.push(
        `${sdk}: no snapshot and no sources (${SOURCES[sdk].files.join(", ")})`,
      );
      continue;
    }
    inventories.set(sdk, current);
    if (!sdks.includes(sdk)) {
      continue;
    }
    if (!extracted) {
      notes.push(
        `${sdk}: sources absent from this checkout — using the committed snapshot (${current.symbols.length} symbols)`,
      );
      continue;
    }
    if (serialize(extracted) !== onDisk(snapshotPath(sdk))) {
      snapshotDrift.push(sdk);
    }
  }

  const files = new Map();
  const stale = [];
  for (const [sdk, inventory] of inventories) {
    const section = planSection(inventory);
    for (const name of section.unmapped) {
      errors.push(
        `${sdk}: ${name} has no page — add its declaring type to OWNERS in scripts/api/inventory.mjs`,
      );
    }
    for (const [path, content] of section.files) {
      files.set(path, { content, changed: content !== onDisk(path) });
    }
    stale.push(...staleFiles(section.files));
  }

  const inline = planInline(inventories);
  errors.push(...inline.missing);
  for (const [path, content] of inline.files) {
    files.set(path, { content, changed: content !== onDisk(path) });
  }

  return { inventories, snapshotDrift, files, stale, errors, notes };
}
