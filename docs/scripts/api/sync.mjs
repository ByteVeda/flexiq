#!/usr/bin/env node
// Regenerate the generated half of the API reference (run: pnpm sync:api).
//
// Writes three things:
//   content/api/<sdk>.json                     the extracted inventory snapshot
//   content/docs/<sdk>/api-reference/symbols/  the generated symbol index
//   the `api:` blocks inside the hand-written reference pages
//
// `--check` makes the same plan and compares it with the tree instead of
// writing, which is what `pnpm check:parity` runs. `--sdk <id>` narrows the
// snapshot comparison to one SDK — the Node CI job uses it after
// `pnpm build:native`, the only place `native/index.d.ts` exists.

import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import { dirname, relative } from "node:path";
import { CONTENT_DIR, loadContentFiles } from "../parity/content.mjs";
import { computeCoverage } from "./coverage.mjs";
import {
  collectInventories,
  qualify,
  SDKS,
  SOURCES,
  writeSnapshot,
} from "./inventory.mjs";
import { runApiSyncPlan } from "./plan.mjs";

if (process.argv.includes("--help") || process.argv.includes("-h")) {
  console.log(`Regenerate the generated API reference from the SDK declarations.

usage: node scripts/api/sync.mjs [--check|--backlog] [--sdk <${SDKS.join("|")}>]

  --check        report what would change and exit 1, writing nothing
  --backlog      list the symbols no hand-written reference page mentions
  --sdk <id>     only compare that SDK's snapshot against its sources

Sources are listed in scripts/api/inventory.mjs. An SDK whose sources are
absent from the checkout keeps its committed snapshot — that is how the docs
CI job runs without a Rust toolchain.`);
  process.exit(0);
}

const check = process.argv.includes("--check");
const requested = process.argv[process.argv.indexOf("--sdk") + 1];
const sdks = process.argv.includes("--sdk") ? [requested] : SDKS;
if (!sdks.every((sdk) => SDKS.includes(sdk))) {
  console.error(`unknown sdk: ${requested}`);
  process.exit(2);
}

const plan = runApiSyncPlan(collectInventories(), sdks);
const label = (path) => relative(CONTENT_DIR, path).split("\\").join("/");

for (const note of plan.notes) {
  console.log(note);
}

if (process.argv.includes("--backlog")) {
  for (const [sdk, coverage] of computeCoverage(
    loadContentFiles(),
    plan.inventories,
  )) {
    console.log(
      `\n== ${sdk}: ${coverage.undocumented.length} of ${coverage.expected.length} symbols have no hand-written reference entry`,
    );
    for (const symbol of coverage.undocumented) {
      console.log(`  ${qualify(symbol)}`);
    }
  }
  process.exit(0);
}

for (const error of plan.errors) {
  console.error(`ERROR ${error}`);
}

if (check) {
  const stale = [
    ...plan.snapshotDrift.map(
      (sdk) =>
        `content/api/${sdk}.json is stale against ${SOURCES[sdk].files.join(", ")}`,
    ),
    ...[...plan.files]
      .filter(([, file]) => file.changed)
      .map(([path]) => label(path)),
    ...plan.stale.map((path) => `${label(path)} (no longer generated)`),
  ];
  for (const entry of stale) {
    console.error(`STALE ${entry}`);
  }
  if (stale.length > 0 || plan.errors.length > 0) {
    console.error("\nrun `pnpm sync:api` to regenerate");
    process.exit(1);
  }
  console.log("generated API reference is up to date");
  process.exit(0);
}

for (const sdk of plan.snapshotDrift) {
  writeSnapshot(plan.inventories.get(sdk));
  console.log(`wrote content/api/${sdk}.json`);
}
let written = 0;
for (const [path, file] of plan.files) {
  if (!file.changed) {
    continue;
  }
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, file.content);
  written += 1;
}
for (const path of plan.stale) {
  rmSync(path);
  console.log(`removed ${label(path)}`);
}
console.log(`synced ${written} generated file${written === 1 ? "" : "s"}`);
process.exit(plan.errors.length > 0 ? 1 : 0);
