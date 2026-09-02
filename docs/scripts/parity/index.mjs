#!/usr/bin/env node
import { checkApiCoverage } from "./checks/api-coverage.mjs";
import { checkCodeTabs } from "./checks/code-tabs.mjs";
import { checkCollisions } from "./checks/collisions.mjs";
import { checkDrift } from "./checks/drift.mjs";
import { checkRedirectShadowing } from "./checks/redirect-shadowing.mjs";
import { loadContentFiles } from "./content.mjs";

// Content-parity gate for the docs site (run: pnpm check:parity).
// Blocking: CodeTabs SDK coverage on shared pages, slug collisions, redirect
// shadowing, API reference coverage. Informational: per-SDK drift report (the
// migration queue).

const files = loadContentFiles();
const collisions = checkCollisions(files);
const results = [
  checkCodeTabs(files),
  collisions,
  checkRedirectShadowing(collisions.slugs),
  checkApiCoverage(files),
  checkDrift(files),
];

let failed = false;
for (const { name, errors, report } of results) {
  const status = errors.length > 0 ? "FAIL" : "ok";
  console.log(`\n== ${name}: ${status}`);
  for (const line of report) {
    console.log(line);
  }
  for (const error of errors) {
    console.error(`ERROR ${error}`);
    failed = true;
  }
}

process.exit(failed ? 1 : 0);
