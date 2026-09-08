#!/usr/bin/env node
// Render an LHCI run as a markdown table on the job summary.
//
// Usage: node .github/scripts/lighthouse-summary.mjs <label> <.lighthouseci dir>
//
// Reads the `manifest.json` that `upload.target: filesystem` writes, and keeps
// only the representative run per URL — the median one LHCI asserted against,
// so the table always agrees with the pass/fail verdict.

import { existsSync, readFileSync } from "node:fs";
import { appendFileSync } from "node:fs";
import { join } from "node:path";

const [label, dir] = process.argv.slice(2);
if (!label || !dir) {
  console.error("usage: lighthouse-summary.mjs <label> <.lighthouseci dir>");
  process.exit(2);
}

const manifestPath = join(dir, "manifest.json");
if (!existsSync(manifestPath)) {
  console.error(`no manifest at ${manifestPath} — did lhci run?`);
  process.exit(1);
}

const CATEGORIES = ["performance", "accessibility", "best-practices", "seo"];

// A category switched off in lighthouserc is absent from the summary.
const cell = (score) => {
  if (typeof score !== "number") return "—";
  const pct = Math.round(score * 100);
  const mark = pct >= 90 ? "🟢" : pct >= 50 ? "🟠" : "🔴";
  return `${mark} ${pct}`;
};

const runs = JSON.parse(readFileSync(manifestPath, "utf8")).filter((r) => r.isRepresentativeRun);

const lines = [
  `### Lighthouse — ${label}`,
  "",
  `| URL | ${CATEGORIES.join(" | ")} |`,
  `| --- | ${CATEGORIES.map(() => "---:").join(" | ")} |`,
];

for (const run of runs) {
  const path = new URL(run.url).pathname;
  lines.push(`| \`${path}\` | ${CATEGORIES.map((c) => cell(run.summary?.[c])).join(" | ")} |`);
}

lines.push("", "_Median of 3 runs, desktop preset. Floors live in `lighthouserc.yml`._", "");

const out = lines.join("\n");
console.log(out);

if (process.env.GITHUB_STEP_SUMMARY) {
  appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${out}\n`);
}
