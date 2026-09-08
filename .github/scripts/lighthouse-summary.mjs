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

// A score alone cannot be acted on, and a runner does not always agree with a
// laptop. Name the audits that cost points, and the elements they fired on, so
// a failure is diagnosable from the log without downloading the report.
const shortfalls = [];
for (const run of runs) {
  const lhr = JSON.parse(readFileSync(run.jsonPath, "utf8"));
  for (const cat of CATEGORIES) {
    const category = lhr.categories[cat];
    if (!category || category.score === null || category.score === 1) continue;
    for (const ref of category.auditRefs) {
      const audit = lhr.audits[ref.id];
      if (!audit || audit.score === null || audit.score === 1) continue;
      if (audit.scoreDisplayMode === "notApplicable" || audit.scoreDisplayMode === "informative") continue;
      if (ref.weight === 0) continue;
      // Timing metrics rarely land on exactly 1.0; only surface a real shortfall.
      if (audit.score >= 0.9) continue;
      shortfalls.push({
        url: new URL(run.url).pathname,
        cat,
        id: ref.id,
        weight: ref.weight,
        title: audit.title,
        nodes: (audit.details?.items ?? [])
          .map((i) => i.node?.selector)
          .filter(Boolean)
          .slice(0, 5),
      });
    }
  }
}

if (shortfalls.length) {
  lines.push("<details><summary>Audits below full marks</summary>", "");
  for (const s of shortfalls) {
    lines.push(`- \`${s.url}\` **${s.cat}** · \`${s.id}\` (weight ${s.weight}) — ${s.title}`);
    for (const n of s.nodes) lines.push(`  - \`${n}\``);
  }
  lines.push("", "</details>", "");
}

const out = lines.join("\n");
console.log(out);

if (process.env.GITHUB_STEP_SUMMARY) {
  appendFileSync(process.env.GITHUB_STEP_SUMMARY, `${out}\n`);
}
