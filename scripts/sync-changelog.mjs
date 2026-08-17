#!/usr/bin/env node
// Generate the docs changelog page + version constant from the canonical root
// CHANGELOG.md.
//
// CHANGELOG.md (Keep a Changelog) is the single source of truth — edit it there.
// This script wraps its body in the frontmatter the docs site needs (writing
// docs/content/docs/resources/changelog.mdx) and emits the latest version to
// docs/app/lib/version.ts. Run automatically before `docs` dev/build.
import { readFileSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

if (process.argv.includes("--help") || process.argv.includes("-h")) {
  console.log(`Regenerate the docs changelog page and version constant from CHANGELOG.md.

usage: node scripts/sync-changelog.mjs

Takes no options. Writes docs/content/docs/resources/changelog.mdx and
docs/app/lib/version.ts; both are generated — edit CHANGELOG.md instead.
Runs automatically before \`docs\` dev/build.`);
  process.exit(0);
}

const repoRoot = new URL("../", import.meta.url);
const source = fileURLToPath(new URL("CHANGELOG.md", repoRoot));
const target = fileURLToPath(
  new URL("docs/content/docs/resources/changelog.mdx", repoRoot),
);
const versionTarget = fileURLToPath(
  new URL("docs/app/lib/version.ts", repoRoot),
);

const changelog = readFileSync(source, "utf8");
// Drop the top-level "# Changelog" heading — the frontmatter title renders it.
const body = changelog.replace(/^#\s+Changelog\s*\n/, "").trimStart();

// The first `## x.y.z` heading is the latest release — the docs footer reads it.
const latest = body.match(/^##\s+(\d+\.\d+\.\d+[\w.-]*)/m)?.[1];
if (!latest) {
  throw new Error(
    "sync-changelog: no `## x.y.z` release heading found in CHANGELOG.md",
  );
}
writeFileSync(
  versionTarget,
  `// AUTO-GENERATED from /CHANGELOG.md by scripts/sync-changelog.mjs — do not edit directly.\nexport const VERSION = "${latest}";\n`,
);
console.log(`synced ${versionTarget} (v${latest})`);

const frontmatter = [
  "---",
  "title: Changelog",
  'description: "Release history for flexiq — every notable change, fix, and feature."',
  "---",
  "",
  "{/* AUTO-GENERATED from /CHANGELOG.md by scripts/sync-changelog.mjs — do not edit directly. */}",
  "",
  '<Callout type="info">Code snippets below use the Python API for illustration. Every release ships all three SDKs — Python, Node, and Java — in lock-step; see each SDK\'s API reference for its exact syntax.</Callout>',
  "",
].join("\n");

writeFileSync(target, `${frontmatter}\n${body}\n`);
console.log(`synced ${target} from CHANGELOG.md`);
