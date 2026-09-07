#!/usr/bin/env node
// One version for the whole repo: `[workspace.package].version` in the root
// Cargo.toml. Everything else either derives it natively — the seven crates via
// `version.workspace`, the Python wheel via maturin, the Gradle subprojects via
// gradle.properties — or is mirrored here.
//
//   node scripts/version.mjs --check       verify nothing has drifted (CI gate)
//   node scripts/version.mjs --current     print the version the repo declares
//   node scripts/version.mjs --set 0.21.0  bump the source and every mirror
import { readFileSync, readdirSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const repoRoot = new URL("../", import.meta.url);
const abs = (relative) => fileURLToPath(new URL(relative, repoRoot));
const read = (relative) => readFileSync(abs(relative), "utf8");

const SEMVER = /^\d+\.\d+\.\d+[\w.-]*$/;

// The canonical declaration. Every mirror below is rewritten to match it.
const SOURCE = {
  file: "Cargo.toml",
  pattern: /^version = "(.+?)"$/m,
  label: "workspace.package",
};

// Manifests with no way to reference the Cargo version natively.
const MIRRORS = [
  // Published crates depend on each other by `path` + `version`. The path is
  // what builds here; the version is what cargo writes into the packaged
  // manifest, so it has to name a release that exists on crates.io. There is no
  // `version.workspace` for a dependency, hence the literals.
  {
    file: "Cargo.toml",
    pattern: /^(flexiq-core = \{ path = "crates\/flexiq-core", version = ")(.+?)(" \})$/m,
    label: "flexiq-core registry coordinate",
  },
  {
    file: "Cargo.toml",
    pattern:
      /^(flexiq-workflows = \{ path = "crates\/flexiq-workflows", version = ")(.+?)(" \})$/m,
    label: "flexiq-workflows registry coordinate",
  },
  {
    file: "Cargo.toml",
    pattern: /^(flexiq-mesh = \{ path = "crates\/flexiq-mesh", version = ")(.+?)(" \})$/m,
    label: "flexiq-mesh registry coordinate",
  },
  {
    file: "sdks/node/package.json",
    pattern: /^(  "version": ")(.+?)(",)$/m,
    label: "npm package",
  },
  {
    file: "sdks/java/gradle.properties",
    pattern: /^(version=)(.+)()$/m,
    label: "Gradle projects",
  },
  {
    file: "sdks/python/flexiq/__init__.py",
    pattern: /^(    __version__ = ")(.+?)(")$/m,
    label: "Python source-tree fallback",
  },
  {
    file: "deploy/helm/flexiq-server/Chart.yaml",
    pattern: /^(version: )(.+)()$/m,
    label: "Helm chart",
  },
  {
    file: "deploy/helm/flexiq-server/Chart.yaml",
    // The chart only ever ships the image built from the same commit, so
    // appVersion tracks the workspace rather than lagging it.
    pattern: /^(appVersion: ")(.+?)(")$/m,
    label: "Helm chart appVersion",
  },
];

// Install snippets a reader copies verbatim. The version repeats within a file,
// so every occurrence is checked and rewritten — and each pattern is anchored on
// a flexiq coordinate, never a bare `<version>`, so a neighbouring plugin or
// Micrometer pin in the same snippet is left alone.
const SNIPPET_PATTERNS = [
  // Gradle: implementation("org.byteveda:flexiq-test:0.21.0") — the optional
  // trailing `:classifier` is outside the capture and survives untouched.
  /(org\.byteveda:flexiq[\w-]*:)(\d+\.\d+\.\d+[\w.-]*)()/g,
  // Maven: <artifactId>flexiq</artifactId> followed by its <version> tag.
  /(<artifactId>flexiq[\w-]*<\/artifactId>\s*<version>)(\d+\.\d+\.\d+[\w.-]*)(<\/version>)/g,
  // npm: "@byteveda/flexiq": "0.21.0" — an exact pin only. A range starts with
  // a non-digit, so a deliberate `^`/`~` is left alone instead of being frozen.
  /("@byteveda\/flexiq[\w-]*": ")(\d+\.\d+\.\d+[\w.-]*)(")/g,
  // pip: flexiq==0.21.0
  /(\bflexiq[\w-]*==)(\d+\.\d+\.\d+[\w.-]*)()/g,
  // GHCR: ghcr.io/byteveda/flexiq-server:0.21.0
  /(ghcr\.io\/byteveda\/flexiq-server:)(\d+\.\d+\.\d+[\w.-]*)()/g,
];

const SNIPPETS = [
  "sdks/java/README.md",
  "docs/content/docs/java/getting-started/installation.mdx",
  "docs/content/docs/java/api-reference/testing.mdx",
  "docs/content/docs/java/guides/extend/index.mdx",
  "docs/content/docs/java/guides/extend/spring.mdx",
  "docs/content/docs/java/modules/injection/testing.mdx",
  "docs/content/docs/shared/guides/extend/testing.mdx",
  // The polyglot example pins every SDK so a reader reproduces one known-good
  // combination. Listing the manifests here is what keeps those pins from
  // silently aging past the release they claim to demonstrate.
  "examples/polyglot/README.md",
  "examples/polyglot/node-worker/package.json",
  "examples/polyglot/java-worker/build.gradle.kts",
  "docker/README.md",
  "docs/content/docs/shared/operate/deployment.mdx",
];

// Checked, never written: release notes are authored by hand, but shipping a
// version with no section of its own is a mistake worth failing CI over.
const CHANGELOG = {
  file: "CHANGELOG.md",
  pattern: /^## (\d+\.\d+\.\d+[\w.-]*)$/m,
  label: "latest CHANGELOG section",
};

// Files that must keep deriving the version instead of restating it — a
// hardcoded literal here would silently win over the source.
function guards() {
  const crates = readdirSync(abs("crates")).map((crate) => ({
    file: `crates/${crate}/Cargo.toml`,
    pattern: /^version = "/m,
    hint: "use `version.workspace = true`",
  }));
  return [
    ...crates,
    {
      file: "sdks/node/src/cli/index.ts",
      pattern: /\.version\("/m,
      hint: "read it from package.json — see the manifest import at the top",
    },
    {
      file: "sdks/python/pyproject.toml",
      pattern: /^version = "/m,
      hint: 'use `dynamic = ["version"]` so maturin reads Cargo.toml',
    },
    {
      file: "docker/scheduler.Dockerfile",
      pattern: /^ARG VERSION=\d/m,
      hint: "keep the `dev` default — releases pass `--build-arg VERSION=$(node scripts/version.mjs --current)`",
    },
    ...["", "spring/", "processor/", "test-support/", "graalvm-smoke/"].map(
      (project) => ({
        file: `sdks/java/${project}build.gradle.kts`,
        pattern: /^version = "/m,
        hint: "set `version` in sdks/java/gradle.properties",
      }),
    ),
  ];
}

// Reads the single capture the pattern is expected to find, or explains where
// the file stopped matching — a silent miss would let drift through the gate.
function extract({ file, pattern, label }, group = 1) {
  const found = read(file).match(pattern);
  if (!found) {
    throw new Error(`${file}: no ${label} version found (pattern drifted?)`);
  }
  return found[group];
}

// Every coordinate in a snippet file, so a page with one stale literal among
// several fresh ones still fails instead of passing on the first match.
function snippetVersions(file) {
  const contents = read(file);
  const versions = SNIPPET_PATTERNS.flatMap((pattern) =>
    [...contents.matchAll(pattern)].map((match) => match[2]),
  );
  if (versions.length === 0) {
    throw new Error(`${file}: no flexiq coordinate found (pattern drifted?)`);
  }
  return versions;
}

function rewriteSnippet(contents, next) {
  return SNIPPET_PATTERNS.reduce(
    (text, pattern) =>
      text.replace(pattern, (_, before, __, after) => `${before}${next}${after}`),
    contents,
  );
}

function sourceVersion() {
  const version = extract(SOURCE);
  if (!SEMVER.test(version)) {
    throw new Error(`${SOURCE.file}: "${version}" is not a semantic version`);
  }
  return version;
}

function check() {
  const expected = sourceVersion();
  const problems = [];

  for (const mirror of [...MIRRORS, CHANGELOG]) {
    const actual = extract(mirror, mirror === CHANGELOG ? 1 : 2);
    const status = actual === expected ? "ok" : `MISMATCH (${actual})`;
    console.log(`  ${actual === expected ? "✓" : "✗"} ${mirror.file} — ${status}`);
    if (actual !== expected) {
      problems.push(`${mirror.file} declares ${actual}, expected ${expected}`);
    }
  }

  for (const file of SNIPPETS) {
    const stale = [...new Set(snippetVersions(file))].filter((v) => v !== expected);
    const status = stale.length === 0 ? "ok" : `MISMATCH (${stale.join(", ")})`;
    console.log(`  ${stale.length === 0 ? "✓" : "✗"} ${file} — ${status}`);
    if (stale.length > 0) {
      problems.push(`${file} pins ${stale.join(", ")} in install snippets, expected ${expected}`);
    }
  }

  for (const { file, pattern, hint } of guards()) {
    if (pattern.test(read(file))) {
      problems.push(`${file} hardcodes a version — ${hint}`);
    }
  }

  if (problems.length > 0) {
    console.error(`\nVersion drift (source of truth: ${expected}):`);
    for (const problem of problems) console.error(`  - ${problem}`);
    console.error("\nRun `node scripts/version.mjs --set <version>` to resync.");
    process.exit(1);
  }
  console.log(`\nAll manifests agree on ${expected}.`);
}

function set(next) {
  if (!SEMVER.test(next)) {
    throw new Error(`"${next}" is not a semantic version`);
  }
  const previous = sourceVersion();

  writeFileSync(
    abs(SOURCE.file),
    read(SOURCE.file).replace(SOURCE.pattern, `version = "${next}"`),
  );
  for (const { file, pattern } of MIRRORS) {
    writeFileSync(
      abs(file),
      read(file).replace(pattern, (_, before, __, after) => `${before}${next}${after}`),
    );
  }

  for (const file of SNIPPETS) {
    snippetVersions(file); // fail loudly rather than write a file nothing matched
    writeFileSync(abs(file), rewriteSnippet(read(file), next));
  }

  console.log(`${previous} -> ${next}`);
  console.log(`  ${[SOURCE.file, ...MIRRORS.map((m) => m.file), ...SNIPPETS].join("\n  ")}`);
  console.log(`\nAdd a \`## ${next}\` section to CHANGELOG.md to complete the bump.`);
}

const USAGE = `Keep one version across the repo, sourced from ${SOURCE.file}.

usage: node scripts/version.mjs <command>

  --check           verify every manifest agrees; exits 1 on drift (CI gate)
  --current         print the version the repo declares
  --set <version>   rewrite the source and every mirror
  --help            show this message

Mirrors rewritten by --set:
  ${MIRRORS.map((mirror) => `${mirror.file} (${mirror.label})`).join("\n  ")}

Install snippets rewritten by --set (every flexiq coordinate in the file):
  ${SNIPPETS.join("\n  ")}

${CHANGELOG.file} is checked but never written — add its \`## <version>\` section by hand.`;

const [command, argument] = process.argv.slice(2);
try {
  if (command === "--check") check();
  else if (command === "--current") console.log(sourceVersion());
  else if (command === "--set" && argument) set(argument);
  else if (command === "--help" || command === "-h") console.log(USAGE);
  else {
    console.error(USAGE);
    process.exit(2);
  }
} catch (error) {
  console.error(`version: ${error.message}`);
  process.exit(1);
}
