// Parse every Mermaid chart in the docs and fail on the first one that does not.
//
// Charts render in the browser, inside `<Mermaid>`'s effect, so a syntax error
// never reaches the build: `pnpm build` prerenders the page around an empty
// diagram and exits 0. The page then ships with a hole in it. This is the only
// thing standing between a typo and that.
//
// Mermaid needs a DOM to initialise even for `parse`, hence jsdom.

import { readFileSync } from "node:fs";
import { readdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { JSDOM } from "jsdom";

const CONTENT = fileURLToPath(new URL("../content", import.meta.url));

/** `<Mermaid chart={`...`}/>`, the only form the docs use. */
const CHART = /<Mermaid\s+chart=\{`([\s\S]*?)`\}\s*\/>/g;

async function mdxFiles(dir) {
  const found = [];
  for (const entry of await readdir(dir, { withFileTypes: true })) {
    const full = path.join(dir, entry.name);
    if (entry.isDirectory()) {
      found.push(...(await mdxFiles(full)));
    } else if (entry.name.endsWith(".mdx")) {
      found.push(full);
    }
  }
  return found;
}

/** Install the globals mermaid reaches for at import time. */
function installDom() {
  const dom = new JSDOM("<!doctype html><html><body></body></html>", {
    pretendToBeVisual: true,
  });
  const names = [
    "window",
    "document",
    "Element",
    "SVGElement",
    "HTMLElement",
    "DOMParser",
    "Node",
    "getComputedStyle",
    "requestAnimationFrame",
    "MutationObserver",
  ];
  for (const name of names) {
    const value = name === "window" ? dom.window : dom.window[name];
    // `navigator` and friends are getter-only on newer Node, so assignment
    // throws where defineProperty does not.
    try {
      globalThis[name] = value;
    } catch {
      Object.defineProperty(globalThis, name, { value, configurable: true });
    }
  }
  dom.window.matchMedia ??= () => ({
    matches: false,
    addEventListener() {},
    removeEventListener() {},
  });
}

installDom();
const mermaid = (await import("mermaid")).default;
mermaid.initialize({
  startOnLoad: false,
  theme: "base",
  securityLevel: "loose",
  flowchart: { htmlLabels: true },
});

const failures = [];
let charts = 0;

for (const file of (await mdxFiles(CONTENT)).sort()) {
  const source = readFileSync(file, "utf8");
  for (const [index, match] of [...source.matchAll(CHART)].entries()) {
    charts += 1;
    try {
      await mermaid.parse(match[1]);
    } catch (error) {
      const reason = String(error?.message ?? error).split("\n")[0];
      failures.push(
        `${path.relative(CONTENT, file)} [chart ${index}]: ${reason}`,
      );
    }
  }
}

console.log(`\n== Mermaid charts: ${charts} parsed`);
if (failures.length > 0) {
  console.error(`\n${failures.length} chart(s) failed to parse:`);
  for (const failure of failures) console.error(`  - ${failure}`);
  process.exit(1);
}
console.log("== All charts parse: ok\n");
