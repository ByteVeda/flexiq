import { readdirSync } from "node:fs";
import { join, relative, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { mountsForRelPath } from "./doc-slugs";

// Filesystem walk of the MDX content tree, used by react-router.config's
// `prerender()` to enumerate every static doc URL at build time. The runtime
// router (app/lib/content.ts) maps the same files through the same doc-slugs
// module, so the prerendered set matches the served routes — including shared
// files that fan out to one URL per SDK.

const CONTENT_DIR = fileURLToPath(
  new URL("../../content/docs", import.meta.url),
);

function toRelPath(absFile: string): string {
  return relative(CONTENT_DIR, absFile).split(sep).join("/");
}

function walk(dir: string, out: string[]): void {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      walk(full, out);
    } else if (entry.name.endsWith(".mdx")) {
      out.push(full);
    }
  }
}

/** Every doc URL (e.g. `/architecture/scheduler`, `/node/modules/workflows/saga`). */
export function allDocPaths(): string[] {
  const files: string[] = [];
  walk(CONTENT_DIR, files);
  return files
    .flatMap((file) => mountsForRelPath(toRelPath(file)))
    .map((mount) => mount.slug)
    .filter((slug) => slug !== "/");
}
