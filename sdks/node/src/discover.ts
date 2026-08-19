// The directory walk behind `Queue.discover`. Importing the modules is the
// load-bearing half — `task()` registers on import, so walking the tree is what
// makes the declarations happen. The walk alone would find nothing.

import type { Dirent } from "node:fs";
import { readdir } from "node:fs/promises";
import { extname, join } from "node:path";
import { pathToFileURL } from "node:url";
import { TaskDiscoveryError } from "./errors";
import type { DiscoverOptions } from "./types";

/** The extensions {@link Queue.discover} imports unless told otherwise. */
export const DISCOVER_EXTENSIONS: readonly string[] = [
  ".js",
  ".mjs",
  ".cjs",
  ".ts",
  ".mts",
  ".cts",
];

/** `.d.ts`, `.d.mts`, `.d.cts` — declarations, not modules. */
const DECLARATION_FILE = /\.d\.[cm]?ts$/;

/** A thrown value's message, for an error string that names the real failure. */
function reason(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/**
 * Import every task module under `dir`, depth first in name order.
 *
 * Skips `node_modules`, dot-directories, and symlinks — a symlinked directory is
 * the cheap way to walk in a circle, and one file reached by two paths registers
 * its tasks twice.
 *
 * @throws {TaskDiscoveryError} A directory could not be read, or a module threw
 * on import. Both are fatal: a module that quietly failed to import
 * dead-letters every job belonging to the tasks it declares.
 */
export function importTaskModules(dir: string, options?: DiscoverOptions): Promise<void> {
  return walk(dir, new Set(options?.extensions ?? DISCOVER_EXTENSIONS));
}

async function walk(dir: string, extensions: ReadonlySet<string>): Promise<void> {
  let entries: Dirent[];
  try {
    entries = await readdir(dir, { withFileTypes: true });
  } catch (error) {
    throw new TaskDiscoveryError(
      dir,
      `cannot read task directory "${dir}": ${reason(error)}`,
      error,
    );
  }
  // Sorted with a plain comparison rather than `localeCompare`: registration
  // order has to be the same on every machine, and collation is not.
  entries.sort((left, right) => (left.name < right.name ? -1 : left.name > right.name ? 1 : 0));

  for (const entry of entries) {
    if (entry.name === "node_modules" || entry.name.startsWith(".")) {
      continue;
    }
    const full = join(dir, entry.name);
    if (entry.isDirectory()) {
      await walk(full, extensions);
      continue;
    }
    // False for symlinks, sockets, and devices as well as for directories.
    if (!entry.isFile()) {
      continue;
    }
    if (DECLARATION_FILE.test(entry.name) || !extensions.has(extname(entry.name))) {
      continue;
    }
    try {
      // Sequential rather than `Promise.all`: the first failure names its own
      // file instead of racing the others into the rejection.
      await import(pathToFileURL(full).href);
    } catch (error) {
      throw new TaskDiscoveryError(
        full,
        `task module "${full}" failed to import: ${reason(error)}`,
        error,
      );
    }
  }
}
