import { resolve } from "node:path";
import { pathToFileURL } from "node:url";
import type { Queue } from "../queue";

/**
 * Import the user's app module and return its configured queue.
 *
 * Shared by `run` and `executor`: both need the same registry of tasks, and
 * only differ in where the jobs come from.
 */
export async function loadApp(appPath: string): Promise<Queue> {
  const module = (await import(pathToFileURL(resolve(appPath)).href)) as Record<string, unknown>;
  const candidate = module.default ?? module.queue;
  if (!candidate || typeof (candidate as Queue).runWorker !== "function") {
    throw new Error(`module "${appPath}" must export a Queue (default export or \`queue\`)`);
  }
  return candidate as Queue;
}
