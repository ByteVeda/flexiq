// Per-task middleware disable list. Persisted under
// `middleware:disabled:<task_name>` as a JSON array of middleware names and
// read at every invocation, so toggles take effect on the next job without
// a worker restart.

import type { Middleware } from "../../middleware";
import { updateSetting } from "../../settingsKv";
import { createLogger } from "../../utils";
import { ValidationError } from "../errors";
import type { SettingsAccess } from "./overrides";

const DISABLE_PREFIX = "middleware:disabled:";

const log = createLogger("dashboard");

/** The stable name a middleware is keyed on in the disable list. */
export function middlewareKey(middleware: Middleware, index: number): string {
  const named = (middleware as { name?: unknown }).name;
  if (typeof named === "string" && named) {
    return named;
  }
  const className = middleware.constructor?.name;
  if (className && className !== "Object") {
    return className;
  }
  return `middleware:${index}`;
}

function parse(raw: string | null): string[] {
  if (!raw) {
    return [];
  }
  try {
    const data = JSON.parse(raw);
    return Array.isArray(data) ? data.filter((x): x is string => typeof x === "string") : [];
  } catch {
    log.warn(() => "middleware disable list is not valid JSON; treating as empty");
    return [];
  }
}

/** List/set/clear per-task middleware disables. */
export class MiddlewareDisableStore {
  constructor(private readonly settings: SettingsAccess) {}

  private key(taskName: string): string {
    return DISABLE_PREFIX + taskName;
  }

  /** `{task_name: [disabled...]}` for every task with at least one disable. */
  listAll(): Record<string, string[]> {
    const out: Record<string, string[]> = {};
    for (const [key, raw] of Object.entries(this.settings.listSettings())) {
      if (!key.startsWith(DISABLE_PREFIX)) {
        continue;
      }
      const names = parse(raw);
      if (names.length > 0) {
        out[key.slice(DISABLE_PREFIX.length)] = names;
      }
    }
    return out;
  }

  getFor(taskName: string): string[] {
    return parse(this.settings.getSetting(this.key(taskName)));
  }

  /**
   * Flip a middleware on/off for a task; returns the new disable list.
   *
   * An emptied list leaves a `[]` row rather than deleting it. Deleting sat
   * outside the compare-and-set, so a concurrent writer's entry could be added
   * between the swap and the delete and then removed by it — the very lost
   * update the compare-and-set exists to prevent. Nothing reads the difference:
   * {@link getFor} parses `[]` as "nothing disabled", {@link listAll} filters
   * empty lists out, and the key sits under a reserved prefix, so the generic
   * settings view does not show it either.
   */
  setDisabled(taskName: string, middlewareName: string, disabled: boolean): string[] {
    if (!taskName) {
      throw new ValidationError("task_name must not be empty");
    }
    if (!middlewareName) {
      throw new ValidationError("middleware name must not be empty");
    }
    return updateSetting(this.settings, this.key(taskName), parse, (names) => {
      const already = names.includes(middlewareName);
      if (disabled) {
        if (!already) {
          names.push(middlewareName);
        }
      } else {
        names.splice(0, names.length, ...names.filter((n) => n !== middlewareName));
      }
      return [...names];
    });
  }

  clearFor(taskName: string): boolean {
    return this.settings.deleteSetting(this.key(taskName));
  }
}
