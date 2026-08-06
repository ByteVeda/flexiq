// Standalone liveness/readiness probes over a Queue handle. Usable on their own —
// wire them into any HTTP framework, a CLI check, or a container probe; the
// dashboard and the REST helpers are just callers.

import type { Queue } from "./queue";

/** Liveness payload: the process answered, nothing else is asserted. */
export interface HealthReport {
  status: "ok";
}

/** Worker check: how many workers heartbeated recently. */
export interface WorkersCheck {
  count: number;
  status: "ok" | "none";
}

/** Worker-resource check: names of resources not reporting `healthy`. */
export interface ResourcesCheck {
  count: number;
  unhealthy: string[];
  status: "ok" | "degraded";
}

/**
 * Per-dependency results. A check that threw is reported as the string
 * `"error: <message>"` instead of its object, and forces `degraded`.
 * `resources` is absent when no worker advertises any resource.
 */
export interface ReadinessChecks {
  storage: string;
  workers: WorkersCheck | string;
  resources?: ResourcesCheck | string;
}

/** Readiness payload: `ready` only when every check passed. */
export interface ReadinessReport {
  status: "ready" | "degraded";
  checks: ReadinessChecks;
}

/** Basic liveness check — always ok. */
export function checkHealth(): HealthReport {
  return { status: "ok" };
}

/**
 * Readiness: storage reachable, workers alive, resources healthy. Never
 * throws — a failing dependency lands in `checks` and degrades the status,
 * so a probe endpoint can always answer.
 */
export async function checkReadiness(queue: Queue): Promise<ReadinessReport> {
  const checks: ReadinessChecks = { storage: "ok", workers: { count: 0, status: "none" } };
  let allOk = true;

  try {
    await queue.stats();
  } catch (error) {
    checks.storage = `error: ${String(error)}`;
    allOk = false;
  }

  try {
    const workers = await queue.listWorkers();
    checks.workers = { count: workers.length, status: workers.length > 0 ? "ok" : "none" };
  } catch (error) {
    checks.workers = `error: ${String(error)}`;
    allOk = false;
  }

  try {
    const resources = await resourceStatus(queue);
    if (resources.length > 0) {
      const unhealthy = resources.filter((r) => r.health !== "healthy").map((r) => r.name);
      checks.resources = {
        count: resources.length,
        unhealthy,
        status: unhealthy.length > 0 ? "degraded" : "ok",
      };
      if (unhealthy.length > 0) {
        allOk = false;
      }
    }
  } catch (error) {
    checks.resources = `error: ${String(error)}`;
    allOk = false;
  }

  return { status: allOk ? "ready" : "degraded", checks };
}

/** One worker resource as seen across every live worker's heartbeat. */
export interface ResourceStatusEntry {
  name: string;
  scope: string;
  health: string;
  init_duration_ms: number;
  recreations: number;
  depends_on: string[];
}

/**
 * Worker-resource health aggregated from heartbeat snapshots: any `unhealthy`
 * report wins; all `healthy` → `healthy`; anything else (a `degraded` report,
 * or an unrecognised one) → `degraded`; advertised but not yet reported →
 * `not_initialized`.
 */
export async function resourceStatus(queue: Queue): Promise<ResourceStatusEntry[]> {
  const observed = new Map<string, string[]>();
  const advertised = new Set<string>();
  for (const worker of await queue.listWorkers()) {
    for (const name of parseJsonArray(worker.resources)) {
      advertised.add(name);
    }
    const report = parseJsonObject(worker.resourceHealth);
    for (const [name, healthValue] of Object.entries(report)) {
      const entries = observed.get(name) ?? [];
      entries.push(String(healthValue).toLowerCase());
      observed.set(name, entries);
    }
  }

  const names = new Set([...advertised, ...observed.keys()]);
  return [...names].sort().map((name) => {
    const reports = observed.get(name) ?? [];
    // The worst report wins, so one sick worker is never averaged away and a
    // fleet that is merely degraded is never escalated to unhealthy.
    let healthState = "not_initialized";
    if (reports.some((r) => r === "unhealthy")) {
      healthState = "unhealthy";
    } else if (reports.length > 0) {
      healthState = reports.every((r) => r === "healthy") ? "healthy" : "degraded";
    }
    return {
      name,
      scope: "worker",
      health: healthState,
      init_duration_ms: 0,
      recreations: 0,
      depends_on: [],
    };
  });
}

function parseJsonArray(raw: string | undefined | null): string[] {
  if (!raw) {
    return [];
  }
  try {
    const parsed = JSON.parse(raw);
    return Array.isArray(parsed) ? parsed.map(String) : [];
  } catch {
    return [];
  }
}

function parseJsonObject(raw: string | undefined | null): Record<string, unknown> {
  if (!raw) {
    return {};
  }
  try {
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" && !Array.isArray(parsed) ? parsed : {};
  } catch {
    return {};
  }
}
