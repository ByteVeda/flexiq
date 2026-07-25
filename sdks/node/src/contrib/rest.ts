// Framework-neutral REST route table shared by the Express and Fastify helpers.
// Each route maps a plain request (params/query/body) to a status + JSON body, so the
// adapters only translate framework request/response objects — no logic duplicated.

import { checkHealth, checkReadiness, resourceStatus } from "../health";
import type { Queue } from "../queue";
import type { EnqueueOptions } from "../types";

/** Options controlling which routes a helper exposes. */
export interface RestOptions {
  /** Expose only these route names (mutually exclusive with `excludeRoutes`). */
  includeRoutes?: string[];
  /** Expose all routes except these. */
  excludeRoutes?: string[];
  /** Max ms `GET /jobs/:id/result` blocks for a terminal state (default 5000). */
  resultTimeoutMs?: number;
}

/** A framework-neutral request. */
export interface RestRequest {
  params: Record<string, string | undefined>;
  query: Record<string, string | undefined>;
  body: unknown;
}

// Keys that would pollute Object.prototype if carried over from user-controlled query.
const FORBIDDEN_KEYS = new Set(["__proto__", "constructor", "prototype"]);

/** Pick a single string from a parsed query value (`string` or `string[]`). */
function firstString(value: unknown): string | undefined {
  if (typeof value === "string") {
    return value;
  }
  if (Array.isArray(value) && typeof value[0] === "string") {
    return value[0];
  }
  return undefined;
}

/**
 * Flatten a framework's parsed query (`string | string[]`) into plain
 * `string | undefined` values. Built via `Object.fromEntries` over filtered
 * entries — never a dynamic `obj[userKey] =` write — and prototype-polluting
 * keys are dropped, so user-controlled query names can't inject properties.
 */
export function flattenQueryParams(query: unknown): Record<string, string | undefined> {
  const entries = Object.entries((query as Record<string, unknown>) ?? {})
    .filter(([key]) => !FORBIDDEN_KEYS.has(key))
    .map(([key, value]) => [key, firstString(value)] as const)
    .filter((entry): entry is readonly [string, string] => entry[1] !== undefined);
  return Object.fromEntries(entries);
}

/** A framework-neutral response: HTTP status + a JSON-serializable body. */
export interface RestResponse {
  status: number;
  body: unknown;
}

/** One REST route. `path` uses `:param` syntax understood by both Express and Fastify. */
export interface RestRoute {
  name: string;
  method: "GET" | "POST";
  path: string;
  handle: (queue: Queue, req: RestRequest) => RestResponse | Promise<RestResponse>;
}

function toInt(value: string | undefined): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  const parsed = Number.parseInt(value, 10);
  return Number.isFinite(parsed) ? parsed : undefined;
}

/** Drop the raw result Buffer from a job view — use the result route for the value. */
function jobView(job: NonNullable<ReturnType<Queue["getJob"]>>): Record<string, unknown> {
  const { result: _result, ...rest } = job;
  return rest;
}

function defineRoutes(resultTimeoutMs: number): RestRoute[] {
  return [
    {
      name: "enqueue",
      method: "POST",
      path: "/enqueue",
      handle: (queue, { body }) => {
        const input = body as { task?: unknown; args?: unknown; options?: unknown } | undefined;
        if (!input || typeof input.task !== "string") {
          return { status: 400, body: { error: "body must include a 'task' string" } };
        }
        const args = Array.isArray(input.args) ? input.args : [];
        const options = (input.options as EnqueueOptions | undefined) ?? undefined;
        const jobId = queue.enqueue(input.task, args, options);
        return { status: 201, body: { jobId } };
      },
    },
    {
      name: "stats",
      method: "GET",
      path: "/stats",
      handle: async (queue) => ({ status: 200, body: await queue.stats() }),
    },
    {
      name: "queue-stats",
      method: "GET",
      path: "/stats/queues",
      handle: async (queue, { query }) => {
        const body = query.queue
          ? await queue.statsByQueue(query.queue)
          : await queue.statsAllQueues();
        return { status: 200, body };
      },
    },
    {
      name: "job",
      method: "GET",
      path: "/jobs/:id",
      handle: (queue, { params }) => {
        const job = queue.getJob(params.id ?? "");
        return job
          ? { status: 200, body: jobView(job) }
          : { status: 404, body: { error: "not found" } };
      },
    },
    {
      name: "job-errors",
      method: "GET",
      path: "/jobs/:id/errors",
      handle: async (queue, { params }) => ({
        status: 200,
        body: await queue.getJobErrors(params.id ?? ""),
      }),
    },
    {
      name: "job-result",
      method: "GET",
      path: "/jobs/:id/result",
      handle: async (queue, { params, query }) => {
        const id = params.id ?? "";
        // Clamp client-supplied timeouts so a huge value can't pin the
        // connection (and its polling loop) open indefinitely.
        const timeoutMs = Math.min(
          Math.max(toInt(query.timeoutMs) ?? resultTimeoutMs, 0),
          resultTimeoutMs,
        );
        try {
          const result = await queue.result(id, { timeoutMs });
          return { status: 200, body: { jobId: id, status: "completed", result } };
        } catch (err) {
          const status = queue.getJob(id)?.status ?? "pending";
          const error = err instanceof Error ? err.message : String(err);
          return { status: 200, body: { jobId: id, status, error } };
        }
      },
    },
    {
      name: "cancel",
      method: "POST",
      path: "/jobs/:id/cancel",
      handle: (queue, { params }) => ({
        status: 200,
        body: { cancelled: queue.requestCancel(params.id ?? "") },
      }),
    },
    {
      name: "dead-letters",
      method: "GET",
      path: "/dead-letters",
      handle: async (queue, { query }) => ({
        status: 200,
        body: await queue.deadLetters(toInt(query.limit), toInt(query.offset)),
      }),
    },
    {
      name: "retry-dead",
      method: "POST",
      path: "/dead-letters/:id/retry",
      handle: (queue, { params }) => ({
        status: 200,
        body: { jobId: queue.retryDead(params.id ?? "") },
      }),
    },
    {
      name: "health",
      method: "GET",
      path: "/health",
      handle: () => ({ status: 200, body: checkHealth() }),
    },
    {
      name: "readiness",
      method: "GET",
      path: "/readiness",
      // 503 when degraded, so orchestrators stop routing to this instance.
      handle: async (queue) => {
        const body = await checkReadiness(queue);
        return { status: body.status === "ready" ? 200 : 503, body };
      },
    },
    {
      name: "resources",
      method: "GET",
      path: "/resources",
      handle: async (queue) => ({ status: 200, body: await resourceStatus(queue) }),
    },
  ];
}

/** Build the route set for a helper, applying `includeRoutes`/`excludeRoutes`. */
export function buildRestRoutes(options: RestOptions = {}): RestRoute[] {
  if (options.includeRoutes && options.excludeRoutes) {
    throw new Error("includeRoutes and excludeRoutes are mutually exclusive");
  }
  const routes = defineRoutes(options.resultTimeoutMs ?? 5000);
  if (options.includeRoutes) {
    const allow = new Set(options.includeRoutes);
    return routes.filter((route) => allow.has(route.name));
  }
  if (options.excludeRoutes) {
    const deny = new Set(options.excludeRoutes);
    return routes.filter((route) => !deny.has(route.name));
  }
  return routes;
}
