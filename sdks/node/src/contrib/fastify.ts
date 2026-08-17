// Fastify integration for FlexiQ. Optional — import from `flexiq/contrib/fastify`;
// requires `fastify` as a peer.
//
//   app.register(flexiqFastify, { queue, prefix: "/tasks" });           // JSON API
//   app.register(flexiqDashboardPlugin, { queue, prefix: "/admin" });   // dashboard SPA + /api/*

import { fileURLToPath } from "node:url";
import type { FastifyPluginAsync, FastifyReply, FastifyRequest } from "fastify";
import { createDashboardHandler } from "../dashboard";
import type { Queue } from "../queue";
import { buildRestRoutes, flattenQueryParams, type RestOptions, type RestRequest } from "./rest";

/** Options for {@link flexiqFastify}. */
export interface FlexiQFastifyOptions extends RestOptions {
  /** The queue to operate over. */
  queue: Queue;
}

/** Options for {@link flexiqDashboardPlugin}. */
export interface FlexiQDashboardPluginOptions {
  /** The queue to operate over. */
  queue: Queue;
  /** Path to the built SPA assets (defaults to the package's bundled `static/dashboard`). */
  staticDir?: string;
  /** Legacy shared-token gate. When set, the session-auth login flow is disabled. */
  auth?: { token: string };
  /** Mark session cookies `Secure` (default true). Disable only for plain-HTTP dev. */
  secureCookies?: boolean;
}

/**
 * Fastify plugin exposing the FlexiQ REST API (enqueue, stats, job lookup, cancel,
 * dead-letters). Register with a `prefix` to mount the routes under a base path.
 */
export const flexiqFastify: FastifyPluginAsync<FlexiQFastifyOptions> = async (fastify, options) => {
  const { queue } = options;
  for (const route of buildRestRoutes(options)) {
    fastify.route({
      method: route.method,
      url: route.path,
      handler: async (request: FastifyRequest, reply: FastifyReply) => {
        const request_: RestRequest = {
          params: request.params as Record<string, string | undefined>,
          query: flattenQueryParams(request.query),
          body: request.body,
        };
        const result = await route.handle(queue, request_);
        reply.code(result.status).send(result.body);
      },
    });
  }
};

// Resolved relative to this entry's location in dist/contrib/ → package `static/dashboard`.
const STATIC_REL = ["..", "..", "static", "dashboard"].join("/");

function defaultStaticDir(): string {
  return fileURLToPath(new URL(STATIC_REL, import.meta.url));
}

/**
 * Fastify plugin that serves the FlexiQ dashboard (SPA + `/api/*`). Register with a
 * `prefix`; the plugin strips it from the raw URL so the dashboard handler sees `/api/*`
 * and SPA paths, then hands the raw request/response to the handler via `reply.hijack()`.
 */
export const flexiqDashboardPlugin: FastifyPluginAsync<FlexiQDashboardPluginOptions> = async (
  fastify,
  options,
) => {
  const handler = createDashboardHandler(options.queue, options.staticDir ?? defaultStaticDir(), {
    auth: options.auth,
    secureCookies: options.secureCookies,
  });
  const prefix = fastify.prefix;

  // Leave the request body stream intact so the dashboard handler can read POST bodies.
  fastify.addContentTypeParser("*", (_request, payload, done) => done(null, payload));

  const serve = (request: FastifyRequest, reply: FastifyReply): void => {
    const url = request.raw.url ?? "/";
    request.raw.url = url.startsWith(prefix) ? url.slice(prefix.length) || "/" : url;
    reply.hijack();
    handler(request.raw, reply.raw);
  };

  fastify.all("/", serve);
  fastify.all("/*", serve);
};
