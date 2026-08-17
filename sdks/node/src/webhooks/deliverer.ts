import { createHmac, randomUUID } from "node:crypto";
import { request as httpRequest, type IncomingMessage } from "node:http";
import { request as httpsRequest } from "node:https";
import type { LookupFunction } from "node:net";
import type { EventName, EventPayload } from "../events";
import { createLogger } from "../utils";
import { UnsafeWebhookUrlError } from "./errors";
import type { Delivery, Webhook } from "./types";
import { assertSafeWebhookUrl, createSafeLookup } from "./urlSafety";

const MAX_RECENT = 100;
const MAX_BACKOFF_MS = 30_000;
const MAX_RESPONSE_BODY_CHARS = 2048;

const log = createLogger("webhooks");

/** Signs and POSTs webhook payloads with retries; keeps a recent-delivery log. */
export class Deliverer {
  private readonly recent: Delivery[] = [];

  recentFor(webhookId: string): Delivery[] {
    return this.recent.filter((delivery) => delivery.webhookId === webhookId);
  }

  async deliver(
    webhook: Webhook,
    event: EventName,
    payload: EventPayload | Record<string, unknown>,
  ): Promise<Delivery> {
    const payloadRecord: Record<string, unknown> = { event, ...payload };
    const body = JSON.stringify(payloadRecord);
    const headers: Record<string, string> = {
      "content-type": "application/json",
      ...webhook.headers,
    };
    if (webhook.secret) {
      const signature = createHmac("sha256", webhook.secret).update(body).digest("hex");
      headers["x-flexiq-signature"] = `sha256=${signature}`;
    }

    // Resolution is bounded by the same budget as the request it precedes.
    const safeLookup = createSafeLookup({ timeoutMs: webhook.timeoutMs });
    const createdAt = Date.now();
    let attempts = 0;
    let responseCode: number | null = null;
    let responseBody: string | null = null;
    let error: string | undefined;
    // Set when delivery is refused on policy grounds — retrying cannot help.
    let nonRetryable = false;
    while (attempts <= webhook.maxRetries) {
      try {
        // Re-checked per attempt so a subscription whose URL policy changed
        // mid-chain stops here. Whether the *name* points somewhere private is
        // settled by `safeLookup` at connect time, not guessed at up front.
        assertSafeWebhookUrl(webhook.url);
      } catch (cause) {
        if (!(cause instanceof UnsafeWebhookUrlError)) {
          throw cause;
        }
        responseCode = null;
        responseBody = null;
        error = cause.message;
        nonRetryable = true;
        break;
      }
      attempts += 1;
      try {
        const response = await post(webhook.url, {
          headers,
          body,
          timeoutMs: webhook.timeoutMs,
          lookup: safeLookup,
        });
        responseCode = response.status;
        responseBody = response.body;
        if (response.status >= 200 && response.status < 300) {
          error = undefined;
          break;
        }
        // Redirects are never followed — one could point past the SSRF guard.
        if (response.status >= 300 && response.status < 400) {
          error = `redirect not followed (HTTP ${response.status})`;
          nonRetryable = true;
          break;
        }
        error = `HTTP ${response.status}`;
      } catch (cause) {
        responseCode = null;
        responseBody = null;
        error = cause instanceof Error ? cause.message : String(cause);
        // The address the socket resolved to is blocked; retrying re-resolves
        // to the same place.
        if (cause instanceof UnsafeWebhookUrlError) {
          nonRetryable = true;
          break;
        }
      }
      if (attempts <= webhook.maxRetries) {
        await sleep(backoff(attempts, webhook.retryBackoff));
      }
    }

    const completedAt = Date.now();
    const ok = responseCode !== null && responseCode >= 200 && responseCode < 300;
    const delivery: Delivery = {
      id: randomUUID(),
      webhookId: webhook.id,
      event,
      // The retry chain is inline: a refused delivery is `failed` (retrying
      // cannot help), anything else that never landed is `dead`.
      status: ok ? "delivered" : nonRetryable ? "failed" : "dead",
      ok,
      attempts,
      payload: payloadRecord,
      // Not every event concerns a job — record identities only when present.
      taskName: typeof payloadRecord.taskName === "string" ? payloadRecord.taskName : null,
      jobId: typeof payloadRecord.jobId === "string" ? payloadRecord.jobId : null,
      responseCode,
      responseBody,
      latencyMs: completedAt - createdAt,
      error,
      createdAt,
      completedAt,
    };
    this.recent.push(delivery);
    if (this.recent.length > MAX_RECENT) {
      this.recent.shift();
    }
    if (!delivery.ok) {
      log.warn(
        () =>
          `delivery of ${event} to ${redactUrl(webhook.url)} failed after ${attempts} attempt(s): ${error}`,
      );
    }
    return delivery;
  }
}

interface PostOptions {
  headers: Record<string, string>;
  body: string;
  timeoutMs: number;
  /** Resolver for the destination; rejects addresses the guard blocks. */
  lookup: LookupFunction;
}

interface PostResult {
  status: number;
  body: string | null;
}

/**
 * POST `body` to `url` over `node:http(s)` rather than `fetch`.
 *
 * The core HTTP client takes a `lookup`, which is what lets the SSRF guard
 * decide the address the socket dials, and it never follows redirects, so a
 * `3xx` surfaces as itself on every runtime.
 */
function post(url: string, options: PostOptions): Promise<PostResult> {
  return new Promise((resolve, reject) => {
    const target = new URL(url);
    const send = target.protocol === "https:" ? httpsRequest : httpRequest;
    const request = send(
      target,
      {
        method: "POST",
        headers: {
          ...options.headers,
          "content-length": Buffer.byteLength(options.body).toString(),
        },
        lookup: options.lookup,
      },
      (response) => {
        readBody(response).then((text) =>
          resolve({ status: response.statusCode ?? 0, body: text }),
        );
      },
    );
    request.setTimeout(options.timeoutMs, () => {
      request.destroy(new Error(`timed out after ${options.timeoutMs}ms`));
    });
    request.on("error", reject);
    request.end(options.body);
  });
}

/** Read a response body for the delivery log, truncated; `null` if unreadable. */
function readBody(response: IncomingMessage): Promise<string | null> {
  return new Promise((resolve) => {
    let text = "";
    response.setEncoding("utf8");
    response.on("data", (chunk: string) => {
      if (text.length < MAX_RESPONSE_BODY_CHARS) {
        text += chunk;
      }
    });
    response.on("end", () => resolve(text.slice(0, MAX_RESPONSE_BODY_CHARS)));
    response.on("error", () => resolve(null));
  });
}

/** Strip credentials and query string from a URL before logging it. */
function redactUrl(raw: string): string {
  try {
    const url = new URL(raw);
    url.username = "";
    url.password = "";
    url.search = "";
    return url.toString();
  } catch {
    return "<invalid-url>";
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Contract curve: the Nth wait is `base ** N` seconds, N counted from zero. */
function backoff(attempt: number, base: number): number {
  return Math.min(MAX_BACKOFF_MS, base ** (attempt - 1) * 1000);
}
