import { ApiError } from "./api-client";

/**
 * Did this error come from the network being unreachable (DNS failure,
 * connection refused, CORS preflight blocked) rather than a well-formed
 * HTTP response? `fetch` throws a `TypeError` in these cases.
 */
export function isNetworkError(error: unknown): boolean {
  if (error instanceof TypeError) {
    // Chrome: "Failed to fetch", Firefox: "NetworkError when attempting to fetch resource",
    // Safari: "Load failed". Match loosely — all mean the same thing: no response.
    return /fetch|network|load failed/i.test(error.message);
  }
  return false;
}

/**
 * HTTP status codes that typically mean the server is running but cannot
 * serve the request right now (restart in progress, upstream timeout,
 * explicit 503). Surface these with the same "backend unreachable" page.
 */
export function isServerUnavailable(error: unknown): boolean {
  if (!(error instanceof ApiError)) return false;
  return error.status === 502 || error.status === 503 || error.status === 504;
}

/**
 * Treat the error as a connection problem (vs. a routing or data error).
 */
export function isBackendUnreachable(error: unknown): boolean {
  return isNetworkError(error) || isServerUnavailable(error);
}

/**
 * Router error boundaries hand us `unknown` — a thrown non-Error (a string,
 * an object) is legal. Normalize so the error views can read `.message`.
 */
export function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}
