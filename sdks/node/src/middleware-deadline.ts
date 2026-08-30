import { createLogger } from "./utils";

const log = createLogger("middleware");

/**
 * Default per-hook budget, in milliseconds. Override with
 * `new Queue({ middlewareTimeoutMs })`; `0` disables the bound.
 */
export const DEFAULT_MIDDLEWARE_TIMEOUT_MS = 5000;

/** Race marker. A symbol, so no hook return value can be mistaken for it. */
const EXPIRED = Symbol("hook-deadline");

/**
 * Await one middleware hook for at most `timeoutMs`, then stop waiting.
 *
 * A task's `timeout` bounds its handler and nothing else, so a hook that blocks
 * — an exporter flushing to an unreachable collector — holds the attempt open
 * past that limit. Nothing can cancel a promise, so the hook itself runs on;
 * what ends is the chain's wait for it, which is what the attempt was paying
 * for.
 *
 * A hook that rejects *before* its deadline still rejects out of here, so a
 * throwing `before` fails the attempt exactly as it did before this bound
 * existed. Only the overrun is swallowed: failing an attempt over its
 * instrumentation is the failure mode the hooks exist to avoid.
 *
 * @param timeoutMs Budget for this one call; `0` or less disables the bound.
 * @param middleware Stable key of the middleware, for the log line.
 * @param hook Hook name, for the log line (`"before"`, `"after"`, …).
 * @param run Invokes the hook.
 */
export async function withHookDeadline(
  timeoutMs: number,
  middleware: string,
  hook: string,
  run: () => void | Promise<void>,
): Promise<void> {
  // An async IIFE, so a hook that throws synchronously rejects rather than
  // escaping past the race and losing its own deadline.
  const running = (async () => run())();
  if (!(timeoutMs > 0)) {
    return running;
  }
  let timer: ReturnType<typeof setTimeout> | undefined;
  const expiry = new Promise<typeof EXPIRED>((resolve) => {
    timer = setTimeout(() => resolve(EXPIRED), timeoutMs);
    // A hook inside its budget clears this, but an abandoned one leaves the
    // race holding a timer the process must not be kept alive by.
    timer.unref?.();
  });
  try {
    const outcome = await Promise.race([running, expiry]);
    if (outcome === EXPIRED) {
      log.warn(
        () =>
          `middleware ${middleware} ${hook}() exceeded ${timeoutMs}ms; abandoned, the chain continues`,
      );
      // The race already holds a handler, so a later rejection cannot go
      // unhandled — this one exists to leave a trace of what it was.
      running.catch((error) =>
        log.debug(() => `abandoned ${middleware} ${hook}() later failed`, error),
      );
    }
  } finally {
    clearTimeout(timer);
  }
}
