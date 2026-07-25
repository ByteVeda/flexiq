import { PredicateValidationError } from "../errors";
import type { PredicateContext } from "./context";

/**
 * What a gate can decide about an enqueue.
 *
 * - `allow` — enqueue unchanged.
 * - `skip` — do not enqueue; `Queue.enqueue` throws `EnqueueSkippedError` and
 *   `Queue.tryEnqueue` returns `null`.
 * - `defer` — enqueue, delayed by `delayMs` (replaces any caller `delayMs`).
 * - `reject` — block the enqueue; `Queue.enqueue` throws
 *   `PredicateRejectedError`.
 */
export type DecisionKind = "allow" | "skip" | "defer" | "reject";

/** Every decision kind, for runtime validation. */
export const DECISION_KINDS: readonly DecisionKind[] = ["allow", "skip", "defer", "reject"];

/** The outcome of an {@link EnqueueGate}. Build one with {@link Decision}. */
export type EnqueueDecision =
  | { readonly kind: "allow" }
  | { readonly kind: "skip"; readonly reason: string }
  | { readonly kind: "defer"; readonly delayMs: number }
  | { readonly kind: "reject"; readonly reason: string };

/**
 * A gate evaluated at enqueue time. Returning `false` rejects the submission
 * with a `PredicateRejectedError`. Register one with {@link Queue.gate}.
 */
export type Predicate = (ctx: PredicateContext) => boolean;

/**
 * The richer form of {@link Predicate}: returns an {@link EnqueueDecision} so a
 * gate can also skip or defer, not just allow or reject. A plain `boolean` is
 * still accepted and means allow/reject.
 */
export type EnqueueGate = (ctx: PredicateContext) => boolean | EnqueueDecision;

const ALLOW: EnqueueDecision = Object.freeze({ kind: "allow" as const });

/** A bare `false` carries no reason — the thrown error keeps its default text. */
const BARE_REJECT: EnqueueDecision = Object.freeze({ kind: "reject" as const, reason: "" });

/** Factories for the four {@link EnqueueDecision} outcomes. */
export const Decision = {
  /** Enqueue unchanged. */
  allow(): EnqueueDecision {
    return ALLOW;
  },
  /** Quietly do not enqueue. `Queue.tryEnqueue` reports this as `null`. */
  skip(reason = ""): EnqueueDecision {
    return { kind: "skip", reason };
  },
  /** Enqueue delayed by `delayMs`, replacing any delay the caller passed. */
  defer(delayMs: number): EnqueueDecision {
    if (!Number.isFinite(delayMs) || delayMs < 0) {
      throw new RangeError(`defer delayMs must be a non-negative finite number, got ${delayMs}`);
    }
    return { kind: "defer", delayMs: Math.round(delayMs) };
  },
  /** Defer until `instant`; already-past instants defer by zero. */
  deferUntil(instant: Date, now: Date = new Date()): EnqueueDecision {
    const delayMs = instant.getTime() - now.getTime();
    if (Number.isNaN(delayMs)) {
      throw new RangeError("deferUntil requires valid dates");
    }
    return Decision.defer(Math.max(0, delayMs));
  },
  /** Block the enqueue; the reason is surfaced on the thrown error. */
  reject(reason = ""): EnqueueDecision {
    return { kind: "reject", reason };
  },
};

/**
 * A null-ish return is a gate bug (a JavaScript caller falling off the end of a
 * function), so treat it as `false` and let the enqueue fail closed.
 */
export function normalizeOutcome(
  outcome: boolean | EnqueueDecision | null | undefined,
): boolean | EnqueueDecision {
  return outcome === null || outcome === undefined ? false : outcome;
}

/**
 * Normalize whatever a gate returned into a decision: `true` allows, `false`
 * (and any null-ish return) rejects fail-closed, and a decision is re-run
 * through its factory so a hand-written object literal can't carry a payload
 * the factory would have refused — `{ kind: "defer", delayMs: -1 }` type-checks
 * but must not reach storage.
 */
export function toDecision(
  outcome: boolean | EnqueueDecision | null | undefined,
  taskName: string,
): EnqueueDecision {
  const normalized = normalizeOutcome(outcome);
  if (typeof normalized === "boolean") {
    return normalized ? ALLOW : BARE_REJECT;
  }
  switch (normalized.kind) {
    case "allow":
      return ALLOW;
    case "defer":
      return Decision.defer(normalized.delayMs);
    case "skip":
      return Decision.skip(asReason(normalized.reason));
    case "reject":
      return Decision.reject(asReason(normalized.reason));
    default: {
      const kind = (normalized as { kind?: unknown }).kind;
      throw new PredicateValidationError(
        `gate for task "${taskName}" returned an unknown decision kind: ${JSON.stringify(kind)} ` +
          `(expected one of ${DECISION_KINDS.join(", ")})`,
      );
    }
  }
}

/** Coerce a decision's `reason` — a JavaScript caller may omit it or pass a non-string. */
function asReason(reason: unknown): string {
  if (typeof reason === "string") {
    return reason;
  }
  return reason === undefined || reason === null ? "" : String(reason);
}
