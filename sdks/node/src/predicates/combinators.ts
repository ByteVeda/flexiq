import {
  type EnqueueDecision,
  type EnqueueGate,
  normalizeOutcome,
  type Predicate,
} from "./decisions";

/**
 * A predicate that passes only when every `predicates` member passes.
 *
 * Boolean predicates compose into a boolean predicate. Mixing in gates that
 * return decisions yields a gate: the first non-`allow` decision short-circuits
 * and is returned as-is, so an out-of-hours `defer` survives the composition.
 */
export function allOf(...predicates: Predicate[]): Predicate;
export function allOf(...gates: EnqueueGate[]): EnqueueGate;
export function allOf(...gates: EnqueueGate[]): EnqueueGate {
  return (ctx) => {
    for (const gate of gates) {
      // `normalizeOutcome` keeps a member gate that returned nothing fail-closed
      // here too, rather than throwing on `.kind` of `undefined`.
      const outcome = normalizeOutcome(gate(ctx));
      if (typeof outcome === "boolean") {
        if (!outcome) {
          return false;
        }
      } else if (outcome.kind !== "allow") {
        return outcome;
      }
    }
    return true;
  };
}

/**
 * A predicate that passes when any `predicates` member passes.
 *
 * With decision-returning gates: any `allow` (or `true`) wins; otherwise the
 * first blocking decision is returned, so `anyOf(urgent, businessHours)` still
 * defers rather than rejecting outright.
 */
export function anyOf(...predicates: Predicate[]): Predicate;
export function anyOf(...gates: EnqueueGate[]): EnqueueGate;
export function anyOf(...gates: EnqueueGate[]): EnqueueGate {
  return (ctx) => {
    let firstBlock: EnqueueDecision | undefined;
    for (const gate of gates) {
      const outcome = normalizeOutcome(gate(ctx));
      if (outcome === true) {
        return true;
      }
      if (typeof outcome !== "boolean") {
        if (outcome.kind === "allow") {
          return true;
        }
        firstBlock ??= outcome;
      }
    }
    return firstBlock ?? false;
  };
}

/**
 * A predicate that passes when `predicate` fails. Boolean-only: inverting a
 * `defer` or `skip` has no meaning, so compose those with {@link allOf} /
 * {@link anyOf} instead.
 */
export function not(predicate: Predicate): Predicate {
  return (ctx) => !predicate(ctx);
}
