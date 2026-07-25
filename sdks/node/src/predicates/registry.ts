import { PredicateValidationError } from "../errors";
import type { EnqueueGate } from "./decisions";

/**
 * Named gates, so a task can be gated from configuration instead of from a
 * closure written at the call site. A Node gate is a plain function with no
 * serializable form, so the *name* is the portable part: register the gate once
 * during startup, then `queue.gate("digest", "business-hours")` — the name can
 * come from a config file, an env var, or a dashboard setting.
 */
export class PredicateRegistry {
  private readonly gates = new Map<string, EnqueueGate>();

  /**
   * Register `gate` under `name`. Registering a different gate under a name
   * already taken needs `replace`, so two modules can't silently fight over it.
   */
  register(name: string, gate: EnqueueGate, options: { replace?: boolean } = {}): void {
    if (!name) {
      throw new PredicateValidationError("predicate name must be non-empty");
    }
    const existing = this.gates.get(name);
    if (existing !== undefined && existing !== gate && options.replace !== true) {
      throw new PredicateValidationError(
        `predicate ${JSON.stringify(name)} is already registered; pass { replace: true } to overwrite it`,
      );
    }
    this.gates.set(name, gate);
  }

  /** The gate registered under `name`, or a throw listing what is registered. */
  lookup(name: string): EnqueueGate {
    const gate = this.gates.get(name);
    if (gate === undefined) {
      throw new PredicateValidationError(
        `unknown predicate: ${JSON.stringify(name)} (registered: ${this.names().join(", ") || "<none>"})`,
      );
    }
    return gate;
  }

  has(name: string): boolean {
    return this.gates.has(name);
  }

  /** Every registered name, sorted. */
  names(): string[] {
    return [...this.gates.keys()].sort();
  }

  /** Drop every registration. Mainly for tests, which share the default registry. */
  clear(): void {
    this.gates.clear();
  }
}

const DEFAULT_REGISTRY = new PredicateRegistry();

/** The process-wide registry that `Queue.gate(name, "…")` resolves against. */
export function defaultRegistry(): PredicateRegistry {
  return DEFAULT_REGISTRY;
}

/** Register `gate` under `name` in the default registry. */
export function registerPredicate(
  name: string,
  gate: EnqueueGate,
  options: { replace?: boolean } = {},
): void {
  DEFAULT_REGISTRY.register(name, gate, options);
}
