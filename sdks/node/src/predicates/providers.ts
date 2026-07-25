import type { PredicateContext } from "./context";

const TRUTHY = new Set(["1", "true", "t", "yes", "y", "on"]);

/**
 * Whether `value` reads as enabled: `1`, `true`, `t`, `yes`, `y`, or `on`, in
 * any case. Anything else — including an unset variable — is disabled. Matches
 * the Python SDK's truthy set so the same env var means the same thing in both.
 */
export function isTruthy(value: string | undefined): boolean {
  return value !== undefined && TRUTHY.has(value.trim().toLowerCase());
}

/**
 * Plug a feature-flag system (LaunchDarkly, Statsig, an internal service) into
 * {@link featureFlag}. Implementations should absorb their own errors and
 * return a safe default rather than throwing — a gate that throws fails the
 * enqueue.
 */
export interface FeatureFlagProvider {
  isEnabled(flag: string, ctx: PredicateContext): boolean;
}

/** The function form of {@link FeatureFlagProvider}, for a one-line lookup. */
export type FlagLookup = (flag: string, ctx: PredicateContext) => boolean;

/**
 * The default provider: reads `${prefix}${FLAG}` from `process.env`, with the
 * flag upper-cased and `-`, `.`, and whitespace folded to `_` (so the flag
 * `"beta-export"` reads `FF_BETA_EXPORT`).
 */
export function envFeatureFlagProvider(prefix = "FF_"): FeatureFlagProvider {
  return {
    isEnabled(flag) {
      return isTruthy(process.env[`${prefix}${flag.toUpperCase().replace(/[-.\s]/g, "_")}`]);
    },
  };
}

/** Accept either provider form and hand back the object form. */
export function toFlagProvider(provider: FeatureFlagProvider | FlagLookup): FeatureFlagProvider {
  return typeof provider === "function" ? { isEnabled: provider } : provider;
}
