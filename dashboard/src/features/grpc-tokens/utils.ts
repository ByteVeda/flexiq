import type { GrpcScope } from "./types";

/** What the dialog narrows the narrowable scopes to. Blank means every name. */
export interface Narrowing {
  queue: string;
  task: string;
}

/**
 * The `scopes` a mint request sends: each selected scope, spelled as a grant.
 *
 * A scope the server marks narrowable carries the queue and task patterns
 * (`produce:queue=emails-*,task=send_receipt`); any other scope is sent whole,
 * because the server refuses a qualifier on a door that cannot enforce it. The
 * patterns are sent as typed — the server is the one that validates them.
 */
export function spellGrants(
  selected: readonly string[],
  available: readonly GrpcScope[],
  { queue, task }: Narrowing,
): string[] {
  const narrowable = new Set(available.filter((s) => s.narrowable).map((s) => s.name));
  const qualifiers = [
    queue.trim() ? `queue=${queue.trim()}` : null,
    task.trim() ? `task=${task.trim()}` : null,
  ].filter((q): q is string => q !== null);
  return selected.map((scope) =>
    narrowable.has(scope) && qualifiers.length > 0 ? `${scope}:${qualifiers.join(",")}` : scope,
  );
}

/** The units an expiry can be typed in. */
export type ExpiryUnit = "days" | "weeks" | "months";

/** Days per unit. A month is 30 days, so "12 months" stays inside the cap. */
export const EXPIRY_UNIT_DAYS: Record<ExpiryUnit, number> = { days: 1, weeks: 7, months: 30 };

/** The server's cap on a token's lifetime, so the form cannot ask for a refusal. */
export const MAX_EXPIRY_DAYS = 365;

export type ParsedExpiry = { ok: true; days: number } | { ok: false; error: string };

/** A typed amount and unit as the `expires_in_days` a mint request sends. */
export function expiryDays(amount: string, unit: ExpiryUnit): ParsedExpiry {
  const raw = amount.trim();
  if (!/^\d+$/.test(raw) || Number(raw) < 1) {
    return { ok: false, error: "Enter a whole number, 1 or more" };
  }
  const days = Number(raw) * EXPIRY_UNIT_DAYS[unit];
  if (days > MAX_EXPIRY_DAYS) {
    return { ok: false, error: `At most ${MAX_EXPIRY_DAYS} days — that is ${days}` };
  }
  return { ok: true, days };
}
