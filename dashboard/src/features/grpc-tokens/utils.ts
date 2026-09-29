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
