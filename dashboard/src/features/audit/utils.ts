import type { AuditFilters, AuditRecord, PrincipalKind } from "./types";

export const PRINCIPAL_KINDS = [
  "token",
  "user",
  "cli",
  "anonymous",
] as const satisfies readonly PrincipalKind[];

/** Target kinds the server records; the filter offers these. */
export const TARGET_KINDS = [
  "job",
  "workflow_run",
  "queue",
  "dead_letter",
  "worker",
  "periodic",
  "task",
  "namespace",
  "token",
  "webhook",
  "webhook_delivery",
  "setting",
  "topic",
  "subscription",
  "middleware",
] as const;

function asTrimmedString(value: unknown): string | undefined {
  if (typeof value !== "string") return undefined;
  const trimmed = value.trim();
  return trimmed === "" ? undefined : trimmed;
}

function asNonNegativeInteger(value: unknown): number | undefined {
  const n = Number(value);
  if (value === undefined || value === "" || !Number.isFinite(n)) return undefined;
  const int = Math.floor(n);
  return int >= 0 ? int : undefined;
}

/**
 * Read `/audit` search params. Invalid values are dropped rather than refused,
 * so a hand-edited link still opens the page.
 */
export function parseAuditSearch(raw: Record<string, unknown>): AuditFilters {
  return {
    tokenId: asTrimmedString(raw.tokenId),
    principalKind: PRINCIPAL_KINDS.find((kind) => kind === raw.principalKind),
    targetKind: asTrimmedString(raw.targetKind),
    target: asTrimmedString(raw.target),
    since: asNonNegativeInteger(raw.since),
    until: asNonNegativeInteger(raw.until),
  };
}

/** The `/api/audit-records` query string for `filters` and a page cursor. */
export function toApiParams(
  filters: AuditFilters,
  limit: number,
  after: string | undefined,
): Record<string, string | number | undefined> {
  return {
    token_id: filters.tokenId,
    principal_kind: filters.principalKind,
    target_kind: filters.targetKind,
    target: filters.target,
    since: filters.since,
    until: filters.until,
    limit,
    after,
  };
}

export function countActiveFilters(filters: AuditFilters): number {
  return Object.values(filters).filter((value) => value !== undefined).length;
}

/**
 * Who a record names, beside its kind. `null` for kinds with no id: the CLI and
 * an auth-off dashboard know what changed but not who changed it.
 */
export function describePrincipal(record: AuditRecord): string | null {
  if (!record.token_id) return null;
  if (record.principal_kind === "token" && record.principal) {
    return `${record.principal} (${record.token_id})`;
  }
  return record.token_id;
}

/** Whether an outcome is the call succeeding — anything else is worth a colour. */
export function isOk(outcome: string): boolean {
  return outcome === "OK";
}
