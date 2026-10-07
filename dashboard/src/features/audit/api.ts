import { ApiError, api } from "@/lib/api-client";
import type { AuditFilters, AuditPage } from "./types";
import { toApiParams } from "./utils";

/**
 * Fetch one page of the audit trail, or `null` when this server keeps none.
 *
 * Only `flexiq-server` serves this route; an SDK dashboard answers 404, and
 * `null` lets the nav hide the page rather than show an error.
 */
export async function listAuditRecords(
  filters: AuditFilters,
  limit: number,
  after: string | undefined,
  signal?: AbortSignal,
): Promise<AuditPage | null> {
  try {
    return await api.get<AuditPage>("/api/audit-records", {
      params: toApiParams(filters, limit, after),
      signal,
    });
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) return null;
    throw error;
  }
}
