import { keepPreviousData, queryOptions, useQuery } from "@tanstack/react-query";
import { listAuditRecords } from "./api";
import type { AuditFilters } from "./types";

export const AUDIT_PAGE_SIZE = 50;

export function auditRecordsQuery(filters: AuditFilters, after?: string) {
  return queryOptions({
    queryKey: ["audit-records", filters, after ?? null],
    queryFn: ({ signal }) => listAuditRecords(filters, AUDIT_PAGE_SIZE, after, signal),
  });
}

export function useAuditRecords(filters: AuditFilters, after?: string) {
  return useQuery({ ...auditRecordsQuery(filters, after), placeholderData: keepPreviousData });
}

/**
 * Whether this server keeps an audit trail this session may read.
 *
 * `undefined` while unknown or refused: an SDK dashboard answers 404 (`null`)
 * and a viewer 403, and both should leave the nav entry hidden. Shares the
 * unfiltered first page, so opening the page costs no extra request.
 */
export function useAuditSupported(): boolean | undefined {
  const { data, isSuccess } = useQuery({ ...auditRecordsQuery({}), refetchInterval: false });
  if (!isSuccess) return undefined;
  return data !== null;
}
