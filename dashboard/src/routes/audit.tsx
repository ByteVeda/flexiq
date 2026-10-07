import { createFileRoute } from "@tanstack/react-router";
import { useState } from "react";
import { PageHeader } from "@/components/layout/page-header";
import { ErrorState, Pagination, Skeleton } from "@/components/ui";
import {
  type AuditFilters,
  AuditFiltersBar,
  AuditTable,
  parseAuditSearch,
  useAuditRecords,
} from "@/features/audit";
import { ApiError } from "@/lib/api-client";

// No loader: a viewer's 403 would land on the router's error boundary, where
// the page's own ErrorState says what happened.
export const Route = createFileRoute("/audit")({
  validateSearch: (search): AuditFilters => parseAuditSearch(search),
  component: AuditPage,
});

function AuditPage() {
  const filters = Route.useSearch();
  const navigate = Route.useNavigate();

  const setFilters = (next: AuditFilters) => {
    navigate({ search: next, replace: true });
  };

  return (
    <div className="flex flex-col gap-[var(--page-gap)]">
      <PageHeader
        eyebrow="Configuration"
        title="Audit trail"
        description="Who did what, newest first: every write the gRPC door served, every change made from this dashboard, and every token created or revoked."
      />
      <AuditFiltersBar filters={filters} onChange={setFilters} />
      {/* Keyed on the filters so a new filter starts again from the first page. */}
      <AuditResults key={JSON.stringify(filters)} filters={filters} />
    </div>
  );
}

/**
 * The trail pages by cursor, so "previous" needs the cursors already walked:
 * `cursors[page]` is the `after` that page was fetched with.
 */
function AuditResults({ filters }: { filters: AuditFilters }) {
  const [cursors, setCursors] = useState<(string | undefined)[]>([undefined]);
  const page = cursors.length - 1;
  const { data, isLoading, error } = useAuditRecords(filters, cursors[page]);

  const setPage = (next: number) => {
    if (next < page) {
      setCursors((prev) => prev.slice(0, next + 1));
    } else if (data?.next_cursor) {
      const cursor = data.next_cursor;
      setCursors((prev) => [...prev, cursor]);
    }
  };

  if (isLoading) return <Skeleton className="h-48" />;
  if (error) {
    if (error instanceof ApiError && error.status === 403) {
      return (
        <ErrorState
          title="Admins only"
          description="The audit trail names every user and token and what each did, so reading it needs the admin role."
        />
      );
    }
    return (
      <ErrorState
        title="Failed to load the audit trail"
        description={error instanceof Error ? error.message : String(error)}
      />
    );
  }
  // `null` is a server that keeps no trail; the nav hides the page, but a
  // bookmark can still land here.
  if (data === null || data === undefined) {
    return (
      <ErrorState
        title="This server keeps no audit trail"
        description="The audit trail is recorded by flexiq-server. A dashboard served from an SDK neither records nor lists it."
      />
    );
  }

  return (
    <>
      <AuditTable records={data.records} />
      <Pagination page={page} hasMore={data.next_cursor !== null} onChange={setPage} />
    </>
  );
}
