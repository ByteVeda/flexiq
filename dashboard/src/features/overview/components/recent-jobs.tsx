import { Link, useNavigate } from "@tanstack/react-router";
import { ListTree } from "lucide-react";
import { useMemo } from "react";
import {
  DataTable,
  type DataTableColumn,
  EmptyState,
  ErrorState,
  Skeleton,
  StatusBadge,
} from "@/components/ui";
import type { Job } from "@/lib/api-types";
import { formatRelative } from "@/lib/time";

interface RecentJobsProps {
  jobs: Job[] | undefined;
  loading: boolean;
  error: Error | null;
  onRetry: () => void;
}

export function RecentJobs({ jobs, loading, error, onRetry }: RecentJobsProps) {
  const navigate = useNavigate();

  const columns = useMemo<DataTableColumn<Job>[]>(
    () => [
      {
        accessorKey: "id",
        header: "Job",
        cell: ({ row }) => (
          <Link
            to="/jobs/$id"
            params={{ id: row.original.id }}
            className="font-mono text-xs text-accent hover:underline"
            onClick={(e: React.MouseEvent<HTMLAnchorElement>) => e.stopPropagation()}
          >
            {row.original.id.slice(0, 8)}…
          </Link>
        ),
      },
      {
        accessorKey: "task_name",
        header: "Task",
        cell: ({ getValue }) => (
          <span className="truncate text-[var(--fg)]">{getValue<string>()}</span>
        ),
      },
      {
        accessorKey: "queue",
        header: "Queue",
        cell: ({ getValue }) => (
          <span className="text-xs text-[var(--fg-muted)]">{getValue<string>()}</span>
        ),
      },
      {
        accessorKey: "status",
        header: "Status",
        cell: ({ row }) => <StatusBadge status={row.original.status} />,
      },
      {
        accessorKey: "created_at",
        header: "Created",
        cell: ({ getValue }) => (
          <span className="text-xs tabular-nums text-[var(--fg-muted)]">
            {formatRelative(getValue<number>())}
          </span>
        ),
      },
    ],
    [],
  );

  if (error) {
    return <ErrorState title="Couldn't load jobs" description={error.message} onRetry={onRetry} />;
  }

  if (loading && !jobs) {
    return <Skeleton className="h-60 w-full" />;
  }

  return (
    <DataTable
      columns={columns}
      data={jobs ?? []}
      rowKey={(j) => j.id}
      empty={
        <EmptyState
          icon={ListTree}
          title="No jobs yet"
          description="Jobs will appear here as soon as workers start processing them."
        />
      }
      onRowClick={(job) => navigate({ to: "/jobs/$id", params: { id: job.id } })}
    />
  );
}
