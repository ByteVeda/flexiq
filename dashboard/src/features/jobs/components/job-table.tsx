import { useNavigate } from "@tanstack/react-router";
import { useCallback, useMemo } from "react";
import {
  DataTable,
  type DataTableColumn,
  EmptyState,
  ErrorState,
  StatusBadge,
  TableSkeleton,
  TaskErrorSummary,
  Tooltip,
  TooltipContent,
  TooltipTrigger,
} from "@/components/ui";
import type { Job } from "@/lib/api-types";
import { formatAbsolute, formatRelative } from "@/lib/time";

interface JobTableProps {
  jobs: Job[] | undefined;
  loading: boolean;
  error: Error | null;
  onRetry: () => void;
}

export function JobTable({ jobs, loading, error, onRetry }: JobTableProps) {
  const navigate = useNavigate();
  const showErrorColumn = useMemo(() => !!jobs?.some((j) => j.error), [jobs]);
  const handleRowClick = useCallback(
    (job: Job) => navigate({ to: "/jobs/$id", params: { id: job.id } }),
    [navigate],
  );

  const columns = useMemo<DataTableColumn<Job>[]>(() => {
    const base: DataTableColumn<Job>[] = [
      {
        accessorKey: "id",
        header: "Job",
        cell: ({ getValue }) => (
          <span className="font-mono text-xs text-accent">{getValue<string>().slice(0, 8)}…</span>
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
        accessorKey: "retry_count",
        header: "Retries",
        cell: ({ row }) => {
          const { retry_count, max_retries } = row.original;
          return (
            <span
              className={`tabular-nums ${retry_count > 0 ? "text-warning" : "text-[var(--fg-muted)]"}`}
            >
              {retry_count}
              {max_retries > 0 ? ` / ${max_retries}` : null}
            </span>
          );
        },
      },
      {
        accessorKey: "created_at",
        header: "Created",
        cell: ({ getValue }) => {
          const ms = getValue<number>();
          return (
            <Tooltip>
              <TooltipTrigger asChild>
                <span className="text-xs tabular-nums text-[var(--fg-muted)]">
                  {formatRelative(ms)}
                </span>
              </TooltipTrigger>
              <TooltipContent>{formatAbsolute(ms)}</TooltipContent>
            </Tooltip>
          );
        },
      },
    ];

    if (showErrorColumn) {
      base.push({
        accessorKey: "error",
        header: "Error",
        cell: ({ getValue }) => {
          const err = getValue<string | null>();
          if (!err) return <span className="text-[var(--fg-subtle)]">—</span>;
          return (
            <TaskErrorSummary
              error={err}
              className="line-clamp-1 max-w-[320px] text-xs text-danger"
            />
          );
        },
      });
    }
    return base;
  }, [showErrorColumn]);

  if (error) {
    return <ErrorState title="Couldn't load jobs" description={error.message} onRetry={onRetry} />;
  }

  if (loading && !jobs) {
    return <TableSkeleton rows={12} columns={["w-20", "w-40", "w-24", "w-16", "w-12", "w-24"]} />;
  }

  if (!jobs || jobs.length === 0) {
    return (
      <EmptyState
        title="No jobs match these filters"
        description="Try clearing a filter or widening the date range."
      />
    );
  }

  return (
    <DataTable columns={columns} data={jobs} rowKey={(j) => j.id} onRowClick={handleRowClick} />
  );
}
