import { Server } from "lucide-react";
import { useMemo } from "react";
import {
  Badge,
  DataTable,
  type DataTableColumn,
  EmptyState,
  ErrorState,
  TableSkeleton,
} from "@/components/ui";
import type { Worker } from "@/lib/api-types";
import { formatRelative } from "@/lib/time";
import { isWorkerStale } from "../utils";

interface WorkersTableProps {
  workers: Worker[] | undefined;
  loading: boolean;
  error: Error | null;
  onRetry: () => void;
}

const TIME_CELL =
  "block w-full text-right font-mono text-[0.82rem] tabular-nums text-[var(--fg-muted)]";

export function WorkersTable({ workers, loading, error, onRetry }: WorkersTableProps) {
  const columns = useMemo<DataTableColumn<Worker>[]>(
    () => [
      {
        accessorKey: "worker_id",
        header: "Worker",
        cell: ({ getValue }) => (
          <span className="font-mono text-xs text-[var(--fg)]">{getValue<string>()}</span>
        ),
      },
      {
        accessorKey: "queues",
        header: "Queues",
        cell: ({ getValue }) => {
          const parts = getValue<string>()
            .split(",")
            .map((s) => s.trim())
            .filter(Boolean);
          return (
            <div className="flex flex-wrap gap-1">
              {parts.map((q) => (
                <Badge key={q} tone="neutral">
                  {q}
                </Badge>
              ))}
            </div>
          );
        },
      },
      {
        id: "sdk",
        header: "SDK",
        // The point of the column in a polyglot fleet is spotting the odd one
        // out, so the version sits next to the name rather than in its own.
        cell: ({ row }) => {
          const { sdk, sdk_version } = row.original;
          if (!sdk) return <span className="text-[var(--fg-subtle)]">—</span>;
          return (
            <span className="text-xs text-[var(--fg-muted)]">
              {sdk}
              {sdk_version ? (
                <span className="ml-1 font-mono text-[var(--fg-subtle)]">{sdk_version}</span>
              ) : null}
            </span>
          );
        },
      },
      {
        accessorKey: "tags",
        header: "Tags",
        cell: ({ getValue }) => {
          const tags = getValue<string | null>();
          if (!tags) return <span className="text-[var(--fg-subtle)]">—</span>;
          return <span className="text-xs text-[var(--fg-muted)]">{tags}</span>;
        },
      },
      {
        accessorKey: "registered_at",
        header: "Registered",
        cell: ({ getValue }) => (
          <span className={TIME_CELL}>{formatRelative(getValue<number>())}</span>
        ),
      },
      {
        accessorKey: "last_heartbeat",
        header: "Last heartbeat",
        cell: ({ getValue }) => (
          <span className={TIME_CELL}>{formatRelative(getValue<number>())}</span>
        ),
      },
      {
        id: "status",
        header: "Status",
        // Recompute staleness against the wall clock on every render so a
        // worker ages from Online to Stale as its heartbeat goes cold (the
        // query refetches on the user interval, re-rendering this table).
        cell: ({ row }) => (
          <div className="flex justify-end">
            {isWorkerStale(row.original) ? (
              <Badge tone="danger" dot>
                Stale
              </Badge>
            ) : (
              <Badge tone="success" dot>
                Online
              </Badge>
            )}
          </div>
        ),
      },
    ],
    [],
  );

  if (error) {
    return (
      <ErrorState title="Couldn't load workers" description={error.message} onRetry={onRetry} />
    );
  }

  if (loading && !workers) {
    return (
      <TableSkeleton rows={4} columns={["w-32", "w-40", "w-20", "w-24", "w-24", "w-24", "w-20"]} />
    );
  }

  if (!workers || workers.length === 0) {
    return (
      <EmptyState
        icon={Server}
        title="No active workers"
        description="Workers register when you call q.start() or run flexiq worker."
      />
    );
  }

  return <DataTable columns={columns} data={workers} rowKey={(w) => w.worker_id} />;
}
