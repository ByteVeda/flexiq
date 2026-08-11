import { Plug } from "lucide-react";
import { useMemo } from "react";
import {
  Badge,
  DataTable,
  type DataTableColumn,
  EmptyState,
  ErrorState,
  TableSkeleton,
} from "@/components/ui";
import type { Executor } from "@/lib/api-types";
import { formatCount } from "@/lib/number";
import { formatRelative } from "@/lib/time";
import { busySlots, isExecutorQuiet } from "../utils";

interface ExecutorsTableProps {
  executors: Executor[] | undefined;
  loading: boolean;
  error: Error | null;
  onRetry: () => void;
}

const NUMERIC_CELL =
  "block w-full text-right font-mono text-[0.82rem] tabular-nums text-[var(--fg-muted)]";

/** Tasks shown inline before the rest collapse into a count. */
const TASKS_SHOWN = 4;

export function ExecutorsTable({ executors, loading, error, onRetry }: ExecutorsTableProps) {
  const columns = useMemo<DataTableColumn<Executor>[]>(
    () => [
      {
        accessorKey: "executor_id",
        header: "Executor",
        cell: ({ getValue }) => (
          <span className="font-mono text-xs text-[var(--fg)]">{getValue<string>()}</span>
        ),
      },
      {
        id: "sdk",
        header: "SDK",
        cell: ({ row }) => (
          <span className="text-xs text-[var(--fg-muted)]">
            {row.original.sdk} {row.original.version}
          </span>
        ),
      },
      {
        accessorKey: "tasks",
        header: "Advertised tasks",
        cell: ({ getValue }) => {
          const tasks = getValue<string[]>();
          if (tasks.length === 0) {
            return <span className="text-[var(--fg-subtle)]">—</span>;
          }
          const shown = tasks.slice(0, TASKS_SHOWN);
          const hidden = tasks.length - shown.length;
          return (
            <div className="flex flex-wrap gap-1">
              {shown.map((task) => (
                <Badge key={task} tone="neutral">
                  {task}
                </Badge>
              ))}
              {hidden > 0 ? (
                <Badge tone="neutral" title={tasks.slice(TASKS_SHOWN).join(", ")}>
                  +{hidden}
                </Badge>
              ) : null}
            </div>
          );
        },
      },
      {
        id: "slots",
        header: "Slots",
        cell: ({ row }) => (
          <span className={NUMERIC_CELL}>
            {formatCount(busySlots(row.original))} / {formatCount(row.original.slots)}
          </span>
        ),
      },
      {
        accessorKey: "in_flight",
        header: "In flight",
        cell: ({ getValue }) => (
          <span className={NUMERIC_CELL}>{formatCount(getValue<number>())}</span>
        ),
      },
      {
        accessorKey: "peer",
        header: "Peer",
        cell: ({ getValue }) => (
          <span className="font-mono text-[0.78rem] text-[var(--fg-muted)]">
            {getValue<string>()}
          </span>
        ),
      },
      {
        accessorKey: "idle_ms",
        header: "Last frame",
        cell: ({ row }) => {
          // `idle_ms` is an age, not a timestamp — turn it into one so it
          // reads like every other time column.
          const seen = Date.now() - row.original.idle_ms;
          return (
            <span className={NUMERIC_CELL}>
              {isExecutorQuiet(row.original) ? (
                <Badge tone="warning">quiet</Badge>
              ) : (
                formatRelative(seen)
              )}
            </span>
          );
        },
      },
    ],
    [],
  );

  if (error) {
    return (
      <ErrorState title="Couldn't load executors" description={error.message} onRetry={onRetry} />
    );
  }

  if (loading && !executors) {
    return (
      <TableSkeleton rows={3} columns={["w-40", "w-28", "w-48", "w-20", "w-20", "w-36", "w-24"]} />
    );
  }

  if (!executors || executors.length === 0) {
    return (
      <EmptyState
        icon={Plug}
        title="No executors attached"
        description="Executors dial in to the scheduler and advertise the tasks they can run. Until one attaches, nothing is dispatchable."
      />
    );
  }

  return <DataTable columns={columns} data={executors} rowKey={(e) => e.executor_id} />;
}
