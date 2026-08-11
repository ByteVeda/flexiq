import { Box, Pause, Play } from "lucide-react";
import { useMemo } from "react";
import {
  Badge,
  Button,
  DataTable,
  type DataTableColumn,
  EmptyState,
  ErrorState,
  QueueBar,
  TableSkeleton,
} from "@/components/ui";
import type { QueueStatsMap } from "@/lib/api-types";
import { formatCount } from "@/lib/number";
import { usePauseQueue, useResumeQueue } from "../hooks";

interface QueueRow {
  name: string;
  paused: boolean;
  pending: number;
  running: number;
  completed: number;
  failed: number;
  dead: number;
  failedTotal: number;
}

interface QueuesTableProps {
  stats: QueueStatsMap | undefined;
  paused: string[] | undefined;
  loading: boolean;
  error: Error | null;
  onRetry: () => void;
}

const NUM_CELL = "block w-full text-right font-mono text-[0.82rem] tabular-nums";

export function QueuesTable({ stats, paused, loading, error, onRetry }: QueuesTableProps) {
  const rows = useMemo<QueueRow[]>(() => {
    if (!stats) return [];
    const pausedSet = new Set(paused ?? []);
    return Object.entries(stats)
      .map(([name, s]) => {
        const failed = s.failed ?? 0;
        const dead = s.dead ?? 0;
        return {
          name,
          paused: pausedSet.has(name),
          pending: s.pending ?? 0,
          running: s.running ?? 0,
          completed: s.completed ?? 0,
          failed,
          dead,
          failedTotal: failed + dead,
        };
      })
      .sort((a, b) => a.name.localeCompare(b.name));
  }, [stats, paused]);

  const columns = useMemo<DataTableColumn<QueueRow>[]>(
    () => [
      {
        accessorKey: "name",
        header: "Queue",
        cell: ({ row }) => (
          <div className="flex items-center gap-2">
            <span className="font-medium text-[var(--fg)]">{row.original.name}</span>
            {row.original.paused ? (
              <Badge tone="warning" dot>
                Paused
              </Badge>
            ) : null}
          </div>
        ),
      },
      {
        id: "mix",
        header: "Mix",
        cell: ({ row }) => (
          <QueueBar
            pending={row.original.pending}
            running={row.original.running}
            failedTotal={row.original.failedTotal}
            className="min-w-[120px]"
          />
        ),
      },
      {
        accessorKey: "pending",
        header: "Pending",
        cell: ({ getValue }) => <span className={NUM_CELL}>{formatCount(getValue<number>())}</span>,
      },
      {
        accessorKey: "running",
        header: "Running",
        cell: ({ getValue }) => (
          <span className={`${NUM_CELL} text-info`}>{formatCount(getValue<number>())}</span>
        ),
      },
      {
        accessorKey: "completed",
        header: "Completed",
        cell: ({ getValue }) => (
          <span className={`${NUM_CELL} text-[var(--fg-muted)]`}>
            {formatCount(getValue<number>())}
          </span>
        ),
      },
      {
        accessorKey: "failedTotal",
        header: "Failed / dead",
        cell: ({ getValue }) => {
          const total = getValue<number>();
          return (
            <span className={`${NUM_CELL} ${total > 0 ? "text-danger" : "text-[var(--fg-muted)]"}`}>
              {formatCount(total)}
            </span>
          );
        },
      },
      {
        id: "actions",
        header: "",
        cell: ({ row }) => (
          <div className="flex justify-end">
            <PauseResumeCell name={row.original.name} paused={row.original.paused} />
          </div>
        ),
      },
    ],
    [],
  );

  if (error) {
    return (
      <ErrorState title="Couldn't load queues" description={error.message} onRetry={onRetry} />
    );
  }

  if (loading && rows.length === 0) {
    return (
      <TableSkeleton rows={6} columns={["w-32", "w-28", "w-16", "w-16", "w-20", "w-20", "w-20"]} />
    );
  }

  if (rows.length === 0) {
    return (
      <EmptyState
        icon={Box}
        title="No queues registered"
        description="Queues appear here once tasks are enqueued."
      />
    );
  }

  return <DataTable columns={columns} data={rows} rowKey={(r) => r.name} />;
}

/**
 * Per-row pause/resume button. Owning the mutation locally ensures only
 * the clicked row is disabled while its request is in flight — other rows
 * stay clickable.
 */
function PauseResumeCell({ name, paused }: { name: string; paused: boolean }) {
  const pause = usePauseQueue();
  const resume = useResumeQueue();
  if (paused) {
    return (
      <Button
        variant="ghost"
        size="sm"
        onClick={(e) => {
          e.stopPropagation();
          resume.mutate(name);
        }}
        disabled={resume.isPending}
      >
        <Play aria-hidden /> Resume
      </Button>
    );
  }
  return (
    <Button
      variant="ghost"
      size="sm"
      onClick={(e) => {
        e.stopPropagation();
        pause.mutate(name);
      }}
      disabled={pause.isPending}
    >
      <Pause aria-hidden /> Pause
    </Button>
  );
}
