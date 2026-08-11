import { Radio } from "lucide-react";
import { useMemo } from "react";
import {
  Badge,
  DataTable,
  type DataTableColumn,
  EmptyState,
  ErrorState,
  TableSkeleton,
} from "@/components/ui";
import type { SubscriptionBacklog } from "@/lib/api-types";
import { formatCount } from "@/lib/number";
import { formatDuration } from "@/lib/time";

interface SubscriptionsTableProps {
  subscriptions: SubscriptionBacklog[] | undefined;
  loading: boolean;
  error: Error | null;
  onRetry: () => void;
}

const NUM_CELL = "block w-full text-right font-mono text-[0.82rem] tabular-nums";

export function SubscriptionsTable({
  subscriptions,
  loading,
  error,
  onRetry,
}: SubscriptionsTableProps) {
  const rows = useMemo<SubscriptionBacklog[]>(
    () =>
      subscriptions
        ? [...subscriptions].sort((a, b) => a.subscription.localeCompare(b.subscription))
        : [],
    [subscriptions],
  );

  const columns = useMemo<DataTableColumn<SubscriptionBacklog>[]>(
    () => [
      {
        accessorKey: "subscription",
        header: "Subscription",
        cell: ({ getValue }) => (
          <span className="font-medium text-[var(--fg)]">{getValue<string>()}</span>
        ),
      },
      {
        accessorKey: "task_name",
        header: "Task",
        cell: ({ getValue }) => (
          <span className="font-mono text-[0.8rem] text-[var(--fg-muted)]">
            {getValue<string>()}
          </span>
        ),
      },
      {
        accessorKey: "queue",
        header: "Queue",
        cell: ({ getValue }) => (
          <span className="text-[var(--fg-muted)]">{getValue<string>()}</span>
        ),
      },
      {
        accessorKey: "active",
        header: "Status",
        cell: ({ getValue }) =>
          getValue<boolean>() ? (
            <Badge tone="success" dot>
              Active
            </Badge>
          ) : (
            <Badge tone="warning" dot>
              Paused
            </Badge>
          ),
      },
      {
        accessorKey: "durable",
        header: "Kind",
        cell: ({ getValue }) =>
          getValue<boolean>() ? (
            <Badge tone="neutral">Durable</Badge>
          ) : (
            <Badge tone="info">Ephemeral</Badge>
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
        accessorKey: "dead",
        header: "Dead",
        cell: ({ getValue }) => {
          const dead = getValue<number>();
          return (
            <span className={`${NUM_CELL} ${dead > 0 ? "text-danger" : "text-[var(--fg-muted)]"}`}>
              {formatCount(dead)}
            </span>
          );
        },
      },
      {
        accessorKey: "oldest_pending_age_ms",
        header: "Oldest pending",
        cell: ({ getValue }) => (
          <span className={`${NUM_CELL} text-[var(--fg-muted)]`}>
            {formatDuration(getValue<number | null>())}
          </span>
        ),
      },
    ],
    [],
  );

  if (error) {
    return (
      <ErrorState
        title="Couldn't load subscriptions"
        description={error.message}
        onRetry={onRetry}
      />
    );
  }

  if (loading && rows.length === 0) {
    return (
      <TableSkeleton
        rows={4}
        columns={["w-32", "w-40", "w-20", "w-16", "w-16", "w-16", "w-16", "w-16", "w-20"]}
      />
    );
  }

  if (rows.length === 0) {
    return (
      <EmptyState
        icon={Radio}
        title="No subscriptions"
        description="This topic has no registered subscriptions."
      />
    );
  }

  return <DataTable columns={columns} data={rows} rowKey={(r) => r.subscription} />;
}
