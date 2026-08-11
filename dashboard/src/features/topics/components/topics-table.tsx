import { useNavigate } from "@tanstack/react-router";
import { Radio } from "lucide-react";
import { useMemo } from "react";
import {
  DataTable,
  type DataTableColumn,
  EmptyState,
  ErrorState,
  TableSkeleton,
} from "@/components/ui";
import type { TopicSummary } from "@/lib/api-types";
import { formatCount } from "@/lib/number";

interface TopicsTableProps {
  topics: TopicSummary[] | undefined;
  loading: boolean;
  error: Error | null;
  onRetry: () => void;
}

const NUM_CELL = "block w-full text-right font-mono text-[0.82rem] tabular-nums";

export function TopicsTable({ topics, loading, error, onRetry }: TopicsTableProps) {
  const navigate = useNavigate();

  const rows = useMemo<TopicSummary[]>(
    () => (topics ? [...topics].sort((a, b) => a.topic.localeCompare(b.topic)) : []),
    [topics],
  );

  const columns = useMemo<DataTableColumn<TopicSummary>[]>(
    () => [
      {
        accessorKey: "topic",
        header: "Topic",
        cell: ({ getValue }) => (
          <span className="font-medium text-[var(--fg)]">{getValue<string>()}</span>
        ),
      },
      {
        accessorKey: "subscription_count",
        header: "Subscriptions",
        cell: ({ getValue }) => <span className={NUM_CELL}>{formatCount(getValue<number>())}</span>,
      },
      {
        accessorKey: "backlog",
        header: "Backlog",
        cell: ({ getValue }) => (
          <span className={`${NUM_CELL} text-info`}>{formatCount(getValue<number>())}</span>
        ),
      },
      {
        accessorKey: "dead",
        header: "DLQ depth",
        cell: ({ getValue }) => {
          const dead = getValue<number>();
          return (
            <span className={`${NUM_CELL} ${dead > 0 ? "text-danger" : "text-[var(--fg-muted)]"}`}>
              {formatCount(dead)}
            </span>
          );
        },
      },
    ],
    [],
  );

  if (error) {
    return (
      <ErrorState title="Couldn't load topics" description={error.message} onRetry={onRetry} />
    );
  }

  if (loading && rows.length === 0) {
    return <TableSkeleton rows={5} columns={["w-40", "w-24", "w-20", "w-20"]} />;
  }

  if (rows.length === 0) {
    return (
      <EmptyState
        icon={Radio}
        title="No topics"
        description="Topics appear here once a subscriber is registered and a message is published."
      />
    );
  }

  return (
    <DataTable
      columns={columns}
      data={rows}
      rowKey={(r) => r.topic}
      onRowClick={(r) => navigate({ to: "/topics/$topic", params: { topic: r.topic } })}
    />
  );
}
