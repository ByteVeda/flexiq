import { Link } from "@tanstack/react-router";
import { ChevronRight, Server, Skull } from "lucide-react";
import { Badge, Card, EmptyState, Skeleton } from "@/components/ui";
import { isWorkerStale } from "@/features/workers/utils";
import type { Worker } from "@/lib/api-types";
import { formatRelative } from "@/lib/time";

const VISIBLE_LIMIT = 6;

interface WorkersCardProps {
  workers: Worker[] | undefined;
  loading: boolean;
}

/** A compact worker roster for the Overview — id, queues, and liveness. */
export function WorkersCard({ workers, loading }: WorkersCardProps) {
  if (loading && !workers) return <Skeleton className="h-56 w-full" />;

  const list = workers ?? [];
  if (list.length === 0) {
    return (
      <Card>
        <EmptyState
          icon={Server}
          title="No active workers"
          description="Workers register when you call q.start() or run flexiq worker."
        />
      </Card>
    );
  }

  return (
    <Card>
      <div className="flex flex-col gap-2.5 p-[var(--pad)]">
        {list.slice(0, VISIBLE_LIMIT).map((w) => {
          const stale = isWorkerStale(w);
          const queues = w.queues
            .split(",")
            .map((q) => q.trim())
            .filter(Boolean)
            .join(" · ");
          return (
            <div key={w.worker_id} className="flex items-center gap-3">
              <span
                className={`grid size-[30px] shrink-0 place-items-center rounded-[9px] ${
                  stale ? "bg-danger-dim text-danger" : "bg-success-dim text-success"
                }`}
              >
                {stale ? <Skull className="size-4" /> : <Server className="size-4" />}
              </span>
              <div className="min-w-0 flex-1">
                <div className="truncate font-mono text-[0.78rem] text-[var(--fg)]">
                  {w.worker_id}
                </div>
                <div className="truncate text-[0.72rem] text-[var(--fg-subtle)]">
                  {queues || "no queues"}
                </div>
              </div>
              {stale ? (
                <Badge tone="danger" dot>
                  Stale
                </Badge>
              ) : (
                <span className="font-mono text-[0.74rem] tabular-nums text-[var(--fg-muted)]">
                  {formatRelative(w.last_heartbeat)}
                </span>
              )}
            </div>
          );
        })}
        {list.length > VISIBLE_LIMIT ? (
          <Link
            to="/workers"
            className="flex items-center justify-center gap-1 rounded-md py-1.5 text-xs font-medium text-[var(--fg-muted)] transition-colors hover:text-[var(--fg)]"
          >
            +{list.length - VISIBLE_LIMIT} more
            <ChevronRight className="size-3.5" aria-hidden />
          </Link>
        ) : null}
      </div>
    </Card>
  );
}
