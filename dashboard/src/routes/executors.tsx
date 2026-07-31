import { createFileRoute } from "@tanstack/react-router";
import { Cpu, Plug, Zap } from "lucide-react";
import { PageHeader } from "@/components/layout";
import { EmptyState, StatCard } from "@/components/ui";
import { ExecutorsTable, useExecutors, utilization } from "@/features/executors";
import { formatCount } from "@/lib/number";

export const Route = createFileRoute("/executors")({
  // No loader: this route 404s on a dashboard without an attach listener, and
  // a loader would turn that into an error boundary instead of the explanation
  // below.
  component: ExecutorsPage,
});

function ExecutorsPage() {
  const inventory = useExecutors();
  const data = inventory.data ?? null;

  return (
    <div className="flex flex-col gap-[var(--page-gap)]">
      <PageHeader
        eyebrow="Infrastructure"
        title="Executors"
        description="Processes attached to this scheduler, and the tasks each one can run."
      />
      {inventory.isSuccess && data === null ? (
        <EmptyState
          icon={Plug}
          title="Not served here"
          description="This dashboard runs inside a worker process, which executes tasks itself. Executors attach to a standalone scheduler instead."
        />
      ) : (
        <>
          <div className="grid gap-[var(--gap)] grid-cols-[repeat(auto-fit,minmax(186px,1fr))]">
            <StatCard
              label="Attached"
              tone="neutral"
              icon={<Plug />}
              value={formatCount(data?.capacity.executors ?? 0)}
            />
            <StatCard
              label="Slots"
              tone="neutral"
              icon={<Cpu />}
              value={formatCount(data?.capacity.total_slots ?? 0)}
              hint={`${formatCount(data?.capacity.free_slots ?? 0)} free`}
            />
            <StatCard
              label="Utilization"
              tone="success"
              icon={<Zap />}
              value={`${Math.round(utilization(data?.executors ?? []) * 100)}%`}
              hint="slots in use"
            />
          </div>
          <ExecutorsTable
            executors={data?.executors}
            loading={inventory.isLoading}
            error={inventory.error}
            onRetry={() => inventory.refetch()}
          />
        </>
      )}
    </div>
  );
}
