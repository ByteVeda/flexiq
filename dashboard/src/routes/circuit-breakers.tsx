import { createFileRoute } from "@tanstack/react-router";
import { CheckCircle2, CircuitBoard, Zap } from "lucide-react";
import { PageHeader } from "@/components/layout";
import { StatCard } from "@/components/ui";
import {
  CircuitBreakersTable,
  circuitBreakersQuery,
  useCircuitBreakers,
} from "@/features/circuit-breakers";
import { formatCount } from "@/lib/number";

export const Route = createFileRoute("/circuit-breakers")({
  loader: ({ context: { queryClient } }) => queryClient.ensureQueryData(circuitBreakersQuery()),
  component: CircuitBreakersPage,
});

function CircuitBreakersPage() {
  const breakers = useCircuitBreakers();
  const all = breakers.data ?? [];
  const tripped = all.filter((b) => b.state !== "closed").length;
  const closed = all.length - tripped;

  return (
    <div className="flex flex-col gap-[var(--page-gap)]">
      <PageHeader
        eyebrow="Reliability"
        title="Circuit breakers"
        description="When a task fails too often, flexiq trips its breaker to give the downstream a rest."
      />
      <div className="grid gap-[var(--gap)] grid-cols-[repeat(auto-fit,minmax(186px,1fr))]">
        <StatCard
          label="Breakers"
          tone="neutral"
          icon={<CircuitBoard />}
          value={formatCount(all.length)}
        />
        <StatCard
          label="Tripped"
          tone={tripped > 0 ? "danger" : "success"}
          icon={<Zap />}
          value={formatCount(tripped)}
          hint={tripped > 0 ? "open or half-open" : "all healthy"}
        />
        <StatCard
          label="Closed"
          tone="success"
          icon={<CheckCircle2 />}
          value={formatCount(closed)}
        />
      </div>
      <CircuitBreakersTable
        breakers={breakers.data}
        loading={breakers.isLoading}
        error={breakers.error}
        onRetry={() => breakers.refetch()}
      />
    </div>
  );
}
