import { createFileRoute } from "@tanstack/react-router";
import { CheckCircle2, KeyRound, TimerReset } from "lucide-react";
import { PageHeader } from "@/components/layout/page-header";
import { ErrorState, Skeleton, StatCard } from "@/components/ui";
import { CreateGrpcTokenDialog, GrpcTokenTable, useGrpcTokens } from "@/features/grpc-tokens";

export const Route = createFileRoute("/grpc-tokens")({
  component: GrpcTokensPage,
});

/** Tokens this close to expiry are worth an operator's attention today. */
const EXPIRY_WARNING_DAYS = 30;

function GrpcTokensPage() {
  const { data, isLoading, error } = useGrpcTokens();

  // `null` means this server has no gRPC door; the nav hides the page, but a
  // bookmark can still land here.
  const tokens = data ?? [];
  const active = tokens.filter((token) => token.status === "active");
  const expiringSoon = active.filter(
    (token) => token.expires_at - Date.now() < EXPIRY_WARNING_DAYS * 86_400_000,
  ).length;

  return (
    <div className="flex flex-col gap-[var(--page-gap)]">
      <PageHeader
        eyebrow="Configuration"
        title="gRPC tokens"
        description="Credentials the gRPC door accepts. Each is scoped to what a client actually needs, bound to this server's namespace, and revocable on its own — a revoked token stops working on its next call, with no restart."
        // `null` is a server with no gRPC door, `undefined` is still loading.
        // Neither should offer an action whose route would 404.
        actions={data ? <CreateGrpcTokenDialog /> : undefined}
      />

      <div className="grid gap-[var(--gap)] grid-cols-[repeat(auto-fit,minmax(186px,1fr))]">
        <StatCard label="Tokens" tone="neutral" icon={<KeyRound />} value={tokens.length} />
        <StatCard label="Active" tone="success" icon={<CheckCircle2 />} value={active.length} />
        <StatCard
          label="Expiring soon"
          tone={expiringSoon > 0 ? "warning" : "neutral"}
          icon={<TimerReset />}
          value={expiringSoon}
          hint={`within ${EXPIRY_WARNING_DAYS} days`}
        />
      </div>

      {isLoading ? (
        <Skeleton className="h-48" />
      ) : error ? (
        <ErrorState
          title="Failed to load tokens"
          description={error instanceof Error ? error.message : String(error)}
        />
      ) : data === null ? (
        <ErrorState
          title="This server has no gRPC door"
          description="gRPC tokens are issued by flexiq-server when FLEXIQ_GRPC_LISTEN is set. Nothing here applies to this deployment."
        />
      ) : (
        <GrpcTokenTable tokens={tokens} />
      )}
    </div>
  );
}
