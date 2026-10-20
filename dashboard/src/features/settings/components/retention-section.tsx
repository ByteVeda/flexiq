import { Trash2 } from "lucide-react";
import {
  Badge,
  Callout,
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
  EmptyState,
  ErrorState,
  Skeleton,
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui";
import { useCanOperate } from "@/features/auth";
import { formatRelative } from "@/lib/time";
import { formatRetentionWindow, RETENTION_TABLES } from "../derived";
import { useRetention, useRetentionDryRun, useRetentionOverride } from "../hooks";
import type { RetentionCounts, RetentionSnapshot } from "../types";
import { RetentionOverrideEditor } from "./retention-override-editor";

/** On / off / unreported — the three states an operator has to tell apart. */
function RetentionStatus({ snapshot }: { snapshot: RetentionSnapshot }) {
  if (!snapshot.reported) return <Badge tone="neutral">Not reported</Badge>;
  return snapshot.enabled ? (
    <Badge tone="warning" dot>
      Deleting history
    </Badge>
  ) : (
    <Badge tone="neutral" dot>
      Disabled
    </Badge>
  );
}

function WindowTable({ snapshot }: { snapshot: RetentionSnapshot }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead>Table</TableHead>
          <TableHead>Deleted after</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {RETENTION_TABLES.map(({ key, label, description }) => (
          <TableRow key={key}>
            <TableCell>
              <div className="font-medium text-[var(--fg)]">{label}</div>
              <div className="mt-0.5 text-xs text-[var(--fg-muted)]">{description}</div>
            </TableCell>
            <TableCell className="whitespace-nowrap tabular-nums">
              {formatRetentionWindow(snapshot.windows[key])}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

/**
 * How many rows a purge would delete right now, without deleting. Computed
 * in-process against the dashboard's default windows, so it always answers —
 * a way to gauge the blast radius before an operator sizes a window. Degrades
 * to nothing on its own error; the windows above carry the primary error state.
 */
function PurgePreview() {
  const { data, isLoading, error } = useRetentionDryRun();

  if (isLoading) return <Skeleton className="h-40 rounded-[var(--card-radius)]" />;
  if (error || !data) return null;

  return (
    <div className="flex flex-col gap-2 border-t border-[var(--border)] pt-[var(--gap)]">
      <div className="flex items-baseline justify-between gap-3">
        <div className="text-sm font-medium text-[var(--fg)]">Purge preview</div>
        <div className="text-xs text-[var(--fg-muted)]">
          {data.total === 0
            ? "Nothing to delete right now"
            : `${data.total.toLocaleString()} rows would be deleted now`}
        </div>
      </div>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Table</TableHead>
            <TableHead className="whitespace-nowrap">Window used</TableHead>
            <TableHead className="text-right">Would delete now</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {RETENTION_TABLES.map(({ key, label }) => {
            const countKey = key.slice(0, -"_ttl_ms".length) as keyof RetentionCounts;
            return (
              <TableRow key={key}>
                <TableCell className="font-medium text-[var(--fg)]">{label}</TableCell>
                <TableCell className="whitespace-nowrap tabular-nums">
                  {formatRetentionWindow(data.windows[key])}
                </TableCell>
                <TableCell className="text-right tabular-nums">
                  {data.counts[countKey].toLocaleString()}
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>
      <div className="text-xs text-[var(--fg-muted)]">
        Previews {data.defaulted ? "the recommended default windows" : "the reported policy"} ·
        namespace <span className="font-medium text-[var(--fg)]">{data.namespace}</span> · snapshot{" "}
        {formatRelative(data.reference_time)}
      </div>
    </div>
  );
}

/** Where the applied windows came from; absent on servers without overrides. */
function SourceBadge({ snapshot }: { snapshot: RetentionSnapshot }) {
  if (!snapshot.reported || !snapshot.source) return null;
  return (
    <Badge tone={snapshot.source === "dashboard" ? "info" : "neutral"}>
      {snapshot.source === "dashboard" ? "Set from the dashboard" : "From code config"}
    </Badge>
  );
}

/**
 * Why rows vanish from the job, log, and dead-letter listings. Retention runs
 * in the worker process, so these are the windows the elected cleaner reported.
 * An operator can switch them to windows set here; the cleaner picks that up on
 * its next sweep.
 */
export function RetentionSection() {
  const { data, isLoading, error, refetch } = useRetention();
  const stored = useRetentionOverride();
  const canOperate = useCanOperate();

  return (
    <Card>
      <CardHeader>
        <div className="flex items-start justify-between gap-3">
          <div>
            <CardTitle>Retention</CardTitle>
            <CardDescription>
              How long each history table keeps a row before auto-cleanup deletes it. Set in the
              worker's code config or here; reported by the worker that enforces it.
            </CardDescription>
          </div>
          {data ? (
            <div className="flex flex-wrap justify-end gap-2">
              <SourceBadge snapshot={data} />
              <RetentionStatus snapshot={data} />
            </div>
          ) : null}
        </div>
      </CardHeader>
      <CardContent className="flex flex-col gap-[var(--gap)]">
        {isLoading ? (
          <Skeleton className="h-64 rounded-[var(--card-radius)]" />
        ) : error || !data ? (
          // Never a bare skeleton: a failed read must say so and offer a retry,
          // not spin forever looking like a policy that is still loading.
          <ErrorState
            title="Couldn't load the retention windows"
            description={error instanceof Error ? error.message : "The backend did not respond."}
            onRetry={() => refetch()}
          />
        ) : !data.reported ? (
          <EmptyState
            icon={Trash2}
            title="No worker has reported its retention windows"
            description="The worker elected to run cleanup publishes them on its first sweep. Until then the active policy is unknown — not necessarily off."
          />
        ) : (
          <>
            {data.defaulted ? (
              <Callout tone="warning">
                Running on the recommended defaults — no windows were configured. History is being
                deleted on the schedule below.
              </Callout>
            ) : null}
            {!data.enabled ? (
              <Callout tone="info">
                Retention is disabled: no table has a window, so history is kept until you delete
                it. Jobs and dead letters carry their own per-entry TTLs, which still apply.
              </Callout>
            ) : (
              <WindowTable snapshot={data} />
            )}
            <div className="text-xs text-[var(--fg-muted)]">
              Namespace <span className="font-medium text-[var(--fg)]">{data.namespace}</span> ·
              reported {formatRelative(data.reported_at)}
            </div>
          </>
        )}
        {data && canOperate && stored.data ? (
          // Remounted per save, so the form reopens on what the server stored.
          <RetentionOverrideEditor
            key={stored.data.updated_at ?? "unsaved"}
            snapshot={data}
            stored={stored.data}
          />
        ) : null}
        {/* Independent of the echo above: computed in-process, so it renders
            even before a worker has reported its windows. */}
        {!isLoading && !error ? <PurgePreview /> : null}
      </CardContent>
    </Card>
  );
}
