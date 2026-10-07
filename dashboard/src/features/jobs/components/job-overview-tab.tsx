import { ExternalLink as ExternalLinkIcon } from "lucide-react";
import { type ReactNode, useMemo } from "react";
import {
  buttonVariants,
  Card,
  CardContent,
  CardHeader,
  CardTitle,
  TaskErrorBlock,
} from "@/components/ui";
import { applyJobContext, useIntegrations } from "@/features/settings";
import type { Job } from "@/lib/api-types";
import { cn } from "@/lib/cn";
import { formatAbsolute, formatDuration, formatRelative } from "@/lib/time";

interface JobOverviewTabProps {
  job: Job;
}

export function JobOverviewTab({ job }: JobOverviewTabProps) {
  const startedToCompleted =
    job.started_at && job.completed_at ? job.completed_at - job.started_at : null;

  return (
    <div className="grid gap-4 lg:grid-cols-2">
      <JobIntegrations job={job} />
      <Card>
        <CardHeader className="pb-2">
          <CardTitle>Identity</CardTitle>
        </CardHeader>
        <CardContent>
          <Dl>
            <Row label="Task">{job.task_name}</Row>
            <Row label="Queue">{job.queue}</Row>
            <Row label="Priority">{job.priority}</Row>
            {job.unique_key ? <Row label="Unique key">{job.unique_key}</Row> : null}
            {job.enqueued_by ? <Row label="Enqueued by">{job.enqueued_by}</Row> : null}
          </Dl>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle>Execution</CardTitle>
        </CardHeader>
        <CardContent>
          <Dl>
            <Row label="Retries">
              {job.retry_count}
              {job.max_retries > 0 ? ` / ${job.max_retries}` : null}
            </Row>
            {job.progress != null ? (
              <Row label="Progress">{(job.progress * 100).toFixed(0)}%</Row>
            ) : null}
            <Row label="Timeout">{formatDuration(job.timeout_ms)}</Row>
            {startedToCompleted != null ? (
              <Row label="Duration">{formatDuration(startedToCompleted)}</Row>
            ) : null}
          </Dl>
        </CardContent>
      </Card>

      <Card>
        <CardHeader className="pb-2">
          <CardTitle>Timeline</CardTitle>
        </CardHeader>
        <CardContent>
          <Dl>
            <Row label="Created">
              <Timestamp ms={job.created_at} />
            </Row>
            <Row label="Scheduled">
              <Timestamp ms={job.scheduled_at} />
            </Row>
            {job.started_at ? (
              <Row label="Started">
                <Timestamp ms={job.started_at} />
              </Row>
            ) : null}
            {job.completed_at ? (
              <Row label="Completed">
                <Timestamp ms={job.completed_at} />
              </Row>
            ) : null}
          </Dl>
        </CardContent>
      </Card>

      {job.metadata ? (
        <Card>
          <CardHeader className="pb-2">
            <CardTitle>Metadata</CardTitle>
          </CardHeader>
          <CardContent>
            <pre className="max-h-60 overflow-auto rounded-md bg-[var(--surface-2)] p-3 font-mono text-[11px] text-[var(--fg)]">
              {tryPrettyJson(job.metadata)}
            </pre>
          </CardContent>
        </Card>
      ) : null}

      <NotesCard raw={job.notes} />

      {job.error ? (
        <Card className="lg:col-span-2">
          <CardHeader className="pb-2">
            <CardTitle>Last error</CardTitle>
          </CardHeader>
          <CardContent>
            <TaskErrorBlock error={job.error} className="max-h-80 bg-danger-dim/40 p-3" />
          </CardContent>
        </Card>
      ) : null}
    </div>
  );
}

function Dl({ children }: { children: ReactNode }) {
  return <dl className="grid grid-cols-[140px_1fr] gap-y-2 text-sm">{children}</dl>;
}

function Row({ label, children }: { label: string; children: ReactNode }) {
  return (
    <>
      <dt className="text-xs uppercase tracking-wider text-[var(--fg-subtle)]">{label}</dt>
      <dd className="text-[var(--fg)]">{children}</dd>
    </>
  );
}

function Timestamp({ ms }: { ms: number }) {
  return (
    <span className="inline-flex flex-col">
      <span className="tabular-nums">{formatAbsolute(ms)}</span>
      <span className="text-xs text-[var(--fg-subtle)]">{formatRelative(ms)}</span>
    </span>
  );
}

function tryPrettyJson(raw: string): string {
  try {
    return JSON.stringify(JSON.parse(raw), null, 2);
  } catch {
    return raw;
  }
}

/**
 * Render the structured ``notes`` blob as a fixed-size key/value table.
 *
 * The contract on the server side caps notes at 15 top-level keys, so this
 * always fits in a card. Non-string leaves are stringified via
 * ``JSON.stringify`` to keep the renderer total. If the raw payload
 * can't be parsed (e.g. the column was tampered with out of band), we
 * fall back to the raw text in a `<pre>` so the operator can still see
 * something useful.
 */
function NotesCard({ raw }: { raw: string | null }) {
  if (!raw) return null;

  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    return (
      <Card>
        <CardHeader className="pb-2">
          <CardTitle>Notes</CardTitle>
        </CardHeader>
        <CardContent>
          <pre className="max-h-60 overflow-auto rounded-md bg-[var(--surface-2)] p-3 font-mono text-[11px] text-[var(--fg)]">
            {raw}
          </pre>
        </CardContent>
      </Card>
    );
  }

  if (
    parsed === null ||
    typeof parsed !== "object" ||
    Array.isArray(parsed) ||
    Object.keys(parsed).length === 0
  ) {
    return null;
  }

  const entries = Object.entries(parsed as Record<string, unknown>);
  return (
    <Card>
      <CardHeader className="pb-2">
        <CardTitle>Notes</CardTitle>
      </CardHeader>
      <CardContent>
        <Dl>
          {entries.map(([key, value]) => (
            <Row key={key} label={key}>
              <span className="break-all">
                {typeof value === "string" ? value : JSON.stringify(value)}
              </span>
            </Row>
          ))}
        </Dl>
      </CardContent>
    </Card>
  );
}

/**
 * Render configured integration shortcuts (Grafana / Sentry / OTel) for
 * the given job. Each URL may contain a ``{job_id}`` placeholder; if it
 * doesn't, the configured URL opens as-is. Renders nothing when no
 * integration is configured.
 */
function JobIntegrations({ job }: { job: Job }) {
  const integrations = useIntegrations();
  const links = useMemo(
    () =>
      (
        [
          { label: "Open in Grafana", href: integrations.grafana },
          { label: "Open in Sentry", href: integrations.sentry },
          { label: "Open in OTel", href: integrations.otel },
        ] as const
      ).filter((entry) => entry.href),
    [integrations.grafana, integrations.sentry, integrations.otel],
  );

  if (links.length === 0) return null;

  return (
    <Card className="lg:col-span-2">
      <CardHeader className="pb-2">
        <CardTitle>Integrations</CardTitle>
      </CardHeader>
      <CardContent className="flex flex-wrap gap-2">
        {links.map(({ label, href }) => (
          <a
            key={label}
            href={applyJobContext(href, job.id)}
            target="_blank"
            rel="noreferrer noopener"
            className={cn(buttonVariants({ variant: "outline", size: "sm" }))}
          >
            <ExternalLinkIcon className="size-4" aria-hidden />
            {label}
          </a>
        ))}
      </CardContent>
    </Card>
  );
}
