import { X } from "lucide-react";
import { useEffect, useRef, useState } from "react";
import {
  Button,
  DateTimeInput,
  Input,
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui";
import { useDebouncedValue } from "@/hooks";
import type { JobStatus } from "@/lib/api-types";
import { cn } from "@/lib/cn";
import { JOB_STATUS_LABEL } from "@/lib/status";
import type { JobFilters } from "../types";
import { countActiveFilters } from "../utils";

const STATUS_OPTIONS: JobStatus[] = [
  "pending",
  "running",
  "complete",
  "failed",
  "dead",
  "cancelled",
];

interface JobFiltersBarProps {
  filters: JobFilters;
  onChange: (filters: JobFilters) => void;
  className?: string;
}

/**
 * Debounced filter bar for the jobs list.
 *
 * Text inputs (queue/task/metadata/error) keep local state and push through
 * after a 300ms idle window; the Status select and date range are applied
 * immediately since they're deliberate clicks. This keeps the API from
 * being hammered while the user is still typing.
 */
export function JobFiltersBar({ filters, onChange, className }: JobFiltersBarProps) {
  const [local, setLocal] = useState(() => ({
    queue: filters.queue ?? "",
    task: filters.task ?? "",
    metadata: filters.metadata ?? "",
    error: filters.error ?? "",
  }));

  // Reflect external changes (e.g., URL navigation) back into local state.
  useEffect(() => {
    setLocal({
      queue: filters.queue ?? "",
      task: filters.task ?? "",
      metadata: filters.metadata ?? "",
      error: filters.error ?? "",
    });
  }, [filters.queue, filters.task, filters.metadata, filters.error]);

  const debouncedQueue = useDebouncedValue(local.queue, 300);
  const debouncedTask = useDebouncedValue(local.task, 300);
  const debouncedMetadata = useDebouncedValue(local.metadata, 300);
  const debouncedError = useDebouncedValue(local.error, 300);

  // Hold the latest filters/onChange in refs so the propagation effect re-runs
  // only when debounced values settle. Listing them as deps would re-fire the
  // effect on every parent render and defeat the debounce.
  const filtersRef = useRef(filters);
  const onChangeRef = useRef(onChange);
  useEffect(() => {
    filtersRef.current = filters;
    onChangeRef.current = onChange;
  });

  useEffect(() => {
    const current = filtersRef.current;
    const next: JobFilters = {
      ...current,
      queue: debouncedQueue || undefined,
      task: debouncedTask || undefined,
      metadata: debouncedMetadata || undefined,
      error: debouncedError || undefined,
    };
    if (
      next.queue !== current.queue ||
      next.task !== current.task ||
      next.metadata !== current.metadata ||
      next.error !== current.error
    ) {
      onChangeRef.current(next);
    }
  }, [debouncedQueue, debouncedTask, debouncedMetadata, debouncedError]);

  const activeCount = countActiveFilters(filters);

  return (
    <div
      className={cn(
        "flex flex-col gap-3 rounded-[var(--card-radius)] border border-[var(--border)] bg-[var(--surface)] p-3 shadow-[var(--card-shadow)]",
        className,
      )}
    >
      <div className="grid gap-2 md:grid-cols-5">
        <Select
          value={filters.status ?? "all"}
          onValueChange={(v) =>
            onChange({ ...filters, status: v === "all" ? undefined : (v as JobStatus) })
          }
        >
          <SelectTrigger aria-label="Status">
            <SelectValue placeholder="Status" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="all">All statuses</SelectItem>
            {STATUS_OPTIONS.map((s) => (
              <SelectItem key={s} value={s}>
                {JOB_STATUS_LABEL[s]}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Input
          value={local.queue}
          onChange={(e) => setLocal((p) => ({ ...p, queue: e.target.value }))}
          placeholder="Queue name"
        />
        <Input
          value={local.task}
          onChange={(e) => setLocal((p) => ({ ...p, task: e.target.value }))}
          placeholder="Task name"
        />
        <Input
          value={local.metadata}
          onChange={(e) => setLocal((p) => ({ ...p, metadata: e.target.value }))}
          placeholder="Metadata contains…"
        />
        <Input
          value={local.error}
          onChange={(e) => setLocal((p) => ({ ...p, error: e.target.value }))}
          placeholder="Error contains…"
        />
      </div>

      <div className="grid gap-2 md:grid-cols-[1fr_1fr_auto]">
        <DateTimeInput
          label="Created after"
          value={filters.createdAfter}
          onChange={(ts) => onChange({ ...filters, createdAfter: ts })}
        />
        <DateTimeInput
          label="Created before"
          value={filters.createdBefore}
          onChange={(ts) => onChange({ ...filters, createdBefore: ts })}
        />
        <Button
          variant="ghost"
          size="sm"
          disabled={activeCount === 0}
          onClick={() => onChange({})}
          className="self-end"
        >
          <X aria-hidden /> Clear {activeCount > 0 ? `(${activeCount})` : null}
        </Button>
      </div>
    </div>
  );
}
