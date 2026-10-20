import { useState } from "react";
import { Button, ConfirmDialog, Input, Segmented } from "@/components/ui";
import { formatRelative } from "@/lib/time";
import { formatRetentionWindow, RETENTION_TABLES } from "../derived";
import { useClearRetentionOverride, useSaveRetentionOverride } from "../hooks";
import {
  daysFromWindows,
  type RetentionKey,
  shortenedTables,
  type WindowDays,
  windowsFromDays,
} from "../retention-form";
import type {
  RetentionOverride,
  RetentionSnapshot,
  RetentionSource,
  RetentionWindows,
} from "../types";

const SOURCES: { value: RetentionSource; label: string }[] = [
  { value: "code", label: "Code config" },
  { value: "dashboard", label: "Dashboard" },
];

interface RetentionOverrideEditorProps {
  snapshot: RetentionSnapshot;
  stored: RetentionOverride;
}

/**
 * The toggle between the worker's code config and windows set here. The
 * dashboard's windows are kept while the toggle sits on code, so switching
 * back restores the last save rather than an empty form.
 */
export function RetentionOverrideEditor({ snapshot, stored }: RetentionOverrideEditorProps) {
  const codeWindows = snapshot.code_windows ?? null;
  // A first edit starts from what applies today, not from an empty form.
  const startFrom = stored.updated_at !== null ? stored.windows : (codeWindows ?? snapshot.windows);
  const [source, setSource] = useState<RetentionSource>(stored.source);
  const [days, setDays] = useState<WindowDays>(() => daysFromWindows(startFrom));
  const [confirming, setConfirming] = useState<RetentionKey[] | null>(null);
  const save = useSaveRetentionOverride();
  const clear = useClearRetentionOverride();

  const parsed = windowsFromDays(days);
  const pending = save.isPending || clear.isPending;

  function submit(windows: RetentionWindows) {
    save.mutate({ source, windows });
  }

  function onSave() {
    if (!parsed.ok) return;
    // Code mode stores the windows without applying them: nothing to confirm.
    const shortened =
      source === "dashboard" ? shortenedTables(snapshot.windows, parsed.windows) : [];
    if (shortened.length > 0) {
      setConfirming(shortened);
      return;
    }
    submit(parsed.windows);
  }

  return (
    <div className="flex flex-col gap-3 border-t border-[var(--border)] pt-[var(--gap)]">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <div className="text-sm font-medium text-[var(--fg)]">Windows applied</div>
          <div className="text-xs text-[var(--fg-muted)]">
            Takes effect on the next cleanup sweep, within a few minutes.
            {stored.updated_at !== null ? ` Last saved ${formatRelative(stored.updated_at)}.` : ""}
          </div>
        </div>
        <Segmented
          aria-label="Retention source"
          options={SOURCES}
          value={source}
          onChange={setSource}
        />
      </div>

      <div className="grid gap-2">
        {RETENTION_TABLES.map(({ key, label }) => {
          const invalid = !parsed.ok && parsed.key === key;
          return (
            <div
              key={key}
              className="grid grid-cols-[minmax(0,1fr)_8rem_minmax(0,10rem)] items-center gap-3"
            >
              <span className="text-sm text-[var(--fg)]">{label}</span>
              <Input
                inputMode="decimal"
                placeholder="Forever"
                aria-invalid={invalid}
                aria-label={`${label} window in days`}
                value={days[key]}
                disabled={source === "code" || pending}
                onChange={(event) => setDays({ ...days, [key]: event.target.value })}
                className={invalid ? "ring-[var(--danger)]" : undefined}
              />
              <span className="text-xs text-[var(--fg-muted)]">
                {source === "code"
                  ? `Code: ${codeWindows ? formatRetentionWindow(codeWindows[key]) : "unknown"}`
                  : "days"}
              </span>
            </div>
          );
        })}
      </div>
      {!parsed.ok ? (
        <div role="alert" className="text-xs text-[var(--danger)]">
          {RETENTION_TABLES.find((table) => table.key === parsed.key)?.label}: {parsed.error}
        </div>
      ) : null}

      <div className="flex flex-wrap justify-end gap-2">
        {stored.updated_at !== null ? (
          <Button variant="ghost" size="sm" onClick={() => clear.mutate()} disabled={pending}>
            Forget dashboard windows
          </Button>
        ) : null}
        <Button size="sm" onClick={onSave} disabled={!parsed.ok || pending}>
          Save
        </Button>
      </div>

      <ConfirmDialog
        open={confirming !== null}
        onOpenChange={(open) => !open && setConfirming(null)}
        variant="danger"
        title="Delete history sooner?"
        description={`The next sweep deletes older rows from ${(confirming ?? [])
          .map((key) => RETENTION_TABLES.find((table) => table.key === key)?.label)
          .join(", ")}. Deleted rows cannot be recovered.`}
        confirmLabel="Save and apply"
        pending={save.isPending}
        onConfirm={() => {
          if (parsed.ok) submit(parsed.windows);
        }}
      />
    </div>
  );
}
