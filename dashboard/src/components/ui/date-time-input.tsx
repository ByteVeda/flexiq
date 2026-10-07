interface DateTimeInputProps {
  label: string;
  /** Unix milliseconds, or `undefined` for no value. */
  value: number | undefined;
  onChange: (ts: number | undefined) => void;
}

/** A labelled local date-time picker that reads and writes Unix milliseconds. */
export function DateTimeInput({ label, value, onChange }: DateTimeInputProps) {
  // Backend stores unix milliseconds; <input type="datetime-local"> uses "YYYY-MM-DDTHH:mm".
  const localValue = value ? msToLocalDatetime(value) : "";
  return (
    <label className="flex flex-col gap-1 text-xs text-[var(--fg-subtle)]">
      <span>{label}</span>
      <input
        type="datetime-local"
        value={localValue}
        onChange={(e) => {
          const raw = e.target.value;
          if (!raw) {
            onChange(undefined);
            return;
          }
          const ms = new Date(raw).getTime();
          if (!Number.isFinite(ms)) return;
          onChange(ms);
        }}
        className="h-9 rounded-md bg-[var(--surface)] px-3 text-sm ring-1 ring-inset ring-[var(--border-strong)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--color-ring)]"
      />
    </label>
  );
}

function msToLocalDatetime(ms: number): string {
  const date = new Date(ms);
  const pad = (n: number) => String(n).padStart(2, "0");
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}T${pad(date.getHours())}:${pad(date.getMinutes())}`;
}
