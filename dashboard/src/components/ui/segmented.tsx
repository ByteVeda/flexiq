import type { ReactNode } from "react";
import { cn } from "@/lib/cn";

export interface SegmentedOption<T extends string> {
  value: T;
  label: ReactNode;
}

interface SegmentedProps<T extends string> {
  options: SegmentedOption<T>[];
  value: T;
  onChange: (value: T) => void;
  "aria-label"?: string;
  className?: string;
}

/** A segmented single-select control (status / level / window filters). */
export function Segmented<T extends string>({
  options,
  value,
  onChange,
  className,
  "aria-label": ariaLabel,
}: SegmentedProps<T>) {
  return (
    <fieldset
      className={cn(
        // `relative` anchors the sr-only legend: left to the nearest positioned
        // ancestor it escapes the scroll container and stretches the page.
        "relative m-0 inline-flex min-w-0 overflow-hidden rounded-[var(--btn-radius)] border border-[var(--border-strong)] p-0",
        className,
      )}
    >
      {ariaLabel ? <legend className="sr-only">{ariaLabel}</legend> : null}
      {options.map((option, i) => {
        const active = option.value === value;
        return (
          <button
            key={option.value}
            type="button"
            aria-pressed={active}
            onClick={() => onChange(option.value)}
            className={cn(
              "h-[34px] px-[13px] text-[0.8rem] font-medium transition-colors",
              i > 0 && "border-l border-[var(--border)]",
              active
                ? "bg-accent-dim font-semibold text-[var(--accent-ink)]"
                : "bg-[var(--surface)] text-[var(--fg-muted)] hover:text-[var(--fg)]",
            )}
          >
            {option.label}
          </button>
        );
      })}
    </fieldset>
  );
}
