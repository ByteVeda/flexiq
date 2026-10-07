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
import { cn } from "@/lib/cn";
import type { AuditFilters, PrincipalKind } from "../types";
import { countActiveFilters, PRINCIPAL_KINDS, TARGET_KINDS } from "../utils";

const ANY = "any";

interface Props {
  filters: AuditFilters;
  onChange: (filters: AuditFilters) => void;
  className?: string;
}

/**
 * Filter bar for the audit trail — the same filters `ListAuditRecords` takes.
 * Free text pushes through after a 300ms idle window; selects and the time
 * range apply at once.
 */
export function AuditFiltersBar({ filters, onChange, className }: Props) {
  const [local, setLocal] = useState(() => ({
    tokenId: filters.tokenId ?? "",
    target: filters.target ?? "",
  }));

  // Reflect URL navigation (a clicked principal, Clear) back into the inputs.
  useEffect(() => {
    setLocal({ tokenId: filters.tokenId ?? "", target: filters.target ?? "" });
  }, [filters.tokenId, filters.target]);

  const debouncedTokenId = useDebouncedValue(local.tokenId, 300);
  const debouncedTarget = useDebouncedValue(local.target, 300);

  // Refs keep the propagation effect keyed on the debounced values only.
  const filtersRef = useRef(filters);
  const onChangeRef = useRef(onChange);
  useEffect(() => {
    filtersRef.current = filters;
    onChangeRef.current = onChange;
  });

  useEffect(() => {
    const current = filtersRef.current;
    const tokenId = debouncedTokenId.trim() || undefined;
    const target = debouncedTarget.trim() || undefined;
    if (tokenId !== current.tokenId || target !== current.target) {
      onChangeRef.current({ ...current, tokenId, target });
    }
  }, [debouncedTokenId, debouncedTarget]);

  const activeCount = countActiveFilters(filters);

  return (
    <div
      className={cn(
        "flex flex-col gap-3 rounded-[var(--card-radius)] border border-[var(--border)] bg-[var(--surface)] p-3 shadow-[var(--card-shadow)]",
        className,
      )}
    >
      <div className="grid gap-2 md:grid-cols-4">
        <Select
          value={filters.principalKind ?? ANY}
          onValueChange={(v) =>
            onChange({ ...filters, principalKind: v === ANY ? undefined : (v as PrincipalKind) })
          }
        >
          <SelectTrigger aria-label="Principal kind">
            <SelectValue placeholder="Principal kind" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ANY}>Any principal</SelectItem>
            {PRINCIPAL_KINDS.map((kind) => (
              <SelectItem key={kind} value={kind}>
                {kind}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Input
          value={local.tokenId}
          onChange={(e) => setLocal((p) => ({ ...p, tokenId: e.target.value }))}
          placeholder="Token id or username"
          aria-label="Token id or username"
        />
        <Select
          value={filters.targetKind ?? ANY}
          onValueChange={(v) => onChange({ ...filters, targetKind: v === ANY ? undefined : v })}
        >
          <SelectTrigger aria-label="Target kind">
            <SelectValue placeholder="Target kind" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ANY}>Any target</SelectItem>
            {TARGET_KINDS.map((kind) => (
              <SelectItem key={kind} value={kind}>
                {kind.replaceAll("_", " ")}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        <Input
          value={local.target}
          onChange={(e) => setLocal((p) => ({ ...p, target: e.target.value }))}
          placeholder="Target id or name"
          aria-label="Target id or name"
        />
      </div>

      <div className="grid gap-2 md:grid-cols-[1fr_1fr_auto]">
        <DateTimeInput
          label="Since"
          value={filters.since}
          onChange={(ts) => onChange({ ...filters, since: ts })}
        />
        <DateTimeInput
          label="Until"
          value={filters.until}
          onChange={(ts) => onChange({ ...filters, until: ts })}
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
