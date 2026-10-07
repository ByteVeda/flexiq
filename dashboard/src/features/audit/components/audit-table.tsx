import { Link } from "@tanstack/react-router";
import { ShieldCheck } from "lucide-react";
import { useMemo } from "react";
import { Badge, DataTable, type DataTableColumn, EmptyState } from "@/components/ui";
import { formatAbsolute, formatRelative } from "@/lib/time";
import type { AuditRecord } from "../types";
import { describePrincipal, isOk } from "../utils";
import { AuditTarget } from "./audit-target";

const TIME_CELL = "font-mono text-[0.82rem] tabular-nums text-[var(--fg-muted)]";

interface Props {
  records: AuditRecord[];
}

export function AuditTable({ records }: Props) {
  const columns = useMemo<DataTableColumn<AuditRecord>[]>(
    () => [
      {
        accessorKey: "at",
        header: "When",
        cell: ({ row }) => (
          <span className={TIME_CELL} title={formatAbsolute(row.original.at)}>
            {formatRelative(row.original.at)}
          </span>
        ),
      },
      {
        id: "principal",
        header: "Who",
        cell: ({ row }) => {
          const record = row.original;
          const label = describePrincipal(record);
          return (
            <div className="flex items-center gap-1.5">
              <Badge tone="neutral">{record.principal_kind}</Badge>
              {/* Kinds with an id narrow the trail to that principal. */}
              {label === null ? null : (
                <Link
                  to="/audit"
                  search={{ principalKind: record.principal_kind, tokenId: record.token_id }}
                  className="text-[var(--fg)] hover:underline"
                >
                  {label}
                </Link>
              )}
            </div>
          );
        },
      },
      {
        accessorKey: "operation",
        header: "Operation",
        cell: ({ getValue }) => (
          <span className="font-mono text-[0.8rem] text-[var(--fg)]">{getValue<string>()}</span>
        ),
      },
      {
        id: "target",
        header: "Target",
        cell: ({ row }) => {
          const { target_kind, target } = row.original;
          // A call refused before its target was known names none.
          if (!target_kind || !target) {
            return <span className="text-[var(--fg-subtle)]">—</span>;
          }
          return (
            <div className="flex items-center gap-1.5">
              <span className="text-xs text-[var(--fg-subtle)]">
                {target_kind.replaceAll("_", " ")}
              </span>
              <AuditTarget kind={target_kind} target={target} />
            </div>
          );
        },
      },
      {
        accessorKey: "outcome",
        header: "Outcome",
        cell: ({ getValue }) => {
          const outcome = getValue<string>();
          return (
            <Badge tone={isOk(outcome) ? "success" : "danger"} dot>
              {outcome}
            </Badge>
          );
        },
      },
    ],
    [],
  );

  if (records.length === 0) {
    return (
      <EmptyState
        icon={ShieldCheck}
        title="No matching records"
        description="Nothing in the trail matches these filters. Records older than the retention window (FLEXIQ_AUDIT_RETENTION_DAYS) are pruned."
      />
    );
  }

  return <DataTable columns={columns} data={records} rowKey={(record) => record.id} />;
}
