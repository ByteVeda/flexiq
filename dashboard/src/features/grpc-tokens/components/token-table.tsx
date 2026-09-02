import { KeyRound } from "lucide-react";
import { useMemo } from "react";
import { Badge, DataTable, type DataTableColumn, EmptyState } from "@/components/ui";
import { formatAbsolute, formatRelative } from "@/lib/time";
import type { GrpcToken, GrpcTokenStatus } from "../types";
import { GrpcTokenRowActions } from "./token-row-actions";

const TIME_CELL =
  "block w-full text-right font-mono text-[0.82rem] tabular-nums text-[var(--fg-muted)]";

/** How close to expiry a token has to be before the column says so in colour. */
const EXPIRY_WARNING_DAYS = 30;

const STATUS_TONE: Record<GrpcTokenStatus, "success" | "warning" | "danger"> = {
  active: "success",
  expired: "warning",
  revoked: "danger",
};

interface Props {
  tokens: GrpcToken[];
}

export function GrpcTokenTable({ tokens }: Props) {
  const columns = useMemo<DataTableColumn<GrpcToken>[]>(
    () => [
      {
        accessorKey: "name",
        header: "Name",
        cell: ({ row }) => (
          <div className="flex flex-col gap-0.5">
            <span className="text-[var(--fg)]">{row.original.name}</span>
            {/* The id is public and is what `revoke` takes, so it is next to the
                name rather than in a column an operator has to go looking for. */}
            <span className="font-mono text-[0.7rem] text-[var(--fg-subtle)]">
              {row.original.id}
            </span>
          </div>
        ),
      },
      {
        accessorKey: "scopes",
        header: "Scopes",
        cell: ({ getValue }) => (
          <div className="flex flex-wrap gap-1">
            {getValue<string[]>().map((scope) => (
              <Badge key={scope} tone="neutral">
                {scope}
              </Badge>
            ))}
          </div>
        ),
      },
      {
        accessorKey: "status",
        header: "Status",
        cell: ({ row }) => (
          <Badge tone={STATUS_TONE[row.original.status]} dot>
            {row.original.status}
          </Badge>
        ),
      },
      {
        accessorKey: "created_by",
        header: "Created by",
        cell: ({ getValue }) => {
          const by = getValue<string | null>();
          return (
            <span className="text-xs text-[var(--fg-muted)]">
              {by ?? <span className="text-[var(--fg-subtle)]">—</span>}
            </span>
          );
        },
      },
      {
        accessorKey: "last_used_at",
        header: "Last used",
        cell: ({ getValue }) => {
          const at = getValue<number | null>();
          // Never used is a real answer, not a missing one: it is how an
          // operator finds the credential nobody needed.
          if (at === null) {
            return <span className={TIME_CELL}>never</span>;
          }
          return (
            <span className={TIME_CELL} title={formatAbsolute(at)}>
              {formatRelative(at)}
            </span>
          );
        },
      },
      {
        accessorKey: "expires_at",
        header: "Expires",
        cell: ({ row }) => {
          const { expires_at, status } = row.original;
          const soon =
            status === "active" && expires_at - Date.now() < EXPIRY_WARNING_DAYS * 86_400_000;
          return (
            <span
              className={soon ? `${TIME_CELL} !text-warning` : TIME_CELL}
              title={formatAbsolute(expires_at)}
            >
              {formatRelative(expires_at)}
            </span>
          );
        },
      },
      {
        id: "actions",
        header: "",
        cell: ({ row }) => <GrpcTokenRowActions token={row.original} />,
      },
    ],
    [],
  );

  if (tokens.length === 0) {
    return (
      <EmptyState
        icon={KeyRound}
        title="No tokens yet"
        description="The gRPC door refuses every call until one exists. Create one here, or run `flexiq-server token create` where the server can reach its database."
      />
    );
  }

  return <DataTable columns={columns} data={tokens} rowKey={(token) => token.id} />;
}
