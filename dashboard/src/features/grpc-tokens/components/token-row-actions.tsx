import { MoreHorizontal, ShieldOff } from "lucide-react";
import { useState } from "react";
import {
  Button,
  DestructiveConfirmDialog,
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui";
import { useRevokeGrpcToken } from "../hooks";
import type { GrpcToken } from "../types";

interface Props {
  token: GrpcToken;
}

export function GrpcTokenRowActions({ token }: Props) {
  const revoke = useRevokeGrpcToken();
  const [confirm, setConfirm] = useState(false);

  // A revoked token has nothing left to do to it. The row stays, because it is
  // the record that the credential existed and who used it.
  if (token.revoked_at !== null) return null;

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="ghost" size="icon" aria-label="Token actions">
            <MoreHorizontal className="size-4" aria-hidden />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="w-48">
          <DropdownMenuItem
            onClick={() => setConfirm(true)}
            className="text-danger focus:text-danger"
          >
            <ShieldOff aria-hidden /> Revoke
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>

      <DestructiveConfirmDialog
        open={confirm}
        onOpenChange={setConfirm}
        title={`Revoke "${token.name}"?`}
        description="Any client presenting this token starts failing on its next call. This cannot be undone — issue a new token instead."
        confirmLabel="Revoke"
        confirmPhrase="revoke"
        pending={revoke.isPending}
        onConfirm={async () => {
          await revoke.mutateAsync(token.id);
        }}
      />
    </>
  );
}
