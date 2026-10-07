import { Link } from "@tanstack/react-router";
import { History, MoreHorizontal, ShieldOff } from "lucide-react";
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
  // A revoked token cannot be revoked again, but what it did while valid is
  // the question a revocation usually raises — so the trail stays one click away.
  const revoked = token.revoked_at !== null;

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="ghost" size="icon" aria-label="Token actions">
            <MoreHorizontal className="size-4" aria-hidden />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="w-48">
          <DropdownMenuItem asChild>
            <Link
              to="/audit"
              search={{ principalKind: "token", tokenId: token.id }}
              className="flex w-full cursor-default items-center gap-2"
            >
              <History aria-hidden /> What this token did
            </Link>
          </DropdownMenuItem>
          {revoked ? null : (
            <DropdownMenuItem
              onClick={() => setConfirm(true)}
              className="text-danger focus:text-danger"
            >
              <ShieldOff aria-hidden /> Revoke
            </DropdownMenuItem>
          )}
        </DropdownMenuContent>
      </DropdownMenu>

      {revoked ? null : (
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
      )}
    </>
  );
}
