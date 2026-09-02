import { Link } from "@tanstack/react-router";
import { Eye, History, MoreHorizontal, RotateCcw, Trash2 } from "lucide-react";
import { useState } from "react";
import {
  Button,
  ConfirmDialog,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
  SecretReveal,
} from "@/components/ui";
import { DestructiveConfirmDialog } from "@/components/ui/destructive-confirm-dialog";
import { useDeleteWebhook, useRotateSecret } from "../hooks";
import type { Webhook } from "../types";

interface Props {
  webhook: Webhook;
}

export function WebhookRowActions({ webhook }: Props) {
  const remove = useDeleteWebhook();
  const rotate = useRotateSecret();

  const [confirmDelete, setConfirmDelete] = useState(false);
  const [confirmRotate, setConfirmRotate] = useState(false);
  const [revealedSecret, setRevealedSecret] = useState<string | null>(null);

  function onRotate() {
    rotate.mutate(webhook.id, {
      onSuccess: (result) => {
        setRevealedSecret(result.secret);
      },
    });
  }

  return (
    <>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="ghost" size="icon" aria-label="Webhook actions">
            <MoreHorizontal className="size-4" aria-hidden />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" className="w-48">
          <DropdownMenuItem asChild>
            <Link
              to="/webhooks/$id/deliveries"
              params={{ id: webhook.id }}
              className="flex w-full cursor-default items-center gap-2"
            >
              <History aria-hidden /> View deliveries
            </Link>
          </DropdownMenuItem>
          <DropdownMenuItem onClick={() => setConfirmRotate(true)}>
            <RotateCcw aria-hidden /> Rotate secret
          </DropdownMenuItem>
          <DropdownMenuSeparator />
          <DropdownMenuItem
            onClick={() => setConfirmDelete(true)}
            className="text-danger focus:text-danger"
          >
            <Trash2 aria-hidden /> Delete
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>

      <DestructiveConfirmDialog
        open={confirmDelete}
        onOpenChange={setConfirmDelete}
        title="Delete this webhook?"
        description={`Deliveries to ${webhook.url} will stop immediately.`}
        confirmLabel="Delete"
        confirmPhrase="delete"
        pending={remove.isPending}
        onConfirm={async () => {
          await remove.mutateAsync(webhook.id);
        }}
      />

      <ConfirmDialog
        open={confirmRotate}
        onOpenChange={setConfirmRotate}
        title="Rotate signing secret?"
        description="The previous secret stops working immediately. Update your receiver before continuing — flexiq will sign the next delivery with the new value."
        confirmLabel="Rotate"
        onConfirm={() => {
          setConfirmRotate(false);
          onRotate();
        }}
      />

      <Dialog
        open={revealedSecret !== null}
        onOpenChange={(open) => !open && setRevealedSecret(null)}
      >
        <DialogContent className="sm:max-w-md">
          <DialogHeader>
            <DialogTitle>New signing secret</DialogTitle>
            <DialogDescription>Configure your receiver with this value.</DialogDescription>
          </DialogHeader>
          {revealedSecret ? (
            <SecretReveal
              secret={revealedSecret}
              note="flexiq keeps this secret so it can sign your payloads, but never shows it again. If you lose it, rotate to a new one."
            />
          ) : null}
          <Button onClick={() => setRevealedSecret(null)}>
            <Eye aria-hidden /> Done
          </Button>
        </DialogContent>
      </Dialog>
    </>
  );
}
