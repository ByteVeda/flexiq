import { AlertCircle, Plus } from "lucide-react";
import { type FormEvent, useState } from "react";
import {
  Button,
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
  Input,
  SecretReveal,
  Stepper,
  Switch,
} from "@/components/ui";
import { ApiError } from "@/lib/api-client";
import { useCreateGrpcToken, useGrpcScopes } from "../hooks";
import type { CreatedGrpcToken } from "../types";

/** Matches the server's default; the server is still the one that enforces it. */
const DEFAULT_DAYS = 90;
/** Matches the server's cap, so the stepper cannot ask for a refusal. */
const MAX_DAYS = 365;

/** What each scope opens, in the operator's words rather than the package's. */
const SCOPE_HELP: Record<string, string> = {
  produce: "Submit, read and cancel work.",
  execute: "Claim work and report on it.",
};

export function CreateGrpcTokenDialog() {
  const [open, setOpen] = useState(false);
  const [name, setName] = useState("");
  const [scopes, setScopes] = useState<string[]>(["produce"]);
  const [days, setDays] = useState(DEFAULT_DAYS);
  const [created, setCreated] = useState<CreatedGrpcToken | null>(null);
  const create = useCreateGrpcToken();
  const { data: available } = useGrpcScopes();

  function reset() {
    setName("");
    setScopes(["produce"]);
    setDays(DEFAULT_DAYS);
    setCreated(null);
    create.reset();
  }

  function onOpenChange(next: boolean) {
    if (!next) reset();
    setOpen(next);
  }

  function toggleScope(scope: string, on: boolean) {
    setScopes((current) =>
      on ? [...new Set([...current, scope])] : current.filter((name) => name !== scope),
    );
  }

  function onSubmit(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    create.mutate(
      { name, scopes, expires_in_days: days },
      { onSuccess: (token) => setCreated(token) },
    );
  }

  const errorMessage =
    create.error instanceof ApiError
      ? create.error.message
      : create.error
        ? "Failed to create token."
        : null;

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogTrigger asChild>
        <Button>
          <Plus aria-hidden /> New token
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        {created ? (
          <SuccessView token={created} onDone={() => onOpenChange(false)} />
        ) : (
          <form onSubmit={onSubmit} className="flex flex-col gap-4">
            <DialogHeader>
              <DialogTitle>New gRPC token</DialogTitle>
              <DialogDescription>
                A credential a client presents to the gRPC door. Scoped, expiring, and revocable on
                its own.
              </DialogDescription>
            </DialogHeader>
            <label htmlFor="token-name" className="flex flex-col gap-1.5 text-sm">
              <span className="font-medium">Name</span>
              <Input
                id="token-name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="ci-pipeline"
                required
              />
              <span className="text-xs text-[var(--fg-subtle)]">
                Which client holds it — this is what you will read when deciding what to revoke.
              </span>
            </label>
            <div className="flex flex-col gap-2 text-sm">
              <span className="font-medium">Scopes</span>
              {(available ?? []).map((scope) => (
                <div
                  key={scope.name}
                  className="flex items-center justify-between gap-4 rounded-md border border-[var(--border)] px-3 py-2"
                >
                  <div className="flex flex-col gap-0.5">
                    <span className="font-mono text-xs">{scope.name}</span>
                    <span className="text-xs text-[var(--fg-subtle)]">
                      {SCOPE_HELP[scope.name] ?? " "}
                    </span>
                  </div>
                  <Switch
                    checked={scopes.includes(scope.name)}
                    onCheckedChange={(on) => toggleScope(scope.name, on)}
                    aria-label={`Grant ${scope.name}`}
                  />
                </div>
              ))}
              <span className="text-xs text-[var(--fg-subtle)]">
                Grant only what the client needs — a token that submits work should not be able to
                claim it.
              </span>
            </div>
            <div className="flex flex-col gap-1.5 text-sm">
              <span className="font-medium">Expires in</span>
              <Stepper
                value={days}
                onChange={setDays}
                min={1}
                max={MAX_DAYS}
                step={30}
                format={(v) => `${v} days`}
                aria-label="days until expiry"
              />
              <span className="text-xs text-[var(--fg-subtle)]">
                At most {MAX_DAYS} days. A credential with no end date is a permanent one with extra
                steps.
              </span>
            </div>
            {errorMessage ? (
              <div
                role="alert"
                className="flex items-start gap-2 rounded-md bg-danger-dim px-3 py-2 text-sm text-danger"
              >
                <AlertCircle className="mt-0.5 size-4 shrink-0" aria-hidden />
                <span>{errorMessage}</span>
              </div>
            ) : null}
            <DialogFooter>
              <Button
                type="button"
                variant="secondary"
                onClick={() => onOpenChange(false)}
                disabled={create.isPending}
              >
                Cancel
              </Button>
              <Button type="submit" disabled={create.isPending || !name || scopes.length === 0}>
                {create.isPending ? "Creating…" : "Create token"}
              </Button>
            </DialogFooter>
          </form>
        )}
      </DialogContent>
    </Dialog>
  );
}

function SuccessView({ token, onDone }: { token: CreatedGrpcToken; onDone: () => void }) {
  return (
    <div className="flex flex-col gap-4">
      <DialogHeader>
        <DialogTitle>Token created</DialogTitle>
        <DialogDescription>
          Clients present it as <code className="font-mono text-xs">authorization: Bearer …</code>.
        </DialogDescription>
      </DialogHeader>
      <div className="grid grid-cols-2 gap-2 rounded-md border border-[var(--border)] bg-[var(--surface-2)] px-3 py-2 text-sm">
        <div>
          <div className="text-xs text-[var(--fg-subtle)]">Name</div>
          <div className="text-xs">{token.name}</div>
        </div>
        <div>
          <div className="text-xs text-[var(--fg-subtle)]">Namespace</div>
          <div className="font-mono text-xs">{token.namespace}</div>
        </div>
      </div>
      <SecretReveal
        secret={token.token}
        hint="API token"
        note="flexiq stores only a hash of this token. It cannot be shown again — if you lose it, revoke it and create another."
      />
      <DialogFooter>
        <Button onClick={onDone}>Done</Button>
      </DialogFooter>
    </div>
  );
}
