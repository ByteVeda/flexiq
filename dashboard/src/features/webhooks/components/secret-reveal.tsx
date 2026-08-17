import { Check, Copy, Eye, EyeOff, KeyRound, Shield } from "lucide-react";
import { useState } from "react";
import { Button, Callout } from "@/components/ui";

interface Props {
  secret: string;
  hint?: string;
}

/**
 * One-shot secret display. Shows a masked value, lets the user reveal and
 * copy it, and reminds them that the secret won't be shown again. Used by
 * the create response and the rotate-secret response.
 */
export function SecretReveal({ secret, hint }: Props) {
  const [shown, setShown] = useState(false);
  const [copied, setCopied] = useState(false);

  async function copyToClipboard() {
    try {
      await navigator.clipboard.writeText(secret);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // Clipboard write can fail (e.g. http context); the user can still
      // select-and-copy the visible value.
    }
  }

  return (
    <div className="flex flex-col gap-3">
      <Callout tone="warning" icon={<Shield aria-hidden />}>
        For security, flexiq stores only a hash of this secret. If you lose it you'll need to rotate
        to a new one.
      </Callout>
      <div className="flex flex-col gap-2">
        <div className="flex items-center gap-2 text-xs font-medium text-[var(--fg)]">
          <KeyRound className="size-3.5" aria-hidden />
          {hint ?? "Signing secret"}
        </div>
        <div className="flex items-center gap-2 rounded-md border border-[var(--border)] bg-[var(--bg-subtle)] p-1.5">
          <code className="grow break-all px-1.5 font-mono text-xs">
            {shown ? secret : "•".repeat(Math.min(secret.length, 48))}
          </code>
          <Button
            type="button"
            variant="ghost"
            size="icon"
            aria-label={shown ? "Hide secret" : "Show secret"}
            onClick={() => setShown((v) => !v)}
          >
            {shown ? <EyeOff className="size-4" /> : <Eye className="size-4" />}
          </Button>
          <Button
            type="button"
            variant="ghost"
            size="icon"
            aria-label="Copy secret"
            onClick={copyToClipboard}
          >
            {copied ? <Check className="size-4 text-success" /> : <Copy className="size-4" />}
          </Button>
        </div>
        <p className="text-[11px] text-[var(--fg-subtle)]">
          Store this securely — it will not be shown again.
        </p>
      </div>
    </div>
  );
}
