import { useRouter } from "@tanstack/react-router";
import { ExternalLink, Power, RefreshCw, Terminal } from "lucide-react";
import { useTransition } from "react";
import { Button } from "@/components/ui";

/**
 * Shown when a route loader (or in-flight query) can't reach the FlexiQ
 * backend — either the network request failed outright or the server
 * replied with a 502/503/504. Gives the user actionable next steps instead
 * of the generic "something went wrong" screen.
 */
export function BackendOffline({ error }: { error: Error }) {
  const router = useRouter();
  const [pending, startTransition] = useTransition();

  const retry = () => {
    startTransition(() => {
      router.invalidate();
    });
  };

  return (
    <div className="grid min-h-[60vh] place-items-center px-4">
      <div className="w-full max-w-lg">
        <div className="flex items-center gap-3">
          <div className="grid size-10 place-items-center rounded-full bg-[var(--surface-2)] text-[var(--fg-muted)]">
            <Power className="size-5" aria-hidden />
          </div>
          <div>
            <h1 className="text-lg font-semibold tracking-tight">Can't reach the FlexiQ backend</h1>
            <p className="text-sm text-[var(--fg-muted)]">
              The dashboard tried to fetch data but didn't get a response.
            </p>
          </div>
        </div>

        <div className="mt-6 rounded-lg border border-[var(--border)] bg-[var(--surface)] p-4">
          <p className="text-sm font-medium text-[var(--fg)]">Common causes</p>
          <ul className="mt-2 flex flex-col gap-2 text-sm text-[var(--fg-muted)]">
            <li className="flex items-start gap-2">
              <span
                className="mt-1 size-1.5 shrink-0 rounded-full bg-[var(--fg-subtle)]"
                aria-hidden
              />
              <span>
                The dashboard process isn't running. Start it with:
                <code className="ml-1 rounded bg-[var(--surface-2)] px-1.5 py-0.5 font-mono text-xs text-[var(--fg)]">
                  flexiq dashboard --app myapp:queue
                </code>
              </span>
            </li>
            <li className="flex items-start gap-2">
              <span
                className="mt-1 size-1.5 shrink-0 rounded-full bg-[var(--fg-subtle)]"
                aria-hidden
              />
              <span>
                The worker process crashed. Tail its logs or restart it with:
                <code className="ml-1 rounded bg-[var(--surface-2)] px-1.5 py-0.5 font-mono text-xs text-[var(--fg)]">
                  flexiq worker --app myapp:queue
                </code>
              </span>
            </li>
            <li className="flex items-start gap-2">
              <span
                className="mt-1 size-1.5 shrink-0 rounded-full bg-[var(--fg-subtle)]"
                aria-hidden
              />
              <span>
                A restart or deploy is in progress — the server is up but returning 5xx. Usually
                resolves in a few seconds.
              </span>
            </li>
          </ul>
        </div>

        {error.message ? (
          <details className="mt-3 rounded-lg border border-[var(--border)] bg-[var(--surface)] p-3 text-sm">
            <summary className="flex cursor-pointer items-center gap-2 text-[var(--fg-muted)]">
              <Terminal className="size-3.5" aria-hidden />
              Technical details
            </summary>
            <pre className="mt-2 overflow-x-auto whitespace-pre-wrap font-mono text-xs text-[var(--fg-subtle)]">
              {error.message}
            </pre>
          </details>
        ) : null}

        <div className="mt-5 flex items-center gap-2">
          <Button onClick={retry} disabled={pending}>
            <RefreshCw aria-hidden className={pending ? "animate-spin" : undefined} />
            {pending ? "Retrying…" : "Retry"}
          </Button>
          <a
            href="https://docs.byteveda.org/flexiq/guide/observability/dashboard"
            target="_blank"
            rel="noreferrer noopener"
            className="inline-flex items-center gap-1.5 text-sm text-[var(--fg-muted)] hover:text-[var(--fg)]"
          >
            Docs <ExternalLink className="size-3.5" aria-hidden />
          </a>
        </div>
      </div>
    </div>
  );
}
