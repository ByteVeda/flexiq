import { createFileRoute, Navigate, useRouter } from "@tanstack/react-router";
import { AlertOctagon } from "lucide-react";
import { LoginForm, SetupForm, useAuthStatus, useWhoami } from "@/features/auth";

export const Route = createFileRoute("/login")({
  component: LoginPage,
});

/**
 * Standalone auth route — no AppShell wrapping. Shows the setup form when
 * no users exist yet, the login form otherwise. Logged-in visitors are
 * bounced back to the dashboard root.
 */
function LoginPage() {
  const router = useRouter();
  const status = useAuthStatus();
  const whoami = useWhoami();

  if (status.data?.auth_enabled === false || whoami.data?.user) {
    return <Navigate to="/" />;
  }

  const loading = status.isLoading || whoami.isLoading;

  return (
    <div className="grid min-h-screen place-items-center bg-[var(--bg)] px-4">
      <div className="flex w-full max-w-sm flex-col items-center gap-6">
        <div className="flex items-center gap-2">
          <div className="grid place-items-center size-9 rounded-md bg-accent text-accent-fg">
            <AlertOctagon className="size-5" aria-hidden />
          </div>
          <span className="text-base font-semibold tracking-tight">flexiq</span>
        </div>
        {loading ? (
          <div className="text-sm text-[var(--fg-muted)]">Loading…</div>
        ) : status.data?.setup_required ? (
          <SetupForm />
        ) : (
          <LoginForm />
        )}
        <button
          type="button"
          className="text-xs text-[var(--fg-subtle)] hover:text-[var(--fg)] transition-colors"
          onClick={() => router.invalidate()}
        >
          Refresh
        </button>
      </div>
    </div>
  );
}
