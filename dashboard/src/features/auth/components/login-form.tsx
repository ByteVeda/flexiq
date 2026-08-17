import { useNavigate, useSearch } from "@tanstack/react-router";
import { AlertCircle, LogIn } from "lucide-react";
import { type FormEvent, useState } from "react";
import { Button } from "@/components/ui";
import { Input } from "@/components/ui/input";
import { ApiError } from "@/lib/api-client";
import { useAuthProviders, useLogin } from "../hooks";
import { OAuthButton } from "./oauth-button";

const ERROR_MESSAGES: Record<string, string> = {
  invalid_credentials: "Invalid username or password.",
  setup_required: "Dashboard setup is required before login.",
  oauth_state_invalid: "OAuth session expired or was already used. Try again.",
  oauth_failed: "OAuth sign-in failed. The provider returned an error.",
  oauth_denied: "Access denied. Your account is not in the allowed list.",
};

/**
 * Post-login redirects must stay on this origin: only root-relative paths
 * (``/...`` but not protocol-relative ``//...``) are honoured, mirroring the
 * server-side ``next`` validation on the OAuth callback.
 */
function safeNextPath(next: unknown): string | undefined {
  if (typeof next !== "string") return undefined;
  // Backslashes are rejected too: WHATWG URL parsing normalizes "\" to "/"
  // for http(s), so "/\evil.com" would escape the origin.
  return next.startsWith("/") && !next.startsWith("//") && !next.includes("\\") ? next : undefined;
}

export function LoginForm() {
  const navigate = useNavigate();
  const search = useSearch({ strict: false }) as { next?: string; error?: string } | undefined;
  const nextPath = safeNextPath(search?.next);
  const oauthError =
    typeof search?.error === "string" ? (ERROR_MESSAGES[search.error] ?? search.error) : null;

  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const login = useLogin();
  const providers = useAuthProviders();

  // Default to password-on while the providers query is in flight so the
  // form doesn't flash empty on the first render.
  const passwordEnabled = providers.data?.password_enabled ?? true;
  const oauthProviders = providers.data?.providers ?? [];
  const hasOAuth = oauthProviders.length > 0;

  function onSubmit(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    login.mutate(
      { username, password },
      {
        onSuccess: () => {
          void navigate({ to: nextPath ?? "/" });
        },
      },
    );
  }

  const error = errorMessage(login.error);
  const disabled = login.isPending || !username || !password;

  return (
    <div className="flex w-full max-w-sm flex-col gap-4 rounded-xl border border-[var(--border)] bg-[var(--surface-1)] p-6 shadow-sm">
      <div>
        <h1 className="text-lg font-semibold">Sign in</h1>
        <p className="mt-1 text-sm text-[var(--fg-muted)]">
          {passwordEnabled
            ? "Enter your dashboard credentials to continue."
            : "Choose a provider to continue."}
        </p>
      </div>

      {oauthError ? (
        <div
          role="alert"
          className="flex items-start gap-2 rounded-md bg-danger-dim px-3 py-2 text-sm text-danger"
        >
          <AlertCircle className="mt-0.5 size-4 shrink-0" aria-hidden />
          <span>{oauthError}</span>
        </div>
      ) : null}

      {hasOAuth ? (
        <div className="flex flex-col gap-2">
          {oauthProviders.map((provider) => (
            <OAuthButton key={provider.slot} provider={provider} next={nextPath} />
          ))}
        </div>
      ) : null}

      {hasOAuth && passwordEnabled ? (
        <div className="flex items-center gap-3 text-xs text-[var(--fg-subtle)]">
          <div className="h-px flex-1 bg-[var(--border)]" />
          <span>or sign in with password</span>
          <div className="h-px flex-1 bg-[var(--border)]" />
        </div>
      ) : null}

      {passwordEnabled ? (
        <form onSubmit={onSubmit} className="flex flex-col gap-4">
          <label htmlFor="login-username" className="flex flex-col gap-1.5 text-sm">
            <span className="font-medium">Username</span>
            <Input
              id="login-username"
              type="text"
              autoComplete="username"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              required
              autoFocus={!hasOAuth}
            />
          </label>
          <label htmlFor="login-password" className="flex flex-col gap-1.5 text-sm">
            <span className="font-medium">Password</span>
            <Input
              id="login-password"
              type="password"
              autoComplete="current-password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
            />
          </label>
          {error ? (
            <div
              role="alert"
              className="flex items-start gap-2 rounded-md bg-danger-dim px-3 py-2 text-sm text-danger"
            >
              <AlertCircle className="mt-0.5 size-4 shrink-0" aria-hidden />
              <span>{error}</span>
            </div>
          ) : null}
          <Button type="submit" disabled={disabled}>
            <LogIn aria-hidden /> {login.isPending ? "Signing in…" : "Sign in"}
          </Button>
        </form>
      ) : null}

      {!passwordEnabled && !hasOAuth ? (
        <div
          role="alert"
          className="flex items-start gap-2 rounded-md bg-warning-dim px-3 py-2 text-sm text-warning"
        >
          <AlertCircle className="mt-0.5 size-4 shrink-0" aria-hidden />
          <span>
            No login methods are configured. Set{" "}
            <code>FLEXIQ_DASHBOARD_PASSWORD_AUTH_ENABLED=true</code> or configure an OAuth provider.
          </span>
        </div>
      ) : null}
    </div>
  );
}

function errorMessage(error: unknown): string | null {
  if (!error) return null;
  if (error instanceof ApiError) {
    const code =
      typeof error.body === "object" && error.body && "error" in error.body
        ? String((error.body as { error: unknown }).error)
        : "";
    return ERROR_MESSAGES[code] ?? error.message ?? "Sign-in failed.";
  }
  return "Sign-in failed.";
}
