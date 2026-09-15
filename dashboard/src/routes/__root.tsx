import type { QueryClient } from "@tanstack/react-query";
import type { ErrorComponentProps } from "@tanstack/react-router";
import { createRootRouteWithContext, Link, Outlet, useLocation } from "@tanstack/react-router";
import { AlertTriangle, ArrowLeft, Home } from "lucide-react";
import { AppShell, BackendOffline } from "@/components/layout";
import { Button, buttonVariants } from "@/components/ui";
import { AuthGate } from "@/features/auth";
import { cn } from "@/lib/cn";
import { asError, isBackendUnreachable } from "@/lib/errors";

export interface RouterContext {
  queryClient: QueryClient;
}

export const Route = createRootRouteWithContext<RouterContext>()({
  component: RootLayout,
  errorComponent: ErrorView,
  notFoundComponent: NotFoundView,
});

/**
 * Public routes that render without the AppShell or the auth gate. The
 * login route handles its own redirect when a session is already active.
 */
const UNAUTHED_ROUTES = new Set<string>(["/login"]);

function RootLayout() {
  const { pathname } = useLocation();
  if (UNAUTHED_ROUTES.has(pathname)) {
    return <Outlet />;
  }
  return (
    <AuthGate>
      <AppShell>
        <Outlet />
      </AppShell>
    </AuthGate>
  );
}

/**
 * Top-level error boundary for loader failures and uncaught render errors.
 *
 * Network/5xx failures get a dedicated "backend unreachable" page with
 * actionable steps. Everything else falls through to the generic error
 * view — usually a programming bug the user can't fix on their own.
 */
function ErrorView({ error: caught }: ErrorComponentProps) {
  // `asError` is identity for real Errors, so the ApiError/TypeError checks
  // inside `isBackendUnreachable` still see the original instance.
  const error = asError(caught);
  if (isBackendUnreachable(error)) {
    return (
      <AppShell>
        <BackendOffline error={error} />
      </AppShell>
    );
  }
  return (
    <AppShell>
      <div className="grid min-h-[50vh] place-items-center">
        <div className="max-w-md text-center">
          <div className="mx-auto mb-4 grid size-12 place-items-center rounded-full bg-danger-dim text-danger">
            <AlertTriangle className="size-5" aria-hidden />
          </div>
          <h1 className="text-lg font-semibold">Something went wrong</h1>
          <p className="mt-2 text-sm text-[var(--fg-muted)]">{error.message}</p>
          <Button variant="secondary" className="mt-5" onClick={() => window.location.reload()}>
            <ArrowLeft aria-hidden /> Reload
          </Button>
        </div>
      </div>
    </AppShell>
  );
}

function NotFoundView() {
  return (
    <div className="grid min-h-[50vh] place-items-center">
      <div className="max-w-md text-center">
        <div className="mx-auto mb-4 grid size-12 place-items-center rounded-full bg-[var(--surface-2)] text-[var(--fg-muted)]">
          <span className="font-mono text-base">404</span>
        </div>
        <h1 className="text-lg font-semibold">Page not found</h1>
        <p className="mt-2 text-sm text-[var(--fg-muted)]">
          The page you are looking for doesn't exist or has been moved.
        </p>
        <Link to="/" className={cn(buttonVariants({ variant: "secondary" }), "mt-5")}>
          <Home aria-hidden /> Go home
        </Link>
      </div>
    </div>
  );
}
