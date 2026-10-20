import { useLocation } from "@tanstack/react-router";
import type { ReactNode } from "react";
import { TooltipProvider } from "@/components/ui";
import { ReadOnlyBanner } from "@/features/demo";
import { useApplyAccent } from "@/features/settings";
import { CommandPalette } from "./command-palette";
import { Header } from "./header";
import { RouteErrorBoundary } from "./route-error-boundary";
import { Sidebar } from "./sidebar";
import { TopProgressBar } from "./top-progress-bar";

export function AppShell({ children }: { children: ReactNode }) {
  useApplyAccent();
  const { pathname } = useLocation();
  return (
    <TooltipProvider delayDuration={300}>
      <div className="flex h-screen overflow-hidden bg-[var(--bg)] text-[var(--fg)]">
        <Sidebar />
        <div className="flex min-w-0 flex-1 flex-col overflow-y-auto">
          <Header />
          <ReadOnlyBanner />
          <TopProgressBar />
          <main className="flex-1 px-4 py-5 md:px-6 md:py-6">
            <div key={pathname} className="mx-auto max-w-[1240px] animate-page-rise">
              <RouteErrorBoundary>{children}</RouteErrorBoundary>
            </div>
          </main>
        </div>
        <CommandPalette />
      </div>
    </TooltipProvider>
  );
}
