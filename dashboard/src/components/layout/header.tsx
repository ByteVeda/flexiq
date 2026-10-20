import { Search } from "lucide-react";
import { Button, Kbd } from "@/components/ui";
import { UserMenu } from "@/features/auth";
import { useCommandPalette } from "@/providers";
import { LastRefreshed } from "./last-refreshed";
import { MobileMenu } from "./mobile-menu";
import { ThemeToggle } from "./theme-toggle";

export function Header() {
  const { setOpen } = useCommandPalette();
  return (
    <header className="sticky top-0 z-20 flex h-14 shrink-0 items-center gap-3 border-b border-[var(--border)] bg-[var(--bg)]/85 px-4 md:px-5 backdrop-blur supports-[backdrop-filter]:bg-[var(--bg)]/70">
      <MobileMenu />
      <Button
        variant="outline"
        onClick={() => setOpen(true)}
        className="hidden w-[300px] max-w-[38vw] justify-between font-normal text-[var(--fg-subtle)] md:inline-flex"
      >
        <span className="inline-flex items-center gap-2">
          <Search className="size-3.5" aria-hidden />
          Search jobs, queues…
        </span>
        <span className="inline-flex items-center gap-1">
          <Kbd>⌘</Kbd>
          <Kbd>K</Kbd>
        </span>
      </Button>
      <Button
        variant="ghost"
        size="icon"
        aria-label="Open command palette"
        onClick={() => setOpen(true)}
        className="md:hidden"
      >
        <Search className="size-4" />
      </Button>
      <div className="ml-auto flex items-center gap-3">
        <LastRefreshed className="hidden sm:inline-flex" />
        <ThemeToggle />
        <UserMenu />
      </div>
    </header>
  );
}
