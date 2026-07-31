import { Link, useLocation } from "@tanstack/react-router";
import { ExternalLink as ExternalLinkIcon } from "lucide-react";
import { LiveDot } from "@/components/ui";
import { useExecutorsSupported } from "@/features/executors";
import { useStats } from "@/features/overview/hooks";
import { useBranding, useExternalLinks } from "@/features/settings";
import { cn } from "@/lib/cn";
import { formatCount } from "@/lib/number";
import { site } from "@/lib/site";
import { BrandMark } from "./brand-mark";
import { visibleNav } from "./nav-config";

export function Sidebar() {
  const { pathname } = useLocation();
  const { title } = useBranding();
  const externalLinks = useExternalLinks();
  const { data: stats, isError, isPending } = useStats();
  const deadCount = stats?.dead ?? 0;
  const nav = visibleNav({ "/executors": useExecutorsSupported() });

  const coreTone = isError ? "danger" : isPending && !stats ? "warning" : "success";
  const coreLabel = isError
    ? "Core unreachable"
    : isPending && !stats
      ? "Connecting…"
      : "Core healthy";
  const isDefaultBrand = title === site.name;

  return (
    <aside className="hidden h-screen w-[248px] shrink-0 flex-col border-r border-[var(--border)] bg-[var(--bg-subtle)] lg:flex">
      <div className="flex items-center gap-2.5 px-[18px] pt-[18px] pb-4">
        <BrandMark size={38} />
        <div className="leading-none">
          <div className="text-[1.18rem] font-semibold tracking-[-0.02em]">
            {isDefaultBrand ? (
              <>
                task<span className="text-[var(--accent-ink)]">ito</span>
              </>
            ) : (
              title
            )}
          </div>
          <div className="mt-[3px] font-mono text-[0.66rem] uppercase tracking-[0.16em] text-[var(--fg-subtle)]">
            Queue console
          </div>
        </div>
      </div>
      <nav className="flex-1 overflow-y-auto px-3 pb-4">
        {nav.map((group) => (
          <div key={group.title} className="mt-[18px] first:mt-1.5">
            <div className="px-2.5 pb-[7px] font-mono text-[0.62rem] font-semibold uppercase tracking-[0.13em] text-[var(--fg-subtle)]">
              {group.title}
            </div>
            <ul className="flex flex-col gap-0.5">
              {group.items.map(({ to, label, icon: Icon }) => {
                const active = pathname === to || (to !== "/" && pathname.startsWith(`${to}/`));
                const count = to === "/dead-letters" && deadCount > 0 ? deadCount : null;
                return (
                  <li key={to}>
                    <Link
                      to={to}
                      aria-current={active ? "page" : undefined}
                      className={cn(
                        "relative flex items-center gap-2.5 rounded-[9px] px-2.5 py-2 text-sm transition-colors",
                        active
                          ? "bg-[var(--surface)] font-semibold text-[var(--fg)] shadow-[var(--card-shadow)]"
                          : "font-normal text-[var(--fg-muted)] hover:bg-[var(--surface-2)]/70 hover:text-[var(--fg)]",
                      )}
                    >
                      {active ? (
                        <span
                          aria-hidden
                          className="absolute -left-3 inset-y-2 w-[3px] rounded-r-full bg-accent"
                        />
                      ) : null}
                      <Icon
                        className={cn("size-[17px] shrink-0", active && "text-accent")}
                        aria-hidden
                      />
                      <span className="truncate">{label}</span>
                      {count != null ? (
                        <span className="ml-auto font-mono text-[0.7rem] font-semibold tabular-nums text-danger">
                          {formatCount(count)}
                        </span>
                      ) : null}
                    </Link>
                  </li>
                );
              })}
            </ul>
          </div>
        ))}
        {externalLinks.length > 0 ? (
          <div className="mt-[18px]">
            <div className="px-2.5 pb-[7px] font-mono text-[0.62rem] font-semibold uppercase tracking-[0.13em] text-[var(--fg-subtle)]">
              Links
            </div>
            <ul className="flex flex-col gap-0.5">
              {externalLinks.map((link) => (
                <li key={`${link.label}|${link.url}`}>
                  <a
                    href={link.url}
                    target="_blank"
                    rel="noreferrer noopener"
                    className="flex items-center gap-2.5 rounded-[9px] px-2.5 py-2 text-sm text-[var(--fg-muted)] transition-colors hover:bg-[var(--surface-2)] hover:text-[var(--fg)]"
                  >
                    <ExternalLinkIcon className="size-[17px] shrink-0" aria-hidden />
                    <span className="truncate">{link.label}</span>
                  </a>
                </li>
              ))}
            </ul>
          </div>
        ) : null}
      </nav>
      <div className="flex items-center gap-2 border-t border-[var(--border)] px-[18px] py-3 text-[0.72rem] text-[var(--fg-subtle)]">
        <LiveDot tone={coreTone} />
        {coreLabel} · {import.meta.env.DEV ? "dev build" : "production"}
      </div>
    </aside>
  );
}
