import { useEffect, useState } from "react";
import { Outlet, useLocation } from "react-router";
import { SearchModal, Sidebar, Toc } from "@/components/docs";
import { SiteNav } from "@/components/ui";
import { useActiveTier, useSdk } from "@/hooks";
import { forcedSdkForPath } from "@/lib";

/** Shell for every docs page: top nav + sidebar + article outlet + on-this-page TOC. */
export default function DocsLayout() {
  const [searchOpen, setSearchOpen] = useState(false);
  const [navOpen, setNavOpen] = useState(false);
  const { pathname } = useLocation();
  const { setSdk } = useSdk();
  // Search is scoped to the tier, not the language: `/server/*` has a sidebar of
  // its own, and the palette should offer what that sidebar does.
  const tier = useActiveTier();

  // Close the mobile sidebar drawer whenever the route changes (i.e. a nav link
  // was tapped) so it never lingers over the freshly-loaded page.
  // biome-ignore lint/correctness/useExhaustiveDependencies: close on navigation
  useEffect(() => {
    setNavOpen(false);
  }, [pathname]);

  // Visiting an SDK-specific page (`/python/*`,`/node/*`) makes that SDK sticky,
  // so walking onto a shared page keeps the choice. No-op on shared pages.
  useEffect(() => {
    const forced = forcedSdkForPath(pathname.replace(/\/$/, "") || "/");
    if (forced) {
      setSdk(forced);
    }
  }, [pathname, setSdk]);

  // ⌘K / Ctrl-K opens search anywhere in the docs.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setSearchOpen(true);
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, []);
  return (
    <>
      <SiteNav
        onSearch={() => setSearchOpen(true)}
        onMenu={() => setNavOpen(true)}
      />
      <div className="docs-shell">
        <Sidebar
          open={navOpen}
          onClose={() => setNavOpen(false)}
          onSearch={() => setSearchOpen(true)}
        />
        <Outlet />
        <Toc />
      </div>
      <SearchModal
        open={searchOpen}
        onClose={() => setSearchOpen(false)}
        tier={tier}
      />
    </>
  );
}
