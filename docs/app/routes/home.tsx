import { useEffect, useMemo, useState } from "react";
import { SearchModal } from "@/components/docs";
import {
  Footer,
  Hero,
  HowItWorks,
  ScenarioFinder,
  SectionGrid,
  ServerFold,
  useReveal,
} from "@/components/landing";
import { SiteNav } from "@/components/ui";
import { useActiveTier } from "@/hooks";
import { landingTiers } from "@/lib/search";
import type { Route } from "./+types/home";

/**
 * The documentation index, not a product page.
 *
 * flexiq.byteveda.org is the marketing surface; a second pitch here competed
 * with it for the same readers and restated `about/comparison` and
 * `about/capabilities` besides. What is left answers the questions a docs root
 * should: what this is (hero), how it fits together (how it works), that it is
 * reachable without an SDK at all (the server fold — everything above it reads
 * as an embedded library), which page solves the problem I actually have (the
 * scenario finder), and what else is here (the section grid).
 *
 * The route itself has to stay. `"/"` is hardcoded in the prerender list, and
 * with no index route the `*` splat under `docs-layout` claims it and ships a
 * "Page not found" at the site root — silently, with the build still green.
 */
export function meta(_: Route.MetaArgs) {
  return [
    { title: "FlexiQ documentation" },
    {
      name: "description",
      content:
        "Guides, API reference and architecture for FlexiQ — the Rust-powered task queue for Python, Node and Java. Start with a quickstart or find the guide for the problem you have.",
    },
  ];
}

export default function Home() {
  useReveal();
  const [searchOpen, setSearchOpen] = useState(false);
  // The landing has no sidebar for the palette to agree with, so it offers the
  // tier the hero is showing *and* the server tier — a reader here has not
  // picked a door, and the one that needs no SDK is the one they cannot know to
  // search for. Memoised: it is a dependency of the palette's query effect.
  const tier = useActiveTier();
  const tiers = useMemo(() => landingTiers(tier), [tier]);

  // ⌘K / Ctrl-K opens search on the landing page too.
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
      <div className="bgfx" aria-hidden="true">
        <div className="grid" />
        <div className="glow" />
        <div className="glow two" />
      </div>
      <SiteNav onSearch={() => setSearchOpen(true)} showTierSelect={false} />
      <main>
        <Hero />
        <HowItWorks />
        <ServerFold />
        <ScenarioFinder />
        <SectionGrid />
      </main>
      <Footer />
      <SearchModal
        open={searchOpen}
        onClose={() => setSearchOpen(false)}
        tiers={tiers}
      />
    </>
  );
}
