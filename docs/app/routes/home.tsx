import { useEffect, useState } from "react";
import { SearchModal } from "@/components/docs";
import {
  Footer,
  Hero,
  HowItWorks,
  ScenarioFinder,
  SectionGrid,
  useReveal,
} from "@/components/landing";
import { SiteNav } from "@/components/ui";
import { useActiveSdk } from "@/hooks";
import type { Route } from "./+types/home";

/**
 * The documentation index, not a product page.
 *
 * flexiq.byteveda.org is the marketing surface; a second pitch here competed
 * with it for the same readers and restated `about/comparison` and
 * `about/capabilities` besides. What is left answers the questions a docs root
 * should: what this is (hero), how it fits together (how it works), which page
 * solves the problem I actually have (the scenario finder), and what else is
 * here (the section grid).
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
  // Scope landing search to the SDK chosen in the hero (store-backed on `/`).
  const sdk = useActiveSdk();

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
      <SiteNav onSearch={() => setSearchOpen(true)} showSdkSelect={false} />
      <main>
        <Hero />
        <HowItWorks />
        <ScenarioFinder />
        <SectionGrid />
      </main>
      <Footer />
      <SearchModal
        open={searchOpen}
        onClose={() => setSearchOpen(false)}
        sdk={sdk}
      />
    </>
  );
}
