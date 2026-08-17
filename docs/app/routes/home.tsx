import { useEffect, useState } from "react";
import { SearchModal } from "@/components/docs";
import {
  Comparison,
  CTA,
  Features,
  Footer,
  Hero,
  HowItWorks,
  Integrations,
  ScenarioFinder,
  UseCases,
  useReveal,
} from "@/components/landing";
import { SiteNav } from "@/components/ui";
import { useActiveSdk } from "@/hooks";
import type { Route } from "./+types/home";

export function meta(_: Route.MetaArgs) {
  return [
    { title: "FlexiQ — background jobs without a broker" },
    {
      name: "description",
      content:
        "Keep your app fast — run slow work in the background. A Rust-powered task queue with no message broker, for Python, Node, and Java.",
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
        <Features />
        <UseCases />
        <Comparison />
        <Integrations />
        <CTA />
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
