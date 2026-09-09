import { useEffect } from "react";
import { prefetchTierDocs } from "@/lib/prefetch";
import { useActiveTier } from "./use-tier";

/**
 * Warm the active tier's docs in the background. Runs on mount and whenever the
 * tier changes (hero tab, sidebar switcher, or a `/node|/python|/server` URL),
 * so the first navigation into that tier's docs is instant.
 */
export function usePrefetchDocs(): void {
  const tier = useActiveTier();
  useEffect(() => {
    prefetchTierDocs(tier);
  }, [tier]);
}
