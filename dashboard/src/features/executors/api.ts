import { ApiError, api } from "@/lib/api-client";
import type { ExecutorInventory } from "@/lib/api-types";

/**
 * Fetch attached executors, or `null` when this server has no attach listener.
 *
 * Only the standalone server serves this route; an SDK dashboard answers 404.
 * Returning `null` rather than throwing is what lets the nav hide the page
 * instead of surfacing an error the operator can do nothing about.
 */
export async function fetchExecutors(signal?: AbortSignal): Promise<ExecutorInventory | null> {
  try {
    return await api.get<ExecutorInventory>("/api/executors", { signal });
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) return null;
    throw error;
  }
}
