import { queryOptions, useQuery } from "@tanstack/react-query";
import { useRefreshInterval } from "@/providers";
import { fetchExecutors } from "./api";

export function executorsQuery() {
  return queryOptions({
    queryKey: ["executors"],
    queryFn: ({ signal }) => fetchExecutors(signal),
    staleTime: 10_000,
  });
}

/** Attached executors, polled on the user's refresh interval. */
export function useExecutors() {
  const { intervalMs } = useRefreshInterval();
  return useQuery({ ...executorsQuery(), refetchInterval: intervalMs });
}

/**
 * Whether this server exposes executors at all.
 *
 * `undefined` while unknown, so the nav can stay quiet rather than flashing an
 * entry it may be about to remove. The probe shares the executors query, so
 * enabling the page costs no extra request.
 */
export function useExecutorsSupported(): boolean | undefined {
  const { data, isSuccess } = useQuery({ ...executorsQuery(), refetchInterval: false });
  if (!isSuccess) return undefined;
  return data !== null;
}
