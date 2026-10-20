import { queryOptions, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { ApiError } from "@/lib/api-client";
import {
  clearRetentionOverride,
  deleteSetting,
  fetchRetention,
  fetchRetentionDryRun,
  fetchRetentionOverride,
  fetchSettings,
  saveRetentionOverride,
  setSetting,
} from "./api";
import type { RetentionOverride, RetentionOverrideInput, SettingsSnapshot } from "./types";

const KEY = ["settings"] as const;
// Every retention query sits under this prefix, so one invalidation after a
// save refreshes the echo, the preview and the editor together.
const RETENTION_KEY = ["retention"] as const;
const RETENTION_DRY_RUN_KEY = ["retention", "dry-run"] as const;
const RETENTION_OVERRIDE_KEY = ["retention", "override"] as const;

/**
 * Return a user-friendly error description.
 *
 * Validation errors (4xx) carry messages we wrote ourselves and are useful
 * to surface verbatim. Anything else (5xx, network, unknown) is hidden
 * behind a generic message so internal details never leak to the UI.
 */
function describeError(error: unknown): string | undefined {
  if (error instanceof ApiError && error.status >= 400 && error.status < 500) {
    return error.message;
  }
  return undefined;
}

export function settingsQuery() {
  return queryOptions({
    queryKey: KEY,
    queryFn: ({ signal }) => fetchSettings(signal),
  });
}

export function useSettings() {
  return useQuery(settingsQuery());
}

export function retentionQuery() {
  return queryOptions({
    queryKey: RETENTION_KEY,
    queryFn: ({ signal }) => fetchRetention(signal),
  });
}

export function useRetention() {
  return useQuery(retentionQuery());
}

export function retentionDryRunQuery() {
  return queryOptions({
    queryKey: RETENTION_DRY_RUN_KEY,
    queryFn: ({ signal }) => fetchRetentionDryRun(signal),
  });
}

export function useRetentionDryRun() {
  return useQuery(retentionDryRunQuery());
}

/**
 * The stored retention override. ``data`` is ``null`` on a backend that has
 * no override route (404), so the card stays read-only there instead of
 * offering an editor whose save cannot work.
 */
export function useRetentionOverride() {
  return useQuery({
    queryKey: RETENTION_OVERRIDE_KEY,
    queryFn: async ({ signal }): Promise<RetentionOverride | null> => {
      try {
        return await fetchRetentionOverride(signal);
      } catch (e) {
        if (e instanceof ApiError && (e.status === 404 || e.status === 405)) return null;
        throw e;
      }
    },
    retry: false,
  });
}

export function useSaveRetentionOverride() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (input: RetentionOverrideInput) => saveRetentionOverride(input),
    onError: (error) =>
      toast.error("Couldn't save retention", { description: describeError(error) }),
    onSuccess: (saved) =>
      toast.success(
        saved.source === "dashboard"
          ? "Dashboard windows saved — applied on the next cleanup sweep"
          : "Switched to the code config — applied on the next cleanup sweep",
      ),
    onSettled: () => qc.invalidateQueries({ queryKey: RETENTION_KEY }),
  });
}

export function useClearRetentionOverride() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: clearRetentionOverride,
    onError: (error) =>
      toast.error("Couldn't clear retention", { description: describeError(error) }),
    onSuccess: () => toast.success("Dashboard windows cleared — the code config applies"),
    onSettled: () => qc.invalidateQueries({ queryKey: RETENTION_KEY }),
  });
}

interface UpdateInput {
  key: string;
  value: unknown;
}

/**
 * Optimistically update a setting in the cache, then sync with the server.
 * Rolls back on error to keep the UI consistent.
 */
export function useUpdateSetting() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: ({ key, value }: UpdateInput) => setSetting(key, value),
    onMutate: async ({ key, value }) => {
      await qc.cancelQueries({ queryKey: KEY });
      const prev = qc.getQueryData<SettingsSnapshot>(KEY);
      const encoded = typeof value === "string" ? value : JSON.stringify(value);
      qc.setQueryData<SettingsSnapshot>(KEY, { ...(prev ?? {}), [key]: encoded });
      return { prev };
    },
    onError: (error, _input, ctx) => {
      if (ctx?.prev) qc.setQueryData(KEY, ctx.prev);
      toast.error("Couldn't save setting", { description: describeError(error) });
    },
    onSuccess: () => toast.success("Setting saved"),
    onSettled: () => qc.invalidateQueries({ queryKey: KEY }),
  });
}

export function useDeleteSetting() {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: (key: string) => deleteSetting(key),
    onMutate: async (key) => {
      await qc.cancelQueries({ queryKey: KEY });
      const prev = qc.getQueryData<SettingsSnapshot>(KEY);
      if (prev) {
        const next = { ...prev };
        delete next[key];
        qc.setQueryData<SettingsSnapshot>(KEY, next);
      }
      return { prev };
    },
    onError: (error, _key, ctx) => {
      if (ctx?.prev) qc.setQueryData(KEY, ctx.prev);
      toast.error("Couldn't delete setting", { description: describeError(error) });
    },
    onSuccess: () => toast.success("Setting cleared"),
    onSettled: () => qc.invalidateQueries({ queryKey: KEY }),
  });
}
