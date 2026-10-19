import { api } from "@/lib/api-client";
import type {
  RetentionOverride,
  RetentionOverrideInput,
  RetentionPreview,
  RetentionSnapshot,
  SettingsSnapshot,
} from "./types";

export function fetchSettings(signal?: AbortSignal): Promise<SettingsSnapshot> {
  return api.get<SettingsSnapshot>("/api/settings", { signal });
}

export function fetchRetention(signal?: AbortSignal): Promise<RetentionSnapshot> {
  return api.get<RetentionSnapshot>("/api/retention", { signal });
}

export function fetchRetentionDryRun(signal?: AbortSignal): Promise<RetentionPreview> {
  return api.get<RetentionPreview>("/api/retention/dry-run", { signal });
}

export function fetchRetentionOverride(signal?: AbortSignal): Promise<RetentionOverride> {
  return api.get<RetentionOverride>("/api/retention/override", { signal });
}

export function saveRetentionOverride(input: RetentionOverrideInput): Promise<RetentionOverride> {
  return api.put<RetentionOverride>("/api/retention/override", input);
}

export function clearRetentionOverride(): Promise<{ deleted: boolean }> {
  return api.delete<{ deleted: boolean }>("/api/retention/override");
}

export interface SettingResponse {
  key: string;
  value: string;
}

export function setSetting(key: string, value: unknown): Promise<SettingResponse> {
  return api.put<SettingResponse>(`/api/settings/${encodeURIComponent(key)}`, { value });
}

export function deleteSetting(key: string): Promise<{ deleted: boolean }> {
  return api.delete<{ deleted: boolean }>(`/api/settings/${encodeURIComponent(key)}`);
}
