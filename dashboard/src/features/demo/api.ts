import { api } from "@/lib/api-client";
import type { TriggerResponse } from "./types";

/** Post an empty event to the demo trigger, which enqueues its fixed task. */
export function enqueueDemoJob(path: string): Promise<TriggerResponse> {
  return api.post<TriggerResponse>(path, {});
}
