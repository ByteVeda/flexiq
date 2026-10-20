import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { ApiError } from "@/lib/api-client";
import { enqueueDemoJob } from "./api";

/** Enqueue through the demo trigger, then refresh every view. */
export function useDemoEnqueue(path: string) {
  const qc = useQueryClient();
  return useMutation({
    mutationFn: () => enqueueDemoJob(path),
    onError: (error) => {
      // The trigger's rate bucket is shared by every visitor.
      const limited = error instanceof ApiError && error.status === 429;
      toast.error(limited ? "The demo is busy — try again in a minute" : "Couldn't enqueue", {
        description: limited ? undefined : error instanceof Error ? error.message : String(error),
      });
    },
    onSuccess: (result) => {
      const id = result.jobs[0]?.id;
      toast.success("Job enqueued", { description: id });
    },
    onSettled: () => qc.invalidateQueries(),
  });
}
