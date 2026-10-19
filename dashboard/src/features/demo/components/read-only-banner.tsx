import { Eye, Play } from "lucide-react";
import { Button } from "@/components/ui";
import { useAuthStatus } from "@/features/auth";
import { useDemoEnqueue } from "../hooks";

/** Shown on a public read-only dashboard, with the demo's one write. */
export function ReadOnlyBanner() {
  const status = useAuthStatus();
  if (!status.data?.read_only) return null;
  const enqueue = status.data.demo_enqueue;

  return (
    <div className="flex shrink-0 flex-wrap items-center gap-3 border-b border-info/30 bg-info-dim px-4 py-2 text-[0.82rem] md:px-6">
      <Eye className="size-4 shrink-0 text-info" aria-hidden />
      <span className="flex-1">
        Read-only demo. Browse freely; cancel, replay and other changes are switched off.
      </span>
      {enqueue ? <EnqueueButton path={enqueue} /> : null}
    </div>
  );
}

function EnqueueButton({ path }: { path: string }) {
  const enqueue = useDemoEnqueue(path);
  return (
    <Button size="sm" onClick={() => enqueue.mutate()} disabled={enqueue.isPending}>
      <Play className="size-3.5" aria-hidden />
      Enqueue a job
    </Button>
  );
}
