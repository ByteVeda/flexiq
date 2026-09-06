import { Suspense } from "react";
import { useThemeMode } from "@/lib/theme";
import { demoComponent } from "./registry";
import type { DemoId } from "./types";

/**
 * The interactive demos as MDX components, one barrel in the shape of
 * `@/components/diagrams` so a page can write the bare tag with no import.
 *
 * Two things the modal used to supply and MDX does not:
 *
 * - **`theme`.** MDX passes no props, so each demo's `DemoProps` comes from
 *   {@link useThemeMode}, which reads `<html data-theme>` directly and needs no
 *   provider above it.
 * - **`.dm-stage`.** Every rule in `demos.css` is scoped under that class on
 *   purpose — the demos use generic names (`.stage`, `.ctl`, `.seg`, `.legend`)
 *   that would collide with the docs stylesheet unscoped. The wrapper keeps the
 *   scope; `.doc-demo` re-adds the frame the dialog used to draw.
 *
 * The `lazy()` split in {@link demoComponent} is kept: `mdxComponents` is loaded
 * by every doc page, so an eager barrel would put all seven demos on all of
 * them. Prerender resolves the chunk through `Suspense`, so the static HTML
 * still carries the demo.
 */
function DocDemo({ id }: { id: DemoId }) {
  const theme = useThemeMode();
  const Demo = demoComponent(id);
  if (!Demo) {
    return null;
  }
  return (
    <div className="dm-stage doc-demo">
      <Suspense
        fallback={
          <div className="dm-loading">
            <span className="dm-spin" />
            Loading demo…
          </div>
        }
      >
        <Demo theme={theme} />
      </Suspense>
    </div>
  );
}

/** Chunked upload streaming progress back to the caller. */
export function ProgressDemo() {
  return <DocDemo id="progress" />;
}

/** A token bucket pacing dispatch against a provider limit. */
export function RateLimitDemo() {
  return <DocDemo id="ratelimit" />;
}

/** Attempts, backoff and the hand-off to the dead-letter queue. */
export function RecoveryDemo() {
  return <DocDemo id="recovery" />;
}

/** Worker-pool size against throughput and latency. */
export function ScalingDemo() {
  return <DocDemo id="scaling" />;
}

/** A workflow DAG running, with per-node dependencies and status. */
export function WorkflowDemo() {
  return <DocDemo id="workflow" />;
}

/** A multi-step process failing and compensating backwards. */
export function SagaDemo() {
  return <DocDemo id="saga" />;
}

/**
 * Routing keys fanning into the pool built to run them (gpu / default / email).
 *
 * Named for what it shows rather than `MeshDemo`, which is already taken by the
 * work-stealing diagram in `@/components/diagrams` — both land in the same MDX
 * component map, so the identifiers cannot repeat.
 */
export function TaskAffinityDemo() {
  return <DocDemo id="mesh" />;
}
