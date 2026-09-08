import { Suspense } from "react";
import { useThemeMode } from "@/lib/theme";
import { demoComponent } from "./registry";
import type { DemoId } from "./types";

/**
 * One interactive demo, framed and ready to drop into a page.
 *
 * Two things the demo modal used to supply and a plain page does not:
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
 * by every doc page, so an eager barrel would put every demo on all of them.
 * Prerender resolves the chunk through `Suspense`, so the static HTML still
 * carries the demo.
 */
export function DocDemo({ id }: { id: DemoId }) {
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
