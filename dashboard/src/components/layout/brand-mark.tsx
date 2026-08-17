/**
 * The flexiq mark — the same artwork the docs site and README use, bundled as
 * an image rather than drawn inline.
 *
 * Unlike the drawn mark it replaced, this does not follow `--accent` (or an
 * operator's `brandAccent` override); the logo has fixed brand colors. The
 * wordmark beside it in the sidebar still tracks the accent.
 *
 * `size` is the rendered height — the artwork is wider than tall, so the width
 * follows from its aspect ratio.
 */
export function BrandMark({ size = 38, className }: { size?: number; className?: string }) {
  return (
    <img
      src="/logo.png"
      alt=""
      aria-hidden="true"
      style={{ height: size, width: "auto" }}
      className={className}
    />
  );
}
