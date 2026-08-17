// Open-redirect guard for post-login redirect targets.

/**
 * Whether `path` is safe as a same-origin redirect target. Accepts only
 * relative paths rooted at `/`; rejects absolute URLs, protocol-relative
 * URLs (`//evil.com/x`), and backslash tricks. Empty input is rejected so
 * callers fall back to a default explicitly.
 */
export function isSafeRedirect(path: string | null | undefined): boolean {
  if (!path?.startsWith("/")) {
    return false;
  }
  if (path.startsWith("//") || path.startsWith("/\\")) {
    return false;
  }
  // A parseable absolute URL (with scheme or authority) is never safe.
  try {
    // Base-relative parse: if the path smuggles a scheme/host, the resolved
    // origin differs from the sentinel base.
    const resolved = new URL(path, "http://flexiq.invalid");
    return resolved.origin === "http://flexiq.invalid";
  } catch {
    return false;
  }
}
