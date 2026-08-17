// Pre-hydration theme bootstrap. Lives in its own file (not inline in
// index.html) so the servers can ship a CSP with `script-src 'self'` —
// an inline script would need a hash that drifts with every edit.
(() => {
  const stored = localStorage.getItem("taskito.theme");
  const preferred =
    stored ?? (window.matchMedia("(prefers-color-scheme: dark)").matches ? "dark" : "light");
  document.documentElement.classList.toggle("dark", preferred === "dark");
})();
