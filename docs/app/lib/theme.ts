import { useEffect, useState } from "react";

export type ThemeMode = "dark" | "light";

/** Current theme from the `<html data-theme>` attribute (dark default / SSR). */
function readTheme(): ThemeMode {
  if (typeof document === "undefined") {
    return "dark";
  }
  return document.documentElement.getAttribute("data-theme") === "light"
    ? "light"
    : "dark";
}

/** Reactive current theme, kept in sync with `<html data-theme>` changes. */
export function useThemeMode(): ThemeMode {
  const [mode, setMode] = useState<ThemeMode>("dark");
  useEffect(() => {
    setMode(readTheme());
    const observer = new MutationObserver(() => setMode(readTheme()));
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["data-theme"],
    });
    return () => observer.disconnect();
  }, []);
  return mode;
}

/** Flip and persist the theme (consumed by the no-flash bootstrap in root.tsx). */
export function toggleTheme(): void {
  const next: ThemeMode = readTheme() === "light" ? "dark" : "light";
  document.documentElement.setAttribute("data-theme", next);
  try {
    localStorage.setItem("flexiq-theme", next);
  } catch {
    // ignore storage failures (private mode etc.)
  }
}
