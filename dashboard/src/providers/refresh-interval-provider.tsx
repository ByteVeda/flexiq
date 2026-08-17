import { createContext, type ReactNode, useContext, useMemo, useState } from "react";

export type RefreshOption = "2s" | "5s" | "10s" | "off";

const REFRESH_MS: Record<RefreshOption, number | false> = {
  "2s": 2_000,
  "5s": 5_000,
  "10s": 10_000,
  off: false,
};

const STORAGE_KEY = "flexiq.refresh";
const DEFAULT_OPTION: RefreshOption = "5s";

interface RefreshContextValue {
  option: RefreshOption;
  intervalMs: number | false;
  setOption: (option: RefreshOption) => void;
}

const RefreshContext = createContext<RefreshContextValue | null>(null);

export function parseRefreshOption(stored: string | null): RefreshOption {
  if (stored === "2s" || stored === "5s" || stored === "10s" || stored === "off") return stored;
  return DEFAULT_OPTION;
}

export function refreshIntervalMs(option: RefreshOption): number | false {
  return REFRESH_MS[option];
}

function readStored(): RefreshOption {
  return parseRefreshOption(localStorage.getItem(STORAGE_KEY));
}

export function RefreshIntervalProvider({ children }: { children: ReactNode }) {
  const [option, setOptionState] = useState<RefreshOption>(() => readStored());

  const value = useMemo<RefreshContextValue>(
    () => ({
      option,
      intervalMs: refreshIntervalMs(option),
      setOption: (next) => {
        setOptionState(next);
        localStorage.setItem(STORAGE_KEY, next);
      },
    }),
    [option],
  );

  return <RefreshContext.Provider value={value}>{children}</RefreshContext.Provider>;
}

export function useRefreshInterval(): RefreshContextValue {
  const ctx = useContext(RefreshContext);
  if (!ctx) throw new Error("useRefreshInterval must be used within RefreshIntervalProvider");
  return ctx;
}
