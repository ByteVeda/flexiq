import {
  Children,
  type ComponentProps,
  isValidElement,
  type ReactElement,
  type ReactNode,
  useId,
  useRef,
} from "react";
import { Link } from "react-router";
import { type Sdk, useActiveSdk, useSdk } from "@/hooks";
import { SDK_PROFILES } from "@/lib";

/** Show children only under one SDK. All variants ship in the HTML; the inactive
 *  one is hidden by CSS off `<html data-sdk>` — no flash, no hydration mismatch. */
export function SdkOnly({ sdk, children }: { sdk: Sdk; children: ReactNode }) {
  return <div data-sdk-variant={sdk}>{children}</div>;
}

/** Internal link from a shared page into an SDK-specific tree. `to="guides/x"`
 *  resolves to `/{activeSdk}/guides/x` and re-points when the SDK switches. */
export function SdkLink({
  to,
  children,
  ...rest
}: { to: string; children: ReactNode } & Omit<
  ComponentProps<typeof Link>,
  "to" | "children"
>) {
  // useActiveSdk, not useSdk: on a /python/* URL the store can still hold
  // another SDK from localStorage until docs-layout's effect syncs it, and the
  // link would resolve to that SDK's mount for the first paint. Same reason
  // `<Card to=>` reads it; the two are one component's worth of behaviour.
  const sdk = useActiveSdk();
  const path = to.startsWith("/") ? to : `/${to}`;
  return (
    <Link to={`/${sdk}${path}`} {...rest}>
      {children}
    </Link>
  );
}

type TabChild = ReactElement<{ sdk: Sdk; children: ReactNode }>;

/** Synced code/prose tabs: one panel per `<Tab sdk="...">`. Selecting a tab sets
 *  the global SDK, so every CodeTabs on the page and the sidebar switch together.
 *  Visibility is CSS-driven (data-sdk-variant); the tablist is APG-compliant. */
export function CodeTabs({ children }: { children: ReactNode }) {
  const { sdk, setSdk } = useSdk();
  const baseId = useId();
  const panels = Children.toArray(children).filter(
    isValidElement,
  ) as TabChild[];
  const order = panels.map((p) => p.props.sdk);
  const tabs = useRef<(HTMLButtonElement | null)[]>([]);
  const tabId = (variant: Sdk) => `${baseId}-tab-${variant}`;
  const panelId = (variant: Sdk) => `${baseId}-panel-${variant}`;

  function onKeyDown(event: React.KeyboardEvent, index: number) {
    const delta =
      event.key === "ArrowRight" ? 1 : event.key === "ArrowLeft" ? -1 : 0;
    if (delta === 0) {
      return;
    }
    event.preventDefault();
    const next = (index + delta + order.length) % order.length;
    setSdk(order[next]);
    tabs.current[next]?.focus();
  }

  return (
    <div className="tabs">
      <div className="tablist" role="tablist" aria-label="SDK">
        {order.map((variant, index) => {
          const selected = variant === sdk;
          return (
            <button
              key={variant}
              type="button"
              role="tab"
              id={tabId(variant)}
              aria-controls={panelId(variant)}
              ref={(el) => {
                tabs.current[index] = el;
              }}
              data-sdk-variant={variant}
              className="tab sdk-tab"
              aria-selected={selected}
              tabIndex={selected ? 0 : -1}
              onClick={() => setSdk(variant)}
              onKeyDown={(e) => onKeyDown(e, index)}
            >
              {SDK_PROFILES[variant].label}
            </button>
          );
        })}
      </div>
      {panels.map((panel) => (
        <div
          key={panel.props.sdk}
          role="tabpanel"
          id={panelId(panel.props.sdk)}
          aria-labelledby={tabId(panel.props.sdk)}
          data-sdk-variant={panel.props.sdk}
          className="sdk-tabpanel"
        >
          {panel.props.children}
        </div>
      ))}
    </div>
  );
}
