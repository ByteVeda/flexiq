import type { ReactNode } from "react";
import { Link } from "react-router";
import { useActiveSdk } from "@/hooks";

/** Design-matched replacement for `fumadocs-ui/components/card` (aliased in vite). */
export function Cards({ children }: { children: ReactNode }) {
  return <div className="next-grid">{children}</div>;
}

export function Card({
  title,
  href,
  to,
  icon,
  description,
  children,
}: {
  title: string;
  /** Absolute destination, for SDK-neutral pages. */
  href?: string;
  /** SDK-relative destination (`to="modules/steps"`), for shared pages — the
   *  active SDK's prefix is added, exactly like `<SdkLink>`. Checked by
   *  `scripts/parity/checks/links.mjs` under the same rule. */
  to?: string;
  icon?: ReactNode;
  description?: ReactNode;
  children?: ReactNode;
}) {
  // useActiveSdk, not useSdk: on a /python/* URL the store can still hold the
  // default until an effect syncs it, and a `to=` card would point at the
  // wrong SDK's mount. Every other SDK-relative link resolves the same way.
  const sdk = useActiveSdk();
  const body = (
    <>
      <span className="nt">
        {icon}
        {title}
      </span>
      {(description ?? children) ? (
        <span className="nd">{description ?? children}</span>
      ) : null}
    </>
  );
  const target =
    to === undefined ? href : `/${sdk}${to.startsWith("/") ? to : `/${to}`}`;
  if (!target) {
    return <div className="next-card">{body}</div>;
  }
  if (/^(https?:|mailto:)/.test(target)) {
    return (
      <a className="next-card" href={target}>
        {body}
      </a>
    );
  }
  return (
    <Link className="next-card" to={target}>
      {body}
    </Link>
  );
}
