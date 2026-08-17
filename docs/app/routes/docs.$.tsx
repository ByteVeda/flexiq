import { MDXProvider } from "@mdx-js/react";
import { Fragment, lazy, Suspense, useEffect, useMemo } from "react";
import { Link, type MetaDescriptor, useNavigate } from "react-router";
import { PrevNext } from "@/components/docs";
import { mdxComponents } from "@/components/mdx";
import { getDocLoader } from "@/lib/content";
import { docMeta } from "@/lib/manifest";
import { redirectFor } from "@/lib/redirects";
import { isSdk } from "@/lib/sdk-registry";
import type { Route } from "./+types/docs.$";

function pathOf(params: { "*"?: string }): string {
  return `/${params["*"] ?? ""}`;
}

/** Breadcrumb from the URL segments; the leading segment links to its section index. */
function Breadcrumb({ path }: { path: string }) {
  const segs = path.split("/").filter(Boolean);
  if (segs.length < 2) {
    return null;
  }
  const crumbs = segs.slice(0, -1).map((seg, i) => {
    const href = `/${segs.slice(0, i + 1).join("/")}`;
    const label =
      docMeta(href)?.title ??
      seg.replace(/-/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
    // The SDK root (e.g. `/python`) has no page of its own — it only redirects
    // to the first doc — so render it as a plain label, not a link.
    return { href, label, linked: !(i === 0 && isSdk(seg)) };
  });
  return (
    <div className="crumb">
      {crumbs.map((c) => (
        <Fragment key={c.href}>
          {c.linked ? (
            <Link to={c.href}>{c.label}</Link>
          ) : (
            <span className="crumb-root">{c.label}</span>
          )}
          <span className="sep">/</span>
        </Fragment>
      ))}
    </div>
  );
}

export function meta({ params }: Route.MetaArgs) {
  const path = pathOf(params);
  const dest = redirectFor(path);
  if (dest) {
    // Static redirect stub for a moved URL. The meta-refresh resolves against
    // the origin, so it needs the deploy base path; SPA navigate() adds it.
    const base = import.meta.env.BASE_URL.replace(/\/$/, "");
    const target = `${base}${dest}`;
    return [
      { title: "Redirecting… | FlexiQ" },
      { httpEquiv: "refresh", content: `0; url=${target}` },
      { tagName: "link", rel: "canonical", href: target },
    ];
  }
  const meta = docMeta(path);
  const tags: MetaDescriptor[] = [
    { title: meta?.title ? `${meta.title} | FlexiQ` : "FlexiQ" },
    { name: "description", content: meta?.description ?? "" },
  ];
  if (meta?.canonical) {
    // Shared pages mount at one URL per SDK; point search engines at the
    // default-SDK mount (self-referential there — still valid).
    const base = import.meta.env.BASE_URL.replace(/\/$/, "");
    tags.push({
      tagName: "link",
      rel: "canonical",
      href: `${base}${meta.canonical}`,
    });
  }
  return tags;
}

/** Stub shown at a moved URL: meta-refresh handles direct hits, this handles
 *  client-side (SPA) navigation by replacing the history entry. */
function RedirectStub({ to }: { to: string }) {
  const navigate = useNavigate();
  useEffect(() => {
    navigate(to, { replace: true });
  }, [to, navigate]);
  return (
    <article className="article">
      <h1>Redirecting…</h1>
      <p className="lead">
        This page moved to <Link to={to}>{to}</Link>.
      </p>
    </article>
  );
}

export default function DocRoute({ params }: Route.ComponentProps) {
  const path = pathOf(params);
  const dest = redirectFor(path);
  const loader = getDocLoader(path);
  const meta = docMeta(path);

  // Each page is its own lazily-loaded chunk; React.lazy is resolved during
  // prerender (Suspense) so the static HTML still carries the full content.
  const Page = useMemo(() => (loader ? lazy(loader) : null), [loader]);

  if (dest) {
    return <RedirectStub to={dest} />;
  }

  if (!Page) {
    return (
      <article className="article">
        <h1>Page not found</h1>
        <p className="lead">No documentation page matches this URL.</p>
      </article>
    );
  }

  // Last path segment as a page id, so page-specific styling (e.g. FAQ question
  // numbering) can hook a single page via `.article[data-page="faq"]`.
  const pageId = path.split("/").filter(Boolean).pop() ?? "";

  return (
    <article className="article" data-page={pageId}>
      <Breadcrumb path={path} />
      {meta?.title ? <h1>{meta.title}</h1> : null}
      {meta?.description ? <p className="lead">{meta.description}</p> : null}
      <MDXProvider components={mdxComponents}>
        {/* Key by path: client-side navigation between two pages that share this
            splat route re-suspends the lazy page. Without a fresh boundary per
            path, React retains the previous committed tree for the whole route.

            React.lazy has no SSR hydration: on the client the chunk re-loads, so
            the prerendered body briefly drops to the fallback and re-expands.
            Reserve vertical space so that swap doesn't shove the prev/next footer
            up and down (layout shift). */}
        <Suspense
          key={path}
          fallback={<div className="doc-loading" aria-hidden="true" />}
        >
          <Page />
        </Suspense>
      </MDXProvider>
      <PrevNext />
    </article>
  );
}
