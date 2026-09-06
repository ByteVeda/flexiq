import {
  isRouteErrorResponse,
  Links,
  Meta,
  Outlet,
  Scripts,
  ScrollRestoration,
} from "react-router";
import { usePrefetchDocs } from "@/hooks";
import { DEFAULT_SDK, SDK_IDS } from "@/lib";
import type { Route } from "./+types/root";
import "./app.css";

// `public/` assets are copied verbatim, so they need the deploy base prefix
// (`/flexiq/` on Pages, `/` locally) rather than a bare absolute path.
const asset = (file: string) => `${import.meta.env.BASE_URL}${file}`;

export const links: Route.LinksFunction = () => [
  {
    rel: "icon",
    type: "image/x-icon",
    href: asset("favicon.ico"),
    sizes: "48x48",
  },
  {
    rel: "icon",
    type: "image/png",
    href: asset("favicon-96x96.png"),
    sizes: "96x96",
  },
  {
    rel: "apple-touch-icon",
    href: asset("apple-touch-icon.png"),
    sizes: "180x180",
  },
  { rel: "manifest", href: asset("site.webmanifest") },
  { rel: "preconnect", href: "https://fonts.googleapis.com" },
  {
    rel: "preconnect",
    href: "https://fonts.gstatic.com",
    crossOrigin: "anonymous",
  },
  {
    rel: "stylesheet",
    href: "https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600;700&family=IBM+Plex+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500;600;700&family=Shantell+Sans:wght@500;600;700&display=optional",
  },
];

// Apply the persisted theme before paint to avoid a light/dark flash.
const THEME_INIT = `(function(){try{var t=localStorage.getItem('flexiq-theme')||'dark';document.documentElement.setAttribute('data-theme',t);}catch(e){}})();`;

// Registry ids + default + deploy base as inert JSON on the <html> data
// attribute below; the base lets the boot script find the URL's SDK segment.
const SDK_CONFIG = JSON.stringify({
  ids: [...SDK_IDS],
  def: DEFAULT_SDK,
  base: import.meta.env.BASE_URL.replace(/\/$/, ""),
});

// No-flash SDK bootstrap: URL prefix > query > localStorage > default — same
// precedence as useActiveSdk, so shared pages (one file mounted per SDK) paint
// the URL's SDK variants immediately. Static string (no interpolation); keeps
// the SSR default on error.
const SDK_INIT = `(function(){try{var c=JSON.parse(document.documentElement.getAttribute('data-sdk-config')),ids=c.ids,def=c.def;var seg=location.pathname.slice(c.base.length).split('/')[1];var u=new URLSearchParams(location.search).get('sdk');var s=ids.indexOf(seg)>=0?seg:ids.indexOf(u)>=0?u:(localStorage.getItem('flexiq-sdk')||def);if(ids.indexOf(s)<0){s=def;}document.documentElement.setAttribute('data-sdk',s);}catch(e){}})();`;

export function Layout({ children }: { children: React.ReactNode }) {
  return (
    <html
      lang="en"
      data-theme="dark"
      data-sdk={DEFAULT_SDK}
      data-sdk-config={SDK_CONFIG}
      suppressHydrationWarning
    >
      <head>
        <meta charSet="utf-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1" />
        {/* biome-ignore lint/security/noDangerouslySetInnerHtml: tiny no-flash theme bootstrap */}
        <script dangerouslySetInnerHTML={{ __html: THEME_INIT }} />
        {/* biome-ignore lint/security/noDangerouslySetInnerHtml: static no-flash sdk bootstrap */}
        <script dangerouslySetInnerHTML={{ __html: SDK_INIT }} />
        <Meta />
        <Links />
      </head>
      <body>
        {children}
        <ScrollRestoration />
        <Scripts />
      </body>
    </html>
  );
}

export default function App() {
  usePrefetchDocs();
  return <Outlet />;
}

export function ErrorBoundary({ error }: Route.ErrorBoundaryProps) {
  const message = isRouteErrorResponse(error)
    ? `${error.status} ${error.statusText}`
    : "Something went wrong";
  return (
    <main className="error-page">
      <h1>{message}</h1>
      {/* Raw anchor, not <Link> — the router may be the thing that failed. So
          the base prefix has to be applied by hand, or this lands on the domain
          root instead of the site root under DOCS_BASE_PATH. */}
      <a href={import.meta.env.BASE_URL}>Back to the docs</a>
    </main>
  );
}
