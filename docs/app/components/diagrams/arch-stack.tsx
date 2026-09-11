import { type CSSProperties, Fragment, type ReactNode } from "react";
import { SdkBinding, SdkName, SdkSwap } from "@/components/sdk-text";

// Layered architecture stack — exact port of the prototype's `.archstack` HTML
// (semantic layers + binding/store boundaries as direct children). The
// user-facing layer adapts to the active SDK via the SDK-text atoms.

type Kind = "py" | "rust" | "store" | "";

interface Layer {
  tag: ReactNode;
  /** Pill colour: `.ltag t-*`. */
  tagKind?: Kind;
  /** `.layer` modifier; defaults to `tagKind`. The prototype sometimes pairs a
   *  `rust` layer surface with a `py` pill (e.g. the resource-proxy layer). */
  layerKind?: Kind;
  style?: CSSProperties;
  title: string;
  body: ReactNode;
  role?: string;
}

function ArchStack({
  layers,
  bounds,
  legend,
}: {
  layers: Layer[];
  bounds: ReactNode[]; // one fewer than layers
  legend?: ReactNode;
}) {
  return (
    <div className="archstack">
      {legend ? <div className="arch-legend">{legend}</div> : null}
      {layers.map((l, i) => (
        <Fragment key={l.title}>
          <div
            className={`layer ${l.layerKind ?? l.tagKind ?? ""}`.trim()}
            style={l.style}
          >
            <span className={`ltag t-${l.tagKind ?? "store"}`}>{l.tag}</span>
            <div className="lbody">
              <div className="lt">{l.title}</div>
              <div className="ld">{l.body}</div>
            </div>
            {l.role ? <span className="lrole">{l.role}</span> : null}
          </div>
          {i < bounds.length ? <div className="bound">{bounds[i]}</div> : null}
        </Fragment>
      ))}
    </div>
  );
}

/** `architecture/overview.mdx` — the Python / Rust / Store stack. */
export function ArchitectureStack() {
  return (
    <ArchStack
      legend={
        <>
          <span className="al-item">
            <span className="al-arrow down">↓</span> you call down — enqueue
            jobs
          </span>
          <span className="al-item">
            <span className="al-arrow up">↑</span> results &amp; state flow back
            up
          </span>
        </>
      }
      layers={[
        {
          tag: <SdkName />,
          tagKind: "py",
          title: "User-facing API",
          body: (
            <>
              <code>Queue</code>,{" "}
              <SdkSwap
                python={
                  <>
                    <code>@task</code>, <code>.delay()</code>
                  </>
                }
                node={
                  <>
                    <code>.task()</code>, <code>.enqueue()</code>
                  </>
                }
                java={
                  <>
                    <code>Task.of()</code>, <code>enqueue()</code>
                  </>
                }
                rust={
                  <>
                    <code>#[task]</code>, <code>::call()</code>
                  </>
                }
              />
              , results,{" "}
              <SdkSwap
                python="workflows, resources"
                node="workflows, resources"
                java="workflows, resources"
                // The Rust shell wraps neither: workflows are the separate
                // `flexiq-workflows` crate behind a feature, and there is no
                // resource injection at all.
                rust="durable steps"
              />{" "}
              — the surface you write against.
            </>
          ),
          role: "what you write",
        },
        {
          tag: "Rust",
          tagKind: "rust",
          title: "Engine",
          body: (
            <>
              Scheduler, dispatcher, worker pool, rate limiter — Tokio runtime,
              OS-thread pool, near-zero <SdkName /> overhead.
            </>
          ),
          role: "where the work runs",
        },
        {
          tag: "Store",
          tagKind: "store",
          title: "Storage layer",
          body: "SQLite (bundled) or Postgres — jobs, results, schedules, rate-limit state, all in one place.",
          role: "where state lives",
        },
      ]}
      bounds={[
        <>
          <span className="bdir">↓</span>
          <span className="pyo3">
            <SdkBinding /> boundary
          </span>
          <span className="bdir">↑</span>
        </>,
        <>
          <span className="bdir">↓</span>reads &amp; writes
          <span className="bdir">↑</span>
        </>,
      ]}
    />
  );
}

/** `architecture/resources.mdx` — the 3-layer resource pipeline. */
export function ResourcePipeline() {
  return (
    <ArchStack
      layers={[
        {
          tag: "Layer 1",
          tagKind: "py",
          title: "Argument interception",
          body: (
            <>
              The <code>ArgumentInterceptor</code> walks every argument before
              serialization. CONVERT types become JSON-safe markers, REDIRECT
              types become DI placeholders, PROXY types are deconstructed,
              REJECT types raise in strict mode.
            </>
          ),
        },
        {
          tag: "Layer 2",
          tagKind: "py",
          title: "Worker resource runtime",
          body: (
            <>
              <code>ResourceRuntime</code> initializes resources at worker
              startup in topological order, then injects requested ones
              <SdkSwap
                python={
                  <>
                    {" "}
                    (via <code>inject=</code> or <code>Inject["name"]</code>)
                  </>
                }
                node={
                  <>
                    {" "}
                    (via <code>inject:</code> or <code>useResource()</code>)
                  </>
                }
                java={
                  <>
                    {" "}
                    (via <code>Resources.use()</code>)
                  </>
                }
                // Not a missing binding — the Rust shell has no injection at
                // all, so the clause has to go rather than name a spelling.
                rust=" — though never into a Rust task, which takes its arguments and nothing else"
              />
              . Task-scoped resources come from a semaphore pool.
            </>
          ),
        },
        {
          tag: "Layer 3",
          tagKind: "py",
          layerKind: "rust",
          style: {
            borderColor: "var(--indigo-line)",
            background:
              "linear-gradient(100deg,var(--indigo-soft),var(--panel))",
          },
          title: "Resource proxies",
          body: (
            <>
              <code>ProxyHandler</code>s deconstruct live objects (file handles,
              HTTP sessions, cloud clients) into a JSON recipe and reconstruct
              them on the worker. Recipes are optionally HMAC-signed for tamper
              detection.
            </>
          ),
        },
      ]}
      bounds={["enqueue → worker", "before task()"]}
    />
  );
}
