import { type KeyboardEvent, useEffect, useRef, useState } from "react";
import { Link } from "react-router";
import { RawHtml } from "@/components/ui";
import { type Sdk, useActiveSdk } from "@/hooks";
import { DemoModal, type DemoTarget } from "./demo-modal";

// Scenario guides are authored as Python paths; most map to another SDK by
// swapping the SDK segment, but a few pages live under a different slug there.
const GUIDE_OVERRIDES: Record<string, Record<string, string>> = {
  "/python/guides/core/streaming": {
    node: "/node/guides/core/streaming",
    java: "/java/guides/core/streaming",
  },
};

/** Resolve a scenario's Python guide path to the active SDK's docs. */
function guideHref(pyPath: string, sdk: Sdk): string {
  if (sdk === "python") return pyPath;
  return (
    GUIDE_OVERRIDES[pyPath]?.[sdk] ?? pyPath.replace("/python/", `/${sdk}/`)
  );
}

interface Bullet {
  /** Short mono label in the left rail. */
  k: string;
  /** Trusted inline HTML (may contain `<code>`). */
  v: string;
}

interface Scenario {
  /** Slug — stable key + tab/panel id. */
  id: string;
  /** Plain-language problem shown in the left pane. */
  q: string;
  /** Category surfaced on the answer card. */
  tag: string;
  /** Inner SVG markup for a 24×24 stroke icon. */
  icon: React.ReactNode;
  title: string;
  /** Trusted inline HTML (may contain `<code>`). */
  blurb: string;
  /** Exactly three points on how it works; `v` is trusted inline HTML. */
  bullets: [Bullet, Bullet, Bullet];
  /** Pre-highlighted Python (span-tokenized HTML, `\n`-separated). */
  code: string;
  /** Primary CTA — opens the matching live demo in a modal. */
  demo: DemoTarget & { label: string };
  /** Secondary CTA — the guide that goes deeper. */
  guide: { to: string; label: string };
}

const SCENARIOS: Scenario[] = [
  {
    id: "heavy",
    q: "A user uploaded something huge and the request is hanging",
    tag: "Heavy processing",
    icon: (
      <>
        <path d="M12 3v12" />
        <path d="m8 11 4 4 4-4" />
        <path d="M4 17v2a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-2" />
      </>
    ),
    title: "Hand it off, return instantly, stream the progress back",
    blurb:
      "Never make a user watch a spinner. Push the work into flexiq, respond in milliseconds, and report a live percentage as it runs.",
    bullets: [
      {
        k: "Respond now",
        v: "<code>.delay()</code> queues the job and your endpoint returns immediately",
      },
      {
        k: "Report progress",
        v: "call <code>progress.update()</code> from inside the task",
      },
      {
        k: "User sees it move",
        v: "your UI subscribes and shows a live % bar",
      },
    ],
    code: `<span class="fn">@queue.task</span>
<span class="kw">def</span> <span class="def">process</span>(file_id):
    <span class="kw">for</span> i, chunk <span class="kw">in</span> <span class="def">enumerate</span>(chunks):
        <span class="def">crunch</span>(chunk)
        progress.<span class="def">update</span>((i+<span class="num">1</span>)/total)  <span class="cmt"># ← live %</span>`,
    demo: {
      id: "progress",
      title: "Progress streaming — hand off & report live %",
      label: "Watch the progress stream",
    },
    guide: {
      to: "/python/guides/core/streaming",
      label: "Read the guide",
    },
  },
  {
    id: "blocked",
    q: "My API provider keeps blocking me with 429 errors",
    tag: "Rate limiting",
    icon: (
      <>
        <circle cx="12" cy="12" r="9" />
        <path d="M5.6 5.6l12.8 12.8" />
      </>
    ),
    title: "Back off, retry, and keep everything else flowing",
    blurb:
      "flexiq catches the 429, waits with exponential backoff, and retries — while your other jobs keep draining. Add a rate limit and you never hammer them again.",
    bullets: [
      {
        k: "Auto-retry",
        v: "a raised error re-queues the job, it is never dropped",
      },
      { k: "Exponential backoff", v: "each retry waits longer — 1s, 2s, 4s…" },
      {
        k: "Stay under the cap",
        v: "<code>rate_limit</code> throttles calls to their limit",
      },
    ],
    code: `<span class="fn">@queue.task</span>(max_retries=<span class="num">5</span>, backoff=<span class="str">"exponential"</span>)
<span class="fn">@queue.rate_limit</span>(<span class="str">"100/min"</span>)        <span class="cmt"># ← stay under their cap</span>
<span class="kw">def</span> <span class="def">call_api</span>(payload):
    resp = provider.<span class="def">send</span>(payload)
    resp.<span class="def">raise_for_status</span>()         <span class="cmt"># 429 → auto-retry</span>`,
    demo: {
      id: "ratelimit",
      title: "Rate limiting — what happens at 429",
      label: "Trigger a 429 yourself",
    },
    guide: {
      to: "/python/guides/reliability/rate-limiting",
      label: "Read the guide",
    },
  },
  {
    id: "failed",
    q: "A job failed halfway through — now what happens to it?",
    tag: "Failure recovery",
    icon: (
      <>
        <path d="M3 12a9 9 0 1 0 3-6.7L3 8" />
        <path d="M3 3v5h5" />
        <path d="M12 8v4l2 2" />
      </>
    ),
    title: "Retry a few times, then dead-letter it — never silently lose it",
    blurb:
      "Transient failures retry automatically. When a job truly can't succeed, it lands in the dead-letter queue so you can inspect, fix, and replay it — instead of it vanishing.",
    bullets: [
      { k: "Every attempt logged", v: "see the error and timing for each try" },
      {
        k: "Then dead-letter",
        v: "after <code>max_retries</code> the job moves to the DLQ",
      },
      {
        k: "Replay by hand",
        v: "requeue DLQ jobs once you've fixed the cause",
      },
    ],
    code: `<span class="fn">@queue.task</span>(max_retries=<span class="num">3</span>, dead_letter=<span class="kw">True</span>)
<span class="kw">def</span> <span class="def">charge</span>(order_id):
    gateway.<span class="def">charge</span>(order_id)   <span class="cmt"># fails 3× → DLQ, not lost</span>`,
    demo: {
      id: "recovery",
      title: "Failure recovery — a job's lifecycle",
      label: "Scrub a job's lifecycle",
    },
    guide: {
      to: "/python/guides/reliability/retries",
      label: "Read the guide",
    },
  },
  {
    id: "burst",
    q: "I need to fire off thousands of tasks at once",
    tag: "Scaling",
    icon: (
      <>
        <rect x="3" y="3" width="7" height="7" rx="1.5" />
        <rect x="14" y="3" width="7" height="7" rx="1.5" />
        <rect x="3" y="14" width="7" height="7" rx="1.5" />
        <rect x="14" y="14" width="7" height="7" rx="1.5" />
      </>
    ),
    title: "Queue the whole burst, then scale workers to match",
    blurb:
      "Enqueue as fast as you can loop. The queue absorbs the spike and workers pull at their own pace — add more workers or concurrency to go faster, with memory staying flat.",
    bullets: [
      { k: "Nothing is lost", v: "the queue buffers the entire burst" },
      {
        k: "Tune throughput",
        v: "<code>workers × concurrency</code> sets your rate",
      },
      { k: "Backpressure built in", v: "slow consumers don't blow up memory" },
    ],
    code: `<span class="kw">for</span> email <span class="kw">in</span> mailing_list:        <span class="cmt"># 10,000 jobs, instantly</span>
    send_email.<span class="def">delay</span>(email)

<span class="cmt"># $ flexiq worker --workers 6 --concurrency 8</span>`,
    demo: {
      id: "scaling",
      title: "Worker scaling playground",
      label: "Open the scaling playground",
    },
    guide: { to: "/python/guides/core/workers", label: "Read the guide" },
  },
  {
    id: "deps",
    q: "Some tasks can't start until earlier ones finish",
    tag: "Workflows",
    icon: (
      <>
        <circle cx="5" cy="6" r="2.4" />
        <circle cx="19" cy="6" r="2.4" />
        <circle cx="12" cy="18" r="2.4" />
        <path d="M7 7l3.5 9M17 7l-3.5 9" />
      </>
    ),
    title: "Wire them into a workflow and flexiq runs them in order",
    blurb:
      "Chain tasks in sequence, fan them out in parallel, and join the results. flexiq resolves the dependency graph and only starts a step once its inputs are ready.",
    bullets: [
      {
        k: "Sequential",
        v: "<code>chain()</code> runs steps one after another",
      },
      { k: "Parallel", v: "<code>group()</code> fans out independent work" },
      {
        k: "Join",
        v: "<code>chord()</code> collects a group into one callback",
      },
    ],
    code: `workflow = <span class="def">chain</span>(
    fetch.<span class="def">s</span>(url),
    parse.<span class="def">s</span>(),
    <span class="def">group</span>(index.<span class="def">s</span>(), notify.<span class="def">s</span>()),   <span class="cmt"># parallel fan-out</span>
)
workflow.<span class="def">delay</span>()`,
    demo: {
      id: "workflow",
      title: "Workflow execution — DAG",
      label: "Watch the DAG execute",
    },
    guide: { to: "/python/modules/workflows", label: "Read the guide" },
  },
  {
    id: "mesh",
    q: "I need workers spread across many machines or regions",
    tag: "Mesh scheduling",
    icon: (
      <>
        <circle cx="6" cy="6" r="2.2" />
        <circle cx="18" cy="6" r="2.2" />
        <circle cx="6" cy="18" r="2.2" />
        <circle cx="18" cy="18" r="2.2" />
        <path d="M8.2 6H16M6 8.2V16M18 8.2V16M8.2 18H16M7.6 7.6l8.8 8.8" />
      </>
    ),
    title: "Run a worker mesh — route each job to the node built for it",
    blurb:
      "Because the queue lives in a shared store, any number of worker nodes pull straight from it — there's no broker to coordinate. Tag a job with a routing key and it lands on the pool meant to run it: GPU boxes, a specific region, or a low-priority lane.",
    bullets: [
      {
        k: "Brokerless mesh",
        v: "every node reads the same store — add machines and they just join",
      },
      {
        k: "Routing keys",
        v: '<code>queue="gpu"</code> sends a job only to GPU workers',
      },
      {
        k: "Safe coordination",
        v: "distributed locks stop two nodes grabbing the same job",
      },
    ],
    code: `<span class="fn">@queue.task</span>(queue=<span class="str">"gpu"</span>, max_retries=<span class="num">2</span>)
<span class="kw">def</span> <span class="def">render</span>(frame):
    gpu.<span class="def">render</span>(frame)

<span class="cmt"># each machine runs only the pool it is built for:</span>
<span class="cmt"># $ flexiq worker --queues gpu --concurrency 2   # GPU box</span>
<span class="cmt"># $ flexiq worker --queues default,email         # web box</span>`,
    demo: {
      id: "mesh",
      title: "Mesh scheduling — route jobs across the worker mesh",
      label: "Watch the mesh route",
    },
    guide: { to: "/python/modules/mesh", label: "Read the guide" },
  },
  {
    id: "saga",
    q: "A multi-step process failed and I need to undo earlier steps",
    tag: "Saga · compensation",
    icon: (
      <>
        <path d="M9 14 4 9l5-5" />
        <path d="M4 9h11a5 5 0 0 1 0 10h-4" />
      </>
    ),
    title: "Compensate as you go — roll back the steps that already ran",
    blurb:
      "Model a distributed transaction as a workflow where every step carries a compensating action. If a later step fails, flexiq runs the compensations in reverse order — so a half-finished booking never leaves a charged card or a reserved seat stranded.",
    bullets: [
      {
        k: "Per-step rollback",
        v: "attach a compensating task to each step",
      },
      {
        k: "Reverse unwind",
        v: "a failure triggers compensations newest-first",
      },
      {
        k: "No stuck state",
        v: "the saga ends either fully done or fully undone",
      },
    ],
    code: `saga = <span class="def">chain</span>(
    reserve_seat.<span class="def">s</span>(trip),  compensate=release_seat.<span class="def">s</span>(),
    charge_card.<span class="def">s</span>(trip),   compensate=refund_card.<span class="def">s</span>(),
    book_hotel.<span class="def">s</span>(trip),    compensate=cancel_hotel.<span class="def">s</span>(),
)
saga.<span class="def">delay</span>()   <span class="cmt"># any failure → compensations run in reverse</span>`,
    demo: {
      id: "saga",
      title: "Saga — compensation & rollback",
      label: "Watch the saga roll back",
    },
    guide: { to: "/python/modules/workflows/saga", label: "Read the guide" },
  },
  {
    id: "worksteal",
    q: "One region is swamped while another sits idle",
    tag: "Work-stealing",
    icon: (
      <>
        <circle cx="12" cy="12" r="9" />
        <path d="M3 12h18M12 3a14 14 0 0 1 0 18M12 3a14 14 0 0 0 0 18" />
      </>
    ),
    title: "Idle regions steal the backlog from busy ones — over HTTP",
    blurb:
      "Run flexiq in several regions and they gossip queue depth peer-to-peer. When one region's backlog spikes, an idle peer pulls a batch of jobs over HTTP and runs them. Load self-balances across the mesh — still with no central broker to stand up.",
    bullets: [
      { k: "Gossip mesh", v: "regions share queue depth peer-to-peer" },
      {
        k: "Steal over HTTP",
        v: "an idle region pulls a batch from the busiest peer",
      },
      { k: "Self-balancing", v: "load evens out without a coordinator" },
    ],
    code: `queue = <span class="def">Queue</span>(db_path=<span class="str">"tasks.db"</span>, region=<span class="str">"eu-west"</span>)
queue.<span class="def">mesh</span>(peers=[<span class="str">"us-west-2"</span>, <span class="str">"ap-south-1"</span>], steal=<span class="kw">True</span>)

<span class="cmt"># idle workers pull from the busiest peer over HTTP</span>
<span class="cmt"># $ flexiq worker --mesh --steal-threshold 8</span>`,
    demo: {
      id: "worksteal",
      title: "Work-stealing — balance load across regions",
      label: "Watch regions steal work",
    },
    guide: { to: "/python/modules/mesh", label: "Read the guide" },
  },
];

/** Wraps a scenario's inner SVG paths in the shared 24×24 stroke frame. */
function FinderIcon({ children }: { children: React.ReactNode }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={1.9}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {children}
    </svg>
  );
}

const OPTION_ARROW = (
  <svg
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth={2.4}
    strokeLinecap="round"
    strokeLinejoin="round"
    aria-hidden="true"
  >
    <path d="M5 12h14M13 6l6 6-6 6" />
  </svg>
);

const CTA_ARROW = (
  <svg
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth={2.5}
    strokeLinecap="round"
    strokeLinejoin="round"
    aria-hidden="true"
  >
    <path d="M5 12h14M13 6l6 6-6 6" />
  </svg>
);

function motionDisabled(): boolean {
  if (typeof document === "undefined") return true;
  if (document.documentElement.getAttribute("data-motion") === "off")
    return true;
  return Boolean(
    window.matchMedia?.("(prefers-reduced-motion: reduce)").matches,
  );
}

/**
 * "Tell us what's going wrong" — a two-pane problem-picker for first-time
 * visitors. Left rail = APG tablist of plain-language problems; right = an answer
 * card (category, headline, three points, snippet, CTAs). React port of
 * `INTEGRATION-scenario-finder.md`; the primary CTA opens the matching live demo
 * in {@link DemoModal}, the secondary links the guide that goes deeper.
 */
export function ScenarioFinder() {
  const sdk = useActiveSdk();
  const [selected, setSelected] = useState(0);
  // Drives the card's enter animation; re-triggered on every selection change.
  const [entered, setEntered] = useState(true);
  const [activeDemo, setActiveDemo] = useState<DemoTarget | null>(null);
  const optionRefs = useRef<(HTMLButtonElement | null)[]>([]);

  useEffect(() => {
    if (motionDisabled()) {
      setEntered(true);
      return;
    }
    setEntered(false);
    const raf = requestAnimationFrame(() => setEntered(true));
    return () => cancelAnimationFrame(raf);
  }, []);

  function select(next: number, focus: boolean) {
    const i = (next + SCENARIOS.length) % SCENARIOS.length;
    setSelected(i);
    if (!motionDisabled()) {
      setEntered(false);
      requestAnimationFrame(() => setEntered(true));
    }
    if (focus) optionRefs.current[i]?.focus();
  }

  function onKeyDown(e: KeyboardEvent<HTMLButtonElement>, i: number) {
    switch (e.key) {
      case "ArrowDown":
      case "ArrowRight":
        e.preventDefault();
        select(i + 1, true);
        break;
      case "ArrowUp":
      case "ArrowLeft":
        e.preventDefault();
        select(i - 1, true);
        break;
      case "Home":
        e.preventDefault();
        select(0, true);
        break;
      case "End":
        e.preventDefault();
        select(SCENARIOS.length - 1, true);
        break;
      default:
        break;
    }
  }

  const active = SCENARIOS[selected];

  return (
    <section className="section" id="finder">
      <div className="wrap">
        <div className="section-head reveal">
          <div className="kicker">New here?</div>
          <h2>Tell us what's going wrong</h2>
          <p>
            Not sure you need a task queue? Pick the problem that sounds like
            yours — flexiq shows you how it handles it, with the exact code and
            a live demo you can try.
          </p>
        </div>

        <div className="finder reveal">
          <div className="fd-ask">
            <div className="fd-asklabel">
              <span className="fd-pin">?</span>
              Which sounds like you?
            </div>
            <div
              className="fd-opts"
              role="tablist"
              aria-label="Common situations"
              aria-orientation="vertical"
            >
              {SCENARIOS.map((s, i) => (
                <button
                  key={s.id}
                  type="button"
                  role="tab"
                  id={`fd-tab-${s.id}`}
                  className="fd-opt"
                  aria-selected={i === selected}
                  aria-controls="fd-panel"
                  tabIndex={i === selected ? 0 : -1}
                  ref={(el) => {
                    optionRefs.current[i] = el;
                  }}
                  onClick={() => select(i, false)}
                  onKeyDown={(e) => onKeyDown(e, i)}
                >
                  <span className="fd-optq">{s.q}</span>
                  <span className="fd-optarr">{OPTION_ARROW}</span>
                </button>
              ))}
            </div>
            <div className="fd-foot">
              <span className="fd-dots">
                <i />
                <i />
                <i />
              </span>
              Pick a problem — see how flexiq handles it.
            </div>
          </div>

          <div className="fd-answer">
            <div
              id="fd-panel"
              role="tabpanel"
              aria-labelledby={`fd-tab-${active.id}`}
              aria-live="polite"
              className={`fd-card ${entered ? "fd-in" : "fd-enter"}`}
            >
              <div className="fd-cardtop">
                <span className="fd-ic">
                  <FinderIcon>{active.icon}</FinderIcon>
                </span>
                <div className="fd-said">
                  <span className="fd-saidlbl">flexiq handles this</span>
                  <span className="fd-tag">{active.tag}</span>
                </div>
              </div>

              <h3 className="fd-title">{active.title}</h3>
              <RawHtml as="p" className="fd-blurb" html={active.blurb} />

              <ul className="fd-bullets">
                {active.bullets.map((b) => (
                  <li key={b.k}>
                    <span className="fd-bk">{b.k}</span>
                    <RawHtml as="span" className="fd-bv" html={b.v} />
                  </li>
                ))}
              </ul>

              <div className="fd-codewrap">
                <div className="fd-codebar">
                  <span className="fd-dot r" />
                  <span className="fd-dot y" />
                  <span className="fd-dot g" />
                  <span className="fd-codefn">tasks.py</span>
                </div>
                <RawHtml as="pre" className="fd-code" html={active.code} />
              </div>

              <div className="fd-cta">
                <button
                  type="button"
                  className="btn pri"
                  onClick={() =>
                    setActiveDemo({
                      id: active.demo.id,
                      title: active.demo.title,
                    })
                  }
                >
                  {active.demo.label}
                  {CTA_ARROW}
                </button>
                <Link className="btn sec" to={guideHref(active.guide.to, sdk)}>
                  {active.guide.label}
                </Link>
              </div>
            </div>
          </div>
        </div>
      </div>

      <DemoModal demo={activeDemo} onClose={() => setActiveDemo(null)} />
    </section>
  );
}
