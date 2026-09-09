import { useCallback, useEffect, useReducer, useRef, useState } from "react";
import { useRafLoop, useReducedMotion } from "./lib";
import {
  ClientFigure,
  ServerFigure,
  StoreFigure,
  WorkerFigure,
} from "./server-door-figures";
import type { DemoProps } from "./types";

/*
 * Server-door demo — one request walked through `flexiq-server`'s producer door,
 * stage by stage, with a scrubbable playhead.
 *
 * Every string here came off a live door: a `flexiq-server` built with the
 * `grpc` feature, a token minted by `flexiq-server token create`, and curl. The
 * refusal path is the response an `execute`-scoped token actually gets from
 * `POST /v1/jobs` — reason `SCOPE_DENIED`, not a plausible-looking invention —
 * because the one thing a reader can check against their own terminal is
 * exactly the thing that must not be made up.
 */

/** The four boxes a stage can be happening in, left to right. */
type Lane = "client" | "door" | "store" | "worker";

const LANES: { id: Lane; label: string; sub: string }[] = [
  { id: "client", label: "any client", sub: "curl · no SDK" },
  { id: "door", label: "flexiq-server", sub: ":50051" },
  { id: "store", label: "jobs", sub: "SQLite · PG" },
  { id: "worker", label: "worker", sub: "your SDK" },
];

type Tone = "run" | "ok" | "bad";

/** `JobStatus`'s lowercase display names, the form a listing shows. The column
 *  itself is an integer; these are `JobStatus::as_str` in `flexiq-core`. */
type RowStatus = "pending" | "running" | "complete";

/** The one column a later stage rewrites instead of adding. */
const STATUS_COL = "status";

interface Stage {
  /** Milliseconds into the trace at which this stage begins. */
  t: number;
  lane: Lane;
  /** Wire hop drawn while this stage is current, if it is one. */
  hop?: { from: Lane; to: Lane; label: string };
  tone: Tone;
  title: string;
  detail: string;
  /** Door sub-step this stage completes, if any. */
  step?: 1 | 2 | 3;
  /** Field the `jobs` row gains at this stage. */
  row?: string[];
  /** What the row's `status` column reads after this stage. The trace does not
   *  stop at the response — a claim and a result follow it — so leaving this
   *  unset past the insert left the row saying `pending` over a finished job. */
  rowStatus?: RowStatus;
  /** Status line the client is holding after this stage. */
  status?: string;
}

/** The row as the insert writes it; `status` moves on from here. */
const ROW: [string, string][] = [
  ["id", "01a08035-e66c-7b12-8f9b-577416f6fa9f"],
  ["task_name", "send_email"],
  ["namespace", "default"],
  ["queue", "default"],
  [STATUS_COL, "pending"],
];

/** The request the two scenarios both send, shown in the client box. */
const REQUEST = `POST /v1/jobs
authorization: Bearer $FLEXIQ_TOKEN
content-type: application/json

{"taskName": "send_email",
 "structured": {"args": [{"to": "ada@example.com"}]}}`;

const ACCEPTED: Stage[] = [
  {
    t: 0,
    lane: "client",
    tone: "run",
    title: "compose",
    detail:
      "A shell builds ordinary JSON. `structured` exists so this caller never needs a CBOR library — or any binding at all.",
  },
  {
    t: 900,
    lane: "door",
    hop: { from: "client", to: "door", label: "HTTP/1.1" },
    tone: "run",
    title: "reach the listener",
    detail:
      "One listener answers both doors. gRPC is still detected by its HTTP/2 preface, so a gRPC client notices nothing about curl being served beside it.",
  },
  {
    t: 1800,
    lane: "door",
    step: 1,
    tone: "ok",
    title: "scope produce ✓",
    detail:
      "The token carries `produce`. Scopes are not a hierarchy: this credential could not open the executor door, and an `execute` one cannot enqueue.",
  },
  {
    t: 2700,
    lane: "door",
    step: 2,
    tone: "ok",
    title: "structured → CBOR envelope",
    detail:
      "The server builds the same `0x02` envelope an SDK would have built in-process. What JSON cannot carry it refuses rather than rounds — integers past 2^53-1, non-finite numbers.",
  },
  {
    t: 3700,
    lane: "store",
    hop: { from: "door", to: "store", label: "insert" },
    step: 3,
    tone: "ok",
    title: "row into jobs",
    detail:
      "The same table an embedded SDK writes to. Nothing downstream can tell which door the row came through.",
    row: ["id", "task_name", "namespace", "queue", "status"],
  },
  {
    t: 4700,
    lane: "client",
    hop: { from: "door", to: "client", label: "200 OK" },
    tone: "ok",
    title: "200 OK",
    detail:
      "The job document comes back. There is no completion stream to watch here — poll `GetJob`, or take a webhook subscription.",
    status: "HTTP/1.1 200 OK · job.id 01a08035-…",
  },
  {
    t: 5700,
    lane: "worker",
    hop: { from: "store", to: "worker", label: "claim" },
    tone: "run",
    title: "a worker claims it",
    detail:
      "An ordinary SDK worker, unchanged and unaware. It never learns the job was enqueued over HTTP. The claim is what moves the row to `running`.",
    rowStatus: "running",
  },
  {
    t: 6700,
    lane: "store",
    hop: { from: "worker", to: "store", label: "result" },
    tone: "ok",
    title: "result written back",
    detail:
      "The row lands on `complete`, and a `GetJob` from the client that never held a database credential now reads it. Same store, same scheduler, same retry and dead-letter rules as the embedded path — the door changed who could enqueue, and nothing else.",
    rowStatus: "complete",
  },
];

const REFUSED: Stage[] = [
  ACCEPTED[0],
  ACCEPTED[1],
  {
    t: 1800,
    lane: "door",
    step: 1,
    tone: "bad",
    title: "scope produce ✗",
    detail:
      "The credential presented carries `execute`. The gate runs before any handler, so the request never reaches the encoder.",
  },
  {
    t: 2700,
    lane: "client",
    hop: { from: "door", to: "client", label: "403" },
    tone: "bad",
    title: "403 PERMISSION_DENIED",
    detail:
      'reason `SCOPE_DENIED`, metadata `{"scope": "produce"}`. Branch on the reason — the message is for humans and may be reworded in any release.',
    status: "HTTP/1.1 403 Forbidden · SCOPE_DENIED",
  },
];

const SCENARIOS = { accepted: ACCEPTED, refused: REFUSED } as const;
type ScenarioId = keyof typeof SCENARIOS;

const TAIL = 1400;
const durationOf = (stages: Stage[]) => stages[stages.length - 1].t + TAIL;

const TONE: Record<Tone, string> = {
  run: "var(--cyan)",
  ok: "var(--grn)",
  bad: "var(--red)",
};

/** The details are prose, and the identifiers in them are identifiers: render
 *  backtick spans as `<code>` rather than printing the backticks. */
function withCode(text: string): React.ReactNode[] {
  return text.split(/(`[^`]+`)/).map((part, i) =>
    part.startsWith("`") && part.endsWith("`") ? (
      // biome-ignore lint/suspicious/noArrayIndexKey: split of a constant string; parts repeat, their positions do not
      <code key={i}>{part.slice(1, -1)}</code>
    ) : (
      part
    ),
  );
}

export default function ServerDoorDemo(_props: DemoProps) {
  const reduced = useReducedMotion();
  const [scen, setScen] = useState<ScenarioId>("accepted");
  const [playing, setPlaying] = useState(false);
  const playRef = useRef(0);
  const lastT = useRef(0);
  const trackRef = useRef<HTMLDivElement>(null);
  const logRef = useRef<HTMLDivElement>(null);
  const dragging = useRef(false);
  const [, repaint] = useReducer((n: number) => n + 1, 0);

  const stages = SCENARIOS[scen];
  const DUR = durationOf(stages);

  const stopPlay = useCallback(() => setPlaying(false), []);

  const tick = useCallback(
    (now: number) => {
      if (!lastT.current) lastT.current = now;
      const dt = now - lastT.current;
      lastT.current = now;
      playRef.current += dt;
      if (playRef.current >= DUR) {
        playRef.current = DUR;
        setPlaying(false);
      }
      repaint();
    },
    [DUR],
  );

  useRafLoop(tick, playing && !reduced);

  // Reduced motion mid-playback: jump to the end and drop the stale "Pause".
  useEffect(() => {
    if (reduced && playing) {
      playRef.current = DUR;
      setPlaying(false);
      repaint();
    }
  }, [reduced, playing, DUR]);

  const startPlay = useCallback(() => {
    if (reduced) {
      playRef.current = DUR;
      repaint();
      return;
    }
    if (playRef.current >= DUR) playRef.current = 0;
    lastT.current = 0;
    setPlaying(true);
  }, [reduced, DUR]);

  // Auto-play on mount; under reduced motion this shows the finished frame.
  // biome-ignore lint/correctness/useExhaustiveDependencies: run once on mount.
  useEffect(() => {
    startPlay();
  }, []);

  const setFromX = useCallback(
    (clientX: number) => {
      const track = trackRef.current;
      if (!track) return;
      const r = track.getBoundingClientRect();
      const ratio = Math.max(0, Math.min(1, (clientX - r.left) / r.width));
      playRef.current = ratio * DUR;
      stopPlay();
      repaint();
    },
    [DUR, stopPlay],
  );

  const play = playRef.current;
  let idx = 0;
  for (let i = 0; i < stages.length; i++) {
    if (stages[i].t <= play + 0.001) idx = i;
    else break;
  }

  // biome-ignore lint/correctness/useExhaustiveDependencies: scroll when the active row changes
  useEffect(() => {
    const log = logRef.current;
    if (!log) return;
    const cur = log.querySelector<HTMLElement>(".sd-ev.cur");
    if (cur) {
      log.scrollTop = Math.max(
        0,
        cur.offsetTop - log.clientHeight / 2 + cur.clientHeight / 2,
      );
    }
  }, [idx, scen]);

  const stage = stages[idx];
  const pct = (play / DUR) * 100;
  const done = stages.slice(0, idx + 1);
  const steps = new Set(done.map((s) => s.step).filter(Boolean));
  const rowFields = new Set(done.flatMap((s) => s.row ?? []));
  const status = [...done].reverse().find((s) => s.status)?.status;
  // Latest wins, so scrubbing backwards walks the row's status back too.
  const rowStatus = [...done].reverse().find((s) => s.rowStatus)?.rowStatus;

  // The hop is drawn only while its stage is the current one, and travels for
  // the first 70% of that stage — so the packet lands before the box lights up
  // rather than still being in flight when the log says it arrived.
  const nextT = idx + 1 < stages.length ? stages[idx + 1].t : DUR;
  const within = (play - stage.t) / Math.max(1, nextT - stage.t);
  const hopPct = Math.min(1, within / 0.7);
  const wire = hopWire(stage.hop);

  const onKeyDown = (e: React.KeyboardEvent) => {
    const step = DUR / 40;
    if (e.key === "ArrowRight") playRef.current = Math.min(DUR, play + step);
    else if (e.key === "ArrowLeft") playRef.current = Math.max(0, play - step);
    else if (e.key === "Home") playRef.current = 0;
    else if (e.key === "End") playRef.current = DUR;
    else return;
    e.preventDefault();
    stopPlay();
    repaint();
  };

  return (
    <div className="dm-frame-react">
      <div className="demo-bar">
        <span className="title">
          <span className="ld" />
          POST /v1/jobs · producer door
        </span>
        <div className="demo-controls">
          <button
            type="button"
            className="ctl"
            onClick={() => (playing ? stopPlay() : startPlay())}
          >
            {playing ? (
              <svg
                viewBox="0 0 24 24"
                fill="currentColor"
                stroke="none"
                aria-hidden="true"
              >
                <rect x="6" y="5" width="4" height="14" rx="1" />
                <rect x="14" y="5" width="4" height="14" rx="1" />
              </svg>
            ) : (
              <svg
                viewBox="0 0 24 24"
                fill="currentColor"
                stroke="none"
                aria-hidden="true"
              >
                <path d="M8 5v14l11-7z" />
              </svg>
            )}
            {playing ? "Pause" : "Play"}
          </button>
          {/* biome-ignore lint/a11y/useSemanticElements: segmented toggle of aria-pressed buttons; a fieldset/legend would fight the inline-flex .seg styling */}
          <div className="seg" role="group" aria-label="Token scope">
            {(
              [
                { v: "accepted", cls: "good", label: "produce token" },
                { v: "refused", cls: "bad", label: "execute token" },
              ] as const
            ).map((opt) => {
              const on = scen === opt.v;
              return (
                <button
                  type="button"
                  key={opt.v}
                  className={opt.cls}
                  data-on={on ? "1" : "0"}
                  aria-pressed={on}
                  onClick={() => {
                    setScen(opt.v);
                    playRef.current = Math.min(
                      playRef.current,
                      durationOf(SCENARIOS[opt.v]),
                    );
                    stopPlay();
                    repaint();
                  }}
                >
                  <span className="dot" />
                  {opt.label}
                </button>
              );
            })}
          </div>
        </div>
      </div>

      <div className="sd-lanes">
        {LANES.map((lane, i) => (
          <Box
            key={lane.id}
            lane={lane}
            active={stage.lane === lane.id}
            tone={stage.tone}
            hop={wire.index === i ? stage.hop : undefined}
            hopPct={hopPct}
            back={wire.back}
            last={i === LANES.length - 1}
            live={play < DUR}
          >
            {lane.id === "client" ? (
              <ClientBox
                status={status}
                tone={stage.tone}
                sending={stage.lane === "client" && !status}
              />
            ) : lane.id === "door" ? (
              <DoorBox
                steps={steps}
                tone={stage.tone}
                running={stage.lane === "door"}
                failed={stage.tone === "bad"}
              />
            ) : lane.id === "store" ? (
              <StoreBox
                fields={rowFields}
                status={rowStatus}
                tone={stage.tone}
              />
            ) : (
              <WorkerBox
                busy={done.some((s) => s.lane === "worker")}
                tone={stage.tone}
              />
            )}
          </Box>
        ))}
      </div>

      <div className="sd-scrubwrap">
        <div
          className="sd-track"
          ref={trackRef}
          tabIndex={0}
          role="slider"
          aria-label="Request timeline scrubber"
          aria-valuemin={0}
          aria-valuemax={100}
          aria-valuenow={Math.round(pct)}
          onKeyDown={onKeyDown}
          onPointerDown={(e) => {
            dragging.current = true;
            e.currentTarget.setPointerCapture(e.pointerId);
            setFromX(e.clientX);
          }}
          onPointerMove={(e) => {
            if (dragging.current) setFromX(e.clientX);
          }}
          onPointerUp={() => {
            dragging.current = false;
          }}
          onPointerCancel={() => {
            dragging.current = false;
          }}
        >
          <div className="sd-rail" />
          <div className="sd-fill" style={{ width: `${pct}%` }} />
          {stages.map((s, i) =>
            i > 0 ? (
              <div
                key={s.title}
                className="sd-tick"
                style={{
                  left: `${(s.t / DUR) * 100}%`,
                  background: TONE[s.tone],
                }}
              />
            ) : null,
          )}
          <div className="sd-handle" style={{ left: `${pct}%` }} />
        </div>
        <div className="sd-axis">
          <span>0.0s</span>
          <span>{(DUR / 2000).toFixed(1)}s</span>
          <span>{(DUR / 1000).toFixed(1)}s</span>
        </div>
      </div>

      <div className="sd-grid">
        <div className="sd-now">
          <span
            className="sd-pill"
            style={{ "--c": TONE[stage.tone] } as React.CSSProperties}
          >
            <span className="pd" />
            {LANES.find((l) => l.id === stage.lane)?.label}
          </span>
          <div className="big">
            <b>{stage.title}.</b> {withCode(stage.detail)}
          </div>
          <div className="sd-req">
            <div className="sd-reqhd">what the client sent</div>
            <pre>{REQUEST}</pre>
          </div>
        </div>
        <div className="sd-log" ref={logRef}>
          {stages.map((s, i) => (
            <div
              key={s.title}
              className={`sd-ev${i <= idx ? " on" : ""}${i === idx ? " cur" : ""}`}
              style={{ "--c": TONE[s.tone] } as React.CSSProperties}
            >
              <div className="dotcol">
                <span className="ed" />
                <span className="eline" />
              </div>
              <div className="etxt">
                <span className="et">{s.title}</span>
                <span className="ed2">
                  {LANES.find((l) => l.id === s.lane)?.label}
                  {s.hop ? ` · ${s.hop.label}` : ""}
                </span>
              </div>
              <span className="ets">{(s.t / 1000).toFixed(1)}s</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

function laneIndex(lane: Lane): number {
  return LANES.findIndex((l) => l.id === lane);
}

/**
 * Which wire a hop is drawn on, and which way the packet runs.
 *
 * A `Box` renders the wire that *follows* its own lane, so wire `n` joins lanes
 * `n` and `n + 1` — the pair's **lower** index owns it, whichever direction the
 * hop runs. Keying on `hop.from` instead put every reverse hop a wire too far
 * right, and dropped the `worker → jobs` one entirely, since the last lane
 * renders no wire at all.
 */
function hopWire(hop?: { from: Lane; to: Lane }): {
  index: number;
  back: boolean;
} {
  if (!hop) {
    return { index: -1, back: false };
  }
  const from = laneIndex(hop.from);
  const to = laneIndex(hop.to);
  return { index: Math.min(from, to), back: to < from };
}

/** One labelled box, plus the wire leaving it and any packet on that wire. */
function Box({
  lane,
  active,
  tone,
  hop,
  hopPct,
  back,
  last,
  live,
  children,
}: {
  lane: (typeof LANES)[number];
  active: boolean;
  tone: Tone;
  hop?: { from: Lane; to: Lane; label: string };
  hopPct: number;
  back: boolean;
  last: boolean;
  /** The trace has not finished, so the lit box is one still working or still
   *  waiting on something — it breathes rather than just sitting highlighted. */
  live: boolean;
  children: React.ReactNode;
}) {
  return (
    <>
      <div
        className={`sd-box${active ? " on" : ""}${active && live ? " live" : ""}`}
        style={{ "--c": TONE[tone] } as React.CSSProperties}
      >
        <div className="sd-boxhd">
          <b>{lane.label}</b>
          <span>{lane.sub}</span>
        </div>
        {children}
      </div>
      {last ? null : (
        <div className="sd-wire">
          {hop ? (
            <>
              <span
                className={`sd-packet${back ? " back" : ""}`}
                style={{ "--p": `${hopPct * 100}%` } as React.CSSProperties}
              />
              <span className="sd-hoplbl">{hop.label}</span>
            </>
          ) : null}
        </div>
      )}
    </>
  );
}

/** The client box: a laptop, the request it sent, the answer it holds. */
function ClientBox({
  status,
  tone,
  sending,
}: {
  status?: string;
  tone: Tone;
  sending: boolean;
}) {
  return (
    <div className="sd-body">
      <ClientFigure
        sending={sending}
        status={status ? (tone === "bad" ? "bad" : "ok") : undefined}
        tone={TONE[tone]}
      />
      <code className="sd-verb">POST /v1/jobs</code>
      <code className="sd-dim">taskName · structured.args</code>
      {status ? (
        <code
          className="sd-status"
          style={{ "--c": TONE[tone] } as React.CSSProperties}
        >
          {status}
        </code>
      ) : (
        <code className="sd-dim">waiting…</code>
      )}
    </div>
  );
}

const DOOR_STEPS = [
  "check the token's scope",
  "build the CBOR envelope",
  "insert the row",
];

/** The door box: two protocols on one listener, three numbered sub-steps. */
function DoorBox({
  steps,
  tone,
  running,
  failed,
}: {
  steps: Set<number | undefined>;
  tone: Tone;
  running: boolean;
  failed: boolean;
}) {
  const done = new Set(
    [...steps].filter((n): n is number => typeof n === "number"),
  );
  return (
    <div className="sd-body">
      <ServerFigure
        steps={done}
        failedStep={failed && done.has(1) ? 1 : undefined}
        running={running}
        tone={TONE[tone]}
      />
      <div className="sd-protos">
        <span>gRPC</span>
        <span>HTTP + JSON</span>
        <em>one listener</em>
      </div>
      <ol className="sd-steps">
        {DOOR_STEPS.map((label, i) => {
          const n = i + 1;
          const on = steps.has(n);
          const bad = on && tone === "bad" && n === 1;
          return (
            <li
              key={label}
              className={on ? (bad ? "bad" : "on") : ""}
              style={{ "--c": TONE[tone] } as React.CSSProperties}
            >
              <span className="n">{on ? (bad ? "✕" : "✓") : n}</span>
              {label}
            </li>
          );
        })}
      </ol>
    </div>
  );
}

/** The store box: the `jobs` row filling in field by field, then its `status`
 *  moving as the worker claims the job and writes the result back. `status`
 *  falls back to the inserted value, so every other column stays a constant. */
function StoreBox({
  fields,
  status,
  tone,
}: {
  fields: Set<string>;
  status?: RowStatus;
  tone: Tone;
}) {
  return (
    <div className="sd-body">
      <StoreFigure
        filled={ROW.filter(([k]) => fields.has(k)).length}
        total={ROW.length}
        tone={TONE[tone]}
      />
      <table className="sd-row">
        <tbody>
          {ROW.map(([k, v]) => (
            <tr key={k} className={fields.has(k) ? "on" : ""}>
              <th scope="row">{k}</th>
              <td>
                {fields.has(k) ? (k === STATUS_COL ? (status ?? v) : v) : "—"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** The worker box: an unchanged SDK pool, idle until the row exists. */
function WorkerBox({ busy, tone }: { busy: boolean; tone: Tone }) {
  return (
    <div className="sd-body">
      <WorkerFigure busy={busy} tone={TONE[tone]} />
      <code className="sd-dim">{busy ? "send_email · running" : "idle"}</code>
      <code className="sd-dim">never learns it came over HTTP</code>
    </div>
  );
}
