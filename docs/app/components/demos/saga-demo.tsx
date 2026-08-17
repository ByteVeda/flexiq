import { useCallback, useEffect, useReducer, useRef, useState } from "react";
import { useRafLoop, useReducedMotion } from "./lib";
import type { DemoProps } from "./types";

/*
 * Saga / compensation demo — a distributed transaction as a chain of steps,
 * each with a compensating action. Pick where it fails and run it: forward
 * steps commit, then on failure the compensations unwind in reverse so nothing
 * is left half-done. React port of the vendored demo-saga.js (SVG + RAF).
 */

const W = 940;
const H = 292;
const FDUR = 720; // forward step duration (ms)
const CDUR = 620; // compensation step duration (ms)
const CYC = 104;
const CW = 178;
const CH = 88;
const CX = [143, 361, 579, 797];

interface Step {
  fwd: string;
  fn: string;
  comp: string;
}
const STEPS: Step[] = [
  { fwd: "reserve seat", fn: "reserve_seat.s()", comp: "release seat" },
  { fwd: "charge card", fn: "charge_card.s()", comp: "refund card" },
  { fwd: "book hotel", fn: "book_hotel.s()", comp: "cancel hotel" },
  { fwd: "issue ticket", fn: "issue_ticket.s()", comp: "void ticket" },
];
const N = STEPS.length;

type Status = "pending" | "running" | "done" | "failed" | "comp" | "undone";
type Phase = "idle" | "forward" | "compensate" | "done";
type Outcome = "" | "committed" | "rolled back";

interface ColorSet {
  fill: string;
  stroke: string;
  pip: string;
  t: string;
}
const COL: Record<Status, ColorSet> = {
  pending: {
    fill: "var(--panel2)",
    stroke: "var(--line2)",
    pip: "var(--dim)",
    t: "var(--txt2)",
  },
  running: {
    fill: "color-mix(in oklch,var(--cyan) 15%,transparent)",
    stroke: "color-mix(in oklch,var(--cyan) 52%,transparent)",
    pip: "var(--cyan)",
    t: "var(--txt)",
  },
  done: {
    fill: "color-mix(in oklch,var(--grn) 13%,transparent)",
    stroke: "color-mix(in oklch,var(--grn) 48%,transparent)",
    pip: "var(--grn)",
    t: "var(--txt)",
  },
  failed: {
    fill: "color-mix(in oklch,var(--red) 14%,transparent)",
    stroke: "color-mix(in oklch,var(--red) 52%,transparent)",
    pip: "var(--red)",
    t: "var(--txt)",
  },
  comp: {
    fill: "color-mix(in oklch,var(--amber) 15%,transparent)",
    stroke: "color-mix(in oklch,var(--amber) 52%,transparent)",
    pip: "var(--amber)",
    t: "var(--txt)",
  },
  undone: {
    fill: "var(--panel)",
    stroke: "var(--line2)",
    pip: "var(--dim)",
    t: "var(--dim)",
  },
};

interface Sim {
  st: Status[];
  phase: Phase;
  idx: number;
  stepStart: number;
  compQueue: number[];
  outcome: Outcome;
  dotProg: number;
  /** Forward edge i committed (green). */
  fCommitted: boolean[];
  /** Reverse edge i opacity during compensation. */
  rOpacity: number[];
}

function freshSim(): Sim {
  return {
    st: STEPS.map(() => "pending"),
    phase: "idle",
    idx: 0,
    stepStart: 0,
    compQueue: [],
    outcome: "",
    dotProg: 0,
    fCommitted: Array(N - 1).fill(false),
    rOpacity: Array(N - 1).fill(0),
  };
}

/** Final committed/rolled-back frame, without animating (reduced motion). */
function instantSim(failAt: number): Sim {
  const sim = freshSim();
  sim.phase = "done";
  if (failAt < 0) {
    sim.st = STEPS.map(() => "done");
    sim.outcome = "committed";
    sim.fCommitted = Array(N - 1).fill(true);
  } else {
    for (let i = 0; i < failAt; i++) sim.st[i] = "undone";
    sim.st[failAt] = "failed";
    sim.outcome = "rolled back";
  }
  return sim;
}

export default function SagaDemo(_props: DemoProps) {
  const reduced = useReducedMotion();
  const simRef = useRef<Sim>(freshSim());
  const [failAt, setFailAt] = useState(1);
  const [running, setRunning] = useState(false);
  const [, repaint] = useReducer((n: number) => n + 1, 0);

  const finish = useCallback((outcome: Outcome) => {
    simRef.current.phase = "done";
    simRef.current.outcome = outcome;
    setRunning(false);
  }, []);

  // One animation frame of the saga state machine.
  const tick = useCallback(
    (now: number) => {
      const sim = simRef.current;
      if (sim.stepStart === 0) sim.stepStart = now; // lazy-init on first frame

      if (sim.phase === "forward") {
        const prog = (now - sim.stepStart) / FDUR;
        sim.dotProg = Math.min(1, prog);
        if (prog >= 1) {
          if (sim.idx === failAt) {
            sim.st[sim.idx] = "failed";
            sim.compQueue = [];
            for (let j = sim.idx - 1; j >= 0; j--) {
              if (sim.st[j] === "done") sim.compQueue.push(j);
            }
            if (sim.compQueue.length) {
              sim.phase = "compensate";
              const ci = sim.compQueue[0];
              sim.st[ci] = "comp";
              sim.stepStart = now;
              if (ci < N - 1) sim.rOpacity[ci] = 0.9;
            } else {
              finish("rolled back");
            }
          } else {
            sim.st[sim.idx] = "done";
            if (sim.idx < N - 1) {
              sim.fCommitted[sim.idx] = true;
              sim.idx += 1;
              sim.st[sim.idx] = "running";
              sim.stepStart = now;
              sim.dotProg = 0;
            } else {
              finish("committed");
            }
          }
        }
      } else if (sim.phase === "compensate") {
        const ci = sim.compQueue[0];
        const cp = (now - sim.stepStart) / CDUR;
        if (cp >= 1) {
          sim.st[ci] = "undone";
          sim.compQueue.shift();
          if (ci < N - 1) sim.rOpacity[ci] = 0;
          if (ci > 0) sim.rOpacity[ci - 1] = 0.9;
          if (sim.compQueue.length) {
            sim.stepStart = now;
            sim.st[sim.compQueue[0]] = "comp";
          } else {
            finish("rolled back");
          }
        }
      }
      repaint();
    },
    [failAt, finish],
  );

  const run = useCallback(() => {
    const sim = freshSim();
    sim.phase = "forward";
    sim.st[0] = "running";
    simRef.current = sim;
    setRunning(true);
  }, []);

  // Auto-run when the demo mounts; render a static final frame if reduced motion.
  // Re-runs when the fail point changes or the motion preference flips.
  // biome-ignore lint/correctness/useExhaustiveDependencies: run/tick are stable callbacks; we intentionally re-seed only on failAt/reduced change.
  useEffect(() => {
    if (reduced) {
      simRef.current = instantSim(failAt);
      setRunning(false);
      repaint();
    } else {
      run();
    }
  }, [failAt, reduced]);

  useRafLoop(tick, running && !reduced);

  const sim = simRef.current;
  const committed = sim.st.filter((s) => s === "done").length;
  const compensated = sim.st.filter((s) => s === "undone").length;
  const stepLabel =
    sim.phase === "idle"
      ? "—"
      : sim.phase === "forward"
        ? `${sim.idx + 1} / ${N}`
        : sim.phase === "compensate"
          ? "unwind"
          : "done";
  const dotVisible =
    sim.phase === "forward" && sim.st[sim.idx] === "running" && sim.idx < N - 1;
  const dotX = dotVisible
    ? CX[sim.idx] +
      CW / 2 +
      (CX[sim.idx + 1] - CW / 2 - (CX[sim.idx] + CW / 2)) * sim.dotProg
    : 0;

  return (
    <div className="dm-frame-react">
      <div className="demo-bar">
        <span className="title">
          <span className="ld" />
          booking-saga · transaction
        </span>
        <div className="demo-controls">
          {/* biome-ignore lint/a11y/useSemanticElements: segmented toggle of aria-pressed buttons; a fieldset/legend would fight the inline-flex .seg styling */}
          <div className="seg" role="group" aria-label="Where it fails">
            {[
              { v: 1, cls: "bad", label: "charge fails" },
              { v: 2, cls: "bad", label: "hotel fails" },
              { v: -1, cls: "good", label: "all succeed" },
            ].map((opt) => {
              const on = failAt === opt.v;
              return (
                <button
                  type="button"
                  key={opt.v}
                  className={opt.cls}
                  data-on={on ? "1" : "0"}
                  aria-pressed={on}
                  onClick={() => setFailAt(opt.v)}
                >
                  <span className="dot" />
                  {opt.label}
                </button>
              );
            })}
          </div>
          <button
            type="button"
            className="ctl pri"
            disabled={running}
            onClick={run}
          >
            {running ? (
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
            {running ? "Running…" : "Run saga"}
          </button>
        </div>
      </div>

      <div className="stage">
        <svg
          viewBox={`0 0 ${W} ${H}`}
          role="img"
          aria-label="A booking saga: reserve seat, charge card, book hotel, issue ticket. Each step has a compensating action. When a step fails, flexiq runs the compensations of the committed steps in reverse order."
        >
          <defs>
            <marker
              id="saga-ar-f"
              viewBox="0 0 10 10"
              refX="8"
              refY="5"
              markerWidth="6"
              markerHeight="6"
              orient="auto"
            >
              <path
                d="M1 1 L9 5 L1 9"
                fill="none"
                stroke="var(--line3)"
                strokeWidth="1.7"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </marker>
            <marker
              id="saga-ar-c"
              viewBox="0 0 10 10"
              refX="8"
              refY="5"
              markerWidth="6"
              markerHeight="6"
              orient="auto"
            >
              <path
                d="M1 1 L9 5 L1 9"
                fill="none"
                stroke="var(--amber)"
                strokeWidth="1.7"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </marker>
          </defs>

          {/* edges */}
          <g>
            {STEPS.slice(0, N - 1).map((_, i) => {
              const x1 = CX[i] + CW / 2;
              const x2 = CX[i + 1] - CW / 2;
              return (
                <path
                  // biome-ignore lint/suspicious/noArrayIndexKey: fixed edge list
                  key={`f${i}`}
                  d={`M${x1} ${CYC - 6} H ${x2}`}
                  fill="none"
                  stroke={
                    sim.fCommitted[i]
                      ? "color-mix(in oklch,var(--grn) 45%,transparent)"
                      : "var(--line2)"
                  }
                  strokeWidth="2"
                  strokeLinecap="round"
                  markerEnd="url(#saga-ar-f)"
                />
              );
            })}
            {STEPS.slice(0, N - 1).map((_, i) => {
              const x1 = CX[i] + CW / 2;
              const x2 = CX[i + 1] - CW / 2;
              return (
                <path
                  // biome-ignore lint/suspicious/noArrayIndexKey: fixed edge list
                  key={`r${i}`}
                  d={`M${x2} ${CYC + CH / 2 + 30} H ${x1}`}
                  fill="none"
                  stroke="color-mix(in oklch,var(--amber) 50%,transparent)"
                  strokeWidth="2"
                  strokeLinecap="round"
                  markerEnd="url(#saga-ar-c)"
                  opacity={sim.rOpacity[i]}
                />
              );
            })}
          </g>

          {/* cards */}
          <g>
            {STEPS.map((s, i) => {
              const status = sim.st[i];
              const k = COL[status];
              const x = CX[i] - CW / 2;
              const y = CYC - CH / 2;
              const compActive = status === "comp" || status === "undone";
              return (
                <g key={s.fwd}>
                  <rect
                    x={x}
                    y={y}
                    width={CW}
                    height={CH}
                    rx="13"
                    fill={k.fill}
                    stroke={k.stroke}
                  />
                  <text
                    x={x + 15}
                    y={y + 22}
                    fontFamily="var(--mono)"
                    fontSize="9.5"
                    letterSpacing=".06em"
                    fill="var(--dim)"
                  >
                    STEP {i + 1}
                  </text>
                  <circle cx={x + CW - 16} cy={y + 18} r="4.5" fill={k.pip} />
                  <text
                    x={x + 15}
                    y={y + 45}
                    fontFamily="var(--sans)"
                    fontSize="14.5"
                    fontWeight="600"
                    fill={k.t}
                    textDecoration={
                      status === "undone" ? "line-through" : "none"
                    }
                  >
                    {s.fwd}
                  </text>
                  <text
                    x={x + 15}
                    y={y + 66}
                    fontFamily="var(--code)"
                    fontSize="11"
                    fill="var(--mut)"
                  >
                    {s.fn}
                  </text>
                  <rect
                    x={x + 18}
                    y={CYC + CH / 2 + 16}
                    width={CW - 36}
                    height="28"
                    rx="8"
                    fill={
                      status === "comp"
                        ? "color-mix(in oklch,var(--amber) 12%,transparent)"
                        : "var(--panel)"
                    }
                    stroke={
                      status === "comp"
                        ? "color-mix(in oklch,var(--amber) 52%,transparent)"
                        : status === "undone"
                          ? "var(--line3)"
                          : "var(--line2)"
                    }
                    strokeDasharray={compActive ? "" : "3 4"}
                  />
                  <path
                    d={`M${x + 30} ${CYC + CH / 2 + 27} l-5 3 l5 3`}
                    fill="none"
                    stroke={
                      status === "comp"
                        ? "var(--amber)"
                        : status === "undone"
                          ? "var(--txt2)"
                          : "var(--dim)"
                    }
                    strokeWidth="1.6"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                  />
                  <text
                    x={x + 40}
                    y={CYC + CH / 2 + 34}
                    fontFamily="var(--mono)"
                    fontSize="10.5"
                    fill={
                      status === "comp"
                        ? "var(--amber)"
                        : status === "undone"
                          ? "var(--txt2)"
                          : "var(--dim)"
                    }
                  >
                    {s.comp}
                  </text>
                </g>
              );
            })}
          </g>

          {/* forward token */}
          <g>
            <circle
              r="6"
              fill="var(--cyan)"
              cx={dotX}
              cy={CYC - 6}
              opacity={dotVisible ? 1 : 0}
            />
          </g>
        </svg>
      </div>

      <div className="legend">
        <span>
          <i style={{ background: "var(--cyan)" }} />
          running
        </span>
        <span>
          <i style={{ background: "var(--grn)" }} />
          committed
        </span>
        <span>
          <i style={{ background: "var(--red)" }} />
          failed
        </span>
        <span>
          <i style={{ background: "var(--amber)" }} />
          compensating
        </span>
        <span>
          <i style={{ background: "var(--dim)" }} />
          undone
        </span>
      </div>

      <div className="readout">
        <div className="stat">
          <span className="k">step</span>
          <span className="v" style={{ fontSize: 16 }}>
            {stepLabel}
          </span>
        </div>
        <div className="stat">
          <span className="k">committed</span>
          <span className="v good" style={{ fontSize: 16 }}>
            {committed}
          </span>
        </div>
        <div className="stat">
          <span className="k">compensated</span>
          <span className="v warn" style={{ fontSize: 16 }}>
            {compensated}
          </span>
        </div>
        <div className="stat">
          <span className="k">outcome</span>
          <span
            className={`v ${sim.outcome === "committed" ? "good" : sim.outcome === "rolled back" ? "bad" : ""}`}
            style={{ fontSize: 15 }}
          >
            {sim.outcome || "—"}
          </span>
        </div>
      </div>
    </div>
  );
}
