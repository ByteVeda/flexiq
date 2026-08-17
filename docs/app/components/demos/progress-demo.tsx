import { useCallback, useEffect, useReducer, useRef, useState } from "react";
import { useRafLoop, useReducedMotion } from "./lib";
import type { DemoProps } from "./types";

/*
 * Progress-streaming demo — the heavy-processing scenario. A big upload is handed
 * off with .delay(); the endpoint returns 202 in milliseconds while a worker
 * crunches the file chunk by chunk, calling progress.update() per chunk so the
 * caller's UI shows a live % bar. SVG scene + RAF simulation.
 */

const W = 940;
const H = 270;
const RESPONSE_MS = 3; // the instant hand-off — the whole point of .delay()
const CHUNK_MS = 180; // wall time the worker spends per chunk

interface SizeDef {
  n: number;
  label: string;
}
const SIZES: Record<"small" | "large", SizeDef> = {
  small: { n: 16, label: "12 MB" },
  large: { n: 40, label: "60 MB" },
};

interface FlowNode {
  x: number;
  w: number;
  title: string;
  sub: string;
}
const NODE_Y = 24;
const NODE_H = 54;
const NODE_W = 190;
const NODES: FlowNode[] = [
  { x: 24, w: NODE_W, title: "POST /process", sub: "your endpoint" },
  { x: 375, w: NODE_W, title: "flexiq queue", sub: "buffers the job" },
  { x: 726, w: NODE_W, title: "worker", sub: "" },
];

// Chunk grid geometry — laid out left-to-right, wrapping at COLS_MAX per row.
const GRID_X = 24;
const GRID_Y = 202;
const GRID_W = W - 48;
const COLS_MAX = 20;
const CELL_H = 16;
const CELL_GAP = 6;
function gridCell(i: number, n: number) {
  const cols = Math.min(n, COLS_MAX);
  const cellW = (GRID_W - (cols - 1) * CELL_GAP) / cols;
  const col = i % cols;
  const row = Math.floor(i / cols);
  return {
    x: GRID_X + col * (cellW + CELL_GAP),
    y: GRID_Y + row * (CELL_H + CELL_GAP),
    w: cellW,
  };
}

export default function ProgressDemo(_props: DemoProps) {
  const reduced = useReducedMotion();
  const [size, setSize] = useState<"small" | "large">("large");
  const [paused, setPaused] = useState(false);
  const tRef = useRef(0); // virtual sim time (ms), advances only while running
  const lastRef = useRef(0); // last wall-clock RAF stamp, for real dt
  const [, repaint] = useReducer((n: number) => n + 1, 0);

  const n = SIZES[size].n;
  const total = n * CHUNK_MS;

  const tick = useCallback(
    (wall: number) => {
      if (!lastRef.current) lastRef.current = wall;
      const dt = Math.min(wall - lastRef.current, 100);
      lastRef.current = wall;
      tRef.current = Math.min(total, tRef.current + dt);
      repaint();
    },
    [total],
  );

  // Reset to the start on size change (total changes with size); reduced motion
  // shows the finished frame.
  useEffect(() => {
    tRef.current = reduced ? total : 0;
    lastRef.current = 0;
    repaint();
  }, [reduced, total]);

  const finished = tRef.current >= total;
  useRafLoop(tick, !paused && !reduced && !finished);

  const restart = () => {
    // Under reduced motion the RAF loop never runs, so show the finished frame.
    tRef.current = reduced ? total : 0;
    lastRef.current = 0;
    setPaused(false);
    repaint();
  };
  const togglePause = () => {
    // Drop the stale wall stamp so resume doesn't jump by the paused gap.
    lastRef.current = 0;
    setPaused((p) => !p);
  };

  const t = tRef.current;
  const done = Math.min(n, Math.floor(t / CHUNK_MS));
  const frac = finished ? 1 : (done + (t % CHUNK_MS) / CHUNK_MS) / n;
  const pct = Math.round(frac * 100);
  const elapsed = (t / 1000).toFixed(1);
  const workerSub = finished
    ? "result ready"
    : t === 0
      ? "idle"
      : `chunk ${done + 1} / ${n}`;

  return (
    <div className="dm-frame-react">
      <div className="demo-bar">
        <span className="title">
          <span className="ld" />
          upload · handed off with .delay()
        </span>
        <div className="demo-controls">
          {/* biome-ignore lint/a11y/useSemanticElements: segmented toggle of aria-pressed buttons; a fieldset/legend would fight the inline-flex .seg styling */}
          <div className="seg" role="group" aria-label="File size">
            {(
              [
                { v: "small", label: SIZES.small.label },
                { v: "large", label: SIZES.large.label },
              ] as const
            ).map((opt) => {
              const on = size === opt.v;
              return (
                <button
                  type="button"
                  key={opt.v}
                  data-on={on ? "1" : "0"}
                  aria-pressed={on}
                  onClick={() => setSize(opt.v)}
                >
                  {opt.label}
                </button>
              );
            })}
          </div>
          <button type="button" className="ctl" onClick={restart}>
            <svg
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2.2"
              strokeLinecap="round"
              strokeLinejoin="round"
              aria-hidden="true"
            >
              <path d="M3 12a9 9 0 1 0 3-6.7L3 8" />
              <path d="M3 3v5h5" />
            </svg>
            Restart
          </button>
          <button
            type="button"
            className="ctl"
            aria-pressed={paused}
            disabled={finished}
            onClick={togglePause}
          >
            {paused ? (
              <svg
                viewBox="0 0 24 24"
                fill="currentColor"
                stroke="none"
                aria-hidden="true"
              >
                <path d="M8 5v14l11-7z" />
              </svg>
            ) : (
              <svg
                viewBox="0 0 24 24"
                fill="currentColor"
                stroke="none"
                aria-hidden="true"
              >
                <rect x="6" y="5" width="4" height="14" rx="1" />
                <rect x="14" y="5" width="4" height="14" rx="1" />
              </svg>
            )}
            {paused ? "Resume" : "Pause"}
          </button>
        </div>
      </div>

      <div className="stage">
        <svg
          viewBox={`0 0 ${W} ${H}`}
          role="img"
          aria-label={`A request posts a ${SIZES[size].label} file to your endpoint, which hands it to flexiq with .delay() and returns 202 in ${RESPONSE_MS} milliseconds. A worker then processes ${n} chunks, calling progress.update() per chunk so the caller's UI shows a live ${pct}% progress bar.`}
        >
          <defs>
            <marker
              id="pg-arrow"
              viewBox="0 0 10 10"
              refX="8"
              refY="5"
              markerWidth="6"
              markerHeight="6"
              orient="auto-start-reverse"
            >
              <path d="M0 0 L10 5 L0 10 z" fill="var(--line3)" />
            </marker>
          </defs>

          {/* flow: endpoint → queue → worker */}
          <g>
            {NODES.map((nd) => (
              <g key={nd.title}>
                <rect
                  x={nd.x}
                  y={NODE_Y}
                  width={nd.w}
                  height={NODE_H}
                  rx="12"
                  fill="var(--panel)"
                  stroke="var(--line2)"
                />
                <text
                  x={nd.x + 18}
                  y={NODE_Y + 23}
                  fontFamily="var(--code)"
                  fontSize="13"
                  fontWeight="700"
                  fill="var(--txt)"
                >
                  {nd.title}
                </text>
                <text
                  x={nd.x + 18}
                  y={NODE_Y + 41}
                  fontFamily="var(--mono)"
                  fontSize="11"
                  fill="var(--dim)"
                >
                  {nd.title === "worker" ? workerSub : nd.sub}
                </text>
              </g>
            ))}
          </g>

          {/* endpoint → queue, with the instant 202 hand-off */}
          <g>
            <line
              x1={NODES[0].x + NODES[0].w}
              y1={NODE_Y + NODE_H / 2}
              x2={NODES[1].x}
              y2={NODE_Y + NODE_H / 2}
              stroke="var(--line3)"
              strokeWidth="1.6"
              markerEnd="url(#pg-arrow)"
            />
            <text
              x={(NODES[0].x + NODES[0].w + NODES[1].x) / 2}
              y={NODE_Y + NODE_H / 2 - 10}
              textAnchor="middle"
              fontFamily="var(--mono)"
              fontSize="10.5"
              fill="var(--mut)"
            >
              .delay()
            </text>
            <g>
              <rect
                x={(NODES[0].x + NODES[0].w + NODES[1].x) / 2 - 47}
                y={NODE_Y + NODE_H / 2 + 6}
                width="94"
                height="20"
                rx="7"
                fill="color-mix(in oklch,var(--grn) 12%,var(--panel))"
                stroke="color-mix(in oklch,var(--grn) 45%,transparent)"
              />
              <text
                x={(NODES[0].x + NODES[0].w + NODES[1].x) / 2}
                y={NODE_Y + NODE_H / 2 + 20}
                textAnchor="middle"
                fontFamily="var(--mono)"
                fontSize="10.5"
                fontWeight="600"
                fill="var(--grn)"
              >
                202 · {RESPONSE_MS} ms
              </text>
            </g>
          </g>

          {/* queue → worker */}
          <g>
            <line
              x1={NODES[1].x + NODES[1].w}
              y1={NODE_Y + NODE_H / 2}
              x2={NODES[2].x}
              y2={NODE_Y + NODE_H / 2}
              stroke="var(--line3)"
              strokeWidth="1.6"
              markerEnd="url(#pg-arrow)"
            />
            <text
              x={(NODES[1].x + NODES[1].w + NODES[2].x) / 2}
              y={NODE_Y + NODE_H / 2 - 10}
              textAnchor="middle"
              fontFamily="var(--mono)"
              fontSize="10.5"
              fill="var(--mut)"
            >
              dequeue
            </text>
          </g>

          {/* live progress bar — streamed to the caller's UI */}
          <text
            x={GRID_X}
            y={116}
            fontFamily="var(--mono)"
            fontSize="11.5"
            fill="var(--mut)"
          >
            live progress · streamed to your UI
          </text>
          <text
            x={W - 24}
            y={119}
            textAnchor="end"
            fontFamily="var(--code)"
            fontSize="22"
            fontWeight="700"
            fill={finished ? "var(--grn)" : "var(--txt)"}
            style={{ fontVariantNumeric: "tabular-nums" }}
          >
            {pct}%
          </text>
          <rect
            x={GRID_X}
            y={128}
            width={GRID_W}
            height="22"
            rx="11"
            fill="var(--panel3)"
            stroke="var(--line2)"
          />
          <rect
            x={GRID_X}
            y={128}
            width={Math.max(0, frac * GRID_W)}
            height="22"
            rx="11"
            fill={finished ? "var(--grn)" : "var(--cyan)"}
          />

          {/* chunk grid — one progress.update() per chunk */}
          <text
            x={GRID_X}
            y={186}
            fontFamily="var(--mono)"
            fontSize="11"
            fill="var(--dim)"
          >
            chunks · one progress.update() each
          </text>
          <g>
            {Array.from({ length: n }, (_, i) => {
              const cell = gridCell(i, n);
              const isDone = i < done;
              const active = i === done && !finished && t > 0;
              return (
                <rect
                  // biome-ignore lint/suspicious/noArrayIndexKey: fixed chunk list, index is the chunk's identity
                  key={i}
                  x={cell.x}
                  y={cell.y}
                  width={cell.w}
                  height={CELL_H}
                  rx="3.5"
                  fill={
                    isDone
                      ? "var(--grn)"
                      : active
                        ? "var(--cyan)"
                        : "var(--panel3)"
                  }
                  stroke={
                    isDone
                      ? "color-mix(in oklch,var(--grn) 55%,transparent)"
                      : active
                        ? "color-mix(in oklch,var(--cyan) 55%,transparent)"
                        : "var(--line2)"
                  }
                />
              );
            })}
          </g>
        </svg>
      </div>

      <div className="legend">
        <span>
          <i style={{ background: "var(--grn)" }} />
          processed
        </span>
        <span>
          <i style={{ background: "var(--cyan)" }} />
          processing
        </span>
        <span>
          <i style={{ background: "var(--panel3)" }} />
          queued
        </span>
      </div>

      <div className="readout">
        <div className="stat">
          <span className="k">response</span>
          <span className="v good">
            {RESPONSE_MS}
            <span className="u">ms</span>
          </span>
        </div>
        <div className="stat">
          <span className="k">processed</span>
          <span className="v">
            {done}
            <span className="u">/ {n}</span>
          </span>
        </div>
        <div className="stat">
          <span className="k">progress</span>
          <span className="v">{pct}%</span>
        </div>
        <div className="stat">
          <span className="k">elapsed</span>
          <span className="v">
            {elapsed}
            <span className="u">s</span>
          </span>
        </div>
      </div>
    </div>
  );
}
