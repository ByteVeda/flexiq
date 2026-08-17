import { useCallback, useEffect, useReducer, useRef, useState } from "react";
import { useRafLoop, useReducedMotion } from "./lib";
import type { DemoProps } from "./types";

/*
 * Multi-region work-stealing demo — flexiq runs in several regions that gossip
 * queue depth; when one backlog spikes, an idle peer steals a batch over HTTP so
 * load self-balances with no central broker. React port of demo-worksteal.js
 * (SVG world map + RAF simulation).
 */

const W = 940;
const H = 540;
const JOB_MS = 820;
const STEAL_INT = 620;
const HOT = 5;
const COOL = 2;
const MAPH = 470;
const MAPY = 35;

const proj = (lon: number, lat: number): [number, number] => [
  ((lon + 180) / 360) * W,
  ((90 - lat) / 180) * MAPH + MAPY,
];

const CONTINENTS: [number, number][][] = [
  [
    [-165, 60],
    [-150, 70],
    [-120, 70],
    [-95, 72],
    [-80, 68],
    [-78, 55],
    [-64, 48],
    [-70, 42],
    [-76, 35],
    [-81, 25],
    [-92, 18],
    [-100, 18],
    [-106, 23],
    [-114, 31],
    [-123, 40],
    [-124, 49],
    [-133, 55],
    [-150, 58],
  ],
  [
    [-80, 8],
    [-72, 11],
    [-60, 5],
    [-50, 0],
    [-44, -2],
    [-40, -10],
    [-48, -23],
    [-58, -34],
    [-66, -44],
    [-72, -52],
    [-75, -48],
    [-71, -35],
    [-70, -18],
    [-76, -12],
    [-81, -5],
  ],
  [
    [-16, 14],
    [-12, 28],
    [-2, 35],
    [10, 37],
    [20, 33],
    [32, 31],
    [35, 24],
    [43, 12],
    [51, 11],
    [48, 0],
    [41, -12],
    [35, -22],
    [25, -34],
    [18, -35],
    [11, -16],
    [9, 0],
    [-2, 5],
    [-12, 8],
  ],
  [
    [-9, 36],
    [-2, 44],
    [2, 50],
    [-4, 58],
    [5, 62],
    [14, 66],
    [28, 71],
    [55, 70],
    [75, 73],
    [100, 73],
    [130, 71],
    [155, 68],
    [170, 66],
    [165, 60],
    [150, 58],
    [142, 50],
    [133, 43],
    [123, 40],
    [122, 31],
    [112, 21],
    [103, 14],
    [97, 8],
    [93, 20],
    [88, 21],
    [80, 7],
    [76, 8],
    [70, 18],
    [63, 24],
    [56, 26],
    [48, 30],
    [42, 37],
    [30, 36],
    [22, 40],
    [14, 45],
    [4, 43],
  ],
  [
    [-5, 50],
    [-2, 51],
    [1, 53],
    [-2, 56],
    [-6, 58],
    [-9, 55],
    [-10, 52],
  ],
  [
    [133, 32],
    [138, 34],
    [143, 39],
    [145, 43],
    [143, 45],
    [139, 43],
    [136, 39],
    [133, 36],
    [131, 33],
  ],
  [
    [114, -22],
    [122, -18],
    [131, -12],
    [137, -11],
    [143, -11],
    [147, -20],
    [150, -37],
    [141, -38],
    [131, -32],
    [120, -34],
    [114, -26],
  ],
];
const landPath = (pts: [number, number][]) =>
  `M${pts
    .map((c) =>
      proj(c[0], c[1])
        .map((v) => v.toFixed(1))
        .join(" "),
    )
    .join(" L ")} Z`;
const LAND_D = CONTINENTS.map(landPath).join(" ");

interface RegionDef {
  id: string;
  loc: string;
  lon: number;
  lat: number;
  cardX: number;
  cardY: number;
  q0: number;
  rate: number;
  gx: number;
  gy: number;
}
const REG: RegionDef[] = [
  {
    id: "us-west-2",
    loc: "Oregon",
    lon: -120,
    lat: 45,
    cardX: 44,
    cardY: 58,
    q0: 2,
    rate: 0.3,
  },
  {
    id: "us-east-1",
    loc: "N. Virginia",
    lon: -77,
    lat: 38,
    cardX: 120,
    cardY: 214,
    q0: 2,
    rate: 0.3,
  },
  {
    id: "eu-west",
    loc: "Ireland",
    lon: -7,
    lat: 53,
    cardX: 386,
    cardY: 44,
    q0: 2,
    rate: 1.35,
  },
  {
    id: "ap-south-1",
    loc: "Mumbai",
    lon: 73,
    lat: 19,
    cardX: 560,
    cardY: 300,
    q0: 1,
    rate: 0.34,
  },
  {
    id: "ap-northeast-1",
    loc: "Tokyo",
    lon: 139,
    lat: 36,
    cardX: 742,
    cardY: 66,
    q0: 2,
    rate: 0.34,
  },
].map((r) => {
  const [px, py] = proj(r.lon, r.lat);
  return { ...r, gx: Math.round(px), gy: Math.round(py) };
});

const LINKS: [string, string, number | null][] = [
  ["us-west-2", "eu-west", 85],
  ["eu-west", "ap-northeast-1", 88],
  ["eu-west", "ap-south-1", 139],
  ["us-west-2", "us-east-1", null],
  ["us-east-1", "eu-west", null],
  ["ap-south-1", "ap-northeast-1", null],
  ["us-east-1", "ap-south-1", null],
];
const regById = (id: string) => REG.find((r) => r.id === id) as RegionDef;
const labelW = (s: string) => s.length * 7.1 + 4;

interface RegionSim {
  q: number;
  busy: [number, number];
}
interface StealToken {
  id: number;
  ax: number;
  ay: number;
  bx: number;
  by: number;
  dst: string;
  batch: number;
  t0: number;
  dur: number;
  done: boolean;
}
interface Sim {
  reg: Record<string, RegionSim>;
  completed: number;
  steals: number;
  moved: number;
  tokens: StealToken[];
  linkSolid: number[];
  stealAcc: number;
  /** Virtual sim time (ms) — advances only while running, so pause freezes it. */
  clock: number;
  /** Last wall-clock RAF timestamp, for computing real dt. */
  last: number;
  ids: number;
}
function freshSim(): Sim {
  const reg: Record<string, RegionSim> = {};
  for (const r of REG) reg[r.id] = { q: r.q0, busy: [0, 0] };
  return {
    reg,
    completed: 0,
    steals: 0,
    moved: 0,
    tokens: [],
    linkSolid: LINKS.map(() => 0),
    stealAcc: 0,
    clock: 0,
    last: 0,
    ids: 0,
  };
}

function poisson(m: number): number {
  if (m <= 0) return 0;
  const L = Math.exp(-m);
  let k = 0;
  let p = 1;
  do {
    k++;
    p *= Math.random();
  } while (p > L);
  return k - 1;
}

export default function WorkStealDemo(_props: DemoProps) {
  const reduced = useReducedMotion();
  const simRef = useRef<Sim>(freshSim());
  const [paused, setPaused] = useState(false);
  const [, repaint] = useReducer((n: number) => n + 1, 0);

  const stepSim = useCallback((sim: Sim, now: number, dt: number) => {
    for (const r of REG) {
      const rs = sim.reg[r.id];
      rs.q += poisson(r.rate * dt);
      for (let i = 0; i < 2; i++) {
        if (rs.busy[i] && now >= rs.busy[i]) {
          rs.busy[i] = 0;
          sim.completed++;
        }
        if (!rs.busy[i] && rs.q > 0) {
          rs.q--;
          rs.busy[i] = now + JOB_MS * (0.8 + Math.random() * 0.5);
        }
      }
    }
  }, []);

  const trySteal = useCallback((sim: Sim, now: number, dt: number) => {
    sim.stealAcc += dt * 1000;
    if (sim.stealAcc < STEAL_INT) return;
    sim.stealAcc = 0;
    const hot = [...REG].sort((a, b) => sim.reg[b.id].q - sim.reg[a.id].q)[0];
    if (sim.reg[hot.id].q < HOT) return;
    const peers = LINKS.map((L, i) => ({ L, i }))
      .filter(({ L }) => L[0] === hot.id || L[1] === hot.id)
      .map(({ L, i }) => ({ peer: L[0] === hot.id ? L[1] : L[0], i }))
      .filter(
        ({ peer }) =>
          sim.reg[peer].q <= COOL && sim.reg[peer].busy.indexOf(0) >= 0,
      )
      .sort((a, b) => sim.reg[a.peer].q - sim.reg[b.peer].q);
    if (!peers.length) return;
    const dst = peers[0].peer;
    const batch = Math.max(
      1,
      Math.min(4, Math.floor((sim.reg[hot.id].q - sim.reg[dst].q) / 2)),
    );
    sim.reg[hot.id].q -= batch;
    sim.steals++;
    // mark the link solid + spawn the traveling token
    const linkIdx = LINKS.findIndex(
      (L) =>
        (L[0] === hot.id && L[1] === dst) || (L[0] === dst && L[1] === hot.id),
    );
    if (linkIdx >= 0) sim.linkSolid[linkIdx] = now + 700;
    const a = hot;
    const b = regById(dst);
    const dist = Math.hypot(b.gx - a.gx, b.gy - a.gy);
    sim.tokens.push({
      id: sim.ids++,
      ax: a.gx,
      ay: a.gy,
      bx: b.gx,
      by: b.gy,
      dst,
      batch,
      t0: now,
      dur: Math.max(620, dist * 1.6),
      done: false,
    });
  }, []);

  const moveTokens = useCallback((sim: Sim, now: number) => {
    for (let i = sim.tokens.length - 1; i >= 0; i--) {
      const t = sim.tokens[i];
      const p = Math.min(1, (now - t.t0) / t.dur);
      if (p >= 1 && !t.done) {
        t.done = true;
        sim.reg[t.dst].q += t.batch;
        sim.moved += t.batch;
        sim.tokens.splice(i, 1);
      }
    }
  }, []);

  const loop = useCallback(
    (wall: number) => {
      const sim = simRef.current;
      if (!sim.last) sim.last = wall;
      const dt = Math.min((wall - sim.last) / 1000, 0.05);
      sim.last = wall;
      // Advance a virtual clock by real dt only while running, so a pause truly
      // freezes time — deadlines never expire en masse on resume.
      sim.clock += dt * 1000;
      stepSim(sim, sim.clock, dt);
      trySteal(sim, sim.clock, dt);
      moveTokens(sim, sim.clock);
      repaint();
    },
    [stepSim, trySteal, moveTokens],
  );

  useEffect(() => {
    const sim = freshSim();
    if (reduced) {
      for (const r of REG) sim.reg[r.id].busy = [1, 0];
    }
    simRef.current = sim;
    repaint();
  }, [reduced]);

  useRafLoop(loop, !paused && !reduced);

  const sim = simRef.current;
  const now = sim.clock;
  let hotQ = 0;
  for (const r of REG) {
    if (sim.reg[r.id].q > hotQ) hotQ = sim.reg[r.id].q;
  }

  const burst = () => {
    const hot = [...REG].sort((a, b) => sim.reg[b.id].q - sim.reg[a.id].q)[0];
    sim.reg[hot.id].q += 7;
    repaint();
  };
  const reset = () => {
    const fresh = freshSim();
    if (reduced) {
      for (const r of REG) fresh.reg[r.id].busy = [1, 0];
    }
    simRef.current = fresh;
    repaint();
  };

  return (
    <div className="dm-frame-react">
      <div className="demo-bar">
        <span className="title">
          <span className="ld" />
          region-mesh · gossip
        </span>
        <div className="demo-controls">
          <button type="button" className="ctl pri" onClick={burst}>
            <svg
              viewBox="0 0 24 24"
              fill="none"
              stroke="currentColor"
              strokeWidth="2.2"
              strokeLinecap="round"
              strokeLinejoin="round"
              aria-hidden="true"
            >
              <path d="M12 5v14M5 12h14" />
            </svg>
            Burst
          </button>
          <button type="button" className="ctl" onClick={reset}>
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
            Reset
          </button>
          <button
            type="button"
            className="ctl"
            aria-pressed={paused}
            onClick={() => setPaused((p) => !p)}
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
          aria-label="A world map with five flexiq regions connected by a gossip mesh. When one region's queue spikes, idle regions steal batches of jobs over HTTP so load balances out."
        >
          <defs>
            <pattern
              id="ws-grid"
              width="36"
              height="36"
              patternUnits="userSpaceOnUse"
            >
              <circle cx="1" cy="1" r="1" fill="var(--line)" />
            </pattern>
          </defs>
          <rect width={W} height={H} fill="url(#ws-grid)" opacity="0.7" />
          <g opacity="0.6">
            <path
              d={LAND_D}
              fill="var(--line)"
              stroke="var(--line2)"
              strokeWidth="1"
              strokeLinejoin="round"
            />
          </g>

          {/* links + latency pills */}
          <g>
            {LINKS.map((L, i) => {
              const a = regById(L[0]);
              const b = regById(L[1]);
              const on = sim.linkSolid[i] > now;
              return (
                <line
                  // biome-ignore lint/suspicious/noArrayIndexKey: fixed link list
                  key={`link${i}`}
                  x1={a.gx}
                  y1={a.gy}
                  x2={b.gx}
                  y2={b.gy}
                  stroke={
                    on
                      ? "var(--cyan)"
                      : "color-mix(in oklch,var(--grn) 38%,transparent)"
                  }
                  strokeWidth={on ? 2.2 : 1.6}
                  strokeDasharray={on ? undefined : "2 6"}
                  strokeLinecap="round"
                />
              );
            })}
            {LINKS.map((L, i) => {
              if (L[2] == null) return null;
              const a = regById(L[0]);
              const b = regById(L[1]);
              const mx = (a.gx + b.gx) / 2;
              const my = (a.gy + b.gy) / 2;
              return (
                // biome-ignore lint/suspicious/noArrayIndexKey: fixed link list
                <g key={`lat${i}`}>
                  <rect
                    x={mx - 21}
                    y={my - 11}
                    width="42"
                    height="22"
                    rx="7"
                    fill="var(--panel)"
                    stroke="color-mix(in oklch,var(--grn) 32%,transparent)"
                  />
                  <text
                    x={mx}
                    y={my + 4}
                    textAnchor="middle"
                    fontFamily="var(--mono)"
                    fontSize="10.5"
                    fill="var(--mut)"
                  >
                    {L[2]} ms
                  </text>
                </g>
              );
            })}
          </g>

          {/* leader lines + geographic pins */}
          <g>
            {REG.map((r) => (
              <line
                key={r.id}
                x1={r.cardX + 78}
                y1={r.cardY + 40}
                x2={r.gx}
                y2={r.gy}
                stroke="var(--line2)"
                strokeWidth="1.3"
                strokeDasharray="1 4"
                strokeLinecap="round"
              />
            ))}
          </g>
          <g>
            {REG.map((r) => (
              <g key={r.id}>
                <circle
                  cx={r.gx}
                  cy={r.gy}
                  r="11"
                  fill="color-mix(in oklch,var(--grn) 13%,transparent)"
                />
                <circle
                  cx={r.gx}
                  cy={r.gy}
                  r="4.5"
                  fill="var(--grn)"
                  stroke="var(--panel)"
                  strokeWidth="1.5"
                />
              </g>
            ))}
          </g>

          {/* steal tokens */}
          <g>
            {sim.tokens.map((t) => {
              const p = Math.min(1, (now - t.t0) / t.dur);
              const x = t.ax + (t.bx - t.ax) * p;
              const y = t.ay + (t.by - t.ay) * p;
              const op =
                p < 0.1
                  ? p / 0.1
                  : p > 0.9
                    ? (1 - (p - 0.9) / 0.1) * 0.95
                    : 0.95;
              return (
                <g
                  key={t.id}
                  transform={`translate(${x.toFixed(1)},${y.toFixed(1)})`}
                >
                  <circle r="8" fill="var(--cyan)" opacity={op} />
                  <text
                    textAnchor="middle"
                    y="3.5"
                    fontFamily="var(--code)"
                    fontSize="9"
                    fontWeight="700"
                    fill="#06281f"
                  >
                    {t.batch}
                  </text>
                </g>
              );
            })}
          </g>

          {/* region cards */}
          <g>
            {REG.map((r) => {
              const rs = sim.reg[r.id];
              const hot = rs.q >= HOT;
              const x = r.cardX;
              const y = r.cardY;
              return (
                <g key={r.id}>
                  <rect
                    x={x}
                    y={y}
                    width="156"
                    height="80"
                    rx="13"
                    fill={
                      hot
                        ? "color-mix(in oklch,var(--amber) 7%,var(--panel))"
                        : "var(--panel)"
                    }
                    stroke={
                      hot
                        ? "color-mix(in oklch,var(--amber) 55%,transparent)"
                        : "var(--line2)"
                    }
                  />
                  <circle cx={x + 16} cy={y + 18} r="5.5" fill="var(--grn)" />
                  <text
                    x={x + 30}
                    y={y + 22}
                    fontFamily="var(--code)"
                    fontSize="12.5"
                    fontWeight="700"
                    fill="var(--txt)"
                  >
                    {r.id}
                  </text>
                  <text
                    x={x + 30 + labelW(r.id)}
                    y={y + 22}
                    fontFamily="var(--mono)"
                    fontSize="10"
                    fill="var(--dim)"
                  >
                    {r.loc}
                  </text>
                  {[0, 1].map((i) => {
                    const wbusy = !!rs.busy[i];
                    return (
                      <rect
                        key={i}
                        x={x + 16 + i * 18}
                        y={y + 36}
                        width="13"
                        height="13"
                        rx="3.5"
                        fill={wbusy ? "var(--grn)" : "var(--panel3)"}
                        stroke={
                          wbusy
                            ? "color-mix(in oklch,var(--grn) 55%,transparent)"
                            : "var(--line2)"
                        }
                      />
                    );
                  })}
                  <text
                    x={x + 16}
                    y={y + 70}
                    fontFamily="var(--mono)"
                    fontSize="11"
                    fill="var(--mut)"
                  >
                    <tspan fontWeight="600" fill="var(--txt2)">
                      {rs.q}
                    </tspan>{" "}
                    queued
                  </text>
                </g>
              );
            })}
          </g>
        </svg>
      </div>

      <div className="legend">
        <span>
          <i style={{ background: "var(--grn)" }} />
          region node
        </span>
        <span>
          <i
            style={{
              width: 16,
              height: 0,
              borderTop:
                "2px dashed color-mix(in oklch,var(--grn) 50%,transparent)",
              background: "none",
            }}
          />
          mesh link (gossip)
        </span>
        <span>
          <i
            style={{
              width: 16,
              height: 0,
              borderTop: "2px solid var(--cyan)",
              background: "none",
            }}
          />
          work-steal over HTTP
        </span>
      </div>

      <div className="readout">
        <div className="stat">
          <span className="k">completed</span>
          <span className="v good">{sim.completed}</span>
        </div>
        <div className="stat">
          <span className="k">steals</span>
          <span className="v">{sim.steals}</span>
        </div>
        <div className="stat">
          <span className="k">moved · HTTP</span>
          <span className="v warn">{sim.moved}</span>
        </div>
        <div className="stat">
          <span className="k">busiest queue</span>
          <span className="v">{hotQ}</span>
        </div>
      </div>
    </div>
  );
}
