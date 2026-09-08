import type { ReactNode } from "react";

/** One box in a flow diagram. `pool` swaps the icon for the worker dot grid. */
export type DiagramStation = {
  label: string;
  title: string;
  hint: string;
  accent?: boolean;
  pool?: boolean;
  icon?: ReactNode;
};

/** Inner SVG paths for each station's icon (matches the prototype's flow diagram). */
export function DiagramIcon({ children }: { children: ReactNode }) {
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      {children}
    </svg>
  );
}

/**
 * A row of stations joined by dashed wires, one spark travelling each wire on a
 * stagger. Shared so a second diagram reads as the same system seen from another
 * angle rather than a second system — which is the whole claim the server fold
 * makes about the door.
 */
export function FlowDiagram({ stations }: { stations: DiagramStation[] }) {
  return (
    <div className="flowdiag">
      {stations.map((station, index) => (
        <Station
          key={station.label}
          station={station}
          last={index === stations.length - 1}
          index={index}
        />
      ))}
    </div>
  );
}

function Station({
  station,
  last,
  index,
}: {
  station: DiagramStation;
  last: boolean;
  index: number;
}) {
  return (
    <>
      <div className={`station ${station.accent ? "accent" : ""}`.trim()}>
        <div className="srow">
          {station.pool ? (
            <div className="dpool">
              {Array.from({ length: 6 }).map((_, k) => (
                // biome-ignore lint/suspicious/noArrayIndexKey: fixed decorative dot row
                <span key={k} style={{ "--k": k } as React.CSSProperties} />
              ))}
            </div>
          ) : (
            <div className="dicon">
              <DiagramIcon>{station.icon}</DiagramIcon>
            </div>
          )}
          <div className="smeta">
            <span className="slabel">{station.label}</span>
            <span className="stitle">{station.title}</span>
            <span className="shint">{station.hint}</span>
          </div>
        </div>
      </div>
      {last ? null : (
        <div
          className="wire"
          style={{ "--wd": `${index * 0.5}s` } as React.CSSProperties}
        >
          <span className="spark" />
        </div>
      )}
    </>
  );
}
