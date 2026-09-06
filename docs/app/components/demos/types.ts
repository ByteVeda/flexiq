/** The interactive demos the scenario finder and the docs pages can render. */
export type DemoId =
  | "ratelimit"
  | "recovery"
  | "scaling"
  | "progress"
  | "workflow"
  | "mesh"
  | "saga";

/** Props every demo component receives from {@link DemoModal}. */
export interface DemoProps {
  /** Active host theme, so a demo can pick palette variants if it needs to. */
  theme: "light" | "dark";
}
