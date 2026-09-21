// Single barrel for every diagram component used in MDX. All architecture
// diagrams below are prototype-faithful ports (arch.js + arch-*.html).

export { Mermaid } from "@/components/mermaid";
export { ArchitectureStack, ResourcePipeline } from "./arch-stack";
export {
  BenchmarkChart,
  BenchmarkNotes,
  BenchmarkTable,
} from "./benchmark-chart";
export { CrashRecoveryTimeline } from "./failure-timeline";
export { JobStateMachine } from "./lifecycle-graph";
export { MeshDemo } from "./mesh-demo";
export { SchedulerPollLoop } from "./poll-cycle";
export { SchemaGrid } from "./schema-grid";
export { Seq } from "./seq";
export { WorkerDispatch } from "./worker-fork";
