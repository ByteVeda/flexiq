# flexiq-workflows

Workflow DAG engine for [`flexiq-core`](https://crates.io/crates/flexiq-core).

A workflow is a directed acyclic graph of steps. Each step becomes a job in the
underlying queue, wired to its predecessors through the queue's dependency
support, so a step only becomes runnable once everything it waits on has
completed. Runs, per-node status, and the graph itself are persisted, so a
workflow survives a restart mid-flight.

Graph construction and traversal come from
[`dagron-core`](https://crates.io/crates/dagron-core), re-exported here as
`flexiq_workflows::dagron_core`.

## What it provides

- `WorkflowDefinition` + `StepMetadata` — the persisted graph and the per-step
  task, queue, retry, timeout, and priority settings.
- `WorkflowRun`, `WorkflowNode`, `WorkflowState`, `WorkflowNodeStatus` — run and
  node snapshots.
- `WorkflowStorage` — the backend trait, with `WorkflowSqliteStorage`,
  `WorkflowPostgresStorage`, and `WorkflowRedisStorage` implementations behind a
  `WorkflowStorageBackend` enum that dispatches across whichever is compiled in.
- `topological_order` — parse a serialized DAG into steps in dependency order,
  each carrying its direct predecessors. This is what a submit path uses to
  create jobs with the right `depends_on` chain.

Schema is created lazily on first use through the crate's own migration ledger,
separate from the core queue tables.

## Features

| Feature | Effect |
| --- | --- |
| *(default)* | SQLite workflow storage |
| `postgres` | adds `WorkflowPostgresStorage` |
| `redis` | adds `WorkflowRedisStorage` |

Each forwards to the matching `flexiq-core` feature.

## License

MIT
