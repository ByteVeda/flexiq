# flexiq-mesh

Decentralized mesh scheduling overlay for
[`flexiq-core`](https://crates.io/crates/flexiq-core).

Workers form a peer-to-peer cluster and pull work toward themselves instead of
waiting on a central dispatcher. Membership is tracked by SWIM gossip over UDP,
jobs map to owners through a consistent-hash ring, and an idle node steals from
a busier peer over TCP rather than sitting empty.

The database stays the source of truth. The mesh decides *which* worker picks a
job up next and how much it prefetches — it never becomes the record of what
ran.

## What it provides

- `MeshNode` — owns the local deque, the ring, gossip, and work-stealing for one
  worker.
- `MeshConfig` — virtual nodes per member, local buffer capacity, and the gossip
  and stealing knobs.
- `MeshState`, `MemberState`, `WorkerInfo` — cluster membership as this node
  currently sees it.
- `LocalDeque` — the bounded local buffer a node dispatches from and peers steal
  from.
- `MeshMetrics` / `MetricsSnapshot` / `ClusterInfo` — peer count, capacity, load,
  buffered depth, and adaptive prefetch, for observability.

## License

MIT
