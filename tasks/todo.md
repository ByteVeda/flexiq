# Redis dispatch round trips (#959, #960, #961)

Plan: `tasks/plans/2026-09-24-redis-dispatch-round-trips.md` (tasks + constraints live there).

- [x] Task 1: select + claim in one Lua script (#960)
- [x] Task 2: backend-chosen dispatch batch (#959)
- [x] Task 3: Redis workers wake on enqueue by default (#961)
- [x] Task 4: ready-notify inside the Redis enqueue pipeline
- [x] Task 5: docs
- [x] Task 6: pool Redis connections (found by the local bench run)
- [x] Final review + fix wave; local bench run (uncommitted)
