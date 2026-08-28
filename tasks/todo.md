# #695 — queue cap rejects a debounced enqueue that inserts nothing

## Problem
Every shell runs its `max_pending` admission pre-check before routing to
`Storage::enqueue_debounced`. A coalescing enqueue inserts no row, so a queue at
its cap rejects a call that would only have slid the open window's deadline. The
shell cannot tell slide from insert without asking storage, and asking separately
reopens the race the debounce transaction exists to close.

## Design
The cap travels *into* the debounce write and is enforced on the branch that
inserts, inside the same transaction / Lua script.

- `DebounceOptions` grows `max_pending: Option<i64>` (rather than a third trait
  argument: 7 struct literals vs 53 call sites, and the compiler still forces
  every constructor to answer).
- New `QueueError::QueueFull { queue, pending, cap }`. Its `Display` is the
  cross-SDK wire contract — suffix-anchored so a shell reads the two integers
  back without trusting the queue name:
  `queue '<name>' is full: <pending> pending >= max_pending <cap>`
- Rejection rule matches the shells' today: reject when `pending + 1 > cap`.

## Checklist
- [x] core: `QueueError::QueueFull`, `DebounceOptions.max_pending`, trait doc
- [x] diesel_common: count pending inside the write txn, insert branch only
- [x] redis: fold `SINTERCARD` into `DEBOUNCE_INSERT`, new "full" return shape
- [x] contract suite: at-cap + open window collapses; at-cap + no window rejects
- [x] python: binding arg + stub, drop the pre-check on the debounced path
- [x] node: napi field + TS, drop the pre-check on the debounced path
- [x] java: wire field + `DefaultFlexiQ`, in-memory backend parity
- [x] docs: flow-control + debouncing guides
- [x] verify: cargo (4 feature combos), pytest, node, gradle

## Review

**Shape.** The cap is now an argument of the write. Diesel counts pending inside
`write_transaction`; Redis folds `SINTERCARD 2 KEYS[5] KEYS[3]` into
`DEBOUNCE_INSERT`, whose reply grew a third shape (nil = inserted, bulk string =
the document to slide, one-element array = the refusal and its count). Both
enforce only after the scan has proven no window is open, and both leave the
queue untouched when they refuse.

**The error is a wire.** `QueueError::QueueFull { queue, pending, cap }`, whose
`Display` every shell parses to rebuild its own typed rejection. A typed channel
was not on offer: java's FFM fast path reports errors as a bare string and napi
carries only a status plus a reason. The two integers therefore sit at the *end*
of the message, so an arbitrary queue name in the middle cannot be mistaken for
them; `queue_full_message_is_the_cross_sdk_wire` pins that with a queue named
after the tail itself.

**Behaviour change worth naming.** Node's cap check moved after
`prepareEnqueue` — it has to know the resolved debounce — which also stopped it
reading the pre-middleware queue name. That now matches python and java.

**Trap.** A debounce window is keyed by `(namespace, key)` alone, not by queue.
A new contract test reusing another test's key silently slid *that* test's job
in a different queue and inserted nothing.
