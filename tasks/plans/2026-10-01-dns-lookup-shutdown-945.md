# #945 — a wedged DNS lookup holds worker shutdown past the drain budget

Branch: `fix/dns-lookup-off-blocking-pool` (from `master` @ `b468778a`). Not pushed.

## Finding that picks the route

Neither remedy in the issue is needed in full; a third, narrower one closes it.

- `Runtime` drop waits for **the runtime's own** blocking pool — nothing else.
- Remedy 2 (`shutdown_timeout` in `runner.rs`) is ruled out by more than taste:
  `NativeDispatcher::run` (`worker/dispatcher.rs:101`) returns *before* its
  sync handlers finish — they run on `spawn_blocking`, and their results reach
  the drain thread only because the runtime drop waits for them. A bounded
  shutdown would cut native handlers off mid-run → reaper instead of settle.
  That is the "does not regress native shutdown" acceptance line, concretely.
- Remedy 1 (async DNS crate) costs Cargo.lock nodes; `hickory-*` is not in the
  lockfile today. Not needed.

## Route: run `getaddrinfo` on a detached thread, not the blocking pool

`PinnedResolver::resolve` spawns a named `std::thread` for the lookup and
awaits its answer over a `tokio::sync::oneshot`. A detached thread is outside
the runtime, so dropping the runtime does not wait for it, and process exit
never waits for a detached thread. Dropping the resolution future drops the
receiver; the thread's late `send` fails harmlessly. Zero new deps, no change
to runtime shutdown semantics for attach or native workers.

Bound: spawn_blocking had an implicit cap (512 threads). Keep one — a
per-resolver `Semaphore` (`MAX_CONCURRENT_LOOKUPS`), permit acquired *before*
the spawn (await is cancellable) and moved into the thread, so a wedged
resolver can pin at most that many threads, each for at most the platform
resolver's own ceiling.

## Steps

- [ ] `http/resolver.rs`: lookup seam — `PinnedResolver` holds a
      `Lookup = Arc<dyn Fn(&str) -> io::Result<Vec<SocketAddr>> + Send + Sync>`;
      `new` uses `system_lookup` (`to_socket_addrs`), `#[cfg(test)] with_lookup`
      injects one.
- [ ] `resolve`: acquire permit → spawn `flexiq-dns` thread → await oneshot.
      Spawn failure / dropped sender → `EgressRefusal::Resolve`, same as a
      `JoinError` today.
- [ ] Rewrite the doc note on `PinnedResolver`: drop the limitation, record why
      the lookup is off the blocking pool (so nobody "simplifies" it back).
- [ ] Regression test: wedged lookup (blocks on a channel never signalled
      until test end) started on a multi-thread runtime; dropping the runtime
      returns within a short bound. Same test on `spawn_blocking` hangs —
      verify by running it against the old code once.
- [ ] Test: concurrency cap — with cap N wedged, N+1th lookup waits; dropping
      its future releases nothing it did not take.
- [ ] Existing resolver tests stay green.

## Verification

- `cargo test -j1 -p flexiq-core --lib http::resolver`
- `cargo test -j1 -p flexiq-core --test rust worker` (attach/native shutdown)
- `CARGO_BUILD_JOBS=1 cargo clippy -p flexiq-core --all-targets --features http-target`
- `cargo check -j1 --workspace`

## Commits (kartikeya-27)

1. `fix(core): resolve names off the runtime's blocking pool`
2. `test(core): a wedged lookup does not hold runtime shutdown`
