# Issue #748 — clear the 2579 javadoc warnings in the Java runtime jar

Branch: `docs/748-java-javadoc-runtime`, stacked on `origin/docs/java-javadoc-warnings` (#747),
with `origin/master` merged in (picks up #746's in-memory durable steps).
Baseline verified: `./gradlew :javadoc --rerun-tasks` → **2579 warnings**. Final: **0**.

Decisions taken (the issue asked for both up front):
- `internal` **is documented**, not excluded from the javadoc task.
- No filler. `@param queue the queue` is not an acceptable tag; every tag says what the
  component is *for*, or what a caller has to get right about it.

## Steps — one commit each, smallest-first

- [x] 1. `steps`, `webhooks`, `pubsub`, `proxies` — 2579 → 2315
- [x] 2. small tail (`errors`, `resources`, `predicates`, `serialization`, `middleware`,
      `events`, `logging`, `annotation`, `scheduling`, `locks`, `health`, `scaler`,
      `autoscale`, `interception`, `core`, `batch`, `contrib`, `cli`) — 2315 → 1999
- [x] 3. `dashboard` and its subpackages — 1999 → 1643
- [x] 4. `model`, `task`, `worker` — 1643 → 1187
- [x] 5. `workflows` — 1187 → 954
- [x] 6. `spi` — 954 → 713
- [x] 7. `internal` — 713 → 332
- [x] 8. root package, `FlexiQ.java` last — 332 → 0
- [x] 9. `-Xwerror` on the runtime javadoc

## Review

**Result.** 2579 → 0 across 226 files; ~3,070 `@param`/`@return`/`@throws` tags and every
undocumented public member. `-Xwerror` now holds the runtime at zero, the way #747 holds
`flexiq-test`, so the backlog cannot re-accumulate.

**Verification.**
- `./gradlew :javadoc --rerun-tasks` → 0 warnings, at every step, not just at the end.
- `./gradlew build` green: spotless, checkstyle, NullAway, every module's tests.
- The gate was *proved* to bite: deleting one `@return` from `Queue.name()` fails the build
  with `error: warnings found and -Werror specified`. Restored, green again.

**Two things worth knowing for review.**
- The repetitive surfaces (JSON DTOs, the JNI holders, `QueueBackend`, `FlexiQ`) were
  documented through generators driven by a per-file glossary of parameter meanings, so
  the same concept reads the same everywhere — `handle` is "the queue handle from
  `open`" in all 168 places it appears. The prose is hand-written per method; only the
  mechanical repetition is generated.
- A record's explicit compact constructor needs a *description* but no `@param`s: javadoc
  inherits those from the record's component docs. That is why those blocks look shorter
  than the rest.

**Not done.** Nothing in scope was left out. The one deliberate omission is that `internal`
was documented rather than excluded, which is what was chosen up front; the alternative
one-line exclusion is still available and would take ~380 tags back out.
