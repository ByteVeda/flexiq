package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.JobContext;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.events.SleepEvent;
import org.byteveda.flexiq.model.DeadJob;
import org.byteveda.flexiq.model.Job;
import org.byteveda.flexiq.steps.StepOptions;
import org.byteveda.flexiq.steps.StepSupersededError;
import org.byteveda.flexiq.task.RetryPolicy;
import org.byteveda.flexiq.task.Task;
import org.byteveda.flexiq.test.InMemoryFlexiQ;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Durable steps behave the same over {@code InMemoryFlexiQ} as over a real
 * worker.
 *
 * <p>The harness cannot ask the core what a step key is — it is JNI-free by
 * construction, which is the whole reason it is fast — so it restates the core's
 * identity and sequence rules in Java. This file is what stops that restatement
 * from drifting: <b>one</b> task body per scenario, run over both backends, with
 * the same assertions. A rule the harness gets wrong shows up here as a test that
 * passes on one backend and fails on the other, in CI, rather than as a test that
 * passes in memory for code a worker would dead-letter.
 *
 * <p>Behaviour, not internals, deliberately: comparing derived key strings would
 * pass for a harness that derives the right key and then memoizes against the
 * wrong row.
 */
class StepHarnessParityTest {

    /** Fast enough that a retry is not most of the test's wall time. */
    private static final RetryPolicy FAST = RetryPolicy.delays(Duration.ofMillis(10), Duration.ofMillis(10));

    private static final Duration SETTLE = Duration.ofSeconds(25);

    /** Both queue shapes a step-using task can run under. */
    enum Backend {
        /** The in-memory harness: no JNI, no disk, and the rules restated in Java. */
        IN_MEMORY,
        /** A real worker over a real step store — the behaviour the harness owes. */
        SQLITE
    }

    private final List<AutoCloseable> open = new ArrayList<>();
    private Path scratch;

    @AfterEach
    void closeQueues() throws Exception {
        // Reverse order: a worker has to stop before the queue it polls closes.
        for (int index = open.size() - 1; index >= 0; index--) {
            open.get(index).close();
        }
        open.clear();
        deleteScratch();
    }

    private FlexiQ queue(Backend backend) throws IOException {
        FlexiQ queue;
        if (backend == Backend.IN_MEMORY) {
            queue = InMemoryFlexiQ.open();
        } else {
            scratch = Files.createTempDirectory("flexiq-parity");
            queue = FlexiQ.builder()
                    .backend("sqlite")
                    .url(scratch.resolve("steps.db").toString())
                    .open();
        }
        open.add(queue);
        return queue;
    }

    private Worker start(Worker.Builder builder) {
        Worker worker = builder.start();
        open.add(worker);
        return worker;
    }

    /**
     * Best-effort, and retried: no SDK exposes a storage {@code close()}, so the
     * native SQLite handle is released when it is collected. Deleting the file
     * while it is still open fails on Windows rather than on Linux, and a test
     * that leaves a temp file behind is a smaller problem than one that fails
     * for it.
     */
    private void deleteScratch() {
        Path dir = scratch;
        scratch = null;
        if (dir == null) {
            return;
        }
        for (int attempt = 0; attempt < 3; attempt++) {
            try (var entries = Files.walk(dir)) {
                entries.sorted(java.util.Comparator.reverseOrder())
                        .forEach(path -> path.toFile().delete());
                if (!Files.exists(dir)) {
                    return;
                }
            } catch (IOException | RuntimeException retry) {
                // fall through and try again
            }
            System.gc();
        }
    }

    // ── memoization ─────────────────────────────────────────────────

    @ParameterizedTest
    @EnumSource(Backend.class)
    @Timeout(60)
    void aStepRunsOncePerJobNotOncePerAttempt(Backend backend) throws Exception {
        Task<String> checkout = Task.of("checkout", String.class).maxRetries(2).retryPolicy(FAST);
        FlexiQ queue = queue(backend);
        queue.enqueue(checkout, "go");

        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger charges = new AtomicInteger();
        AtomicReference<Integer> replayed = new AtomicReference<>();
        CountDownLatch done = new CountDownLatch(1);

        start(queue.worker()
                .handle(checkout, (String payload) -> {
                    JobContext ctx = JobContext.current();
                    int attempt = attempts.incrementAndGet();
                    int charge = ctx.step().run("charge", Integer.class, charges::incrementAndGet);
                    if (attempt == 1) {
                        throw new IllegalStateException("died after charging");
                    }
                    replayed.set(charge);
                    return "ok";
                })
                .on(EventName.SUCCESS, event -> done.countDown()));

        assertTrue(done.await(SETTLE.toSeconds(), TimeUnit.SECONDS), "the job should finish on its retry");
        assertEquals(2, attempts.get(), "the handler ran twice");
        assertEquals(1, charges.get(), "but the step body ran once — the replay is a memo hit");
        assertEquals(1, replayed.get(), "and the replay saw the committed value");
    }

    /**
     * Two assertions, and both are needed. The returned <i>values</i> are what
     * separates key matching from position matching — a positional match hands
     * them back in the recorded order. The per-key body <i>counts</i> are what
     * separate key matching from not matching at all: a harness that memoized
     * nothing would also return them in this order, by running both bodies
     * again.
     */
    @ParameterizedTest
    @EnumSource(Backend.class)
    @Timeout(60)
    void aKeyedStepIsMatchedByKeyNotByPosition(Backend backend) throws Exception {
        Task<String> greet = Task.of("greet", String.class).maxRetries(2).retryPolicy(FAST);
        FlexiQ queue = queue(backend);
        queue.enqueue(greet, "go");

        AtomicInteger attempts = new AtomicInteger();
        Map<String, Integer> ran = new ConcurrentHashMap<>();
        List<String> replayed = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);

        start(queue.worker()
                .handle(greet, (String payload) -> {
                    JobContext ctx = JobContext.current();
                    int attempt = attempts.incrementAndGet();
                    // The second attempt asks for the same two keyed steps in the
                    // opposite order.
                    List<String> order = attempt == 1 ? List.of("alice", "bob") : List.of("bob", "alice");
                    List<String> seen = new ArrayList<>();
                    for (String who : order) {
                        seen.add(ctx.step()
                                .run(
                                        "greet",
                                        String.class,
                                        () -> {
                                            ran.merge(who, 1, Integer::sum);
                                            return "hello-" + who;
                                        },
                                        StepOptions.key(who)));
                    }
                    if (attempt == 1) {
                        throw new IllegalStateException("die once, so the second attempt replays");
                    }
                    replayed.addAll(seen);
                    return "ok";
                })
                .on(EventName.SUCCESS, event -> done.countDown()));

        assertTrue(done.await(SETTLE.toSeconds(), TimeUnit.SECONDS), "the job should finish on its retry");
        assertEquals(
                List.of("hello-bob", "hello-alice"),
                replayed,
                "each memo followed its key; a positional match would have returned them the other way round");
        assertEquals(
                Map.of("alice", 1, "bob", 1),
                ran,
                "both replays were memo hits; a body that ran again is a key that matched nothing");
    }

    /**
     * A task that gets further each attempt: the second attempt replays the first
     * step and commits a second one.
     *
     * <p>The commit is the only thing that touches the {@code (owner, attempt)}
     * fence — a replay is answered from the snapshot and never reaches it — so
     * this is the scenario that needs the attempt to ride the dispatch. A backend
     * that dispatches without one fences every later attempt against 0 and
     * refuses the commit as superseded.
     */
    @ParameterizedTest
    @EnumSource(Backend.class)
    @Timeout(60)
    void aStepFirstReachedOnALaterAttemptCommitsUnderThatAttempt(Backend backend) throws Exception {
        Task<String> fulfil = Task.of("fulfil", String.class).maxRetries(2).retryPolicy(FAST);
        FlexiQ queue = queue(backend);
        queue.enqueue(fulfil, "go");

        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger packs = new AtomicInteger();
        AtomicInteger ships = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(1);

        start(queue.worker()
                .handle(fulfil, (String payload) -> {
                    JobContext ctx = JobContext.current();
                    int attempt = attempts.incrementAndGet();
                    ctx.step().run("pack", packs::incrementAndGet);
                    if (attempt == 1) {
                        throw new IllegalStateException("die after packing, before shipping");
                    }
                    ctx.step().run("ship", ships::incrementAndGet);
                    return "ok";
                })
                .on(EventName.SUCCESS, event -> done.countDown()));

        assertTrue(done.await(SETTLE.toSeconds(), TimeUnit.SECONDS), "the second attempt should commit and finish");
        assertEquals(2, attempts.get());
        assertEquals(1, packs.get(), "pack replayed from its memo");
        assertEquals(1, ships.get(), "ship was new ground on the second attempt, and committed there");
    }

    /** The downstream key is the run's identity and the step's position, and nothing else. */
    @ParameterizedTest
    @EnumSource(Backend.class)
    @Timeout(60)
    void anIdempotencyKeyIsTheRunKeyAndTheStepKey(Backend backend) throws Exception {
        Task<String> charge = Task.of("charge", String.class).maxRetries(0);
        FlexiQ queue = queue(backend);
        String jobId = queue.enqueue(charge, "go");

        List<String> keys = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);

        start(queue.worker()
                .handle(charge, (String payload) -> {
                    JobContext ctx = JobContext.current();
                    ctx.step().run("charge", () -> keys.add(ctx.step().idempotencyKey()));
                    ctx.step().run("charge", () -> keys.add(ctx.step().idempotencyKey()));
                    return "ok";
                })
                .on(EventName.SUCCESS, event -> done.countDown()));

        assertTrue(done.await(SETTLE.toSeconds(), TimeUnit.SECONDS), "the job should finish");
        assertEquals(List.of(jobId + ":charge#0", jobId + ":charge#1"), keys);
    }

    // ── divergence ──────────────────────────────────────────────────

    /**
     * A changed sequence dead-letters, and does it without spending the retry
     * budget: the replay would reach the same disagreement, so a budget spent on
     * it buys nothing.
     */
    @ParameterizedTest
    @EnumSource(Backend.class)
    @Timeout(60)
    void aChangedStepSequenceDeadLettersWithoutSpendingTheBudget(Backend backend) throws Exception {
        Task<String> ship = Task.of("ship", String.class).maxRetries(5).retryPolicy(FAST);
        FlexiQ queue = queue(backend);
        queue.enqueue(ship, "go");

        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch dead = new CountDownLatch(1);

        start(queue.worker()
                .handle(ship, (String payload) -> {
                    JobContext ctx = JobContext.current();
                    int attempt = attempts.incrementAndGet();
                    if (attempt == 1) {
                        ctx.step().run("pack", () -> {});
                        throw new IllegalStateException("die once, so the next attempt replays");
                    }
                    // A different first step: the deploy changed underneath the
                    // recorded sequence.
                    ctx.step().run("label", () -> {});
                    return "ok";
                })
                .on(EventName.DEAD, event -> dead.countDown()));

        assertTrue(dead.await(SETTLE.toSeconds(), TimeUnit.SECONDS), "the divergence should dead-letter the job");
        assertEquals(2, attempts.get(), "one attempt recorded the sequence, one diverged — the budget was not spent");

        List<DeadJob> entries = queue.listDead(10, 0);
        assertEquals(1, entries.size());
        assertTrue(entries.get(0).error.contains("step sequence changed"), entries.get(0).error);
    }

    /** A second step started while one is uncommitted has no position to take. */
    @ParameterizedTest
    @EnumSource(Backend.class)
    @Timeout(60)
    void aSecondStepStartedInsideOneIsRefused(Backend backend) throws Exception {
        Task<String> nested = Task.of("nested", String.class).maxRetries(0);
        FlexiQ queue = queue(backend);
        queue.enqueue(nested, "go");

        CountDownLatch dead = new CountDownLatch(1);
        start(queue.worker()
                .handle(nested, (String payload) -> {
                    JobContext ctx = JobContext.current();
                    ctx.step().run("outer", () -> ctx.step().run("inner", () -> {}));
                    return "ok";
                })
                .on(EventName.DEAD, event -> dead.countDown()));

        assertTrue(dead.await(SETTLE.toSeconds(), TimeUnit.SECONDS), "the nested step should fail the attempt");
        assertTrue(queue.listDead(10, 0).get(0).error.contains("uncommitted"));
    }

    /** An empty explicit key is refused, never quietly numbered by occurrence. */
    @ParameterizedTest
    @EnumSource(Backend.class)
    @Timeout(60)
    void anEmptyExplicitKeyIsRefused(Backend backend) throws Exception {
        Task<String> keyed = Task.of("keyed", String.class).maxRetries(0);
        FlexiQ queue = queue(backend);
        queue.enqueue(keyed, "go");

        CountDownLatch dead = new CountDownLatch(1);
        start(queue.worker()
                .handle(keyed, (String payload) -> {
                    JobContext.current().step().run("charge", () -> {}, StepOptions.key(""));
                    return "ok";
                })
                .on(EventName.DEAD, event -> dead.countDown()));

        assertTrue(dead.await(SETTLE.toSeconds(), TimeUnit.SECONDS), "an empty key should fail the attempt");
        assertTrue(queue.listDead(10, 0).get(0).error.contains("empty key"));
    }

    // ── sleep ───────────────────────────────────────────────────────

    @ParameterizedTest
    @EnumSource(Backend.class)
    @Timeout(60)
    void aSleepEndsTheAttemptAndTheJobResumesFromItsMemos(Backend backend) throws Exception {
        Task<String> nurture = Task.of("nurture", String.class).maxRetries(2).retryPolicy(FAST);
        FlexiQ queue = queue(backend);
        String jobId = queue.enqueue(nurture, "go");

        AtomicInteger attempts = new AtomicInteger();
        AtomicInteger welcomes = new AtomicInteger();
        AtomicInteger followUps = new AtomicInteger();
        List<SleepEvent> sleeping = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);

        start(queue.worker()
                .handle(nurture, (String payload) -> {
                    JobContext ctx = JobContext.current();
                    attempts.incrementAndGet();
                    ctx.step().run("welcome", welcomes::incrementAndGet);
                    ctx.step().sleep(Duration.ofMillis(400));
                    ctx.step().run("follow-up", followUps::incrementAndGet);
                    return "ok";
                })
                .onEvent(EventName.JOB_SLEEPING, event -> sleeping.add((SleepEvent) event))
                .on(EventName.SUCCESS, event -> done.countDown()));

        assertTrue(done.await(SETTLE.toSeconds(), TimeUnit.SECONDS), "the job should wake and finish");
        assertEquals(2, attempts.get(), "one attempt slept, the next one finished");
        assertEquals(1, welcomes.get(), "the step before the sleep is a memo hit on wake");
        assertEquals(1, followUps.get(), "the step after it ran once, on the waking attempt");
        assertEquals(1, sleeping.size(), "exactly one job.sleeping");
        assertEquals("sleep#0", sleeping.get(0).stepKey, "the sleep is itself a step row");

        Job job = queue.getJob(jobId).orElseThrow();
        assertEquals(0, job.retryCount, "a sleep costs no retry");
    }

    /**
     * The deadline is fixed by the first commit. A replayed sleep re-reads the
     * stored one, so a job that keeps crashing into it does not keep pushing its
     * wake forward — which would be a sleep that outlives the job.
     */
    @ParameterizedTest
    @EnumSource(Backend.class)
    @Timeout(60)
    void aReplayedSleepDoesNotPushItsDeadlineForward(Backend backend) throws Exception {
        Task<String> waiter = Task.of("waiter", String.class).maxRetries(3).retryPolicy(FAST);
        FlexiQ queue = queue(backend);
        queue.enqueue(waiter, "go");

        AtomicInteger attempts = new AtomicInteger();
        List<SleepEvent> sleeping = new CopyOnWriteArrayList<>();
        CountDownLatch done = new CountDownLatch(1);

        start(queue.worker()
                .handle(waiter, (String payload) -> {
                    JobContext ctx = JobContext.current();
                    int attempt = attempts.incrementAndGet();
                    ctx.step().sleep(Duration.ofMillis(400));
                    // The attempt that wakes fails once, so a third attempt
                    // replays the same sleep after its deadline has passed.
                    if (attempt == 2) {
                        throw new IllegalStateException("die on the waking attempt");
                    }
                    return "ok";
                })
                .onEvent(EventName.JOB_SLEEPING, event -> sleeping.add((SleepEvent) event))
                .on(EventName.SUCCESS, event -> done.countDown()));

        assertTrue(done.await(SETTLE.toSeconds(), TimeUnit.SECONDS), "the job should finish after its retry");
        assertEquals(3, attempts.get(), "slept, woke and died, then replayed the elapsed sleep and finished");
        assertEquals(1, sleeping.size(), "the third attempt replayed the sleep rather than sleeping again");
    }

    // ── the caps ────────────────────────────────────────────────────

    /**
     * A step result past the per-step cap is refused, permanently.
     *
     * <p>The answer to an over-cap step is not a bigger cap — it is storing the
     * value elsewhere and memoizing the handle — so the retry budget must not be
     * spent replaying a blob that will be exactly as large next time.
     */
    @ParameterizedTest
    @EnumSource(Backend.class)
    @Timeout(60)
    void aStepResultPastTheCapIsRefusedWithoutSpendingTheBudget(Backend backend) throws Exception {
        Task<String> render = Task.of("render", String.class).maxRetries(5).retryPolicy(FAST);
        FlexiQ queue = queue(backend);
        queue.enqueue(render, "go");

        // Comfortably past the 256 KiB per-step default, whatever the serializer
        // adds around it.
        String blob = "x".repeat(400 * 1024);
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch dead = new CountDownLatch(1);

        start(queue.worker()
                .handle(render, (String payload) -> {
                    attempts.incrementAndGet();
                    return JobContext.current().step().run("render", String.class, () -> blob);
                })
                .on(EventName.DEAD, event -> dead.countDown()));

        assertTrue(dead.await(SETTLE.toSeconds(), TimeUnit.SECONDS), "an over-cap step should dead-letter the job");
        assertEquals(1, attempts.get(), "a cap is permanent, so the budget was not spent on it");
        assertTrue(
                queue.listDead(10, 0).get(0).error.contains("step bytes"),
                queue.listDead(10, 0).get(0).error);
    }

    // ── the fence ───────────────────────────────────────────────────

    /**
     * An attempt that lost its execution claim cannot commit into the sequence.
     *
     * <p>The claim is taken away while a step body is parked, and the parked body
     * is not released until the attempt that inherited the job has <b>finished</b>
     * — so the commit that follows is unambiguously a write from an attempt that
     * no longer speaks for the job, with no race to decide it.
     *
     * <p>Asserted on the refusal itself rather than on the job's final state: a
     * backend with no fence at all lets the stale commit land, and the job still
     * looks right afterwards.
     */
    @ParameterizedTest
    @EnumSource(Backend.class)
    @Timeout(60)
    void aStepCommittedAfterLosingTheClaimIsRefused(Backend backend) throws Exception {
        Task<String> settle = Task.of("settle", String.class).maxRetries(2).retryPolicy(FAST);
        FlexiQ queue = queue(backend);
        String jobId = queue.enqueue(settle, "go");

        AtomicInteger attempts = new AtomicInteger();
        AtomicReference<Throwable> refused = new AtomicReference<>();
        CountDownLatch parked = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);
        CountDownLatch stale = new CountDownLatch(1);

        start(queue.worker()
                .handle(settle, (String payload) -> {
                    JobContext ctx = JobContext.current();
                    if (attempts.incrementAndGet() > 1) {
                        ctx.step().run("charge", () -> {});
                        return "ok";
                    }
                    try {
                        ctx.step().run("charge", () -> {
                            parked.countDown();
                            assertTrue(release.await(SETTLE.toSeconds(), TimeUnit.SECONDS));
                        });
                    } catch (Throwable signal) {
                        // Recorded, then rethrown: swallowing a control signal
                        // fails the attempt anyway, and this attempt is meant to
                        // fail.
                        refused.set(signal);
                        stale.countDown();
                        throw signal;
                    }
                    return "ok";
                })
                .on(EventName.SUCCESS, event -> done.countDown()));

        assertTrue(parked.await(SETTLE.toSeconds(), TimeUnit.SECONDS), "the first attempt should reach its step");
        assertTrue(queue.requeueJob(jobId), "the job should still be running, so the claim can be taken");
        assertTrue(done.await(SETTLE.toSeconds(), TimeUnit.SECONDS), "the requeued attempt should finish");

        // Only now: the job is settled, so the parked attempt's commit cannot be
        // mistaken for a race with the live one.
        release.countDown();
        assertTrue(stale.await(SETTLE.toSeconds(), TimeUnit.SECONDS), "the stale commit should be refused");

        Throwable signal = refused.get();
        assertNotNull(signal, "the stale commit reported nothing");
        assertInstanceOf(StepSupersededError.class, signal, "was: " + signal);
        assertFalse(((StepSupersededError) signal).shouldRetry(), "a superseded attempt has nothing to retry for");
    }

    // ── the run key ─────────────────────────────────────────────────

    /**
     * An operator's dead-letter retry mints a new job for the same run, and the
     * run key has to survive it — otherwise every idempotency key the job's steps
     * mint changes, and a charge the downstream service already deduped is made
     * a second time.
     */
    @ParameterizedTest
    @EnumSource(Backend.class)
    @Timeout(60)
    void aRunKeySurvivesADeadLetterRetry(Backend backend) throws Exception {
        Task<String> billing = Task.of("billing", String.class).maxRetries(0);
        FlexiQ queue = queue(backend);
        String original = queue.enqueue(billing, "go");

        List<String> keys = new CopyOnWriteArrayList<>();
        AtomicInteger attempts = new AtomicInteger();
        CountDownLatch dead = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(1);

        start(queue.worker()
                .handle(billing, (String payload) -> {
                    JobContext ctx = JobContext.current();
                    // A fresh job every time — the dead-lettered one committed
                    // nothing, so this always runs.
                    ctx.step().run("charge", () -> keys.add(ctx.step().idempotencyKey()));
                    if (attempts.incrementAndGet() == 1) {
                        throw new IllegalStateException("dead-letter me");
                    }
                    return "ok";
                })
                .on(EventName.DEAD, event -> dead.countDown())
                .on(EventName.SUCCESS, event -> done.countDown()));

        assertTrue(dead.await(SETTLE.toSeconds(), TimeUnit.SECONDS), "the first job should dead-letter");
        DeadJob entry = queue.listDead(10, 0).get(0);
        String retried = queue.retryDead(entry.id);
        assertNotNull(retried);

        assertTrue(done.await(SETTLE.toSeconds(), TimeUnit.SECONDS), "the retried job should finish");
        assertEquals(
                List.of(original + ":charge#0", original + ":charge#0"),
                keys,
                "the resurrected job kept the run it belongs to, so its step minted the same key");
    }
}
