package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.JobContext;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.events.OutcomeEvent;
import org.byteveda.flexiq.events.SleepEvent;
import org.byteveda.flexiq.middleware.Middleware;
import org.byteveda.flexiq.middleware.TaskContext;
import org.byteveda.flexiq.model.Job;
import org.byteveda.flexiq.steps.StepOptions;
import org.byteveda.flexiq.task.RetryPolicy;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Durable inline steps end to end, over a real worker and a real step store.
 *
 * <p>Everything is asserted through the queue's own API, its events and the
 * bodies' own counters. Reading the database from this process while a worker is
 * writing to it is not a shortcut — a second SQLite build in one process does
 * not share the first one's WAL index, so the reads come back empty rather than
 * wrong, which reads exactly like flakiness.
 */
class StepsTest {

    private static final RetryPolicy FAST = RetryPolicy.delays(Duration.ofMillis(10), Duration.ofMillis(10));

    private static FlexiQ open(Path dir) {
        return FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("steps.db").toString())
                .open();
    }

    @Test
    @Timeout(30)
    void aStepRunsOncePerJobNotOncePerAttempt(@TempDir Path dir) throws Exception {
        Task<String> checkout = Task.of("checkout", String.class).maxRetries(2).retryPolicy(FAST);

        try (FlexiQ queue = open(dir)) {
            queue.enqueue(checkout, "go");

            AtomicInteger attempts = new AtomicInteger();
            AtomicInteger charges = new AtomicInteger();
            AtomicReference<Integer> replayed = new AtomicReference<>();
            CountDownLatch done = new CountDownLatch(1);

            Worker worker = queue.worker()
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
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(25, TimeUnit.SECONDS), "the job should finish on its retry");

                assertEquals(2, attempts.get(), "the handler ran twice");
                assertEquals(1, charges.get(), "but the step body ran once — the replay is a memo hit");
                assertEquals(1, replayed.get(), "and the replay saw the committed value");
            }
        }
    }

    /**
     * A keyed step is matched by its key wherever it sits in the recorded
     * sequence. Asserted on the returned <i>values</i>: counting how often the
     * bodies ran cannot tell key matching from position matching, because both
     * are memo hits.
     */
    @Test
    @Timeout(30)
    void aKeyedStepIsMatchedByKeyNotByPosition(@TempDir Path dir) throws Exception {
        Task<String> greet = Task.of("greet", String.class).maxRetries(2).retryPolicy(FAST);

        try (FlexiQ queue = open(dir)) {
            queue.enqueue(greet, "go");

            AtomicInteger attempts = new AtomicInteger();
            List<String> replayed = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);

            Worker worker = queue.worker()
                    .handle(greet, (String payload) -> {
                        JobContext ctx = JobContext.current();
                        int attempt = attempts.incrementAndGet();
                        // The second attempt asks for the same two keyed steps
                        // in the opposite order.
                        List<String> order = attempt == 1 ? List.of("alice", "bob") : List.of("bob", "alice");
                        List<String> seen = new ArrayList<>();
                        for (String who : order) {
                            seen.add(ctx.step().run("greet", String.class, () -> "hello-" + who, StepOptions.key(who)));
                        }
                        if (attempt == 1) {
                            throw new IllegalStateException("die once, so the second attempt replays");
                        }
                        replayed.addAll(seen);
                        return "ok";
                    })
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(25, TimeUnit.SECONDS), "the job should finish on its retry");

                assertEquals(
                        List.of("hello-bob", "hello-alice"),
                        replayed,
                        "each memo followed its key; a positional match would have returned them the other way round");
            }
        }
    }

    @Test
    @Timeout(30)
    void aSleepEndsTheAttemptAndTheJobResumesFromItsMemos(@TempDir Path dir) throws Exception {
        Task<String> nurture = Task.of("nurture", String.class).maxRetries(2).retryPolicy(FAST);

        try (FlexiQ queue = open(dir)) {
            String jobId = queue.enqueue(nurture, "go");

            AtomicInteger attempts = new AtomicInteger();
            AtomicInteger welcomes = new AtomicInteger();
            AtomicInteger followUps = new AtomicInteger();
            List<SleepEvent> sleeping = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);

            Worker worker = queue.worker()
                    .handle(nurture, (String payload) -> {
                        JobContext ctx = JobContext.current();
                        attempts.incrementAndGet();
                        ctx.step().run("welcome", welcomes::incrementAndGet);
                        ctx.step().sleep(Duration.ofMillis(400));
                        ctx.step().run("follow-up", followUps::incrementAndGet);
                        return "ok";
                    })
                    .onEvent(EventName.JOB_SLEEPING, event -> sleeping.add((SleepEvent) event))
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(25, TimeUnit.SECONDS), "the job should wake and finish");

                assertEquals(2, attempts.get(), "one attempt slept, the next one finished");
                assertEquals(1, welcomes.get(), "the step before the sleep is a memo hit on wake");
                assertEquals(1, followUps.get(), "the step after it ran once, on the waking attempt");
                assertEquals(1, sleeping.size(), "exactly one job.sleeping");
                assertEquals("sleep#0", sleeping.get(0).stepKey, "the sleep is itself a step row");

                Job job = queue.getJob(jobId).orElseThrow();
                assertEquals(0, job.retryCount, "a sleep costs no retry");
            }
        }
    }

    /**
     * The deadline is fixed by the first commit. A replayed sleep re-reads the
     * stored one, so a job that keeps replaying does not keep pushing its wake
     * forward — which would be a sleep that outlives the job.
     */
    @Test
    @Timeout(30)
    void aReplayedSleepDoesNotPushItsDeadlineForward(@TempDir Path dir) throws Exception {
        Task<String> waiter = Task.of("waiter", String.class).maxRetries(3).retryPolicy(FAST);

        try (FlexiQ queue = open(dir)) {
            queue.enqueue(waiter, "go");

            AtomicInteger attempts = new AtomicInteger();
            List<SleepEvent> sleeping = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);

            Worker worker = queue.worker()
                    .handle(waiter, (String payload) -> {
                        JobContext ctx = JobContext.current();
                        int attempt = attempts.incrementAndGet();
                        ctx.step().sleep(Duration.ofMillis(400));
                        // The attempt that wakes fails once, so a third attempt
                        // replays the same sleep after its deadline has passed.
                        if (attempt == 2) {
                            throw new IllegalStateException("fail after waking");
                        }
                        return "ok";
                    })
                    .onEvent(EventName.JOB_SLEEPING, event -> sleeping.add((SleepEvent) event))
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(25, TimeUnit.SECONDS), "the job should finish after its retry");

                assertEquals(3, attempts.get(), "slept, woke and failed, then succeeded");
                assertEquals(
                        1,
                        sleeping.size(),
                        "the replayed sleep was already elapsed; a recomputed deadline would have slept again");
            }
        }
    }

    @Test
    @Timeout(30)
    void theIdempotencyKeyIsStableAcrossARetry(@TempDir Path dir) throws Exception {
        Task<String> charge = Task.of("charge", String.class).maxRetries(2).retryPolicy(FAST);

        try (FlexiQ queue = open(dir)) {
            String jobId = queue.enqueue(charge, "go");

            AtomicInteger attempts = new AtomicInteger();
            List<String> keys = new CopyOnWriteArrayList<>();
            CountDownLatch done = new CountDownLatch(1);

            Worker worker = queue.worker()
                    .handle(charge, (String payload) -> {
                        JobContext ctx = JobContext.current();
                        int attempt = attempts.incrementAndGet();
                        return ctx.step().run("charge", String.class, () -> {
                            keys.add(ctx.step().idempotencyKey());
                            // Die inside the body, so nothing commits and the
                            // next attempt runs this same step again.
                            if (attempt == 1) {
                                throw new IllegalStateException("died between the 200 and the commit");
                            }
                            return "charged";
                        });
                    })
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(25, TimeUnit.SECONDS), "the job should finish on its retry");

                assertEquals(2, keys.size(), "the uncommitted step ran on both attempts");
                assertEquals(keys.get(0), keys.get(1), "and minted the same downstream key both times");
                assertEquals(jobId + ":charge#0", keys.get(0), "{runKey}:{stepKey}");
            }
        }
    }

    @Test
    @Timeout(30)
    void aDivergenceDeadLettersWithRetriesLeft(@TempDir Path dir) throws Exception {
        Task<String> drifting = Task.of("drifting", String.class).maxRetries(5).retryPolicy(FAST);

        try (FlexiQ queue = open(dir)) {
            queue.enqueue(drifting, "go");

            AtomicInteger attempts = new AtomicInteger();
            AtomicInteger retries = new AtomicInteger();
            AtomicReference<OutcomeEvent> dead = new AtomicReference<>();
            CountDownLatch deadLettered = new CountDownLatch(1);

            Worker worker = queue.worker()
                    .handle(drifting, (String payload) -> {
                        JobContext ctx = JobContext.current();
                        if (attempts.incrementAndGet() == 1) {
                            ctx.step().run("first", String.class, () -> "a");
                            throw new IllegalStateException("die once");
                        }
                        // The recorded sequence says "first" at this position.
                        ctx.step().run("second", String.class, () -> "b");
                        return "ok";
                    })
                    .on(EventName.RETRY, event -> retries.incrementAndGet())
                    .on(EventName.DEAD, event -> {
                        dead.set(event);
                        deadLettered.countDown();
                    })
                    .start();
            try (worker) {
                assertTrue(deadLettered.await(25, TimeUnit.SECONDS), "a divergence should dead-letter");

                assertEquals(2, attempts.get(), "the diverging attempt was the last one");
                assertEquals(1, retries.get(), "only the ordinary failure was retried");
                assertNotNull(dead.get());
                assertTrue(
                        dead.get().error.contains("StepDivergedError"),
                        "the dead letter should name the divergence: " + dead.get().error);
            }
        }
    }

    @Test
    @Timeout(30)
    void swallowingADivergenceStillFailsTheAttempt(@TempDir Path dir) throws Exception {
        Task<String> swallower =
                Task.of("swallower", String.class).maxRetries(5).retryPolicy(FAST);

        try (FlexiQ queue = open(dir)) {
            queue.enqueue(swallower, "go");

            AtomicInteger attempts = new AtomicInteger();
            AtomicInteger caughtAsException = new AtomicInteger();
            AtomicReference<OutcomeEvent> dead = new AtomicReference<>();
            CountDownLatch deadLettered = new CountDownLatch(1);

            Worker worker = queue.worker()
                    .handle(swallower, (String payload) -> {
                        JobContext ctx = JobContext.current();
                        if (attempts.incrementAndGet() == 1) {
                            ctx.step().run("first", String.class, () -> "a");
                            throw new IllegalStateException("die once");
                        }
                        try {
                            try {
                                ctx.step().run("second", String.class, () -> "b");
                            } catch (Exception e) {
                                // Cannot happen: a step signal is an Error.
                                caughtAsException.incrementAndGet();
                            }
                        } catch (Throwable swallowed) {
                            // This one can, and is what the latch is for.
                        }
                        return "returned anyway";
                    })
                    .on(EventName.DEAD, event -> {
                        dead.set(event);
                        deadLettered.countDown();
                    })
                    .start();
            try (worker) {
                assertTrue(deadLettered.await(25, TimeUnit.SECONDS), "a swallowed divergence must not succeed");

                assertEquals(0, caughtAsException.get(), "catch (Exception) never sees a step control signal");
                assertEquals(2, attempts.get(), "the swallow failure is permanent, so no further retry");
                assertNotNull(dead.get());
                assertTrue(
                        dead.get().error.contains("StepSwallowedError"),
                        "the dead letter should name the swallow: " + dead.get().error);
            }
        }
    }

    @Test
    @Timeout(30)
    void everyBeforeIsMatchedByExactlyOneOfAfterOrOnSleep(@TempDir Path dir) throws Exception {
        Task<String> naps = Task.of("naps", String.class).maxRetries(2).retryPolicy(FAST);

        AtomicInteger befores = new AtomicInteger();
        AtomicInteger afters = new AtomicInteger();
        List<Long> sleeps = new CopyOnWriteArrayList<>();

        try (FlexiQ queue = open(dir)) {
            queue.use(new Middleware() {
                @Override
                public void before(TaskContext context) {
                    befores.incrementAndGet();
                }

                @Override
                public void after(TaskContext context, Object result) {
                    afters.incrementAndGet();
                }

                @Override
                public void onSleep(TaskContext context, long wakeAt) {
                    sleeps.add(wakeAt);
                }
            });
            queue.enqueue(naps, "go");

            CountDownLatch done = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .handle(naps, (String payload) -> {
                        JobContext.current().step().sleep(Duration.ofMillis(300));
                        return "ok";
                    })
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(25, TimeUnit.SECONDS), "the job should wake and finish");

                assertEquals(2, befores.get(), "one before per attempt");
                assertEquals(1, afters.get(), "only the attempt that produced a result got after()");
                assertEquals(1, sleeps.size(), "the sleeping attempt got onSleep() instead");
                assertTrue(sleeps.get(0) > 0, "onSleep carries the deadline the job was rescheduled to");
            }
        }
    }

    @Test
    @Timeout(30)
    void sqliteHasAStepStore(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = open(dir)) {
            assertTrue(queue.supportsSteps());
        }
    }

    @Test
    @Timeout(30)
    void aStepWithNoResultStillReplaysAsAMemoHit(@TempDir Path dir) throws Exception {
        Task<String> notify = Task.of("notify", String.class).maxRetries(2).retryPolicy(FAST);

        try (FlexiQ queue = open(dir)) {
            queue.enqueue(notify, "go");

            AtomicInteger attempts = new AtomicInteger();
            AtomicInteger emails = new AtomicInteger();
            CountDownLatch done = new CountDownLatch(1);

            Worker worker = queue.worker()
                    .handle(notify, (String payload) -> {
                        JobContext ctx = JobContext.current();
                        int attempt = attempts.incrementAndGet();
                        ctx.step().run("email", emails::incrementAndGet);
                        if (attempt == 1) {
                            throw new IllegalStateException("die once");
                        }
                        return "ok";
                    })
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                assertTrue(done.await(25, TimeUnit.SECONDS), "the job should finish on its retry");

                assertEquals(2, attempts.get());
                assertEquals(1, emails.get(), "a side-effect step is memoized by the fact that it ran");
            }
        }
    }
}
