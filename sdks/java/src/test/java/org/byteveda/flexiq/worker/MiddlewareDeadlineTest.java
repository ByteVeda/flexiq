package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.events.EventName;
import org.byteveda.flexiq.middleware.Middleware;
import org.byteveda.flexiq.middleware.TaskContext;
import org.byteveda.flexiq.task.Task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A middleware hook that outruns its budget is interrupted, not fatal.
 *
 * <p>A task's own timeout bounds its handler and nothing else, so a
 * {@code before} that blocks holds the attempt open past that limit. The unit
 * tests pin what the bound does to one hook; the worker test pins that the chain
 * is wired to it, by blocking a {@code before} on a latch nobody counts down and
 * still getting a completed job.
 */
class MiddlewareDeadlineTest {

    @Test
    @Timeout(30)
    void interruptsAHookThatOutrunsItsBudget() {
        AtomicBoolean interrupted = new AtomicBoolean();

        HookDeadline.run(20, "Blocking", "before", () -> {
            try {
                // Never counted down: only the deadline ends this wait.
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        });

        assertTrue(interrupted.get(), "the hook should have been interrupted at its deadline");
        // The handler runs next on this very thread, and must not inherit the
        // interrupt that was aimed at the hook.
        assertFalse(Thread.currentThread().isInterrupted(), "the interrupt flag must be cleared");
    }

    @Test
    @Timeout(30)
    void leavesTheInterruptFlagAloneWhenTheHookBehaves() {
        AtomicBoolean ran = new AtomicBoolean();

        HookDeadline.run(10_000, "Quick", "after", () -> ran.set(true));

        assertTrue(ran.get());
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    @Timeout(30)
    void stillPropagatesAFailureTheHookRaisedItself() {
        // Only the overrun is swallowed. A `before` that means to reject a job
        // keeps failing the attempt exactly as it did before this bound existed.
        IllegalStateException thrown = assertThrows(
                IllegalStateException.class,
                () -> HookDeadline.run(10_000, "Angry", "before", () -> {
                    throw new IllegalStateException("nope");
                }));

        assertEquals("nope", thrown.getMessage());
        assertFalse(Thread.currentThread().isInterrupted());
    }

    @Test
    @Timeout(30)
    void aZeroBudgetDisablesTheBound() throws Exception {
        AtomicBoolean interrupted = new AtomicBoolean();
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        Thread hook = new Thread(() -> HookDeadline.run(0, "Unbounded", "before", () -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                interrupted.set(true);
            }
        }));
        hook.start();
        assertTrue(entered.await(20, TimeUnit.SECONDS), "the hook should have started");
        // Long past any default budget, and nothing has interrupted it.
        Thread.sleep(200);
        assertFalse(interrupted.get(), "a zero budget must not arm an alarm");
        release.countDown();
        hook.join(TimeUnit.SECONDS.toMillis(20));
        assertFalse(hook.isAlive(), "the hook thread should have finished");
    }

    @Test
    @Timeout(30)
    void roundsASubMillisecondBudgetUpRatherThanDownToDisabled() {
        // Duration.ZERO is the only value that disables the bound. toMillis()
        // truncates, so without the round-up the tightest budget a caller can
        // express would mean no bound at all.
        assertEquals(0L, HookDeadline.millis(Duration.ZERO));
        assertEquals(1L, HookDeadline.millis(Duration.ofNanos(1)));
        assertEquals(1L, HookDeadline.millis(Duration.ofNanos(999_999)));
        assertEquals(20L, HookDeadline.millis(Duration.ofMillis(20)));
        assertThrows(IllegalArgumentException.class, () -> HookDeadline.millis(Duration.ofMillis(-1)));
    }

    /** Blocks in {@code before} until something interrupts it. */
    static final class Blocking implements Middleware {
        final CountDownLatch interrupted = new CountDownLatch(1);

        @Override
        public void before(TaskContext context) {
            try {
                new CountDownLatch(1).await();
            } catch (InterruptedException e) {
                interrupted.countDown();
            }
        }
    }

    @Test
    @Timeout(60)
    void runsTheTaskEvenWhenAMiddlewaresBeforeBlocksForever(@TempDir Path dir) throws Exception {
        Task<String> bounded = Task.of("bounded", String.class);
        Blocking blocking = new Blocking();

        try (FlexiQ queue = FlexiQ.builder()
                .backend("sqlite")
                .url(dir.resolve("deadline.db").toString())
                .middlewareTimeout(Duration.ofMillis(50))
                .open()) {
            queue.use(blocking);

            CountDownLatch done = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .handle(bounded, (String payload) -> payload)
                    .on(EventName.SUCCESS, event -> done.countDown())
                    .start();
            try (worker) {
                queue.enqueue(bounded, "ran");
                assertTrue(done.await(30, TimeUnit.SECONDS), "the job should complete despite the hung hook");
                assertTrue(
                        blocking.interrupted.await(5, TimeUnit.SECONDS),
                        "the hung before() should have been interrupted");
            }
        }
    }
}
