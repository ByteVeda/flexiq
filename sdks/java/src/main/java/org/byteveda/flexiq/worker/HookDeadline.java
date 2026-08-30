package org.byteveda.flexiq.worker;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.byteveda.flexiq.logging.FlexiQLogger;

/**
 * Bounds one middleware hook by interrupting the thread that runs it.
 *
 * <p>A task's {@code timeout} bounds its handler and nothing else, so a
 * {@code before}, {@code after}, {@code onError} or {@code onSleep} that blocks
 * — an exporter flushing to an unreachable collector — holds the attempt open
 * past that limit.
 *
 * <p>The hook keeps the dispatch thread on purpose: {@code FlexiQObservation}
 * opens a Micrometer scope in {@code before} <em>for the handler</em> and closes
 * it from {@code after}, and both are thread-bound. So the bound is an
 * interrupt rather than a hand-off — best effort by construction, and enough for
 * the blocking that actually happens here ({@code Future.get},
 * {@code awaitTermination}, a socket with a timeout).
 *
 * <p>An overrun is logged and the chain continues. A hook that throws on its own
 * still propagates: failing an attempt over its instrumentation is the failure
 * mode the hooks exist to avoid, but a {@code before} that means to reject a job
 * must keep working.
 */
final class HookDeadline {
    private static final FlexiQLogger LOG = FlexiQLogger.create("worker");

    /** Budget a worker applies when none is configured. */
    static final long DEFAULT_TIMEOUT_MILLIS = 5_000L;

    /**
     * One daemon timer for the process. A thread per hook would cost more than
     * most hooks do, and the alarm never runs anything but {@code interrupt}.
     */
    private static final ScheduledExecutorService TIMER = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "flexiq-hook-deadline");
        thread.setDaemon(true);
        return thread;
    });

    private HookDeadline() {}

    /**
     * A configured budget as milliseconds.
     *
     * @param timeout the per-hook budget; {@link Duration#ZERO} disables the bound
     * @return the budget in milliseconds
     * @throws IllegalArgumentException if {@code timeout} is negative
     */
    static long millis(Duration timeout) {
        Objects.requireNonNull(timeout, "middlewareTimeout");
        if (timeout.isNegative()) {
            throw new IllegalArgumentException("middlewareTimeout must not be negative");
        }
        return timeout.toMillis();
    }

    /**
     * Run {@code body} under a {@code timeoutMillis} budget.
     *
     * @param timeoutMillis budget for this one call; {@code 0} or less disables the bound
     * @param middleware stable name of the middleware, for the log line
     * @param hook hook name, for the log line ({@code "before"}, {@code "after"}, …)
     * @param body invokes the hook
     */
    static void run(long timeoutMillis, String middleware, String hook, Runnable body) {
        if (timeoutMillis <= 0) {
            body.run();
            return;
        }
        Alarm alarm = new Alarm(Thread.currentThread());
        ScheduledFuture<?> scheduled = TIMER.schedule(alarm, timeoutMillis, TimeUnit.MILLISECONDS);
        boolean overran;
        try {
            body.run();
        } catch (RuntimeException | Error thrown) {
            overran = alarm.disarm();
            scheduled.cancel(false);
            if (!overran) {
                throw thrown;
            }
            LOG.warn(overrunMessage(middleware, hook, timeoutMillis), thrown);
            return;
        }
        overran = alarm.disarm();
        scheduled.cancel(false);
        if (overran) {
            // Returned normally despite the interrupt: it either swallowed the
            // InterruptedException or never blocked on anything interruptible.
            // Still over budget, so still worth naming.
            LOG.warn(overrunMessage(middleware, hook, timeoutMillis));
        }
    }

    private static String overrunMessage(String middleware, String hook, long timeoutMillis) {
        return "middleware " + middleware + " " + hook + "() exceeded " + timeoutMillis
                + "ms; interrupted, the chain continues";
    }

    /**
     * The scheduled interrupt, and the answer to "did it land".
     *
     * <p>{@code run} and {@link #disarm()} are mutually exclusive, and the
     * interrupt is delivered under the lock: without that, an alarm that had
     * decided to fire could interrupt a thread that has already left the hook,
     * poisoning the handler that runs next.
     */
    private static final class Alarm implements Runnable {
        private final Thread target;
        private boolean fired;
        private boolean disarmed;

        Alarm(Thread target) {
            this.target = target;
        }

        @Override
        public synchronized void run() {
            if (disarmed) {
                return;
            }
            fired = true;
            target.interrupt();
        }

        /**
         * Stop the alarm and answer whether it had already fired.
         *
         * <p>Clears this thread's interrupt flag when it did: the interrupt may
         * still be pending (the hook never blocked) or the hook may have
         * swallowed the {@code InterruptedException} and left it set. Either way
         * the handler that runs next must not inherit it. An interrupt that was
         * not ours — a pool shutting down mid-hook — is left alone.
         */
        boolean disarm() {
            boolean landed;
            synchronized (this) {
                disarmed = true;
                landed = fired;
            }
            if (landed) {
                Thread.interrupted();
            }
            return landed;
        }
    }
}
