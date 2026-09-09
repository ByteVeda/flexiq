package org.byteveda.flexiq.steps;

import org.jspecify.annotations.Nullable;

/**
 * The swallow defence: the signal the worker checks after the task body returns.
 *
 * <p>{@link StepControlSignal} is an {@link Error}, so a
 * {@code catch (Exception e)} in a task body cannot swallow one. A
 * {@code catch (Throwable t)} can — and a body that catches a sleep and carries
 * on runs the rest of itself with no execution claim, so every side effect
 * after that point happens again on wake; a body that catches a divergence goes
 * on to return a value derived from a memo answering a different question.
 *
 * <p>So the step API latches before it throws, and the worker refuses to report
 * what the body did afterwards. The signal is latched rather than a flag
 * because what the worker owes the scheduler depends on which one it was — see
 * {@link #check()} and {@link #sleep()}.
 */
public final class StepLatch {
    private volatile @Nullable StepControlSignal signal;

    /** A fresh latch for one invocation. */
    public StepLatch() {}

    /**
     * Record the step control signal being thrown out of the body.
     *
     * <p>The first one wins. A sleep releases the claim, so a step called after
     * one was swallowed throws in its turn; latching that would lose the fact
     * that the attempt is already asleep.
     *
     * @param raised the signal on its way out of the step API
     */
    public void latch(StepControlSignal raised) {
        if (signal == null) {
            signal = raised;
        }
    }

    /**
     * Whether a control signal was raised at some point during this attempt.
     *
     * @return {@code true} once {@link #latch(StepControlSignal)} has been called
     */
    public boolean swallowed() {
        return signal != null;
    }

    /**
     * The sleep this attempt committed, if it committed one.
     *
     * <p>Set from the moment the sleep throws — and it throws only once the row,
     * the claim revocation and the reschedule are on disk — so a body that
     * caught the signal cannot hide that its attempt is over. The worker asks
     * before it reports a failure.
     *
     * @return the committed sleep, or {@code null} if this attempt took none
     */
    public @Nullable StepSleepSignal sleep() {
        StepControlSignal raised = signal;
        return raised instanceof StepSleepSignal sleeping ? sleeping : null;
    }

    /**
     * Throw if the body returned normally after swallowing a control signal.
     *
     * <p>Called the moment the handler returns, before the {@code after} hooks:
     * what the body returned is not a result, and those hooks exist to see one.
     *
     * <p>A swallowed <b>sleep</b> is rethrown as the sleep it is. Its row, its
     * claim revocation and its reschedule were committed before the signal was
     * ever thrown, so the attempt is over either way and a failure would speak
     * for a claim it no longer holds. And a sleep leaves {@code retry_count}
     * alone, so the woken attempt reuses the same {@code (owner, attempt)}: the
     * scheduler's fence cannot tell the stale failure from the live claim,
     * authorizes it, and dead-letters a job that is sleeping correctly. Anything
     * else fails the attempt, which is where the latch has to bite — that body
     * still holds its claim and nothing downstream would question the value it
     * returns.
     *
     * @throws StepSleepSignal if the body swallowed the sleep that ended it
     * @throws StepSwallowedError if any other control signal was caught and not rethrown
     */
    public void check() {
        StepSleepSignal sleeping = sleep();
        if (sleeping != null) {
            throw sleeping;
        }
        if (signal != null) {
            throw new StepSwallowedError("the task body caught a step control signal and returned anyway. "
                    + "Whatever it did after that ran without an execution claim, or on a memo answering a "
                    + "different question, so this attempt cannot be trusted. Let the step API's signals "
                    + "propagate — they are Errors precisely so an ordinary catch does not see them.");
        }
    }
}
