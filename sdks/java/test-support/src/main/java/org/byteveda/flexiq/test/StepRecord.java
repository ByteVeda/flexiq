package org.byteveda.flexiq.test;

import org.jspecify.annotations.Nullable;

/**
 * One committed step of one job — the harness's stand-in for a {@code job_steps}
 * row.
 *
 * <p>There is no status and no error field, deliberately, for the reason the
 * real table has none: a step whose body raised is never committed, so a
 * recorded {@code run} is complete by construction and a recorded {@code sleep}
 * is finished exactly when {@code now >= wakeAt}. Every state is derivable from
 * what is stored, and a status column would be a second source of truth for some
 * path to forget to advance.
 *
 * @param seq position in the job's sequence — gapless, so it is also the next
 *     free position
 * @param stepKey this step's identity, {@code name#occurrence} or {@code name:key}
 * @param sleep whether this row holds a deadline rather than a value
 * @param result the committed bytes, or {@code null} for a sleep
 * @param wakeAt the deadline, or {@code null} for a run
 */
record StepRecord(int seq, String stepKey, boolean sleep, byte @Nullable [] result, @Nullable Long wakeAt) {

    /** A committed run holding {@code result}. */
    static StepRecord run(int seq, String stepKey, byte[] result) {
        return new StepRecord(seq, stepKey, false, result, null);
    }

    /** A committed sleep due at {@code wakeAt}. */
    static StepRecord sleep(int seq, String stepKey, long wakeAt) {
        return new StepRecord(seq, stepKey, true, null, wakeAt);
    }

    /** How many bytes this row counts against the job's total. */
    int resultLength() {
        byte[] bytes = result;
        return bytes == null ? 0 : bytes.length;
    }

    /** The word this kind goes by in a divergence message. */
    String kind() {
        return sleep ? "sleep" : "run";
    }
}
