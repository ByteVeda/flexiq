package org.byteveda.flexiq.steps;

import org.byteveda.flexiq.spi.StepSession;

/**
 * Where a durable step commits: one worker's own claim on one job.
 *
 * <p>Only an in-process worker supplies one. An attached executor holds no
 * storage and no channel to commit a step on, so it supplies none and every
 * step refuses with {@link StepUnavailableError} — §9.4's "refuse, never
 * degrade".
 */
@FunctionalInterface
public interface StepStore {
    /**
     * Open the step session for one attempt.
     *
     * @param jobId the running job
     * @param attempt the {@code retryCount} this job was dispatched with, checked
     *     against the row so a superseded attempt cannot write into the live one
     * @return the session this attempt's steps commit through
     */
    StepSession open(String jobId, int attempt);
}
