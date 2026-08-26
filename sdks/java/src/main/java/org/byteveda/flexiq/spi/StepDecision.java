package org.byteveda.flexiq.spi;

import org.jspecify.annotations.Nullable;

/**
 * What the core decided about one durable step, before anything ran.
 *
 * <p>Constructed by the native layer, which is why this is a record with a
 * fixed component order: a divergence is detected here, at the point the step
 * is asked for, rather than at the end of the attempt.
 *
 * @param memoized the stored bytes when this step already ran, or {@code null}
 *     for new ground — in which case the body has to run and its result be
 *     committed
 * @param stepKey this step's identity, {@code name#occurrence} or the explicit key
 * @param idempotencyKey the key to hand the downstream service for this step
 */
public record StepDecision(byte @Nullable [] memoized, String stepKey, String idempotencyKey) {}
