package org.byteveda.flexiq.spi;

/**
 * What a durable sleep did.
 *
 * @param elapsed {@code true} when the deadline had already passed, so nothing
 *     was written and the attempt carries on; {@code false} when the job is now
 *     {@code Pending} at {@code wakeAt} and the task body must unwind
 * @param stepKey identity of the sleep
 * @param wakeAt the deadline the job was actually rescheduled to, in Unix
 *     milliseconds — on a replay the stored one, which is not necessarily the
 *     one this call proposed
 */
public record StepSleepOutcome(boolean elapsed, String stepKey, long wakeAt) {}
