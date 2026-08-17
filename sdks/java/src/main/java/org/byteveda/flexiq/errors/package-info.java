/**
 * Specific unchecked exceptions for FlexiQ failure scenarios.
 *
 * <p>All extend {@link org.byteveda.flexiq.FlexiQException}, so a caller can
 * catch the base type to handle any SDK error, or a specific subtype
 * ({@link org.byteveda.flexiq.errors.SerializationException},
 * {@link org.byteveda.flexiq.errors.WorkflowException}, etc.) to react to one
 * category. Native (JNI) errors surface as the base {@code FlexiQException}.
 *
 * <p>{@link org.byteveda.flexiq.errors.RetryableException} and
 * {@link org.byteveda.flexiq.errors.NonRetryableException} run the other way:
 * a task handler throws them to tell the worker whether the failure is worth
 * retrying.
 *
 * <p>Also home to {@link org.byteveda.flexiq.errors.TaskErrors}, the codec for
 * the structured task-error JSON stored in job and dead-letter {@code error}
 * fields, and its decoded view {@link org.byteveda.flexiq.errors.TaskError}.
 */
@NullMarked
package org.byteveda.flexiq.errors;

import org.jspecify.annotations.NullMarked;
