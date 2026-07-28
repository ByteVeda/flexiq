/**
 * Specific unchecked exceptions for Taskito failure scenarios.
 *
 * <p>All extend {@link org.byteveda.taskito.TaskitoException}, so a caller can
 * catch the base type to handle any SDK error, or a specific subtype
 * ({@link org.byteveda.taskito.errors.SerializationException},
 * {@link org.byteveda.taskito.errors.WorkflowException}, etc.) to react to one
 * category. Native (JNI) errors surface as the base {@code TaskitoException}.
 *
 * <p>{@link org.byteveda.taskito.errors.RetryableException} and
 * {@link org.byteveda.taskito.errors.NonRetryableException} run the other way:
 * a task handler throws them to tell the worker whether the failure is worth
 * retrying.
 *
 * <p>Also home to {@link org.byteveda.taskito.errors.TaskErrors}, the codec for
 * the structured task-error JSON stored in job and dead-letter {@code error}
 * fields, and its decoded view {@link org.byteveda.taskito.errors.TaskError}.
 */
@NullMarked
package org.byteveda.taskito.errors;

import org.jspecify.annotations.NullMarked;
