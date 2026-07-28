/**
 * Optional observability middleware. Each integration's third-party dependency
 * is {@code compileOnly}, so a consumer that uses one adds the matching runtime
 * dependency to their build.
 *
 * <ul>
 *   <li>{@link org.byteveda.taskito.contrib.TaskitoObservation} — Micrometer
 *       Observation per task (one instrumentation yields metrics + a trace span;
 *       plug OpenTelemetry in as the backend).
 *   <li>{@link org.byteveda.taskito.contrib.SentryMiddleware} — report task
 *       failures and dead-letters to Sentry.
 * </ul>
 */
@NullMarked
package org.byteveda.taskito.contrib;

import org.jspecify.annotations.NullMarked;
