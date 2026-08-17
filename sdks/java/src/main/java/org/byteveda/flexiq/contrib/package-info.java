/**
 * Optional observability middleware. Each integration's third-party dependency
 * is {@code compileOnly}, so a consumer that uses one adds the matching runtime
 * dependency to their build.
 *
 * <ul>
 *   <li>{@link org.byteveda.flexiq.contrib.FlexiQObservation} — Micrometer
 *       Observation per task (one instrumentation yields metrics + a trace span;
 *       plug OpenTelemetry in as the backend).
 *   <li>{@link org.byteveda.flexiq.contrib.SentryMiddleware} — report task
 *       failures and dead-letters to Sentry.
 * </ul>
 */
@NullMarked
package org.byteveda.flexiq.contrib;

import org.jspecify.annotations.NullMarked;
