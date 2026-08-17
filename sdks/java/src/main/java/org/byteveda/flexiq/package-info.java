/**
 * FlexiQ Java SDK — a typed client over the Rust task-queue core.
 *
 * <p>This root package is the front door: open a
 * {@link org.byteveda.flexiq.FlexiQ} client via
 * {@link org.byteveda.flexiq.FlexiQ#builder()}, then scope to one named queue
 * with {@link org.byteveda.flexiq.FlexiQ#queue(String)}. Everything else is
 * grouped by feature:
 *
 * <ul>
 *   <li>{@code task} — {@link org.byteveda.flexiq.task.Task} descriptors,
 *       handler functions, enqueue options
 *   <li>{@code model} — immutable views the API returns (jobs, stats, metrics)
 *   <li>{@code worker} — the worker runtime
 *   <li>{@code locks} / {@code scheduling} / {@code workflows} — feature surfaces
 *   <li>{@code serialization} / {@code middleware} / {@code events} — cross-cutting
 *   <li>{@code spi} — the {@link org.byteveda.flexiq.spi.QueueBackend} seam,
 *       whose default implementation lives in {@code internal}
 * </ul>
 */
@NullMarked
package org.byteveda.flexiq;

import org.jspecify.annotations.NullMarked;
