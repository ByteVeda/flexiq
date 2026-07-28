/**
 * Taskito Java SDK — a typed client over the Rust task-queue core.
 *
 * <p>This root package is the front door: open a
 * {@link org.byteveda.taskito.Taskito} client via
 * {@link org.byteveda.taskito.Taskito#builder()}, then scope to one named queue
 * with {@link org.byteveda.taskito.Taskito#queue(String)}. Everything else is
 * grouped by feature:
 *
 * <ul>
 *   <li>{@code task} — {@link org.byteveda.taskito.task.Task} descriptors,
 *       handler functions, enqueue options
 *   <li>{@code model} — immutable views the API returns (jobs, stats, metrics)
 *   <li>{@code worker} — the worker runtime
 *   <li>{@code locks} / {@code scheduling} / {@code workflows} — feature surfaces
 *   <li>{@code serialization} / {@code middleware} / {@code events} — cross-cutting
 *   <li>{@code spi} — the {@link org.byteveda.taskito.spi.QueueBackend} seam,
 *       whose default implementation lives in {@code internal}
 * </ul>
 */
@NullMarked
package org.byteveda.taskito;

import org.jspecify.annotations.NullMarked;
