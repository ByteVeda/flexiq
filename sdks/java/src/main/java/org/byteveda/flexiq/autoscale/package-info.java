/**
 * In-process autoscaling of a worker's handler thread pool. An
 * {@link org.byteveda.flexiq.autoscale.Autoscaler} periodically reads queue
 * depth and resizes a {@link java.util.concurrent.ThreadPoolExecutor} between a
 * min and max, so a worker grows under load and shrinks when idle. Enable it via
 * {@code Worker.Builder.autoscale(...)}.
 */
@NullMarked
package org.byteveda.flexiq.autoscale;

import org.jspecify.annotations.NullMarked;
