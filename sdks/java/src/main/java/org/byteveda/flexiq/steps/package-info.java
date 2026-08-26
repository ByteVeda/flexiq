/**
 * Durable inline steps: {@code JobContext.current().step()}.
 *
 * <p>A step is a checkpoint inside one job. It runs once, its result is
 * committed, and every later attempt of that job returns the committed value
 * instead of running it again. The rules — identity, divergence, the size caps,
 * the sleep decision — live in the Rust core, which is what makes them
 * identical across the SDKs.
 */
@NullMarked
package org.byteveda.flexiq.steps;

import org.jspecify.annotations.NullMarked;
