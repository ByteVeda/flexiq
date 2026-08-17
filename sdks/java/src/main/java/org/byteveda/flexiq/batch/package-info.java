/**
 * Producer-side batching: {@link org.byteveda.flexiq.batch.Batcher} buffers
 * payloads and flushes them in one {@code enqueueMany} when the batch fills or a
 * delay elapses. The worker side already batches via the worker's
 * {@code batchSize} option (which drives the core batch dequeue).
 */
@NullMarked
package org.byteveda.flexiq.batch;

import org.jspecify.annotations.NullMarked;
