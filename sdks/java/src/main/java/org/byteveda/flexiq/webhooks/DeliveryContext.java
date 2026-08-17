package org.byteveda.flexiq.webhooks;

import org.jspecify.annotations.Nullable;

/**
 * The provenance of a webhook delivery — threaded from the outcome (or a test /
 * replay) into the {@link Deliverer} so each recorded {@link Delivery} carries
 * the event and the originating job. All fields may be {@code null} for
 * synthetic events (e.g. a dashboard test ping).
 */
record DeliveryContext(String event, @Nullable String taskName, @Nullable String jobId) {}
