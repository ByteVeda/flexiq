/**
 * A tiny HTTP endpoint that exposes queue depth for an external autoscaler
 * (e.g. KEDA's metrics-api scaler). {@link org.byteveda.flexiq.scaler.Scaler}
 * serves {@code GET /api/scaler} ({@code metricValue}/{@code targetValue}) and
 * {@code GET /health}. Observability (metrics export) is left to the contrib
 * middleware; this only reports depth for scaling decisions.
 */
@NullMarked
package org.byteveda.flexiq.scaler;

import org.jspecify.annotations.NullMarked;
