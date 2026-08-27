package org.byteveda.flexiq.dashboard.api;

import java.util.Map;
import org.byteveda.flexiq.FlexiQ;
import org.byteveda.flexiq.dashboard.support.Http;

/**
 * Aggregated metrics endpoints. {@code /api/metrics} returns the per-task
 * summary the SPA expects (not raw rows); {@code /api/metrics/timeseries}
 * returns time-bucketed points. Both aggregate raw {@link org.byteveda.flexiq.model.TaskMetric}
 * rows in pure Java via {@link Metrics}.
 */
public final class MetricsHandlers {
    private static final long DEFAULT_BUCKET_MS = 60_000;

    private final FlexiQ queue;

    /**
     * Handlers reading one queue's task metrics.
     *
     * @param queue what the routes below read from
     */
    public MetricsHandlers(FlexiQ queue) {
        this.queue = queue;
    }

    /**
     * Task metrics rolled up per task.
     *
     * @param query {@code task} to narrow to one, {@code since} as a Unix-millisecond
     *     floor; both optional
     * @return one row per task
     */
    public Object aggregated(Map<String, String> query) {
        return Metrics.aggregateByTask(queue.metrics(query.get("task"), Http.longParam(query, "since", 0)));
    }

    /**
     * Task metrics bucketed over time, for the charts.
     *
     * @param query {@code task}, {@code since} as a Unix-millisecond floor, and
     *     {@code bucket} as a bucket width in milliseconds; each optional
     * @return one series per task
     */
    public Object timeseries(Map<String, String> query) {
        long since = Http.longParam(query, "since", 0);
        long bucket = Http.longParam(query, "bucket", DEFAULT_BUCKET_MS);
        return Metrics.timeseries(queue.metrics(query.get("task"), since), bucket);
    }
}
