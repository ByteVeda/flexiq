package org.byteveda.flexiq.dashboard.api;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.byteveda.flexiq.model.TaskMetric;

/**
 * Pure aggregation of raw {@link TaskMetric} rows into the dashboard's
 * per-task summary and time-bucketed series. Durations are wall-time
 * nanoseconds rendered as milliseconds; percentiles use the nearest-rank method.
 */
final class Metrics {
    private Metrics() {}

    /** Group by task → {count, success/failure counts, avg/min/max, p50/p95/p99} (all ms). */
    static Map<String, Object> aggregateByTask(List<TaskMetric> rows) {
        Map<String, List<TaskMetric>> byTask = new LinkedHashMap<>();
        for (TaskMetric row : rows) {
            byTask.computeIfAbsent(row.taskName, k -> new ArrayList<>()).add(row);
        }
        Map<String, Object> out = new LinkedHashMap<>();
        byTask.forEach((task, taskRows) -> out.put(task, summarise(taskRows)));
        return out;
    }

    /** Bucket rows by {@code recordedAt / bucketMs}, oldest first. */
    static List<Map<String, Object>> timeseries(List<TaskMetric> rows, long bucketMs) {
        long width = bucketMs > 0 ? bucketMs : 60_000;
        Map<Long, List<TaskMetric>> buckets = new TreeMap<>();
        for (TaskMetric row : rows) {
            long start = (row.recordedAt / width) * width;
            buckets.computeIfAbsent(start, k -> new ArrayList<>()).add(row);
        }
        List<Map<String, Object>> out = new ArrayList<>();
        buckets.forEach((start, bucketRows) -> {
            Map<String, Object> bucket = new LinkedHashMap<>();
            bucket.put("timestamp", start);
            bucket.put("count", bucketRows.size());
            bucket.put("success", countSucceeded(bucketRows));
            bucket.put("failure", bucketRows.size() - countSucceeded(bucketRows));
            double[] sorted = sortedDurations(bucketRows);
            bucket.put("avg_ms", average(sorted));
            bucket.put("p50_ms", percentile(sorted, 50));
            bucket.put("p95_ms", percentile(sorted, 95));
            bucket.put("p99_ms", percentile(sorted, 99));
            out.add(bucket);
        });
        return out;
    }

    private static Map<String, Object> summarise(List<TaskMetric> rows) {
        double[] sorted = sortedDurations(rows);
        int success = countSucceeded(rows);
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("count", rows.size());
        m.put("success_count", success);
        m.put("failure_count", rows.size() - success);
        m.put("avg_ms", average(sorted));
        m.put("p50_ms", percentile(sorted, 50));
        m.put("p95_ms", percentile(sorted, 95));
        m.put("p99_ms", percentile(sorted, 99));
        m.put("min_ms", sorted.length == 0 ? 0.0 : sorted[0]);
        m.put("max_ms", sorted.length == 0 ? 0.0 : sorted[sorted.length - 1]);
        return m;
    }

    private static int countSucceeded(List<TaskMetric> rows) {
        int count = 0;
        for (TaskMetric row : rows) {
            if (row.succeeded) {
                count++;
            }
        }
        return count;
    }

    private static double[] sortedDurations(List<TaskMetric> rows) {
        double[] durations = new double[rows.size()];
        for (int i = 0; i < rows.size(); i++) {
            durations[i] = rows.get(i).wallTimeNs / 1_000_000.0;
        }
        java.util.Arrays.sort(durations);
        return durations;
    }

    private static double average(double[] sorted) {
        if (sorted.length == 0) {
            return 0.0;
        }
        double sum = 0;
        for (double value : sorted) {
            sum += value;
        }
        return sum / sorted.length;
    }

    /** Nearest-rank percentile: index = ceil(p/100 * n) - 1. */
    private static double percentile(double[] sorted, int p) {
        if (sorted.length == 0) {
            return 0.0;
        }
        int index = (int) Math.ceil(p / 100.0 * sorted.length) - 1;
        index = Math.max(0, Math.min(sorted.length - 1, index));
        return sorted[index];
    }
}
