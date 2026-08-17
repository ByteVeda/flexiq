package org.byteveda.flexiq.core;

import java.time.Duration;
import java.util.Optional;
import org.byteveda.flexiq.FlexiQException;
import org.byteveda.flexiq.spi.QueueBackend;

/**
 * Translation + durable-emulation layer between the typed public API and the thin
 * native {@link QueueBackend}. It speaks the core's wire terms (JSON strings,
 * lowercase status) and is the home for behaviours the core does not provide
 * natively — emulated durably (state in job metadata / the settings store), never
 * in worker memory. Today it owns job-await polling; retry-backoff and the
 * periodic catalog join it later.
 */
public final class CoreFacade {
    private final QueueBackend backend;

    public CoreFacade(QueueBackend backend) {
        this.backend = backend;
    }

    public QueueBackend backend() {
        return backend;
    }

    /**
     * Poll until the job reaches a terminal state or {@code timeout} elapses,
     * returning its final JSON view. Intended for tests — production code should
     * use worker event hooks or webhooks. Throws on timeout.
     */
    public Optional<String> awaitJobJson(String jobId, Duration timeout, Duration pollInterval) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            Optional<String> view = backend.getJobJson(jobId);
            if (view.isEmpty()) {
                return Optional.empty();
            }
            if (isTerminal(view.get())) {
                return view;
            }
            if (System.nanoTime() >= deadline) {
                throw new FlexiQException("job '" + jobId + "' did not finish within " + timeout);
            }
            sleep(pollInterval);
        }
    }

    /** Whether a job view's wire status is terminal (complete/failed/dead/cancelled). */
    private static boolean isTerminal(String jobJson) {
        String status = extractStatus(jobJson);
        return "complete".equals(status)
                || "completed".equals(status)
                || "failed".equals(status)
                || "dead".equals(status)
                || "cancelled".equals(status);
    }

    /** Read the lowercase {@code "status"} field from a job view without a full parse. */
    private static String extractStatus(String jobJson) {
        int key = jobJson.indexOf("\"status\"");
        if (key < 0) {
            return "";
        }
        int colon = jobJson.indexOf(':', key);
        int open = jobJson.indexOf('"', colon + 1);
        int close = jobJson.indexOf('"', open + 1);
        return open < 0 || close < 0 ? "" : jobJson.substring(open + 1, close);
    }

    private static void sleep(Duration interval) {
        try {
            Thread.sleep(interval.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlexiQException("interrupted while awaiting a job", e);
        }
    }
}
