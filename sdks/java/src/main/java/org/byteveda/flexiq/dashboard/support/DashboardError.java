package org.byteveda.flexiq.dashboard.support;

import org.jspecify.annotations.Nullable;

/**
 * A handler-level failure that maps to a specific HTTP status and a stable
 * machine-readable {@code error} code in the JSON body. Thrown by handlers and
 * the auth gate; caught centrally by the server, which renders
 * {@code {"error": code}} with {@link #status()}.
 */
public final class DashboardError extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final int status;

    private DashboardError(int status, String code) {
        super(code);
        this.status = status;
    }

    public int status() {
        return status;
    }

    /** The stable error code (also the exception message). */
    public @Nullable String code() {
        return getMessage();
    }

    public static DashboardError of(int status, String code) {
        return new DashboardError(status, code);
    }

    public static DashboardError badRequest(String code) {
        return new DashboardError(400, code);
    }

    public static DashboardError unauthorized(String code) {
        return new DashboardError(401, code);
    }

    public static DashboardError forbidden(String code) {
        return new DashboardError(403, code);
    }

    public static DashboardError notFound(String code) {
        return new DashboardError(404, code);
    }

    public static DashboardError serviceUnavailable(String code) {
        return new DashboardError(503, code);
    }
}
