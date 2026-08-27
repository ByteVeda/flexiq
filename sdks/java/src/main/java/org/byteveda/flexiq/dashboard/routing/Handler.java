package org.byteveda.flexiq.dashboard.routing;

import java.io.IOException;
import org.jspecify.annotations.Nullable;

/**
 * A route handler. Returns the JSON response body (serialised with status 200),
 * or {@code null} to signal 404. Throw {@code DashboardError} for other
 * statuses. May add {@code Set-Cookie} response headers before returning.
 */
@FunctionalInterface
public interface Handler {
    /**
     * Serve one request.
     *
     * @param req the exchange, path parameters, query, body and auth context
     * @return the response body, serialised with status 200, or {@code null} for a 404
     * @throws IOException if reading the body or writing a response fails
     */
    @Nullable
    Object handle(Req req) throws IOException;
}
