package org.byteveda.flexiq.dashboard.routing;

import com.sun.net.httpserver.HttpExchange;
import java.util.List;
import java.util.Map;
import org.byteveda.flexiq.dashboard.auth.RequestContext;
import org.byteveda.flexiq.dashboard.support.Json;
import org.jspecify.annotations.Nullable;

/**
 * Everything a route handler needs: the exchange, matched path parameters
 * (decoded), the parsed query, the request body (null for non-body methods),
 * and the resolved auth context.
 *
 * @param exchange the underlying HTTP exchange
 * @param method the request method, uppercased
 * @param path the request path, without its query string
 * @param params the route's matched path parameters, decoded, in declaration order
 * @param query the parsed query string
 * @param body the request body, or {@code null} for a method that carries none
 * @param ctx the resolved auth context for this request
 */
public record Req(
        HttpExchange exchange,
        String method,
        String path,
        List<String> params,
        Map<String, String> query,
        byte @Nullable [] body,
        RequestContext ctx) {

    /**
     * One matched path parameter.
     *
     * @param index its position among the route pattern's capture groups, from 0
     * @return the decoded value
     */
    public String param(int index) {
        return params.get(index);
    }

    /**
     * Parse the body as a JSON object; empty map if absent/invalid.
     *
     * @return the parsed body, or an empty map — handlers validate the fields they
     *     need rather than the body's shape
     */
    public Map<String, Object> jsonBody() {
        Map<String, Object> parsed = Json.readObject(body);
        return parsed != null ? parsed : Map.of();
    }
}
