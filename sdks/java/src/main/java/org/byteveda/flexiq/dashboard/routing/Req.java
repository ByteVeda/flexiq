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
 */
public record Req(
        HttpExchange exchange,
        String method,
        String path,
        List<String> params,
        Map<String, String> query,
        byte @Nullable [] body,
        RequestContext ctx) {

    public String param(int index) {
        return params.get(index);
    }

    /** Parse the body as a JSON object; empty map if absent/invalid. */
    public Map<String, Object> jsonBody() {
        Map<String, Object> parsed = Json.readObject(body);
        return parsed != null ? parsed : Map.of();
    }
}
