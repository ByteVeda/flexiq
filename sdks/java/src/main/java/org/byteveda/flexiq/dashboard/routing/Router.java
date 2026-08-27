package org.byteveda.flexiq.dashboard.routing;

import com.sun.net.httpserver.HttpExchange;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.byteveda.flexiq.dashboard.auth.Policy;
import org.byteveda.flexiq.dashboard.auth.RequestContext;
import org.byteveda.flexiq.dashboard.support.Http;

/**
 * An ordered method+path route table. The first route whose method and
 * anchored pattern match wins, so register specific paths (e.g.
 * {@code /jobs/<id>/logs}) before catch-alls ({@code /jobs/<id>}). Capture
 * groups become decoded path parameters.
 */
public final class Router {
    private record Route(String method, Pattern pattern, Handler handler) {}

    private final List<Route> routes = new ArrayList<>();

    /** An empty table; add routes in the order they should be matched. */
    public Router() {}

    /**
     * Add a GET route.
     *
     * @param pattern an anchored regex; each capture group becomes a path parameter
     * @param handler what serves the match
     * @return {@code this}, for chaining
     */
    public Router get(String pattern, Handler handler) {
        return add("GET", pattern, handler);
    }

    /**
     * Add a POST route.
     *
     * @param pattern an anchored regex; each capture group becomes a path parameter
     * @param handler what serves the match
     * @return {@code this}, for chaining
     */
    public Router post(String pattern, Handler handler) {
        return add("POST", pattern, handler);
    }

    /**
     * Add a PUT route.
     *
     * @param pattern an anchored regex; each capture group becomes a path parameter
     * @param handler what serves the match
     * @return {@code this}, for chaining
     */
    public Router put(String pattern, Handler handler) {
        return add("PUT", pattern, handler);
    }

    /**
     * Add a DELETE route.
     *
     * @param pattern an anchored regex; each capture group becomes a path parameter
     * @param handler what serves the match
     * @return {@code this}, for chaining
     */
    public Router delete(String pattern, Handler handler) {
        return add("DELETE", pattern, handler);
    }

    private Router add(String method, String pattern, Handler handler) {
        routes.add(new Route(method, Pattern.compile(pattern), handler));
        return this;
    }

    /**
     * Dispatch a request. Returns {@code false} (no route matched) so the caller
     * can 404; otherwise writes the response and returns {@code true}.
     *
     * @param exchange the request, which the matched handler's response is written to
     * @param method the HTTP method
     * @param path the request path, without its query string
     * @param query the parsed query string
     * @param ctx the resolved auth context
     * @return whether a route matched and answered
     * @throws IOException if reading the body or writing the response fails
     */
    public boolean dispatch(
            HttpExchange exchange, String method, String path, Map<String, String> query, RequestContext ctx)
            throws IOException {
        for (Route route : routes) {
            if (!route.method().equals(method)) {
                continue;
            }
            Matcher matcher = route.pattern().matcher(path);
            if (!matcher.matches()) {
                continue;
            }
            List<String> params = new ArrayList<>(matcher.groupCount());
            for (int i = 1; i <= matcher.groupCount(); i++) {
                String group = matcher.group(i);
                params.add(group == null ? null : URLDecoder.decode(group, StandardCharsets.UTF_8));
            }
            byte[] body = Policy.isStateChanging(method) ? Http.readBody(exchange, Http.MAX_BODY_BYTES) : null;
            Object result = route.handler().handle(new Req(exchange, method, path, params, query, body, ctx));
            if (result == null) {
                Http.respondError(exchange, 404, "not found");
            } else {
                Http.respondJson(exchange, 200, result);
            }
            return true;
        }
        return false;
    }
}
