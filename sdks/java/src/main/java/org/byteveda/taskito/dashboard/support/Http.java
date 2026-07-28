package org.byteveda.taskito.dashboard.support;

import com.sun.net.httpserver.HttpExchange;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/** Low-level {@code HttpExchange} helpers shared across dashboard handlers. */
public final class Http {
    public static final int MAX_BODY_BYTES = 1024 * 1024;

    private Http() {}

    public static Map<String, String> query(HttpExchange exchange) {
        Map<String, String> out = new HashMap<>();
        String raw = exchange.getRequestURI().getRawQuery();
        if (raw == null) {
            return out;
        }
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            if (eq < 0) {
                out.putIfAbsent(URLDecoder.decode(pair, StandardCharsets.UTF_8), "");
                continue;
            }
            String key = URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8);
            String value = URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8);
            out.putIfAbsent(key, value);
        }
        return out;
    }

    /** Read the request body, capped at {@code maxBytes}. */
    public static byte[] readBody(HttpExchange exchange, int maxBytes) throws IOException {
        try (InputStream in = exchange.getRequestBody();
                ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[8192];
            int total = 0;
            int read;
            while ((read = in.read(buffer)) != -1) {
                total += read;
                if (total > maxBytes) {
                    throw DashboardError.of(413, "payload too large");
                }
                out.write(buffer, 0, read);
            }
            return out.toByteArray();
        }
    }

    public static void respondJson(HttpExchange exchange, int status, Object body) throws IOException {
        byte[] out = Json.toBytes(body);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, out.length);
        try (OutputStream stream = exchange.getResponseBody()) {
            stream.write(out);
        }
    }

    public static void respondError(HttpExchange exchange, int status, String code) throws IOException {
        respondJson(exchange, status, errorBody(code));
    }

    public static Map<String, Object> errorBody(@Nullable String code) {
        return Collections.singletonMap("error", code);
    }

    /** Parse a numeric query param; {@code fallback} when absent, 400 when malformed. */
    public static long longParam(Map<String, String> query, String key, long fallback) {
        String value = query.get(key);
        if (value == null) {
            return fallback;
        }
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw DashboardError.badRequest(key + " must be a number");
        }
    }

    /** Parse an integer query param; {@code fallback} when absent, 400 when malformed/overflowing. */
    public static int intParam(Map<String, String> query, String key, int fallback) {
        String value = query.get(key);
        if (value == null) {
            return fallback;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw DashboardError.badRequest(key + " must be a number");
        }
    }
}
