package org.byteveda.taskito.scaler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.model.QueueStats;
import org.jspecify.annotations.Nullable;

/**
 * Serves queue depth over HTTP for an external autoscaler. Depth is
 * {@code pending + running} (outstanding work); the autoscaler divides it by
 * {@code targetQueueDepth} to pick a replica count. Start with
 * {@link #start(Taskito, ScalerOptions)} and {@link #close()} to stop.
 */
public final class Scaler implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final HttpServer server;

    private Scaler(HttpServer server) {
        this.server = server;
    }

    /** Start the endpoint; binds immediately and serves on a background selector. */
    public static Scaler start(Taskito queue, ScalerOptions options) {
        HttpServer server;
        try {
            server = HttpServer.create(new InetSocketAddress(options.host(), options.port()), 0);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to start the scaler endpoint", e);
        }
        server.createContext("/api/scaler", exchange -> handleScaler(exchange, queue, options));
        server.createContext("/health", Scaler::handleHealth);
        server.start();
        return new Scaler(server);
    }

    /** The bound port (useful when {@code port} was 0). */
    public int port() {
        return server.getAddress().getPort();
    }

    @Override
    public void close() {
        server.stop(0);
    }

    private static void handleScaler(HttpExchange exchange, Taskito queue, ScalerOptions options) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            send(exchange, 405, Map.of("error", "method not allowed"));
            return;
        }
        String queueName = queryParam(exchange, "queue");
        if (queueName == null) {
            queueName = options.queue();
        }
        try {
            QueueStats stats = queueName == null ? queue.stats() : queue.statsByQueue(queueName);
            long depth = stats.pending + stats.running;
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("metricValue", depth);
            body.put("targetValue", options.targetQueueDepth());
            body.put("queueName", queueName == null ? "all" : queueName);
            send(exchange, 200, body);
        } catch (RuntimeException e) {
            // Never leak backend internals to the scaler caller.
            send(exchange, 500, Map.of("error", "failed to read queue stats"));
        }
    }

    private static void handleHealth(HttpExchange exchange) throws IOException {
        send(exchange, 200, Map.of("status", "ok"));
    }

    private static @Nullable String queryParam(HttpExchange exchange, String key) {
        String query = exchange.getRequestURI().getQuery();
        if (query == null) {
            return null;
        }
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0 && pair.substring(0, eq).equals(key)) {
                return pair.substring(eq + 1);
            }
        }
        return null;
    }

    private static void send(HttpExchange exchange, int status, Map<String, Object> body) throws IOException {
        byte[] bytes;
        try {
            bytes = JSON.writeValueAsBytes(body);
        } catch (Exception e) {
            bytes = "{}".getBytes(StandardCharsets.UTF_8);
            status = 500;
        }
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }
}
