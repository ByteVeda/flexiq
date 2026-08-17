package org.byteveda.flexiq.webhooks;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.byteveda.flexiq.errors.WebhookException;
import org.byteveda.flexiq.logging.FlexiQLogger;

/**
 * Delivers webhook payloads over HTTP with an optional HMAC signature and
 * retries, recording the outcome of each attempt in the {@link DeliveryStore}.
 */
final class Deliverer {
    private static final FlexiQLogger LOG = FlexiQLogger.create("webhooks");
    private static final String ALGORITHM = "HmacSHA256";
    private static final char[] HEX = "0123456789abcdef".toCharArray();
    private static final long MAX_BACKOFF_MS = 30_000;

    private final HttpClient client = HttpClient.newHttpClient();

    /**
     * Send {@code body} to the hook without blocking, retrying server errors
     * with backoff. Records the terminal outcome (delivered on 2xx, else failed)
     * once retries are exhausted.
     */
    void deliver(Webhook hook, byte[] body, DeliveryContext ctx, DeliveryStore store) {
        sendWithRetry(hook, buildRequest(hook, body), 0, ctx, store, System.currentTimeMillis());
    }

    /**
     * Send {@code body} synchronously with a single attempt and record the
     * result. Returns the HTTP status code, or {@code -1} on a transport error.
     * Used by dashboard test-ping and replay actions, which must report inline
     * without blocking a request thread on backoff.
     */
    int deliverSync(Webhook hook, byte[] body, DeliveryContext ctx, DeliveryStore store) {
        long start = System.currentTimeMillis();
        try {
            HttpResponse<String> response = client.send(buildRequest(hook, body), HttpResponse.BodyHandlers.ofString());
            int code = response.statusCode();
            record(store, hook, ctx, 1, code, response.body(), System.currentTimeMillis() - start);
            return code;
        } catch (IOException e) {
            recordError(store, hook, ctx, 1, System.currentTimeMillis() - start, e);
            return -1;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            recordError(store, hook, ctx, 1, System.currentTimeMillis() - start, e);
            return -1;
        }
    }

    private void sendWithRetry(
            Webhook hook, HttpRequest request, int attempt, DeliveryContext ctx, DeliveryStore store, long start) {
        client.sendAsync(request, HttpResponse.BodyHandlers.ofString()).whenComplete((response, error) -> {
            boolean retryable = error != null || response.statusCode() >= 500;
            if (retryable && attempt < hook.maxRetries) {
                CompletableFuture.delayedExecutor(backoffMs(hook, attempt), TimeUnit.MILLISECONDS)
                        .execute(() -> sendWithRetry(hook, request, attempt + 1, ctx, store, start));
                return;
            }
            int attempts = attempt + 1;
            long latency = System.currentTimeMillis() - start;
            if (error != null) {
                recordError(store, hook, ctx, attempts, latency, error);
                LOG.warn("webhook delivery to " + redact(request.uri()) + " failed after " + attempts + " attempt(s): "
                        + error.getClass().getSimpleName());
            } else {
                int code = response.statusCode();
                record(store, hook, ctx, attempts, code, response.body(), latency);
                if (code >= 500) {
                    LOG.warn("webhook delivery to " + redact(request.uri()) + " failed after " + attempts
                            + " attempt(s): HTTP " + code);
                }
            }
        });
    }

    /**
     * The cross-SDK retry curve: the Nth wait (counted from zero) is
     * {@code retryBackoff ^ N} seconds, capped at 30s. A large exponent overflows
     * to infinity rather than wrapping, so the cap still holds.
     */
    static long backoffMs(Webhook hook, int attempt) {
        double seconds = Math.pow(hook.retryBackoff, attempt);
        return (long) Math.min(MAX_BACKOFF_MS, seconds * 1000);
    }

    private static HttpRequest buildRequest(Webhook hook, byte[] body) {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(hook.url))
                .timeout(Duration.ofMillis(hook.timeoutMs))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofByteArray(body));
        if (hook.secret != null) {
            request.header("X-Flexiq-Signature", "sha256=" + sign(hook.secret, body));
        }
        hook.headers.forEach(request::header);
        return request.build();
    }

    private static void record(
            DeliveryStore store, Webhook hook, DeliveryContext ctx, int attempts, int code, String body, long latency) {
        boolean ok = code >= 200 && code < 300;
        String status = ok ? Delivery.DELIVERED : Delivery.FAILED;
        String error = ok ? null : "HTTP " + code;
        persist(store, Delivery.of(hook.id, ctx, status, attempts, code, body, latency, error));
    }

    private static void recordError(
            DeliveryStore store, Webhook hook, DeliveryContext ctx, int attempts, long latency, Throwable error) {
        // The class name only — transport error messages can echo the URL's tokens.
        persist(
                store,
                Delivery.of(
                        hook.id,
                        ctx,
                        Delivery.FAILED,
                        attempts,
                        null,
                        null,
                        latency,
                        error.getClass().getSimpleName()));
    }

    private static void persist(DeliveryStore store, Delivery delivery) {
        try {
            store.record(delivery);
        } catch (RuntimeException e) {
            // Recording is best-effort — a log write failure must not sink the delivery.
            LOG.warn("failed to record webhook delivery: " + e.getClass().getSimpleName());
        }
    }

    /** Origin only (scheme://host:port) — path segments and queries can carry tokens. */
    private static String redact(URI uri) {
        try {
            return new URI(uri.getScheme(), null, uri.getHost(), uri.getPort(), null, null, null).toString();
        } catch (Exception e) {
            return "<invalid-url>";
        }
    }

    static String sign(String secret, byte[] body) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM));
            return hex(mac.doFinal(body));
        } catch (Exception e) {
            throw new WebhookException("webhook signature failed", e);
        }
    }

    private static String hex(byte[] bytes) {
        char[] out = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            int b = bytes[i] & 0xff;
            out[i * 2] = HEX[b >>> 4];
            out[i * 2 + 1] = HEX[b & 0x0f];
        }
        return new String(out);
    }
}
