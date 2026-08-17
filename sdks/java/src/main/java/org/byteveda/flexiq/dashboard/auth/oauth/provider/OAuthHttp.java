package org.byteveda.flexiq.dashboard.auth.oauth.provider;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.StringJoiner;
import org.byteveda.flexiq.dashboard.auth.oauth.error.IdentityFetchError;

/**
 * The tiny slice of HTTP the providers need: form-urlencoded POST and header-only
 * GET, over an injected {@link HttpClient} (a real one in production, a fake one
 * in tests) so the provider layer never hits the network under test.
 *
 * <p>Every call returns the raw status + body as an {@link HttpResult}; callers
 * decide what a non-2xx means. IO/interrupt failures become
 * {@link IdentityFetchError} so the flow layer surfaces a single error type.
 */
final class OAuthHttp {
    private static final Duration TIMEOUT = Duration.ofSeconds(10);

    private OAuthHttp() {}

    /** A raw HTTP response: status code plus the body as text. */
    record HttpResult(int status, String body) {}

    static HttpResult get(HttpClient http, String url, Map<String, String> headers) {
        HttpRequest.Builder builder =
                HttpRequest.newBuilder(URI.create(url)).timeout(TIMEOUT).GET();
        headers.forEach(builder::header);
        return send(http, builder);
    }

    static HttpResult postForm(HttpClient http, String url, Map<String, String> form, Map<String, String> headers) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
                .timeout(TIMEOUT)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(formEncode(form)));
        headers.forEach(builder::header);
        return send(http, builder);
    }

    static String formEncode(Map<String, String> params) {
        StringJoiner joiner = new StringJoiner("&");
        for (Map.Entry<String, String> entry : params.entrySet()) {
            joiner.add(encode(entry.getKey()) + "=" + encode(entry.getValue()));
        }
        return joiner.toString();
    }

    private static HttpResult send(HttpClient http, HttpRequest.Builder builder) {
        try {
            HttpResponse<String> response = http.send(builder.build(), HttpResponse.BodyHandlers.ofString());
            return new HttpResult(response.statusCode(), response.body());
        } catch (java.io.IOException e) {
            throw new IdentityFetchError("HTTP request failed: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IdentityFetchError("HTTP request interrupted", e);
        }
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }
}
