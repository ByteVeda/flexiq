package org.byteveda.flexiq.webhooks;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.byteveda.flexiq.errors.WebhookException;
import org.junit.jupiter.api.Test;

/** SSRF guard: private/loopback/link-local/CGNAT/ULA/IPv4-mapped targets are refused. */
class WebhookUrlValidatorTest {

    // Loopback, RFC1918, link-local (incl. cloud metadata 169.254.169.254), IPv6
    // loopback, IPv6 ULA, IPv4-mapped-IPv6 loopback, CGNAT, and blocked names.
    private static final List<String> BLOCKED = List.of(
            "http://127.0.0.1/x",
            "http://10.0.0.1/x",
            "http://172.16.0.1/x",
            "http://192.168.1.1/x",
            "http://169.254.169.254/x",
            "http://[::1]/x",
            "http://[fc00::1]/x",
            "http://[::ffff:127.0.0.1]/x",
            "http://100.64.0.1/x",
            "http://localhost/x",
            "http://foo.local/x");

    @Test
    void rejectsPrivateAndLoopbackTargets() {
        for (String url : BLOCKED) {
            assertThrows(WebhookException.class, () -> WebhookUrlValidator.validate(url, false), url);
        }
    }

    @Test
    void allowsPublicTarget() {
        assertDoesNotThrow(() -> WebhookUrlValidator.validate("https://example.com/hook", false));
    }

    @Test
    void allowPrivateBypassSkipsAddressChecks() {
        for (String url : BLOCKED) {
            assertDoesNotThrow(() -> WebhookUrlValidator.validate(url, true), url);
        }
    }

    @Test
    void rejectsNonHttpSchemeEvenWhenBypassed() {
        assertThrows(WebhookException.class, () -> WebhookUrlValidator.validate("ftp://example.com/x", true));
    }

    @Test
    void rejectsMissingHostEvenWhenBypassed() {
        assertThrows(WebhookException.class, () -> WebhookUrlValidator.validate("http:///x", true));
    }

    @Test
    void truthyEnvParsing() {
        assertTrue(WebhookUrlValidator.isTruthy("1"));
        assertTrue(WebhookUrlValidator.isTruthy("true"));
        assertTrue(WebhookUrlValidator.isTruthy("yes"));
        assertFalse(WebhookUrlValidator.isTruthy(null));
        assertFalse(WebhookUrlValidator.isTruthy(""));
        assertFalse(WebhookUrlValidator.isTruthy("0"));
        assertFalse(WebhookUrlValidator.isTruthy("false"));
        assertFalse(WebhookUrlValidator.isTruthy("off"));
        assertFalse(WebhookUrlValidator.isTruthy("no"));
    }
}
