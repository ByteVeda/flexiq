package org.byteveda.taskito.webhooks;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.byteveda.taskito.errors.WebhookException;

/**
 * Outbound URL safety check for dashboard-configured webhooks (SSRF guard).
 *
 * <p>An operator who can write to the settings store could otherwise point a
 * webhook at {@code http://169.254.169.254/...} and turn the worker into an SSRF
 * proxy for cloud metadata, loopback services, or the RFC1918 intranet. We
 * refuse to deliver to loopback, link-local, RFC1918, CGNAT, and ULA addresses
 * by default. Set {@code FLEXIQ_WEBHOOKS_ALLOW_PRIVATE} (truthy) to disable the
 * guard for local development against {@code http://localhost}.
 *
 * <p>Mirrors the cross-SDK {@code validate_webhook_url} contract. Java's
 * {@link InetAddress} lacks the CGNAT / ULA / IPv4-mapped-IPv6 predicates the
 * reference relies on implicitly, so those three ranges are checked by hand.
 */
public final class WebhookUrlValidator {
    private static final String ALLOW_ENV_VAR = "FLEXIQ_WEBHOOKS_ALLOW_PRIVATE";

    // Names that never leave this host regardless of DNS, which a raw IP check
    // (run only after resolution) would otherwise miss for unresolvable names.
    private static final Set<String> BLOCKED_HOSTNAMES =
            Set.of("localhost", "localhost.localdomain", "ip6-localhost", "ip6-loopback");
    private static final List<String> BLOCKED_SUFFIXES =
            List.of(".localhost", ".local", ".internal", ".intranet", ".lan", ".private");

    private WebhookUrlValidator() {}

    /** Validate {@code url}, reading the allow-private bypass from the environment. */
    public static void validate(String url) {
        validate(url, allowPrivateFromEnv());
    }

    /**
     * Validate {@code url}, rejecting private/loopback destinations unless
     * {@code allowPrivate}. The {@code allowPrivate} seam keeps tests independent
     * of process environment.
     *
     * @throws WebhookException on a non-http(s) scheme, a missing host, an
     *     unresolvable host, or a host that resolves to an unsafe address.
     */
    public static void validate(String url, boolean allowPrivate) {
        URI uri;
        try {
            uri = URI.create(url);
        } catch (IllegalArgumentException e) {
            throw new WebhookException("webhook URL is not a valid URI");
        }
        String scheme = uri.getScheme();
        if (scheme == null || !(scheme.equalsIgnoreCase("http") || scheme.equalsIgnoreCase("https"))) {
            throw new WebhookException("webhook URL scheme must be http or https");
        }
        String host = uri.getHost();
        if (host == null || host.isEmpty()) {
            throw new WebhookException("webhook URL must include a host");
        }

        // Scheme/host are validated even when the guard is bypassed, matching the
        // reference: an operator gets the same "bad URL" feedback either way.
        if (allowPrivate) {
            return;
        }

        String bareHost = stripBrackets(host);
        if (isBlockedHostname(bareHost)) {
            throw new WebhookException("webhook URL host resolves to a private network");
        }

        InetAddress[] addresses;
        try {
            addresses = InetAddress.getAllByName(bareHost);
        } catch (UnknownHostException e) {
            throw new WebhookException("could not resolve webhook host");
        }
        for (InetAddress address : addresses) {
            if (isBlockedAddress(address)) {
                throw new WebhookException("webhook URL resolves to a private or loopback address");
            }
        }
    }

    private static boolean allowPrivateFromEnv() {
        return isTruthy(System.getenv(ALLOW_ENV_VAR));
    }

    /** Truthy = non-empty and not one of the common falsy strings. */
    static boolean isTruthy(String value) {
        if (value == null) {
            return false;
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        return !(normalized.isEmpty()
                || normalized.equals("0")
                || normalized.equals("false")
                || normalized.equals("no")
                || normalized.equals("off"));
    }

    /** {@code URI.getHost} keeps the brackets around IPv6 literals; strip them for resolution. */
    private static String stripBrackets(String host) {
        if (host.length() >= 2 && host.charAt(0) == '[' && host.charAt(host.length() - 1) == ']') {
            return host.substring(1, host.length() - 1);
        }
        return host;
    }

    private static boolean isBlockedHostname(String host) {
        String lowered = host.toLowerCase(Locale.ROOT);
        if (BLOCKED_HOSTNAMES.contains(lowered)) {
            return true;
        }
        for (String suffix : BLOCKED_SUFFIXES) {
            if (lowered.endsWith(suffix)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isBlockedAddress(InetAddress address) {
        if (address.isLoopbackAddress()
                || address.isAnyLocalAddress()
                || address.isLinkLocalAddress()
                || address.isSiteLocalAddress()
                || address.isMulticastAddress()) {
            return true;
        }
        byte[] bytes = address.getAddress();
        if (bytes.length == 16) {
            // IPv6 unique-local addresses fc00::/7 (first byte 1111110x).
            if ((bytes[0] & 0xFE) == 0xFC) {
                return true;
            }
            // IPv4-mapped IPv6 (::ffff:a.b.c.d): re-check the embedded v4. Most JVMs
            // already fold these to an Inet4Address, but a raw 16-byte form can slip
            // through, so unwrap defensively.
            if (isV4Mapped(bytes)) {
                byte[] v4 = {bytes[12], bytes[13], bytes[14], bytes[15]};
                try {
                    return isBlockedAddress(InetAddress.getByAddress(v4)) || isCgnat(v4);
                } catch (UnknownHostException e) {
                    return true;
                }
            }
        }
        return bytes.length == 4 && isCgnat(bytes);
    }

    private static boolean isV4Mapped(byte[] bytes) {
        for (int i = 0; i < 10; i++) {
            if (bytes[i] != 0) {
                return false;
            }
        }
        return (bytes[10] & 0xFF) == 0xFF && (bytes[11] & 0xFF) == 0xFF;
    }

    /** Carrier-grade NAT 100.64.0.0/10 (first octet 100, second octet 64-127). */
    private static boolean isCgnat(byte[] v4) {
        return (v4[0] & 0xFF) == 100 && (v4[1] & 0xC0) == 0x40;
    }
}
