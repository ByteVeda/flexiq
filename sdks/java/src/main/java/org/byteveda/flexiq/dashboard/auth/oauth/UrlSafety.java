package org.byteveda.flexiq.dashboard.auth.oauth;

import java.net.URI;

/**
 * Open-redirect guard for the post-login {@code next} target.
 *
 * <p>Only same-origin relative paths rooted at {@code /} are accepted. Absolute
 * URLs ({@code https://evil.com}), protocol-relative URLs ({@code //evil.com}),
 * and backslash tricks ({@code /\evil}) are rejected so an attacker can't craft
 * a login link that bounces the browser off-site after authentication.
 */
public final class UrlSafety {

    private UrlSafety() {}

    /**
     * Whether {@code path} is safe to use as a same-origin post-login redirect.
     *
     * @param path the {@code next} target a login link asked for
     * @return whether it is a same-origin path rooted at {@code /}
     */
    public static boolean isSafeRedirect(String path) {
        if (path == null || path.isEmpty()) {
            return false;
        }
        if (!path.startsWith("/")) {
            return false;
        }
        // "//host" is protocol-relative; "/\host" is a browser-normalised variant.
        if (path.startsWith("//") || path.startsWith("/\\")) {
            return false;
        }
        URI uri;
        try {
            uri = URI.create(path);
        } catch (IllegalArgumentException e) {
            return false;
        }
        // A bare rooted path has neither a scheme nor an authority/host.
        return uri.getScheme() == null && uri.getAuthority() == null && uri.getHost() == null;
    }
}
