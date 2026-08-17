package org.byteveda.flexiq.dashboard.oauth;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.byteveda.flexiq.dashboard.auth.oauth.UrlSafety;
import org.junit.jupiter.api.Test;

class UrlSafetyTest {

    @Test
    void acceptsRootedRelativePaths() {
        assertTrue(UrlSafety.isSafeRedirect("/dashboard"));
        assertTrue(UrlSafety.isSafeRedirect("/jobs/123?tab=logs"));
        assertTrue(UrlSafety.isSafeRedirect("/"));
    }

    @Test
    void rejectsOffOriginTargets() {
        assertFalse(UrlSafety.isSafeRedirect("//evil.com"));
        assertFalse(UrlSafety.isSafeRedirect("/\\evil"));
        assertFalse(UrlSafety.isSafeRedirect("https://x"));
        assertFalse(UrlSafety.isSafeRedirect("http://x"));
        assertFalse(UrlSafety.isSafeRedirect("javascript:alert(1)"));
    }

    @Test
    void rejectsEmptyAndNonRooted() {
        assertFalse(UrlSafety.isSafeRedirect(""));
        assertFalse(UrlSafety.isSafeRedirect(null));
        assertFalse(UrlSafety.isSafeRedirect("dashboard"));
    }
}
