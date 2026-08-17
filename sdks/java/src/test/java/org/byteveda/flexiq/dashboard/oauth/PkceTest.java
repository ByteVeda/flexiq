package org.byteveda.flexiq.dashboard.oauth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.byteveda.flexiq.dashboard.auth.oauth.provider.Pkce;
import org.junit.jupiter.api.Test;

class PkceTest {

    @Test
    void challengeMatchesRfc7636Example() {
        // The canonical RFC 7636 appendix B vector.
        String verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
        assertEquals("E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM", Pkce.challenge(verifier));
    }

    @Test
    void challengeIsDeterministicAndUnpadded() {
        String verifier = Pkce.verifier();
        assertEquals(Pkce.challenge(verifier), Pkce.challenge(verifier));
        // base64url of a 32-byte digest is 43 chars with no '=' padding.
        assertEquals(43, Pkce.challenge(verifier).length());
    }

    @Test
    void verifierIsHighEntropyAndUnique() {
        assertEquals(43, Pkce.verifier().length());
        assertNotEquals(Pkce.verifier(), Pkce.verifier());
        assertEquals("S256", Pkce.METHOD);
    }
}
