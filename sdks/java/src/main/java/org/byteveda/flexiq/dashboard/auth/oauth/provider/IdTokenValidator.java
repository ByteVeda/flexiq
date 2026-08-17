package org.byteveda.flexiq.dashboard.auth.oauth.provider;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.byteveda.flexiq.dashboard.auth.oauth.error.IdentityFetchError;

/**
 * OIDC id-token verification: signature, then the OIDC claim checks.
 *
 * <p>This is the only class that touches nimbus-jose-jwt, so when the optional
 * jar is absent the {@link NoClassDefFoundError} surfaces the moment an OIDC
 * provider is constructed (it holds one of these) and the dashboard can degrade
 * to password-only auth. Constructing an instance eagerly references a nimbus
 * type so that failure happens at flow-build time, not mid-login.
 *
 * <p>Security invariants enforced here:
 * <ul>
 *   <li>Signature algorithm is pinned to {@code RS256}/{@code ES256} — an
 *       {@code alg:none} or HMAC token is rejected because the key selector has
 *       no key for those algorithms.
 *   <li>{@code iss}, {@code aud}, {@code exp} (with 60s skew), {@code nonce},
 *       and {@code sub} are all checked; a multi-audience token additionally
 *       requires {@code azp == clientId}.
 * </ul>
 */
public final class IdTokenValidator {
    private static final long CLOCK_SKEW_SECONDS = 60;

    private final Set<JWSAlgorithm> algorithms = Set.of(JWSAlgorithm.RS256, JWSAlgorithm.ES256);
    private final ConcurrentHashMap<String, JWKSource<SecurityContext>> jwkSources = new ConcurrentHashMap<>();

    /**
     * Verify {@code idToken} and return its claims.
     *
     * @param issuer expected {@code iss}; skipped when {@code null}
     * @param expectedNonce expected {@code nonce}; skipped when {@code null}
     * @param nowSeconds current Unix time in seconds (injected for testability)
     * @throws IdentityFetchError if the signature or any claim check fails
     */
    public Map<String, Object> validate(
            String idToken, String jwksUri, String issuer, String clientId, String expectedNonce, long nowSeconds) {
        JWTClaimsSet claims = verifySignature(idToken, jwksUri);
        checkIssuer(claims, issuer);
        checkAudience(claims, clientId);
        checkExpiry(claims, nowSeconds);
        checkNonce(claims, expectedNonce);
        checkSubject(claims);
        return claims.getClaims();
    }

    private JWTClaimsSet verifySignature(String idToken, String jwksUri) {
        ConfigurableJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();
        processor.setJWSKeySelector(new JWSVerificationKeySelector<>(algorithms, jwkSourceFor(jwksUri)));
        // Do the claim checks ourselves so failures carry a precise message; the
        // processor is left to verify only the signature and algorithm.
        processor.setJWTClaimsSetVerifier((claimsSet, context) -> {});
        try {
            return processor.process(idToken, null);
        } catch (BadJOSEException | JOSEException e) {
            throw new IdentityFetchError("id_token signature validation failed: " + e.getMessage(), e);
        } catch (ParseException e) {
            throw new IdentityFetchError("id_token could not be parsed: " + e.getMessage(), e);
        }
    }

    private JWKSource<SecurityContext> jwkSourceFor(String jwksUri) {
        return jwkSources.computeIfAbsent(jwksUri, IdTokenValidator::buildJwkSource);
    }

    private static JWKSource<SecurityContext> buildJwkSource(String jwksUri) {
        try {
            // URI.create(..).toURL() avoids the URL(String) constructor deprecated since Java 20.
            URL url = URI.create(jwksUri).toURL();
            return JWKSourceBuilder.<SecurityContext>create(url).retrying(true).build();
        } catch (MalformedURLException | IllegalArgumentException e) {
            throw new IdentityFetchError("invalid jwks_uri: " + jwksUri, e);
        }
    }

    private static void checkIssuer(JWTClaimsSet claims, String issuer) {
        if (issuer != null && !issuer.equals(claims.getIssuer())) {
            throw new IdentityFetchError(
                    "id_token issuer mismatch: expected " + issuer + ", got " + claims.getIssuer());
        }
    }

    private static void checkAudience(JWTClaimsSet claims, String clientId) {
        List<String> aud = claims.getAudience();
        if (aud == null || !aud.contains(clientId)) {
            throw new IdentityFetchError("id_token audience does not contain client id");
        }
        if (aud.size() > 1) {
            // Multi-audience tokens must name the authorised party explicitly.
            Object azp = claims.getClaim("azp");
            if (!clientId.equals(azp)) {
                throw new IdentityFetchError("id_token azp does not match client id for multi-audience token");
            }
        }
    }

    private static void checkExpiry(JWTClaimsSet claims, long nowSeconds) {
        Date exp = claims.getExpirationTime();
        if (exp == null) {
            throw new IdentityFetchError("id_token missing exp claim");
        }
        long expSeconds = exp.getTime() / 1000;
        if (expSeconds < nowSeconds - CLOCK_SKEW_SECONDS) {
            throw new IdentityFetchError("id_token expired");
        }
    }

    private static void checkNonce(JWTClaimsSet claims, String expectedNonce) {
        if (expectedNonce == null) {
            return;
        }
        Object nonce = claims.getClaim("nonce");
        if (!expectedNonce.equals(nonce)) {
            throw new IdentityFetchError("id_token nonce mismatch");
        }
    }

    private static void checkSubject(JWTClaimsSet claims) {
        String sub = claims.getSubject();
        if (sub == null || sub.isEmpty()) {
            throw new IdentityFetchError("id_token missing sub claim");
        }
    }
}
