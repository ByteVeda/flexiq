package org.byteveda.flexiq.dashboard.oauth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.byteveda.flexiq.dashboard.auth.oauth.OAuthFlow;
import org.byteveda.flexiq.dashboard.auth.oauth.config.OAuthConfig;
import org.byteveda.flexiq.dashboard.auth.oauth.config.OAuthConfig.OidcConfig;
import org.byteveda.flexiq.dashboard.auth.oauth.error.IdentityFetchError;
import org.byteveda.flexiq.dashboard.auth.oauth.model.ProviderIdentity;
import org.byteveda.flexiq.dashboard.auth.oauth.provider.IdTokenValidator;
import org.byteveda.flexiq.dashboard.auth.oauth.provider.OAuthProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * End-to-end OIDC exchange against an in-process fake IdP: a real signed
 * id_token is served, validated through nimbus, and normalised to a
 * {@link ProviderIdentity}. No network is touched.
 */
@Timeout(30)
class OidcRoundTripTest {
    private static final String CLIENT_ID = "corp-client";
    private static final String NONCE = "nonce-abc";

    private HttpServer server;
    private ExecutorService executor;
    private RSAKey signingKey;
    private String issuer;
    private String jwksUri;
    private String discoveryUrl;
    private long now;

    @BeforeEach
    void startFakeIdp() throws Exception {
        now = System.currentTimeMillis() / 1000;
        signingKey = new RSAKeyGenerator(2048)
                .keyID("test-key")
                .keyUse(KeyUse.SIGNATURE)
                .algorithm(JWSAlgorithm.RS256)
                .generate();

        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        int port = server.getAddress().getPort();
        String base = "http://127.0.0.1:" + port;
        issuer = base;
        jwksUri = base + "/jwks";
        discoveryUrl = base + "/.well-known/openid-configuration";

        String discovery = "{\"issuer\":\"" + issuer + "\",\"authorization_endpoint\":\"" + base
                + "/authorize\",\"token_endpoint\":\"" + base + "/token\",\"jwks_uri\":\"" + jwksUri + "\"}";
        String jwks = new JWKSet(signingKey.toPublicJWK()).toString();
        String idToken = signToken(issuer, CLIENT_ID, NONCE, now + 3600);
        String tokenResponse = "{\"access_token\":\"x\",\"token_type\":\"Bearer\",\"id_token\":\"" + idToken + "\"}";

        respondWith("/.well-known/openid-configuration", discovery);
        respondWith("/jwks", jwks);
        respondWith("/token", tokenResponse);
        executor = Executors.newCachedThreadPool();
        server.setExecutor(executor);
        server.start();
    }

    @AfterEach
    void stopFakeIdp() {
        server.stop(0);
        executor.shutdownNow();
    }

    @Test
    void exchangeCodeReturnsNormalisedIdentity() {
        OidcConfig config = new OidcConfig("corp", CLIENT_ID, "secret", discoveryUrl, List.of(), "Corp");
        OAuthConfig oauthConfig = new OAuthConfig("http://localhost", null, null, List.of(config), true, List.of());
        Map<String, OAuthProvider> providers = OAuthFlow.buildProviders(oauthConfig, HttpClient.newHttpClient());

        ProviderIdentity identity =
                providers.get("corp").exchangeCode("auth-code", "verifier", "http://localhost/cb", NONCE);

        assertEquals("corp", identity.slot());
        assertEquals("user-123", identity.subject());
        assertEquals("a@example.com", identity.email());
        assertTrue(identity.emailVerified());
        assertEquals("Ada", identity.name());
    }

    @Test
    void validatorAcceptsAWellFormedToken() {
        String token = signToken(issuer, CLIENT_ID, NONCE, now + 3600);
        Map<String, Object> claims = new IdTokenValidator().validate(token, jwksUri, issuer, CLIENT_ID, NONCE, now);
        assertEquals("user-123", claims.get("sub"));
    }

    @Test
    void validatorRejectsWrongNonce() {
        String token = signToken(issuer, CLIENT_ID, NONCE, now + 3600);
        IdTokenValidator validator = new IdTokenValidator();
        assertThrows(
                IdentityFetchError.class, () -> validator.validate(token, jwksUri, issuer, CLIENT_ID, "other", now));
    }

    @Test
    void validatorRejectsExpiredToken() {
        String expired = signToken(issuer, CLIENT_ID, NONCE, now - 3600);
        IdTokenValidator validator = new IdTokenValidator();
        assertThrows(
                IdentityFetchError.class, () -> validator.validate(expired, jwksUri, issuer, CLIENT_ID, NONCE, now));
    }

    @Test
    void validatorRejectsTamperedSignature() {
        String token = signToken(issuer, CLIENT_ID, NONCE, now + 3600);
        String tampered = tamperSignature(token);
        IdTokenValidator validator = new IdTokenValidator();
        assertThrows(
                IdentityFetchError.class, () -> validator.validate(tampered, jwksUri, issuer, CLIENT_ID, NONCE, now));
    }

    private String signToken(String iss, String audience, String nonce, long expSeconds) {
        try {
            JWTClaimsSet claims = new JWTClaimsSet.Builder()
                    .issuer(iss)
                    .subject("user-123")
                    .audience(audience)
                    .claim("email", "a@example.com")
                    .claim("email_verified", true)
                    .claim("name", "Ada")
                    .claim("nonce", nonce)
                    .issueTime(new Date(now * 1000))
                    .expirationTime(new Date(expSeconds * 1000))
                    .build();
            SignedJWT jwt = new SignedJWT(
                    new JWSHeader.Builder(JWSAlgorithm.RS256).keyID("test-key").build(), claims);
            jwt.sign(new RSASSASigner(signingKey));
            return jwt.serialize();
        } catch (JOSEException e) {
            throw new IllegalStateException("failed to sign test token", e);
        }
    }

    private static String tamperSignature(String token) {
        // Decode the signature, flip a byte, and re-encode so the bytes always
        // differ. Flipping the last base64 char instead would sometimes hit its
        // non-significant low bits and decode to the SAME signature (a flake).
        int dot = token.lastIndexOf('.');
        byte[] signature = Base64.getUrlDecoder().decode(token.substring(dot + 1));
        signature[0] ^= 0x01;
        return token.substring(0, dot + 1)
                + Base64.getUrlEncoder().withoutPadding().encodeToString(signature);
    }

    private void respondWith(String path, String body) {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        server.createContext(path, (HttpExchange exchange) -> {
            try (OutputStream out = exchange.getResponseBody()) {
                exchange.getResponseHeaders().set("Content-Type", "application/json");
                exchange.sendResponseHeaders(200, bytes.length);
                out.write(bytes);
            } catch (IOException ignored) {
                // Client hung up; nothing to do in a test IdP.
            } finally {
                exchange.close();
            }
        });
    }
}
