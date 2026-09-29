package org.byteveda.flexiq.worker;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * The repository's shared TLS test certificates, as a server-side
 * {@link SSLContext} a {@link FakeScheduler} can terminate TLS with.
 *
 * <p>The keys are PKCS#8 PEM, which the JDK reads without a PEM library once
 * the armour is stripped.
 */
final class TlsFixtures {
    /** Where the fixtures live, relative to this Gradle project. */
    static final Path DIR = Path.of("../../crates/flexiq-core/tests/fixtures/tls")
            .toAbsolutePath()
            .normalize();

    private static final char[] PASSWORD = "flexiq".toCharArray();

    private TlsFixtures() {}

    static Path file(String name) {
        return DIR.resolve(name);
    }

    /**
     * A server context presenting the fixture server certificate.
     *
     * @param trustClientsOf the fixture CA a client certificate must chain to, when
     *     the scheduler demands one
     */
    static SSLContext server(String trustClientsOf) throws IOException, GeneralSecurityException {
        KeyStore keys = KeyStore.getInstance("PKCS12");
        keys.load(null, null);
        keys.setKeyEntry("server", privateKey(file("server-key.pem")), PASSWORD, new Certificate[] {
            certificate(file("server.pem"))
        });
        KeyManagerFactory keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagers.init(keys, PASSWORD);

        KeyStore trusted = KeyStore.getInstance("PKCS12");
        trusted.load(null, null);
        trusted.setCertificateEntry("ca", certificate(file(trustClientsOf)));
        TrustManagerFactory trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagers.init(trusted);

        SSLContext context = SSLContext.getInstance("TLS");
        context.init(keyManagers.getKeyManagers(), trustManagers.getTrustManagers(), null);
        return context;
    }

    private static Certificate certificate(Path pem) throws IOException, GeneralSecurityException {
        try (InputStream in = Files.newInputStream(pem)) {
            return CertificateFactory.getInstance("X.509").generateCertificate(in);
        }
    }

    private static PrivateKey privateKey(Path pem) throws IOException, GeneralSecurityException {
        String body = Files.readString(pem, StandardCharsets.US_ASCII)
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replace("-----END PRIVATE KEY-----", "")
                .replaceAll("\\s", "");
        return KeyFactory.getInstance("EC")
                .generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(body)));
    }
}
