package org.byteveda.flexiq.resources;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.byteveda.flexiq.errors.ProxyException;
import org.byteveda.flexiq.proxies.FileProxyHandler;
import org.byteveda.flexiq.proxies.Proxies;
import org.byteveda.flexiq.proxies.ProxyHandler;
import org.byteveda.flexiq.proxies.ProxyRef;
import org.byteveda.flexiq.proxies.ProxySession;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ProxyTest {

    private static final byte[] KEY = "proxy-secret-key".getBytes();
    private final ObjectMapper json = new ObjectMapper();

    @Test
    void roundTripsFileAcrossTheWire(@TempDir Path dir) throws Exception {
        Proxies proxies = new Proxies(KEY).register(new FileProxyHandler());
        File file = dir.resolve("data.txt").toFile();

        ProxyRef ref = proxies.deconstruct(file);
        ProxyRef onWire = json.readValue(json.writeValueAsBytes(ref), ProxyRef.class); // simulate serialization
        File reconstructed = proxies.resolve(onWire);

        assertEquals(file.getAbsolutePath(), reconstructed.getAbsolutePath());
    }

    @Test
    void rejectsTamperedRef(@TempDir Path dir) {
        Proxies proxies = new Proxies(KEY).register(new FileProxyHandler());
        ProxyRef ref = proxies.deconstruct(dir.resolve("a").toFile());
        ProxyRef tampered = new ProxyRef(ref.handler(), Map.of("path", "/etc/passwd"), ref.signature());

        assertThrows(ProxyException.class, () -> proxies.reconstruct(tampered));
    }

    @Test
    void enforcesAllowlist(@TempDir Path dir) {
        Proxies proxies = new Proxies(KEY).register(new FileProxyHandler(List.of(dir)));

        File inside = dir.resolve("ok.txt").toFile();
        File back = proxies.resolve(proxies.deconstruct(inside));
        assertEquals(inside.getAbsolutePath(), back.getAbsolutePath());

        File outside = dir.getParent().resolve("outside.txt").toFile();
        ProxyRef ref = proxies.deconstruct(outside);
        assertThrows(ProxyException.class, () -> proxies.reconstruct(ref));
    }

    @Test
    void allowlistRejectsSymlinkedAncestorEscape(@TempDir Path dir) throws Exception {
        Path allowed = Files.createDirectory(dir.resolve("allowed"));
        Path secret = Files.createDirectory(dir.resolve("secret"));
        Files.writeString(secret.resolve("data.txt"), "top secret");
        // A symlink inside the allowed root pointing at the secret dir: lexically
        // under the allowlist, but its real target is not.
        Path link = allowed.resolve("link");
        Files.createSymbolicLink(link, secret);

        Proxies proxies = new Proxies(KEY).register(new FileProxyHandler(List.of(allowed)));
        ProxyRef ref = proxies.deconstruct(link.resolve("data.txt").toFile());
        assertThrows(ProxyException.class, () -> proxies.reconstruct(ref));
    }

    @Test
    void rejectsValueWithNoHandler() {
        Proxies proxies = new Proxies(KEY);
        assertThrows(ProxyException.class, () -> proxies.deconstruct("not proxyable"));
    }

    @Test
    void rejectsUnknownHandlerOnReconstruct() {
        Proxies proxies = new Proxies(KEY).register(new FileProxyHandler());
        assertThrows(ProxyException.class, () -> proxies.reconstruct(new ProxyRef("nope", Map.of(), "sig")));
    }

    @Test
    void rejectsNullValueOnDeconstruct() {
        Proxies proxies = new Proxies(KEY).register(new FileProxyHandler());
        assertThrows(ProxyException.class, () -> proxies.deconstruct(null));
    }

    @Test
    void rejectsDuplicateHandlerId() {
        Proxies proxies = new Proxies(KEY).register(new FileProxyHandler());
        assertThrows(ProxyException.class, () -> proxies.register(new FileProxyHandler()));
    }

    @Test
    void roundTripsWithinTtl(@TempDir Path dir) {
        Proxies proxies = new Proxies(KEY).register(new FileProxyHandler());
        File file = dir.resolve("data.txt").toFile();

        ProxyRef ref = proxies.deconstruct(file, Duration.ofHours(1));
        File back = proxies.resolve(ref);
        assertEquals(file.getAbsolutePath(), back.getAbsolutePath());
    }

    @Test
    void rejectsExpiredRef(@TempDir Path dir) throws Exception {
        Proxies proxies = new Proxies(KEY).register(new FileProxyHandler());
        ProxyRef ref = proxies.deconstruct(dir.resolve("a").toFile(), Duration.ofMillis(10));

        Thread.sleep(40);
        assertThrows(ProxyException.class, () -> proxies.reconstruct(ref));
    }

    @Test
    void rejectsTamperedExpiry(@TempDir Path dir) {
        Proxies proxies = new Proxies(KEY).register(new FileProxyHandler());
        ProxyRef ref = proxies.deconstruct(dir.resolve("a").toFile(), Duration.ofMillis(10));
        // Extend the expiry while keeping the original signature → signature must fail.
        ProxyRef extended =
                new ProxyRef(ref.handler(), ref.reference(), ref.signature(), ref.expiresAtMs() + 3_600_000L, null);

        assertThrows(ProxyException.class, () -> proxies.reconstruct(extended));
    }

    @Test
    void enforcesPurposeWhenRequested(@TempDir Path dir) {
        Proxies proxies = new Proxies(KEY).register(new FileProxyHandler());
        File file = dir.resolve("a").toFile();
        ProxyRef ref = proxies.deconstruct(file, null, "emails");

        assertEquals(file.getAbsolutePath(), ((File) proxies.reconstruct(ref, "emails")).getAbsolutePath());
        assertEquals(file.getAbsolutePath(), ((File) proxies.reconstruct(ref)).getAbsolutePath()); // unchecked
        assertThrows(ProxyException.class, () -> proxies.reconstruct(ref, "billing"));
    }

    /** Counts handler invocations and records cleanups, for session tests. */
    private static final class CountingFileHandler implements ProxyHandler<File> {
        private final FileProxyHandler delegate = new FileProxyHandler();
        final AtomicInteger deconstructs = new AtomicInteger();
        final AtomicInteger reconstructs = new AtomicInteger();
        final List<File> cleaned = new ArrayList<>();

        @Override
        public String id() {
            return delegate.id();
        }

        @Override
        public boolean handles(Object value) {
            return delegate.handles(value);
        }

        @Override
        public Map<String, Object> deconstruct(File value) {
            deconstructs.incrementAndGet();
            return delegate.deconstruct(value);
        }

        @Override
        public File reconstruct(Map<String, Object> reference) {
            reconstructs.incrementAndGet();
            return delegate.reconstruct(reference);
        }

        @Override
        public void cleanup(File value) {
            cleaned.add(value);
        }
    }

    @Test
    void sessionDedupsSameInstanceOnDeconstruct(@TempDir Path dir) {
        CountingFileHandler handler = new CountingFileHandler();
        Proxies proxies = new Proxies(KEY).register(handler);
        File file = dir.resolve("dedup.txt").toFile();
        try (ProxySession session = proxies.session()) {
            ProxyRef first = session.deconstruct(file);
            ProxyRef second = session.deconstruct(file);
            assertSame(first, second);
        }
        assertEquals(1, handler.deconstructs.get());
    }

    @Test
    void sessionKeepsPurposesDistinct(@TempDir Path dir) {
        Proxies proxies = new Proxies(KEY).register(new FileProxyHandler());
        File file = dir.resolve("purpose.txt").toFile();
        try (ProxySession session = proxies.session()) {
            ProxyRef emails = session.deconstruct(file, null, "emails");
            ProxyRef billing = session.deconstruct(file, null, "billing");
            assertNotSame(emails, billing);
            assertEquals(file.getAbsolutePath(), ((File) proxies.reconstruct(emails, "emails")).getAbsolutePath());
            assertEquals(file.getAbsolutePath(), ((File) proxies.reconstruct(billing, "billing")).getAbsolutePath());
        }
    }

    @Test
    void sessionMemoizesReconstructBySignature(@TempDir Path dir) {
        CountingFileHandler handler = new CountingFileHandler();
        Proxies proxies = new Proxies(KEY).register(handler);
        ProxyRef ref = proxies.deconstruct(dir.resolve("memo.txt").toFile());
        try (ProxySession session = proxies.session()) {
            Object first = session.reconstruct(ref);
            Object second = session.reconstruct(ref);
            assertSame(first, second);
        }
        assertEquals(1, handler.reconstructs.get());
    }

    @Test
    void sessionCleanupRunsOnceLifo(@TempDir Path dir) {
        CountingFileHandler handler = new CountingFileHandler();
        Proxies proxies = new Proxies(KEY).register(handler);
        ProxyRef refA = proxies.deconstruct(dir.resolve("a.txt").toFile());
        ProxyRef refB = proxies.deconstruct(dir.resolve("b.txt").toFile());
        ProxySession session = proxies.session();
        File a = session.resolve(refA);
        File b = session.resolve(refB);
        session.resolve(refA); // memo hit — must not add a second cleanup
        session.close();
        session.close(); // idempotent
        assertEquals(List.of(b, a), handler.cleaned, "cleanup runs once per instance, LIFO");
    }

    @Test
    void sessionsAreIsolated(@TempDir Path dir) {
        CountingFileHandler handler = new CountingFileHandler();
        Proxies proxies = new Proxies(KEY).register(handler);
        ProxyRef ref = proxies.deconstruct(dir.resolve("iso.txt").toFile());
        try (ProxySession one = proxies.session();
                ProxySession two = proxies.session()) {
            File fromOne = one.resolve(ref);
            File fromTwo = two.resolve(ref);
            assertNotSame(fromOne, fromTwo);
            one.close();
            assertEquals(1, handler.cleaned.size(), "closing one session leaves the other live");
            assertEquals(fromOne, handler.cleaned.get(0));
        }
    }

    @Test
    void sessionReverifiesOnMemoHit(@TempDir Path dir) throws Exception {
        Proxies proxies = new Proxies(KEY).register(new FileProxyHandler());
        ProxyRef ref = proxies.deconstruct(dir.resolve("ttl.txt").toFile(), Duration.ofMillis(30));
        try (ProxySession session = proxies.session()) {
            session.resolve(ref);
            Thread.sleep(80);
            assertThrows(ProxyException.class, () -> session.resolve(ref), "expired ref must stop resolving");
        }
    }

    @Test
    void sessionDrainsRemainingCleanupsWhenOneThrowsError(@TempDir Path dir) {
        List<File> cleaned = new ArrayList<>();
        Proxies proxies = new Proxies(KEY).register(new ProxyHandler<File>() {
            private final FileProxyHandler delegate = new FileProxyHandler();

            @Override
            public String id() {
                return delegate.id();
            }

            @Override
            public boolean handles(Object value) {
                return delegate.handles(value);
            }

            @Override
            public Map<String, Object> deconstruct(File value) {
                return delegate.deconstruct(value);
            }

            @Override
            public File reconstruct(Map<String, Object> reference) {
                return delegate.reconstruct(reference);
            }

            @Override
            public void cleanup(File value) {
                cleaned.add(value);
                if (value.getName().equals("boom.txt")) {
                    throw new AssertionError("cleanup error");
                }
            }
        });
        ProxyRef refA = proxies.deconstruct(dir.resolve("a.txt").toFile());
        ProxyRef refBoom = proxies.deconstruct(dir.resolve("boom.txt").toFile());
        ProxySession session = proxies.session();
        session.resolve(refA);
        session.resolve(refBoom); // cleaned last-in-first-out → throws first
        assertThrows(AssertionError.class, session::close);
        assertEquals(2, cleaned.size(), "the error must not abandon the remaining cleanups");
    }

    @Test
    void sessionRejectsUseAfterClose(@TempDir Path dir) {
        Proxies proxies = new Proxies(KEY).register(new FileProxyHandler());
        File file = dir.resolve("closed.txt").toFile();
        ProxyRef ref = proxies.deconstruct(file);
        ProxySession session = proxies.session();
        session.close();
        assertThrows(ProxyException.class, () -> session.deconstruct(file));
        assertThrows(ProxyException.class, () -> session.reconstruct(ref));
    }
}
