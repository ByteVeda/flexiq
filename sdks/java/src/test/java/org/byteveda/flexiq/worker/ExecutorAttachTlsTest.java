package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.byteveda.flexiq.task.Task;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * {@link Executor} dialling a {@code tls://} scheduler.
 *
 * <p>The {@link FakeScheduler} terminates TLS with the JDK's own stack, so what
 * passes here is the executor's TLS against an independent implementation
 * rather than against itself.
 */
class ExecutorAttachTlsTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    private @Nullable FakeScheduler scheduler;
    private @Nullable Executor executor;

    @AfterEach
    void tearDown() throws Exception {
        if (executor != null) {
            executor.close();
            executor = null;
        }
        if (scheduler != null) {
            scheduler.close();
            scheduler = null;
        }
    }

    private static Executor.Builder greeter(int port) {
        Task<String> greet = Task.of("greet", String.class);
        return Executor.builder()
                .register(Handler.of(greet, (String who) -> "hello " + who))
                // `localhost`, so the name verified is the certificate's.
                .attach("tls://localhost:" + port)
                .connectTimeoutMs(5_000)
                .heartbeatIntervalMs(50)
                .shutdownDrainMs(5_000);
    }

    @Test
    @Timeout(60)
    void runsAJobOverTls() throws Exception {
        FakeScheduler fake = FakeScheduler.tls(TlsFixtures.server("ca.pem"), false);
        scheduler = fake;

        executor =
                greeter(fake.port()).tls(TlsFixtures.file("ca.pem"), null, null).start();
        fake.awaitHello();
        fake.sendJob("job-1", "greet", JSON.writeValueAsBytes("ada"));

        JsonNode result = fake.nextResult();
        assertEquals("success", result.path("type").asText());
    }

    @Test
    @Timeout(60)
    void presentsItsCertificateToAnMtlsScheduler() throws Exception {
        FakeScheduler fake = FakeScheduler.tls(TlsFixtures.server("ca.pem"), true);
        scheduler = fake;

        executor = greeter(fake.port())
                .tls(TlsFixtures.file("ca.pem"), TlsFixtures.file("client.pem"), TlsFixtures.file("client-key.pem"))
                .start();
        assertEquals("java", fake.awaitHello().path("sdk").asText());
    }

    @Test
    @Timeout(60)
    void anMtlsSchedulerRefusesAnExecutorWithoutACertificate() throws Exception {
        FakeScheduler fake = FakeScheduler.tls(TlsFixtures.server("ca.pem"), true);
        scheduler = fake;

        assertThrows(RuntimeException.class, () -> greeter(fake.port())
                .tls(TlsFixtures.file("ca.pem"), null, null)
                .start());
    }

    @Test
    @Timeout(60)
    void tlsOptionsBesideAPlaintextAddressAreRefused() {
        RuntimeException error = assertThrows(RuntimeException.class, () -> greeter(1)
                .attach("127.0.0.1:1")
                .tls(TlsFixtures.file("ca.pem"), null, null)
                .start());
        assertTrue(error.getMessage().contains("tls://"), error.getMessage());
    }
}
