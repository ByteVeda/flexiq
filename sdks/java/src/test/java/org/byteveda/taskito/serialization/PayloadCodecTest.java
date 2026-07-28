package org.byteveda.taskito.serialization;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.byteveda.taskito.Taskito;
import org.byteveda.taskito.errors.CryptoException;
import org.byteveda.taskito.errors.SerializationException;
import org.byteveda.taskito.task.Task;
import org.byteveda.taskito.worker.Worker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

class PayloadCodecTest {

    private static final byte[] KEY32 = "0123456789abcdef0123456789abcdef".getBytes();
    private static final byte[] HKEY = "codec-hmac-secret".getBytes();

    @Test
    void gzipRoundTrips() {
        GzipCodec gzip = new GzipCodec();
        byte[] data = "hello world ".repeat(50).getBytes();
        assertArrayEquals(data, gzip.decode(gzip.encode(data)));
    }

    @Test
    void aesHidesPlaintextAndRoundTrips() {
        AesGcmCodec aes = new AesGcmCodec(KEY32);
        byte[] data = "secret payload".getBytes();
        byte[] encoded = aes.encode(data);
        assertFalse(Arrays.equals(data, encoded));
        assertArrayEquals(data, aes.decode(encoded));
    }

    @Test
    void hmacRejectsTamper() {
        HmacCodec hmac = new HmacCodec(HKEY);
        byte[] encoded = hmac.encode("hi".getBytes());
        encoded[encoded.length - 1] ^= 1; // flip a body bit
        assertThrows(CryptoException.class, () -> hmac.decode(encoded));
    }

    @Test
    void chainIsReversibleInReverseOrder() {
        CodecSerializer serializer = new CodecSerializer(
                new JsonSerializer(), List.of(new GzipCodec(), new AesGcmCodec(KEY32), new HmacCodec(HKEY)));
        byte[] bytes = serializer.serialize(42);
        assertEquals(42, (int) serializer.deserialize(bytes, Integer.class));
    }

    @Test
    @Timeout(30)
    void roundTripsThroughWorker(@TempDir Path dir) throws Exception {
        try (Taskito queue = Taskito.builder()
                .url(dir.resolve("cdc.db").toString())
                .codec(new GzipCodec(), new AesGcmCodec(KEY32), new HmacCodec(HKEY))
                .open()) {
            Task<Integer> dbl = Task.of("cdc.double", Integer.class);
            AtomicInteger seen = new AtomicInteger();
            CountDownLatch ran = new CountDownLatch(1);
            Worker worker = queue.worker()
                    .handle(dbl, p -> {
                        seen.set(p);
                        ran.countDown();
                        return p * 2;
                    })
                    .start();
            try (worker) {
                String id = queue.enqueue(dbl, 21);
                assertTrue(ran.await(20, TimeUnit.SECONDS));
                assertEquals(21, seen.get()); // payload decoded on the worker
                queue.awaitJob(id, java.time.Duration.ofSeconds(20));
                assertEquals(42, queue.getResult(id, Integer.class).orElseThrow()); // result decoded back
            }
        }
    }

    @Test
    void gzipDecodeRejectsPayloadExceedingCap() {
        GzipCodec codec = new GzipCodec(16); // cap at 16 bytes decompressed
        byte[] large = new byte[1024]; // compresses tiny (all zeros), expands past the cap
        byte[] compressed = codec.encode(large);
        assertThrows(SerializationException.class, () -> codec.decode(compressed));
    }
}
