package org.byteveda.flexiq.internal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Asserts the FFM transport is selected on JDK 22+ and agrees with JNI on the hot
 * ops. Worker-free: it compares the enqueue/getResult marshalling directly over
 * one shared native handle (present-result fidelity is covered end-to-end by
 * {@code FfmRoundTripTest}). On JDKs without the FFM overlay the FFM-specific
 * assertions are skipped.
 */
class TransportParityTest {

    @Test
    void ffmSelectedOnJava22() {
        NativeTransport transport = NativeTransport.create(0L);
        assumeFalse(transport instanceof JniTransport, "FFM transport not active on this JDK");
        assertEquals("FfmTransport", transport.getClass().getSimpleName());
    }

    @Test
    void jniAndFfmAgreeOnHotOps(@TempDir Path dir) {
        long handle = NativeQueue.open(sqliteOptions(dir.resolve("parity.db")));
        try {
            NativeTransport ffm = NativeTransport.create(handle);
            assumeFalse(ffm instanceof JniTransport, "FFM transport not active on this JDK");
            NativeTransport jni = new JniTransport(handle);

            byte[] payload = binaryPayload();

            // enqueue: both marshal task/payload/options and return a valid id.
            String jniId = jni.enqueue("parity.task", payload, "{}");
            String ffmId = ffm.enqueue("parity.task", payload, "{}");
            assertValidId(jniId);
            assertValidId(ffmId);

            // getResult: a freshly enqueued job has none — both report absent (null).
            assertNull(jni.getResult(jniId));
            assertNull(ffm.getResult(ffmId));
            assertNull(jni.getResult("does-not-exist"));
            assertNull(ffm.getResult("does-not-exist"));

            // enqueueMany: identical batch shape through both transports.
            byte[][] batch = {new byte[0], payload, "third".getBytes()};
            String options = "[{},{},{}]";
            String[] jniIds = jni.enqueueMany("parity.batch", batch, options);
            String[] ffmIds = ffm.enqueueMany("parity.batch", batch, options);
            assertEquals(batch.length, jniIds.length);
            assertEquals(jniIds.length, ffmIds.length);
            for (int i = 0; i < ffmIds.length; i++) {
                assertValidId(jniIds[i]);
                assertValidId(ffmIds[i]);
            }
        } finally {
            NativeQueue.close(handle);
        }
    }

    private static void assertValidId(String id) {
        assertNotNull(id);
        assertFalse(id.isBlank());
    }

    /** A payload spanning every byte value, to catch any sign/encoding mishandling. */
    private static byte[] binaryPayload() {
        byte[] bytes = new byte[256];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) i;
        }
        return bytes;
    }

    private static String sqliteOptions(Path db) {
        String dsn = db.toString().replace("\\", "\\\\");
        return "{\"backend\":\"sqlite\",\"dsn\":\"" + dsn + "\"}";
    }
}
