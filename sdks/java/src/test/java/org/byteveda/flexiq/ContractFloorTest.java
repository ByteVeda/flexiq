package org.byteveda.flexiq;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.file.Path;
import java.util.Map;
import org.byteveda.flexiq.internal.JniQueueBackend;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The storage carries the lowest contract level a process may speak.
 *
 * <p>A build below that floor must refuse to open rather than join a deployment
 * and misread rows its contract never described.
 */
class ContractFloorTest {

    private static final String CONTRACT_FLOOR_SETTING = "contract:min_sdk";

    private static String optionsFor(Path dir) throws Exception {
        return new ObjectMapper()
                .writeValueAsString(
                        Map.of("backend", "sqlite", "dsn", dir.resolve("t.db").toString()));
    }

    @Test
    void leavesAnUnraisedFloorUnwritten(@TempDir Path dir) throws Exception {
        // Opening never writes, so a deployment that leaves the dial alone
        // carries no row for it.
        try (FlexiQ queue = FlexiQ.builder().open(JniQueueBackend.open(optionsFor(dir)))) {
            assertTrue(queue.getSetting(CONTRACT_FLOOR_SETTING).isEmpty());
            assertTrue(queue.minContract() >= 1);
        }
    }

    @Test
    void stillOpensStorageWhoseFloorIsExactlyThisBuild(@TempDir Path dir) throws Exception {
        String options = optionsFor(dir);
        int level;
        try (FlexiQ queue = FlexiQ.builder().open(JniQueueBackend.open(options))) {
            level = queue.minContract();
            queue.setMinContract(level);
        }

        try (FlexiQ reopened = FlexiQ.builder().open(JniQueueBackend.open(options))) {
            assertEquals(level, reopened.minContract());
        }
    }

    @Test
    void refusesToOpenStorageThatRequiresANewerBuild(@TempDir Path dir) throws Exception {
        String options = optionsFor(dir);
        int unreachable;
        try (FlexiQ queue = FlexiQ.builder().open(JniQueueBackend.open(options))) {
            unreachable = queue.minContract() + 1;
            // Written through the raw setting: setMinContract rejects a level
            // this build cannot speak, which the next test exercises.
            queue.setSetting(CONTRACT_FLOOR_SETTING, String.valueOf(unreachable));
        }

        Exception failure = assertThrows(Exception.class, () -> JniQueueBackend.open(options));
        assertTrue(
                failure.getMessage().contains(String.valueOf(unreachable)),
                "the error must name the required level: " + failure.getMessage());
    }

    @Test
    void rejectsAFloorThisBuildCannotSpeak(@TempDir Path dir) throws Exception {
        try (FlexiQ queue = FlexiQ.builder().open(JniQueueBackend.open(optionsFor(dir)))) {
            int before = queue.minContract();

            Exception failure = assertThrows(Exception.class, () -> queue.setMinContract(before + 1));
            assertTrue(failure.getMessage().contains("lock it out"), "unexpected message: " + failure.getMessage());
            assertEquals(before, queue.minContract());
        }
    }
}
