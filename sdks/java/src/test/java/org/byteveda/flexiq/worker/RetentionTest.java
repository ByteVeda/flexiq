package org.byteveda.flexiq.worker;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

class RetentionTest {

    @Test
    void toMapKeepsOnlySetWindowsWithCamelCaseKeys() {
        Map<String, Object> map = Retention.builder()
                .archivedJobs(604_800)
                .taskLogs(259_200)
                .build()
                .toMap();

        assertEquals(604_800, map.get("archivedJobs"));
        assertEquals(259_200, map.get("taskLogs"));
        assertFalse(map.containsKey("deadLetter"), "an unset window must be omitted");
        assertEquals(2, map.size());
    }

    @Test
    void emptyRetentionEncodesEmptyMap() {
        assertEquals(0, Retention.builder().build().toMap().size());
    }

    @Test
    void negativeWindowRejectedAtTheBuilder() {
        assertThrows(IllegalArgumentException.class, () -> Retention.builder().taskLogs(-1));
    }

    @Test
    void zeroWindowIsValid() {
        assertEquals(0, Retention.builder().taskLogs(0).build().toMap().get("taskLogs"));
    }
}
