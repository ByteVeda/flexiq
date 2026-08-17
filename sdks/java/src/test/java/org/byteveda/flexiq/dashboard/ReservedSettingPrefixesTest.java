package org.byteveda.flexiq.dashboard;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.byteveda.flexiq.internal.NativeQueue;
import org.junit.jupiter.api.Test;

/** The in-memory test store mirrors the core's reserved prefixes; this locks the two together. */
class ReservedSettingPrefixesTest {

    @Test
    void inMemoryStoreMirrorsTheCoreList() {
        List<String> core = List.of(NativeQueue.reservedSettingPrefixes());
        assertTrue(core.contains("retention:"), "core must reserve the published retention document");
        assertEquals(core, new InMemorySettings().reservedPrefixes());
    }
}
