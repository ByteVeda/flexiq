package org.byteveda.flexiq.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.byteveda.flexiq.errors.SerializationException;
import org.junit.jupiter.api.Test;

/** The wire forms of the closed-set model enums are a cross-SDK contract. */
class ClosedSetEnumTest {

    @Test
    void dispatchOrderRoundTripsItsWireForm() {
        assertEquals("fifo", DispatchOrder.FIFO.wire());
        assertEquals("lifo", DispatchOrder.LIFO.wire());
        assertEquals(DispatchOrder.LIFO, DispatchOrder.fromWire("lifo"));
        assertEquals(DispatchOrder.FIFO, DispatchOrder.fromWire("FIFO"));
    }

    @Test
    void taskLogLevelRoundTripsItsWireForm() {
        assertEquals("warning", TaskLogLevel.WARNING.wire());
        assertEquals("result", TaskLogLevel.RESULT.wire());
        assertEquals(TaskLogLevel.CRITICAL, TaskLogLevel.fromWire("critical"));
    }

    @Test
    void storageBackendRoundTripsItsWireForm() {
        assertEquals("sqlite", StorageBackend.SQLITE.wire());
        assertEquals("postgres", StorageBackend.POSTGRES.wire());
        assertEquals(StorageBackend.REDIS, StorageBackend.fromWire("redis"));
        assertEquals(StorageBackend.SQLITE, StorageBackend.fromWire("SQLITE"));
    }

    @Test
    void unknownWireFormIsRejected() {
        assertThrows(SerializationException.class, () -> DispatchOrder.fromWire("sideways"));
        assertThrows(SerializationException.class, () -> TaskLogLevel.fromWire("verbose"));
        assertThrows(SerializationException.class, () -> TaskLogLevel.fromWire(null));
        assertThrows(SerializationException.class, () -> StorageBackend.fromWire("mysql"));
    }
}
