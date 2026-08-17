package org.byteveda.flexiq.core;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.byteveda.flexiq.FlexiQException;
import org.byteveda.flexiq.errors.ConfigurationException;
import org.byteveda.flexiq.errors.CryptoException;
import org.byteveda.flexiq.errors.LockException;
import org.byteveda.flexiq.errors.NonRetryableException;
import org.byteveda.flexiq.errors.RetryableException;
import org.byteveda.flexiq.errors.SerializationException;
import org.byteveda.flexiq.errors.WebhookException;
import org.byteveda.flexiq.errors.WorkflowException;
import org.byteveda.flexiq.serialization.JsonSerializer;
import org.byteveda.flexiq.serialization.Serializer;
import org.byteveda.flexiq.serialization.SignedSerializer;
import org.junit.jupiter.api.Test;

class ExceptionHierarchyTest {

    @Test
    void everySpecificExceptionExtendsTheBase() {
        assertTrue(FlexiQException.class.isAssignableFrom(SerializationException.class));
        assertTrue(FlexiQException.class.isAssignableFrom(WorkflowException.class));
        assertTrue(FlexiQException.class.isAssignableFrom(LockException.class));
        assertTrue(FlexiQException.class.isAssignableFrom(ConfigurationException.class));
        assertTrue(FlexiQException.class.isAssignableFrom(WebhookException.class));
        assertTrue(FlexiQException.class.isAssignableFrom(RetryableException.class));
        assertTrue(FlexiQException.class.isAssignableFrom(NonRetryableException.class));
        // CryptoException is a kind of SerializationException.
        assertTrue(SerializationException.class.isAssignableFrom(CryptoException.class));
    }

    @Test
    void malformedPayloadThrowsSerializationException() {
        Serializer json = new JsonSerializer();
        assertThrows(SerializationException.class, () -> json.deserialize("not json".getBytes(), Integer.class));
    }

    @Test
    void tamperedSignedPayloadThrowsCryptoException() {
        Serializer signed = new SignedSerializer(new JsonSerializer(), "secret".getBytes());
        byte[] bytes = signed.serialize(42);
        bytes[0] ^= 0x01; // corrupt the HMAC tag

        CryptoException error = assertThrows(CryptoException.class, () -> signed.deserialize(bytes, Integer.class));
        // A caller may catch it as the category or the base type.
        assertInstanceOf(SerializationException.class, error);
        assertInstanceOf(FlexiQException.class, error);
    }
}
