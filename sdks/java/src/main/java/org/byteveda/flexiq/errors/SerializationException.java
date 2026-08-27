package org.byteveda.flexiq.errors;

import org.byteveda.flexiq.FlexiQException;

/**
 * A payload, option blob, or native response could not be serialized or
 * deserialized — e.g. an unregistered type, malformed JSON, or a result whose
 * shape does not match the expected class.
 */
public class SerializationException extends FlexiQException {
    /**
     * A conversion the serializer refused before attempting it.
     *
     * @param message which payload or type could not be converted
     */
    public SerializationException(String message) {
        super(message);
    }

    /**
     * A conversion that failed inside the codec.
     *
     * @param message which payload or type could not be converted
     * @param cause the codec failure underneath
     */
    public SerializationException(String message, Throwable cause) {
        super(message, cause);
    }
}
