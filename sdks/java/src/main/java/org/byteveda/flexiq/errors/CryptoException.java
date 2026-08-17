package org.byteveda.flexiq.errors;

/**
 * A cryptographic operation in a signing or encrypting serializer failed —
 * encryption/decryption, HMAC computation, a signature mismatch, or a payload
 * too short to carry its tag/IV. A {@link SerializationException} because it
 * occurs while (de)serializing a secured payload.
 */
public class CryptoException extends SerializationException {
    public CryptoException(String message) {
        super(message);
    }

    public CryptoException(String message, Throwable cause) {
        super(message, cause);
    }
}
