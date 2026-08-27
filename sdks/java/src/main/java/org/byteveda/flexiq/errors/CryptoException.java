package org.byteveda.flexiq.errors;

/**
 * A cryptographic operation in a signing or encrypting serializer failed —
 * encryption/decryption, HMAC computation, a signature mismatch, or a payload
 * too short to carry its tag/IV. A {@link SerializationException} because it
 * occurs while (de)serializing a secured payload.
 */
public class CryptoException extends SerializationException {
    /**
     * A crypto failure the serializer diagnosed itself — a tag mismatch, a truncated payload.
     *
     * @param message which cryptographic step failed, without echoing key material
     */
    public CryptoException(String message) {
        super(message);
    }

    /**
     * A crypto failure raised by the JCE provider.
     *
     * @param message which cryptographic step failed, without echoing key material
     * @param cause the underlying JCE failure
     */
    public CryptoException(String message, Throwable cause) {
        super(message, cause);
    }
}
