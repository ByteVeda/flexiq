package org.byteveda.flexiq.serialization;

import java.lang.reflect.Type;
import java.security.SecureRandom;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.byteveda.flexiq.errors.CryptoException;
import org.jspecify.annotations.Nullable;

/**
 * Wraps a delegate serializer with AES-GCM encryption. Each payload is prefixed
 * with a fresh 12-byte IV; the GCM tag provides authentication. The key must be
 * 16, 24, or 32 bytes (AES-128/192/256).
 */
public final class EncryptedSerializer implements Serializer {
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int IV_LENGTH = 12;
    private static final int TAG_BITS = 128;

    private final Serializer delegate;
    private final SecretKeySpec key;
    private final SecureRandom random = new SecureRandom();

    public EncryptedSerializer(Serializer delegate, byte[] key) {
        this.delegate = delegate;
        this.key = new SecretKeySpec(key, "AES");
    }

    @Override
    public byte[] serialize(@Nullable Object value) {
        try {
            byte[] iv = new byte[IV_LENGTH];
            random.nextBytes(iv);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            byte[] ciphertext = cipher.doFinal(delegate.serialize(value));
            byte[] out = new byte[IV_LENGTH + ciphertext.length];
            System.arraycopy(iv, 0, out, 0, IV_LENGTH);
            System.arraycopy(ciphertext, 0, out, IV_LENGTH, ciphertext.length);
            return out;
        } catch (Exception e) {
            throw new CryptoException("encryption failed", e);
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> type) {
        return delegate.deserialize(decrypt(bytes), type);
    }

    @Override
    public Object deserialize(byte[] bytes, Type type) {
        return delegate.deserialize(decrypt(bytes), type);
    }

    /** Strip the IV, decrypt + authenticate (GCM), and return the plaintext. */
    private byte[] decrypt(byte[] bytes) {
        if (bytes.length < IV_LENGTH) {
            throw new CryptoException("encrypted payload is too short");
        }
        try {
            byte[] iv = Arrays.copyOfRange(bytes, 0, IV_LENGTH);
            byte[] ciphertext = Arrays.copyOfRange(bytes, IV_LENGTH, bytes.length);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            return cipher.doFinal(ciphertext);
        } catch (Exception e) {
            throw new CryptoException("decryption failed", e);
        }
    }
}
