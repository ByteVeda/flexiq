package org.byteveda.flexiq.serialization;

import java.security.SecureRandom;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.byteveda.flexiq.errors.CryptoException;

/**
 * A {@link PayloadCodec} that encrypts payloads with AES-GCM. Each payload is
 * prefixed with a fresh 12-byte IV; the GCM tag authenticates it. The key must be
 * 16, 24, or 32 bytes (AES-128/192/256).
 */
public final class AesGcmCodec implements PayloadCodec {
    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int IV_LENGTH = 12;
    private static final int TAG_BITS = 128;

    private final SecretKeySpec key;
    private final SecureRandom random = new SecureRandom();

    /**
     * A codec encrypting with {@code key}.
     *
     * @param key the AES key, 16, 24 or 32 bytes; producer and worker must hold the
     *     same one or the tag check fails
     */
    public AesGcmCodec(byte[] key) {
        this.key = new SecretKeySpec(key, "AES");
    }

    @Override
    public byte[] encode(byte[] data) {
        try {
            byte[] iv = new byte[IV_LENGTH];
            random.nextBytes(iv);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            byte[] ciphertext = cipher.doFinal(data);
            byte[] out = new byte[IV_LENGTH + ciphertext.length];
            System.arraycopy(iv, 0, out, 0, IV_LENGTH);
            System.arraycopy(ciphertext, 0, out, IV_LENGTH, ciphertext.length);
            return out;
        } catch (Exception e) {
            throw new CryptoException("encryption failed", e);
        }
    }

    @Override
    public byte[] decode(byte[] data) {
        if (data.length < IV_LENGTH) {
            throw new CryptoException("encrypted payload is too short");
        }
        try {
            byte[] iv = Arrays.copyOfRange(data, 0, IV_LENGTH);
            byte[] ciphertext = Arrays.copyOfRange(data, IV_LENGTH, data.length);
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            return cipher.doFinal(ciphertext);
        } catch (Exception e) {
            throw new CryptoException("decryption failed", e);
        }
    }
}
