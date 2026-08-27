package org.byteveda.flexiq.serialization;

import java.security.MessageDigest;
import java.util.Arrays;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.byteveda.flexiq.errors.CryptoException;

/**
 * A {@link PayloadCodec} that prefixes each payload with an HMAC-SHA256 tag and,
 * on decode, verifies it in constant time — rejecting tampered bytes.
 */
public final class HmacCodec implements PayloadCodec {
    private static final String ALGORITHM = "HmacSHA256";
    private static final int MAC_LENGTH = 32;

    private final byte[] key;

    /**
     * A codec signing with {@code key}.
     *
     * @param key the HMAC key; producer and worker must hold the same one or every
     *     payload is rejected. Defensively copied
     */
    public HmacCodec(byte[] key) {
        this.key = key.clone();
    }

    @Override
    public byte[] encode(byte[] data) {
        byte[] mac = mac(data);
        byte[] out = new byte[MAC_LENGTH + data.length];
        System.arraycopy(mac, 0, out, 0, MAC_LENGTH);
        System.arraycopy(data, 0, out, MAC_LENGTH, data.length);
        return out;
    }

    @Override
    public byte[] decode(byte[] data) {
        if (data.length < MAC_LENGTH) {
            throw new CryptoException("signed payload is too short");
        }
        byte[] mac = Arrays.copyOfRange(data, 0, MAC_LENGTH);
        byte[] body = Arrays.copyOfRange(data, MAC_LENGTH, data.length);
        if (!MessageDigest.isEqual(mac, mac(body))) {
            throw new CryptoException("signature mismatch");
        }
        return body;
    }

    private byte[] mac(byte[] body) {
        try {
            Mac instance = Mac.getInstance(ALGORITHM);
            instance.init(new SecretKeySpec(key, ALGORITHM));
            return instance.doFinal(body);
        } catch (Exception e) {
            throw new CryptoException("HMAC computation failed", e);
        }
    }
}
