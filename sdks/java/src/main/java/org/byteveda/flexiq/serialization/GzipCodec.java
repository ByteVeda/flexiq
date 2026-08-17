package org.byteveda.flexiq.serialization;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.byteveda.flexiq.errors.SerializationException;

/**
 * A {@link PayloadCodec} that gzip-compresses payloads. Decompression is bounded
 * by {@code maxDecompressedBytes} so a small malicious payload can't expand to
 * exhaust worker memory (zip bomb). Order {@code GzipCodec} <em>before</em> a
 * signing/encryption codec in the list (e.g. {@code List.of(gzip, hmac)}):
 * codecs decode in reverse, so integrity is verified before decompressing.
 */
public final class GzipCodec implements PayloadCodec {
    /** Default cap on decompressed output: 64 MiB. */
    public static final int DEFAULT_MAX_DECOMPRESSED_BYTES = 64 * 1024 * 1024;

    private final int maxDecompressedBytes;

    public GzipCodec() {
        this(DEFAULT_MAX_DECOMPRESSED_BYTES);
    }

    public GzipCodec(int maxDecompressedBytes) {
        if (maxDecompressedBytes <= 0) {
            throw new IllegalArgumentException("maxDecompressedBytes must be > 0");
        }
        this.maxDecompressedBytes = maxDecompressedBytes;
    }

    @Override
    public byte[] encode(byte[] data) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(out)) {
            gzip.write(data);
        } catch (IOException e) {
            throw new SerializationException("gzip compression failed", e);
        }
        return out.toByteArray();
    }

    @Override
    public byte[] decode(byte[] data) {
        try (GZIPInputStream gzip = new GZIPInputStream(new ByteArrayInputStream(data))) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            byte[] buffer = new byte[8192];
            long total = 0;
            int read;
            while ((read = gzip.read(buffer)) != -1) {
                total += read;
                if (total > maxDecompressedBytes) {
                    throw new SerializationException(
                            "gzip payload exceeds max decompressed size of " + maxDecompressedBytes + " bytes");
                }
                out.write(buffer, 0, read);
            }
            return out.toByteArray();
        } catch (IOException e) {
            throw new SerializationException("gzip decompression failed", e);
        }
    }
}
