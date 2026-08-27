package org.byteveda.flexiq.proxies;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.byteveda.flexiq.errors.ProxyException;
import org.jspecify.annotations.Nullable;

/**
 * Registry that deconstructs resources into signed {@link ProxyRef}s and
 * reconstructs them. Construct with an HMAC key (shared by producer and worker),
 * register a {@link ProxyHandler} per resource type, then
 * {@link #deconstruct(Object)} on the producer and {@link #reconstruct(ProxyRef)}
 * (or {@link #resolve(ProxyRef)}) on the worker.
 */
public final class Proxies {
    private static final String ALGORITHM = "HmacSHA256";

    private final Map<String, ProxyHandler<?>> handlers = new LinkedHashMap<>();
    private final byte[] key;
    private final ObjectMapper canonical =
            new ObjectMapper().configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);

    /**
     * A registry signing with {@code hmacKey}.
     *
     * @param hmacKey the key producer and worker share; a worker holding a
     *     different one rejects every ref as a signature mismatch. Defensively
     *     copied
     */
    public Proxies(byte[] hmacKey) {
        this.key = hmacKey.clone();
    }

    /**
     * Register a handler under its non-null, unique id; returns {@code this}.
     *
     * @param handler the handler, whose {@link ProxyHandler#id()} must not already
     *     be taken — overwriting one would let producer and worker disagree on
     *     what a ref's handler id means
     * @return {@code this}, for chaining
     */
    public Proxies register(ProxyHandler<?> handler) {
        String id = handler.id();
        if (id == null) {
            throw new ProxyException("proxy handler id must not be null");
        }
        // Fail fast on a duplicate: silently overwriting would let a producer and
        // worker disagree on what a given ProxyRef's handler id means.
        if (handlers.putIfAbsent(id, handler) != null) {
            throw new ProxyException("proxy handler '" + id + "' is already registered");
        }
        return this;
    }

    /**
     * Deconstruct {@code value} into a signed ref with no expiry or purpose.
     *
     * @param value the resource to proxy
     * @return a ref safe to carry in a job payload
     */
    public ProxyRef deconstruct(Object value) {
        return deconstruct(value, null, null);
    }

    /**
     * Deconstruct {@code value} into a ref that expires after {@code ttl}.
     *
     * @param value the resource to proxy
     * @param ttl how long the ref stays resolvable, or {@code null} for no expiry
     * @return a ref safe to carry in a job payload
     */
    public ProxyRef deconstruct(Object value, @Nullable Duration ttl) {
        return deconstruct(value, ttl, null);
    }

    /**
     * Deconstruct {@code value} into a signed ref bound to {@code ttl} (nullable)
     * and {@code purpose} (nullable); throws if no handler accepts it.
     *
     * @param value the resource to proxy
     * @param ttl how long the ref stays resolvable, or {@code null} for no expiry
     * @param purpose a label the worker can require on reconstruct, so a ref minted
     *     for one use cannot be replayed into another; {@code null} to bind none
     * @return a ref safe to carry in a job payload
     */
    @SuppressWarnings("unchecked")
    public ProxyRef deconstruct(Object value, @Nullable Duration ttl, @Nullable String purpose) {
        if (value == null) {
            throw new ProxyException("cannot deconstruct null");
        }
        Long expiresAtMs = ttl == null ? null : System.currentTimeMillis() + ttl.toMillis();
        for (ProxyHandler<?> handler : handlers.values()) {
            if (handler.handles(value)) {
                Map<String, Object> reference = ((ProxyHandler<Object>) handler).deconstruct(value);
                String signature = sign(handler.id(), reference, expiresAtMs, purpose);
                return new ProxyRef(handler.id(), reference, signature, expiresAtMs, purpose);
            }
        }
        throw new ProxyException("no proxy handler for " + value.getClass().getName());
    }

    /**
     * Verify a ref's signature and expiry, then reconstruct the resource.
     *
     * @param ref the ref that arrived in the payload
     * @return the live resource
     */
    public Object reconstruct(ProxyRef ref) {
        return reconstruct(ref, null);
    }

    /**
     * Verify a ref's signature, expiry, and (when {@code expectedPurpose} is
     * non-null) its bound purpose, then reconstruct the resource.
     *
     * @param ref the ref that arrived in the payload
     * @param expectedPurpose the label the ref must carry, or {@code null} to accept any
     * @return the live resource
     */
    public Object reconstruct(ProxyRef ref, @Nullable String expectedPurpose) {
        ProxyHandler<Object> handler = handlerFor(ref.handler());
        verify(ref, expectedPurpose);
        return handler.reconstruct(ref.reference());
    }

    /**
     * A new {@link ProxySession}: deconstruct/reconstruct with per-instance
     * identity dedup and close-time cleanup. Confine a session to one thread.
     *
     * @return a fresh session over this registry, to be closed when the unit of
     *     work it models ends
     */
    public ProxySession session() {
        return new ProxySession(this);
    }

    /** Look up a handler by id, failing on an unknown id. */
    @SuppressWarnings("unchecked")
    ProxyHandler<Object> handlerFor(String handlerId) {
        ProxyHandler<Object> handler = (ProxyHandler<Object>) handlers.get(handlerId);
        if (handler == null) {
            throw new ProxyException("unknown proxy handler '" + handlerId + "'");
        }
        return handler;
    }

    /** Verify a ref's signature, expiry, and (when requested) bound purpose. */
    void verify(ProxyRef ref, @Nullable String expectedPurpose) {
        byte[] expected = sign(ref.handler(), ref.reference(), ref.expiresAtMs(), ref.purpose())
                .getBytes(StandardCharsets.UTF_8);
        byte[] actual = (ref.signature() == null ? "" : ref.signature()).getBytes(StandardCharsets.UTF_8);
        if (!MessageDigest.isEqual(expected, actual)) {
            throw new ProxyException("proxy signature mismatch for handler '" + ref.handler() + "'");
        }
        if (ref.expiresAtMs() != null && System.currentTimeMillis() > ref.expiresAtMs()) {
            throw new ProxyException("proxy ref expired for handler '" + ref.handler() + "'");
        }
        if (expectedPurpose != null && !expectedPurpose.equals(ref.purpose())) {
            throw new ProxyException("proxy purpose mismatch for handler '" + ref.handler() + "'");
        }
    }

    /**
     * {@link #reconstruct(ProxyRef)} cast to the caller's type.
     *
     * @param ref the ref that arrived in the payload
     * @param <T> what the caller expects the handler to rebuild; unchecked, so a
     *     mismatch surfaces as a {@link ClassCastException} at the use site
     * @return the live resource
     */
    @SuppressWarnings("unchecked")
    public <T> T resolve(ProxyRef ref) {
        return (T) reconstruct(ref);
    }

    /**
     * {@link #reconstruct(ProxyRef, String)} cast to the caller's type.
     *
     * @param ref the ref that arrived in the payload
     * @param expectedPurpose the label the ref must carry
     * @param <T> what the caller expects the handler to rebuild; unchecked, so a
     *     mismatch surfaces as a {@link ClassCastException} at the use site
     * @return the live resource
     */
    @SuppressWarnings("unchecked")
    public <T> T resolve(ProxyRef ref, String expectedPurpose) {
        return (T) reconstruct(ref, expectedPurpose);
    }

    private String sign(
            String handlerId, Map<String, Object> reference, @Nullable Long expiresAtMs, @Nullable String purpose) {
        try {
            Mac mac = Mac.getInstance(ALGORITHM);
            mac.init(new SecretKeySpec(key, ALGORITHM));
            mac.update(handlerId.getBytes(StandardCharsets.UTF_8));
            mac.update((byte) '\n');
            mac.update(canonical.writeValueAsBytes(reference));
            mac.update((byte) '\n');
            mac.update(String.valueOf(expiresAtMs).getBytes(StandardCharsets.UTF_8));
            mac.update((byte) '\n');
            mac.update((purpose == null ? "" : purpose).getBytes(StandardCharsets.UTF_8));
            return Base64.getEncoder().encodeToString(mac.doFinal());
        } catch (Exception e) {
            throw new ProxyException("failed to sign proxy ref", e);
        }
    }
}
