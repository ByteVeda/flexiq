package org.byteveda.flexiq.proxies;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * A serializable, signed reference to a non-serializable resource. Carry it in a
 * job payload; the worker reconstructs the resource with
 * {@link Proxies#reconstruct}. The optional {@code expiresAtMs} and
 * {@code purpose} are folded into the signature, so neither can be tampered with.
 *
 * @param handler the {@link ProxyHandler#id()} that produced (and reconstructs) it
 * @param reference the handler's serializable reference data
 * @param signature an HMAC over {@code handler + reference + expiresAtMs + purpose}
 * @param expiresAtMs wall-clock expiry (epoch ms), or null for no expiry
 * @param purpose an optional binding label the worker can require on reconstruct
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ProxyRef(
        String handler,
        Map<String, Object> reference,
        String signature,
        @Nullable Long expiresAtMs,
        @Nullable String purpose) {

    /** A ref with neither expiry nor purpose binding. */
    public ProxyRef(String handler, Map<String, Object> reference, String signature) {
        this(handler, reference, signature, null, null);
    }
}
