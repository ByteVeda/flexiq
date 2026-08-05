package org.byteveda.taskito.health;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Worker-resource check: the names of resources not reporting {@code healthy}.
 * {@code status} is {@code ok}, {@code degraded}, or {@code error} when the
 * lookup itself failed — in which case {@link #error} carries the reason.
 */
public record ResourcesCheck(int count, List<String> unhealthy, String status, @Nullable String error) {

    /** A failed check reports the error in place of its object, as the other shells do. */
    Object toWire() {
        if (error != null) {
            return error;
        }
        Map<String, Object> wire = new LinkedHashMap<>();
        wire.put("count", count);
        wire.put("unhealthy", unhealthy);
        wire.put("status", status);
        return wire;
    }
}
