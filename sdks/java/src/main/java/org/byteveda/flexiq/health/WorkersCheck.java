package org.byteveda.flexiq.health;

import java.util.LinkedHashMap;
import java.util.Map;
import org.jspecify.annotations.Nullable;

/**
 * Worker check: how many workers heartbeated recently. {@code status} is
 * {@code ok}, {@code none} when nothing is running, or {@code error} when the
 * lookup itself failed — in which case {@link #error} carries the reason.
 */
public record WorkersCheck(int count, String status, @Nullable String error) {

    /** A failed check reports the error in place of its object, as the other shells do. */
    Object toWire() {
        if (error != null) {
            return error;
        }
        Map<String, Object> wire = new LinkedHashMap<>();
        wire.put("count", count);
        wire.put("status", status);
        return wire;
    }
}
