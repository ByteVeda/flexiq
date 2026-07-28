package org.byteveda.taskito.dashboard.auth;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Locale;
import org.jspecify.annotations.Nullable;

/** A dashboard user's access level. Wire form is the lowercase name, stored in settings. */
public enum Role {
    /** Full read/write access to jobs, queues, and users. */
    ADMIN,
    /** Read-only access. */
    VIEWER;

    /** Lowercase wire form persisted in the settings store and sent over the API. */
    @JsonValue
    public String wire() {
        return name().toLowerCase(Locale.ROOT);
    }

    /** Parse a wire form ({@code "admin"}/{@code "viewer"}); null for anything else. */
    @JsonCreator
    public static @Nullable Role fromWire(@Nullable String wire) {
        if (wire == null) {
            return null;
        }
        for (Role role : values()) {
            if (role.wire().equals(wire)) {
                return role;
            }
        }
        return null;
    }

    /**
     * Read a persisted role, failing closed. Rows predate this enum, so an
     * unrecognized value yields the least-privileged role rather than locking the
     * user out or granting more than was stored.
     */
    public static Role orViewer(@Nullable String wire) {
        Role role = fromWire(wire);
        return role == null ? VIEWER : role;
    }
}
