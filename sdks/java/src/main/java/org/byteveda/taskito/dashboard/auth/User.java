package org.byteveda.taskito.dashboard.auth;

import org.jspecify.annotations.Nullable;

/**
 * A dashboard user. {@code createdAt}/{@code lastLoginAt} are Unix
 * milliseconds. {@code email}/{@code displayName} are populated for OAuth users
 * only. Password users have a {@code pbkdf2_sha256$...} {@code passwordHash};
 * OAuth users have the {@code oauth:<slot>} sentinel.
 */
public record User(
        String username,
        String passwordHash,
        String role,
        long createdAt,
        @Nullable Long lastLoginAt,
        @Nullable String email,
        @Nullable String displayName) {

    public boolean isOauth() {
        return PasswordHasher.isOauth(passwordHash);
    }
}
