package org.byteveda.flexiq.dashboard.auth;

import org.jspecify.annotations.Nullable;

/**
 * A dashboard user. {@code createdAt}/{@code lastLoginAt} are Unix
 * milliseconds. {@code email}/{@code displayName} are populated for OAuth users
 * only. Password users have a {@code pbkdf2_sha256$...} {@code passwordHash};
 * OAuth users have the {@code oauth:<slot>} sentinel.
 *
 * @param username the login name, unique across the dashboard
 * @param passwordHash a {@code pbkdf2_sha256$...} digest, or the {@code oauth:<slot>} sentinel
 * @param role what the user may do — {@code admin} or {@code viewer}
 * @param createdAt when the account was created, in Unix milliseconds
 * @param lastLoginAt the most recent successful login, in Unix milliseconds, or {@code null}
 * @param email the address the provider reported, for an OAuth user only
 * @param displayName the name the provider reported, for an OAuth user only
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
