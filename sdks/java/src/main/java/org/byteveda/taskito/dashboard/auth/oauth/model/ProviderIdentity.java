package org.byteveda.taskito.dashboard.auth.oauth.model;

import org.jspecify.annotations.Nullable;

/**
 * The normalised "who just logged in" every provider returns after a successful
 * flow.
 *
 * <p>{@code subject} is the provider's stable unique id (OIDC {@code sub},
 * GitHub numeric id) — never the email, which can change. Together with
 * {@code slot} it forms the Taskito username {@code <slot>:<subject>}.
 * {@code email}/{@code name}/{@code picture} may be {@code null}.
 */
public record ProviderIdentity(
        String slot,
        String subject,
        @Nullable String email,
        boolean emailVerified,
        @Nullable String name,
        @Nullable String picture) {}
