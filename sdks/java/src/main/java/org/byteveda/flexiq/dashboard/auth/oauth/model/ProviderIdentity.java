package org.byteveda.flexiq.dashboard.auth.oauth.model;

import org.jspecify.annotations.Nullable;

/**
 * The normalised "who just logged in" every provider returns after a successful
 * flow.
 *
 * <p>{@code subject} is the provider's stable unique id (OIDC {@code sub},
 * GitHub numeric id) — never the email, which can change. Together with
 * {@code slot} it forms the FlexiQ username {@code <slot>:<subject>}.
 * {@code email}/{@code name}/{@code picture} may be {@code null}.
 *
 * @param slot which configured provider authenticated this identity
 * @param subject the provider's stable unique id — never the email, which can change
 * @param email the address the provider reported, or {@code null}
 * @param emailVerified whether the provider vouches for that address
 * @param name the display name the provider reported, or {@code null}
 * @param picture the avatar URL the provider reported, or {@code null}
 */
public record ProviderIdentity(
        String slot,
        String subject,
        @Nullable String email,
        boolean emailVerified,
        @Nullable String name,
        @Nullable String picture) {}
