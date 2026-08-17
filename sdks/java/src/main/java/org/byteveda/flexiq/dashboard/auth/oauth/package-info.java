/**
 * OAuth2 / OIDC login for the dashboard, alongside password auth.
 *
 * <p>Adds Google, GitHub, and one-or-more generic OIDC providers (Okta, Auth0,
 * Keycloak, Microsoft Entra, …). Auth state lives in the same settings KV store
 * as sessions and users, so no schema changes are needed. OAuth users are keyed
 * {@code <slot>:<subject>} and carry the {@code oauth:<slot>} sentinel that the
 * password verifier rejects.
 *
 * <p>The layer is split into acyclic subpackages (lower ones never import
 * higher):
 * <ul>
 *   <li>{@code error} — the exception hierarchy.
 *   <li>{@code model} — the {@code OAuthState} / {@code ProviderIdentity} records.
 *   <li>{@code config} — {@code OAuthConfig} env-var parsing.
 *   <li>{@code provider} — the {@code OAuthProvider} implementations, PKCE, and
 *       id-token validation (the only nimbus-jose-jwt dependant, so the dashboard
 *       degrades to password-only when that optional jar is absent).
 *   <li>this root package — orchestration: {@link
 *       org.byteveda.flexiq.dashboard.auth.oauth.OAuthFlow}, {@link
 *       org.byteveda.flexiq.dashboard.auth.oauth.OAuthHandlers}, {@link
 *       org.byteveda.flexiq.dashboard.auth.oauth.OAuthStateStore}, and {@link
 *       org.byteveda.flexiq.dashboard.auth.oauth.UrlSafety}.
 * </ul>
 */
@NullMarked
package org.byteveda.flexiq.dashboard.auth.oauth;

import org.jspecify.annotations.NullMarked;
