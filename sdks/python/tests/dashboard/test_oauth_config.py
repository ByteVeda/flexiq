"""Tests for OAuth config parsing from env vars."""

from __future__ import annotations

import pytest

from flexiq.dashboard.oauth.config import (
    GitHubConfig,
    GoogleConfig,
    OAuthConfig,
    OAuthConfigError,
    OIDCConfig,
    from_env,
)


def test_from_env_returns_none_when_unconfigured() -> None:
    assert from_env({}) is None


def test_from_env_requires_base_url_when_any_provider_set() -> None:
    with pytest.raises(OAuthConfigError, match="REDIRECT_BASE_URL"):
        from_env(
            {
                "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID": "gid",
                "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_SECRET": "gsec",
            }
        )


def test_from_env_parses_google_provider() -> None:
    config = from_env(
        {
            "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL": "https://flexiq.acme.com",
            "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID": "gid",
            "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_SECRET": "gsec",
            "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_ALLOWED_DOMAINS": "acme.com, partner.com",
        }
    )
    assert config is not None
    assert config.is_enabled
    assert isinstance(config.google, GoogleConfig)
    assert config.google.client_id == "gid"
    assert config.google.client_secret == "gsec"
    assert config.google.allowed_domains == ("acme.com", "partner.com")
    assert config.github is None
    assert config.oidc == ()


def test_from_env_partial_google_config_raises() -> None:
    with pytest.raises(OAuthConfigError, match="CLIENT_SECRET"):
        from_env(
            {
                "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL": "https://flexiq.acme.com",
                "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID": "gid",
            }
        )


def test_from_env_parses_github_provider() -> None:
    config = from_env(
        {
            "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL": "https://flexiq.acme.com",
            "FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_ID": "hid",
            "FLEXIQ_DASHBOARD_OAUTH_GITHUB_CLIENT_SECRET": "hsec",
            "FLEXIQ_DASHBOARD_OAUTH_GITHUB_ALLOWED_ORGS": "acme,partner",
        }
    )
    assert config is not None
    assert isinstance(config.github, GitHubConfig)
    assert config.github.allowed_orgs == ("acme", "partner")


def test_from_env_parses_multiple_oidc_slots() -> None:
    config = from_env(
        {
            "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL": "https://flexiq.acme.com",
            "FLEXIQ_DASHBOARD_OAUTH_OIDC_PROVIDERS": "okta,microsoft",
            "FLEXIQ_DASHBOARD_OAUTH_OIDC_OKTA_CLIENT_ID": "oid",
            "FLEXIQ_DASHBOARD_OAUTH_OIDC_OKTA_CLIENT_SECRET": "osec",
            "FLEXIQ_DASHBOARD_OAUTH_OIDC_OKTA_DISCOVERY_URL": "https://acme.okta.com/.well-known/openid-configuration",
            "FLEXIQ_DASHBOARD_OAUTH_OIDC_OKTA_LABEL": "Acme SSO",
            "FLEXIQ_DASHBOARD_OAUTH_OIDC_OKTA_ALLOWED_DOMAINS": "acme.com",
            "FLEXIQ_DASHBOARD_OAUTH_OIDC_MICROSOFT_CLIENT_ID": "mid",
            "FLEXIQ_DASHBOARD_OAUTH_OIDC_MICROSOFT_CLIENT_SECRET": "msec",
            "FLEXIQ_DASHBOARD_OAUTH_OIDC_MICROSOFT_DISCOVERY_URL": "https://login.microsoftonline.com/x/v2.0/.well-known/openid-configuration",
        }
    )
    assert config is not None
    assert [p.slot for p in config.oidc] == ["okta", "microsoft"]
    okta = config.oidc[0]
    assert isinstance(okta, OIDCConfig)
    assert okta.label == "Acme SSO"
    assert okta.allowed_domains == ("acme.com",)
    microsoft = config.oidc[1]
    assert microsoft.label == "Microsoft"  # default = title-cased slot


def test_from_env_rejects_duplicate_oidc_slot() -> None:
    with pytest.raises(OAuthConfigError, match="twice"):
        from_env(
            {
                "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL": "https://flexiq.acme.com",
                "FLEXIQ_DASHBOARD_OAUTH_OIDC_PROVIDERS": "okta,okta",
                "FLEXIQ_DASHBOARD_OAUTH_OIDC_OKTA_CLIENT_ID": "oid",
                "FLEXIQ_DASHBOARD_OAUTH_OIDC_OKTA_CLIENT_SECRET": "osec",
                "FLEXIQ_DASHBOARD_OAUTH_OIDC_OKTA_DISCOVERY_URL": "https://x/y",
            }
        )


def test_oidc_slot_must_not_collide_with_reserved_name() -> None:
    with pytest.raises(OAuthConfigError, match=r"reserved|collides|built-in"):
        OIDCConfig(
            slot="google",
            client_id="x",
            client_secret="y",
            discovery_url="https://x/y",
        )


def test_oidc_slot_must_be_url_safe() -> None:
    with pytest.raises(OAuthConfigError):
        OIDCConfig(
            slot="Has Spaces",
            client_id="x",
            client_secret="y",
            discovery_url="https://x/y",
        )


def test_redirect_base_url_must_be_https_for_remote_hosts() -> None:
    with pytest.raises(OAuthConfigError, match="https"):
        OAuthConfig(redirect_base_url="http://flexiq.acme.com")


def test_redirect_base_url_allows_http_for_localhost() -> None:
    # No exception.
    OAuthConfig(redirect_base_url="http://localhost:8000")
    OAuthConfig(redirect_base_url="http://127.0.0.1:8000")


def test_password_auth_flag_parses() -> None:
    config = from_env(
        {
            "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL": "https://flexiq.acme.com",
            "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID": "gid",
            "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_SECRET": "gsec",
            "FLEXIQ_DASHBOARD_PASSWORD_AUTH_ENABLED": "false",
        }
    )
    assert config is not None
    assert config.password_auth_enabled is False


def test_disabling_password_without_providers_is_an_error() -> None:
    with pytest.raises(OAuthConfigError, match="no way to log in"):
        from_env(
            {
                "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL": "https://flexiq.acme.com",
                "FLEXIQ_DASHBOARD_PASSWORD_AUTH_ENABLED": "false",
            }
        )


def test_admin_emails_parsed_as_tuple() -> None:
    config = from_env(
        {
            "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL": "https://flexiq.acme.com",
            "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID": "gid",
            "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_SECRET": "gsec",
            "FLEXIQ_DASHBOARD_OAUTH_ADMIN_EMAILS": " alice@acme.com , bob@acme.com ",
        }
    )
    assert config is not None
    assert config.admin_emails == ("alice@acme.com", "bob@acme.com")


def test_callback_url_built_from_base_url_and_slot() -> None:
    config = from_env(
        {
            "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL": "https://flexiq.acme.com/",
            "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID": "gid",
            "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_SECRET": "gsec",
        }
    )
    assert config is not None
    assert (
        config.callback_url("google") == "https://flexiq.acme.com/api/auth/oauth/callback/google"
    )


def test_find_provider_returns_matching_slot() -> None:
    config = from_env(
        {
            "FLEXIQ_DASHBOARD_OAUTH_REDIRECT_BASE_URL": "https://flexiq.acme.com",
            "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_ID": "gid",
            "FLEXIQ_DASHBOARD_OAUTH_GOOGLE_CLIENT_SECRET": "gsec",
            "FLEXIQ_DASHBOARD_OAUTH_OIDC_PROVIDERS": "okta",
            "FLEXIQ_DASHBOARD_OAUTH_OIDC_OKTA_CLIENT_ID": "oid",
            "FLEXIQ_DASHBOARD_OAUTH_OIDC_OKTA_CLIENT_SECRET": "osec",
            "FLEXIQ_DASHBOARD_OAUTH_OIDC_OKTA_DISCOVERY_URL": "https://acme.okta.com/.well-known/openid-configuration",
        }
    )
    assert config is not None
    assert config.find_provider("google") is config.google
    okta = config.find_provider("okta")
    assert isinstance(okta, OIDCConfig)
    assert config.find_provider("does-not-exist") is None
