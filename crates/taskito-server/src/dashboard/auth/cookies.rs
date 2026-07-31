//! Session cookie construction.
//!
//! One place builds them so the attributes stay in lockstep across password
//! login, provider login, and logout — a `SameSite` that drifts between login
//! paths is a CSRF hole that only shows up on one of them.

use axum::http::header::SET_COOKIE;
use axum::http::HeaderMap;

use crate::dashboard::auth::context::{CSRF_COOKIE, OAUTH_STATE_COOKIE, SESSION_COOKIE};
use crate::dashboard::auth::model::{Session, SESSION_TTL_SECONDS};

/// How long the browser holds the in-flight login marker. Matches the state
/// row's own lifetime.
const OAUTH_STATE_MAX_AGE: i64 = 5 * 60;

/// Headers that establish a session.
///
/// The session cookie is `HttpOnly` so script cannot read it; the CSRF cookie
/// deliberately is not, because the SPA has to echo it back in a header.
pub fn established(session: &Session, secure: bool) -> HeaderMap {
    let secure = if secure { "; Secure" } else { "" };
    build([
        format!(
            "{SESSION_COOKIE}={}; HttpOnly; SameSite=Strict; Path=/{secure}; Max-Age={SESSION_TTL_SECONDS}",
            session.token
        ),
        format!(
            "{CSRF_COOKIE}={}; SameSite=Strict; Path=/{secure}; Max-Age={SESSION_TTL_SECONDS}",
            session.csrf_token
        ),
    ])
}

/// Header binding an in-flight provider login to this browser.
///
/// `SameSite=Lax` rather than `Strict`: the provider redirects the browser back
/// with a top-level GET, and `Strict` would withhold the cookie exactly then.
pub fn oauth_state(token: &str, secure: bool) -> HeaderMap {
    let secure = if secure { "; Secure" } else { "" };
    single(format!(
        "{OAUTH_STATE_COOKIE}={token}; HttpOnly; SameSite=Lax; Path=/{secure}; Max-Age={OAUTH_STATE_MAX_AGE}"
    ))
}

/// Header clearing that marker, whatever the outcome.
pub fn cleared_oauth_state(secure: bool) -> HeaderMap {
    let secure = if secure { "; Secure" } else { "" };
    single(format!(
        "{OAUTH_STATE_COOKIE}=; HttpOnly; SameSite=Lax; Path=/{secure}; Max-Age=0"
    ))
}

fn single(cookie: String) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Ok(value) = cookie.parse() {
        headers.append(SET_COOKIE, value);
    }
    headers
}

/// Headers that clear a session.
pub fn cleared(secure: bool) -> HeaderMap {
    let secure = if secure { "; Secure" } else { "" };
    build([
        format!("{SESSION_COOKIE}=; HttpOnly; SameSite=Strict; Path=/{secure}; Max-Age=0"),
        format!("{CSRF_COOKIE}=; SameSite=Strict; Path=/{secure}; Max-Age=0"),
    ])
}

fn build(cookies: [String; 2]) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for cookie in cookies {
        if let Ok(value) = cookie.parse() {
            headers.append(SET_COOKIE, value);
        }
    }
    headers
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dashboard::auth::model::Role;

    fn session() -> Session {
        Session {
            token: "sess".into(),
            username: "ops".into(),
            role: Role::Admin,
            created_at: 0,
            expires_at: i64::MAX,
            csrf_token: "csrf".into(),
        }
    }

    fn cookie_strings(headers: &HeaderMap) -> Vec<String> {
        headers
            .get_all(SET_COOKIE)
            .iter()
            .map(|value| value.to_str().expect("ascii").to_string())
            .collect()
    }

    #[test]
    fn a_login_sets_both_cookies_with_the_right_flags() {
        let cookies = cookie_strings(&established(&session(), true));
        assert_eq!(cookies.len(), 2);

        let session_cookie = &cookies[0];
        assert!(session_cookie.starts_with("taskito_session=sess"));
        assert!(session_cookie.contains("HttpOnly"));
        assert!(session_cookie.contains("SameSite=Strict"));
        assert!(session_cookie.contains("Secure"));

        let csrf_cookie = &cookies[1];
        assert!(csrf_cookie.starts_with("taskito_csrf=csrf"));
        // The SPA must be able to read this one.
        assert!(!csrf_cookie.contains("HttpOnly"));
    }

    #[test]
    fn insecure_mode_drops_only_the_secure_attribute() {
        let cookies = cookie_strings(&established(&session(), false));
        assert!(!cookies[0].contains("Secure"));
        assert!(cookies[0].contains("HttpOnly"));
    }

    #[test]
    fn the_oauth_marker_is_lax_so_the_provider_redirect_carries_it() {
        let cookies = cookie_strings(&oauth_state("state-token", true));
        assert_eq!(cookies.len(), 1);
        assert!(cookies[0].starts_with("taskito_oauth_state=state-token"));
        assert!(cookies[0].contains("HttpOnly"));
        // Strict would be withheld on the provider's top-level redirect back,
        // which is the one request that has to carry it.
        assert!(cookies[0].contains("SameSite=Lax"));
        assert!(!cookies[0].contains("SameSite=Strict"));

        let cleared = cookie_strings(&cleared_oauth_state(true));
        assert!(cleared[0].contains("Max-Age=0"));
    }

    #[test]
    fn logout_expires_both_cookies() {
        let cookies = cookie_strings(&cleared(true));
        assert_eq!(cookies.len(), 2);
        assert!(cookies.iter().all(|cookie| cookie.contains("Max-Age=0")));
    }
}
