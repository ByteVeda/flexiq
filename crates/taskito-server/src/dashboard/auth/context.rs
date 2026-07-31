//! Per-request auth state: cookies in, session and CSRF verdict out.

use std::collections::HashMap;

use axum::http::HeaderMap;

use crate::dashboard::auth::model::{Role, Session};

/// Cookie carrying the session token. HttpOnly — JavaScript must never read it.
pub const SESSION_COOKIE: &str = "taskito_session";
/// Cookie carrying the CSRF token. Readable by the SPA, which echoes it back.
pub const CSRF_COOKIE: &str = "taskito_csrf";
/// Header the SPA echoes the CSRF token in.
pub const CSRF_HEADER: &str = "x-csrf-token";
/// Cookie binding an in-flight provider login to the browser that began it.
pub const OAUTH_STATE_COOKIE: &str = "taskito_oauth_state";

/// Auth state attached to one request.
#[derive(Debug, Clone, Default)]
pub struct RequestContext {
    /// The live session, when the cookie named one.
    pub session: Option<Session>,
    /// CSRF token presented in the cookie.
    pub csrf_cookie: Option<String>,
    /// CSRF token presented in the header.
    pub csrf_header: Option<String>,
}

impl RequestContext {
    /// Build a context from request headers and the resolved session.
    pub fn new(headers: &HeaderMap, session: Option<Session>) -> Self {
        let cookies = parse_cookies(header_value(headers, "cookie").as_deref());
        Self {
            session,
            csrf_cookie: cookies.get(CSRF_COOKIE).cloned(),
            csrf_header: header_value(headers, CSRF_HEADER),
        }
    }

    /// Whether a live session backs this request.
    pub fn is_authenticated(&self) -> bool {
        self.session.is_some()
    }

    /// The caller's role, when authenticated.
    pub fn role(&self) -> Option<Role> {
        self.session.as_ref().map(|session| session.role)
    }

    /// Double-submit cookie check.
    ///
    /// The header must equal the cookie **and** the session's stored token:
    /// comparing only the first two would accept a token an attacker seeded
    /// into the cookie jar themselves.
    pub fn csrf_valid(&self) -> bool {
        let (Some(session), Some(cookie), Some(header)) = (
            self.session.as_ref(),
            self.csrf_cookie.as_deref(),
            self.csrf_header.as_deref(),
        ) else {
            return false;
        };
        !cookie.is_empty() && cookie == header && cookie == session.csrf_token
    }
}

/// The in-flight login marker this browser presented, if any.
pub fn oauth_state_cookie(headers: &HeaderMap) -> Option<String> {
    parse_cookies(header_value(headers, "cookie").as_deref())
        .get(OAUTH_STATE_COOKIE)
        .cloned()
}

/// Session token presented in the request, if any.
pub fn session_token(headers: &HeaderMap) -> Option<String> {
    parse_cookies(header_value(headers, "cookie").as_deref())
        .get(SESSION_COOKIE)
        .cloned()
}

/// Parse a `Cookie:` header. Malformed pairs are skipped and the first value
/// wins for a repeated name.
pub fn parse_cookies(header: Option<&str>) -> HashMap<String, String> {
    let mut cookies = HashMap::new();
    for part in header.unwrap_or_default().split(';') {
        let Some((name, value)) = part.split_once('=') else {
            continue;
        };
        let (name, value) = (name.trim(), value.trim());
        if !name.is_empty() {
            cookies.entry(name.to_string()).or_insert(value.to_string());
        }
    }
    cookies
}

fn header_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(csrf: &str) -> Session {
        Session {
            token: "token".into(),
            username: "ops".into(),
            role: Role::Admin,
            created_at: 0,
            expires_at: i64::MAX,
            csrf_token: csrf.into(),
        }
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            headers.insert(
                axum::http::HeaderName::from_bytes(name.as_bytes()).expect("valid name"),
                value.parse().expect("valid value"),
            );
        }
        headers
    }

    #[test]
    fn cookies_parse_leniently() {
        let cookies = parse_cookies(Some("a=1; b = 2 ;malformed; a=3; =4"));
        assert_eq!(cookies.get("a").map(String::as_str), Some("1"));
        assert_eq!(cookies.get("b").map(String::as_str), Some("2"));
        assert!(!cookies.contains_key("malformed"));
        assert_eq!(cookies.len(), 2);
    }

    #[test]
    fn csrf_requires_cookie_header_and_session_to_agree() {
        let request = headers(&[("cookie", "taskito_csrf=abc"), ("x-csrf-token", "abc")]);
        let context = RequestContext::new(&request, Some(session("abc")));
        assert!(context.csrf_valid());

        // Header missing.
        let context = RequestContext::new(
            &headers(&[("cookie", "taskito_csrf=abc")]),
            Some(session("abc")),
        );
        assert!(!context.csrf_valid());

        // Attacker-seeded cookie that the session never issued.
        let context = RequestContext::new(&request, Some(session("different")));
        assert!(!context.csrf_valid());

        // No session at all.
        let context = RequestContext::new(&request, None);
        assert!(!context.csrf_valid());
    }

    #[test]
    fn the_session_token_comes_from_its_own_cookie() {
        let request = headers(&[("cookie", "other=1; taskito_session=tok; taskito_csrf=abc")]);
        assert_eq!(session_token(&request).as_deref(), Some("tok"));
        assert_eq!(session_token(&HeaderMap::new()), None);
    }
}
