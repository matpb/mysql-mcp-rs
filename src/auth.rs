use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use subtle::ConstantTimeEq;

/// Constant-time membership check; always false when `keys` is empty.
pub fn key_matches(keys: &[String], presented: &str) -> bool {
    let presented = presented.as_bytes();
    keys.iter().any(|k| {
        let k = k.as_bytes();
        k.len() == presented.len() && k.ct_eq(presented).into()
    })
}

/// Reads `X-API-Key`, then `Authorization: Bearer <token>`. Neither header value is trimmed.
pub fn presented_key(headers: &HeaderMap) -> Option<&str> {
    if let Some(v) = headers.get("x-api-key") {
        return v.to_str().ok();
    }
    let auth = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let (scheme, token) = auth.split_once(' ')?;
    if scheme.eq_ignore_ascii_case("bearer") {
        Some(token)
    } else {
        None
    }
}

fn unauthorized() -> Response {
    let body = json!({
        "error": "unauthorized",
        "message": "missing or invalid API key; send it in the X-API-Key header",
    });
    let mut resp = (StatusCode::UNAUTHORIZED, axum::Json(body)).into_response();
    resp.headers_mut()
        .insert("WWW-Authenticate", "Bearer".parse().unwrap());
    resp
}

pub async fn require_api_key(
    State(keys): State<Arc<Vec<String>>>,
    req: axum::extract::Request,
    next: Next,
) -> Response {
    if keys.is_empty() {
        return next.run(req).await;
    }

    let valid = presented_key(req.headers()).is_some_and(|k| key_matches(&keys, k));
    if !valid {
        tracing::warn!(
            "rejected unauthenticated request: {} {}",
            req.method(),
            req.uri().path()
        );
        return unauthorized();
    }

    next.run(req).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn key_matches_empty_keys_is_false() {
        assert!(!key_matches(&[], "anything"));
    }

    #[test]
    fn key_matches_exact_match() {
        let keys = vec!["abc".to_string()];
        assert!(key_matches(&keys, "abc"));
    }

    #[test]
    fn key_matches_wrong_key() {
        let keys = vec!["abc".to_string()];
        assert!(!key_matches(&keys, "xyz"));
    }

    #[test]
    fn key_matches_prefix_is_false() {
        let keys = vec!["abcdef".to_string()];
        assert!(!key_matches(&keys, "abc"));
    }

    #[test]
    fn key_matches_second_of_two() {
        let keys = vec!["one".to_string(), "two".to_string()];
        assert!(key_matches(&keys, "two"));
    }

    #[test]
    fn presented_key_from_x_api_key() {
        let mut headers = HeaderMap::new();
        headers.insert("x-api-key", HeaderValue::from_static("k1"));
        assert_eq!(presented_key(&headers), Some("k1"));
    }

    #[test]
    fn presented_key_from_bearer() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_static("Bearer k2"),
        );
        assert_eq!(presented_key(&headers), Some("k2"));
    }

    #[test]
    fn presented_key_bearer_case_insensitive() {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_static("bearer k3"),
        );
        assert_eq!(presented_key(&headers), Some("k3"));
    }

    #[test]
    fn presented_key_neither_header_present() {
        let headers = HeaderMap::new();
        assert_eq!(presented_key(&headers), None);
    }
}
