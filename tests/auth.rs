//! Integration tests for API key authentication on the HTTP transport.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use mysql_mcp::build_router;
use mysql_mcp::config::Config;
use mysql_mcp::db::PoolManager;
use tower::ServiceExt;

async fn router(api_keys: Vec<String>) -> axum::Router {
    let mut cfg = Config::from_parts("127.0.0.1", 0, vec![], 1000);
    cfg.api_keys = api_keys;
    let pool_manager = std::sync::Arc::new(PoolManager::new(&cfg).await);
    build_router(&cfg, pool_manager)
}

fn mcp_request(headers: &[(&str, &str)]) -> Request<Body> {
    let mut builder = Request::post("/mcp")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream");
    for (name, value) in headers {
        builder = builder.header(*name, *value);
    }
    builder
        .body(Body::from(r#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#))
        .unwrap()
}

async fn body_string(resp: axum::response::Response) -> String {
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

#[tokio::test]
async fn no_keys_configured_allows_request_through() {
    let app = router(vec![]).await;
    let resp = app.oneshot(mcp_request(&[])).await.unwrap();
    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn keys_configured_no_header_is_unauthorized() {
    let app = router(vec!["secret".into()]).await;
    let resp = app.oneshot(mcp_request(&[])).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    let www_auth = resp
        .headers()
        .get("WWW-Authenticate")
        .map(|v| v.to_str().unwrap().to_string());
    assert_eq!(www_auth.as_deref(), Some("Bearer"));
    let body = body_string(resp).await;
    assert!(body.contains("\"error\":\"unauthorized\""));
}

#[tokio::test]
async fn keys_configured_wrong_key_is_unauthorized() {
    let app = router(vec!["secret".into()]).await;
    let resp = app
        .oneshot(mcp_request(&[("x-api-key", "wrong")]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn keys_configured_correct_x_api_key_passes() {
    let app = router(vec!["secret".into()]).await;
    let resp = app
        .oneshot(mcp_request(&[("x-api-key", "secret")]))
        .await
        .unwrap();
    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn keys_configured_correct_bearer_passes() {
    let app = router(vec!["secret".into()]).await;
    let resp = app
        .oneshot(mcp_request(&[("authorization", "Bearer secret")]))
        .await
        .unwrap();
    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn keys_configured_second_of_two_passes() {
    let app = router(vec!["one".into(), "two".into()]).await;
    let resp = app
        .oneshot(mcp_request(&[("x-api-key", "two")]))
        .await
        .unwrap();
    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn keys_configured_health_stays_open() {
    let app = router(vec!["secret".into()]).await;
    let resp = app
        .oneshot(Request::get("/health").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_string(resp).await;
    assert_eq!(body, "ok");
}

#[tokio::test]
async fn keys_configured_key_with_whitespace_is_unauthorized() {
    let app = router(vec!["secret".into()]).await;
    let resp = app
        .oneshot(mcp_request(&[("x-api-key", " secret ")]))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}
