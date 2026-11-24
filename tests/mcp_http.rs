//! Integration tests for the Streamable HTTP MCP transport: Host handling, response framing and the resource surface.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use http_body_util::BodyExt;
use mysql_mcp::build_router;
use mysql_mcp::config::{Config, load_databases_from_json};
use mysql_mcp::db::PoolManager;
use serde_json::{Value, json};
use tower::ServiceExt;

/// One database whose pool connects lazily, so no MySQL connection is ever attempted.
fn router(api_keys: Vec<String>) -> axum::Router {
    let dbs = load_databases_from_json(
        r#"[{"name":"shop","host":"127.0.0.1","port":1,"user":"u","password":"p","database":"shop_schema"}]"#,
    )
    .unwrap();
    let mut cfg = Config::from_parts("127.0.0.1", 0, dbs, 1000);
    cfg.api_keys = api_keys;
    let pool_manager = std::sync::Arc::new(PoolManager::new_lazy(&cfg));
    build_router(&cfg, pool_manager)
}

struct Reply {
    status: StatusCode,
    content_type: String,
    body: Value,
}

async fn post_mcp(host: &str, payload: Value) -> Reply {
    post_mcp_with_key(vec![], host, None, payload).await
}

async fn post_mcp_with_key(
    api_keys: Vec<String>,
    host: &str,
    api_key: Option<&str>,
    payload: Value,
) -> Reply {
    let mut builder = Request::post("/mcp")
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("host", host);
    if let Some(key) = api_key {
        builder = builder.header("x-api-key", key);
    }
    let req = builder.body(Body::from(payload.to_string())).unwrap();
    let resp = router(api_keys).oneshot(req).await.unwrap();
    let status = resp.status();
    let content_type = resp
        .headers()
        .get("content-type")
        .map(|v| v.to_str().unwrap().to_string())
        .unwrap_or_default();
    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    Reply {
        status,
        content_type,
        body,
    }
}

#[tokio::test]
async fn non_loopback_host_accepted_with_api_key() {
    let key = "k".repeat(32);
    let reply = post_mcp_with_key(
        vec![key.clone()],
        "mysql-mcp.example.run.app",
        Some(&key),
        json!({"jsonrpc":"2.0","id":1,"method":"ping"}),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK);
}

#[tokio::test]
async fn non_loopback_host_rejected_without_api_keys() {
    let reply = post_mcp_with_key(
        vec![],
        "mysql-mcp.example.run.app",
        None,
        json!({"jsonrpc":"2.0","id":1,"method":"ping"}),
    )
    .await;
    assert_eq!(reply.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn loopback_host_accepted_without_api_keys() {
    let reply = post_mcp("localhost", json!({"jsonrpc":"2.0","id":1,"method":"ping"})).await;
    assert_eq!(reply.status, StatusCode::OK);
}

#[tokio::test]
async fn responses_are_plain_json_not_event_stream() {
    let reply = post_mcp("localhost", json!({"jsonrpc":"2.0","id":1,"method":"ping"})).await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(reply.content_type, "application/json");
    assert_eq!(reply.body, json!({"jsonrpc":"2.0","id":1,"result":{}}));
}

#[tokio::test]
async fn initialize_advertises_tools_and_resources_only() {
    let reply = post_mcp(
        "localhost",
        json!({"jsonrpc":"2.0","id":2,"method":"initialize","params":{
            "protocolVersion":"2025-06-18","capabilities":{},
            "clientInfo":{"name":"test","version":"0"}}}),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK);
    let result = &reply.body["result"];
    assert_eq!(result["protocolVersion"], "2025-06-18");
    assert_eq!(result["capabilities"], json!({"tools":{},"resources":{}}));
    assert_eq!(result["serverInfo"]["name"], "mysql-mcp");
    assert_eq!(result["serverInfo"]["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(
        result["instructions"],
        "MySQL MCP Server — read-only access to 1 database(s): shop. Use list_databases to discover available databases. All query tools require a 'database' parameter. Use show_tables to list tables, describe_table for schema, execute_query for SQL. execute_query's description documents how values are encoded (integers past 2^53-1 and DECIMALs are strings, TIMESTAMPs end in Z, column order lives in columns[]); read it before interpreting a result. Resources are available at mysql://<database-name> for each configured database."
    );
}

#[tokio::test]
async fn tools_list_names_are_unchanged() {
    let reply = post_mcp(
        "localhost",
        json!({"jsonrpc":"2.0","id":7,"method":"tools/list"}),
    )
    .await;
    let names: Vec<&str> = reply.body["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "describe_table",
            "execute_query",
            "list_databases",
            "show_tables"
        ]
    );
}

#[tokio::test]
async fn resources_list_has_one_entry_per_database() {
    let reply = post_mcp(
        "localhost",
        json!({"jsonrpc":"2.0","id":3,"method":"resources/list"}),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(
        reply.body["result"]["resources"],
        json!([{
            "uri": "mysql://shop",
            "name": "shop",
            "description": "MySQL database 'shop' (schema: shop_schema)",
            "mimeType": "application/json"
        }])
    );
}

#[tokio::test]
async fn resources_read_returns_database_info_as_text() {
    let reply = post_mcp(
        "localhost",
        json!({"jsonrpc":"2.0","id":4,"method":"resources/read","params":{"uri":"mysql://shop"}}),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK);
    assert_eq!(
        reply.body["result"]["contents"],
        json!([{
            "uri": "mysql://shop",
            "mimeType": "text/plain",
            "text": "{\n  \"name\": \"shop\",\n  \"database\": \"shop_schema\",\n  \"query_timeout_secs\": 30,\n  \"usage\": \"Use this database name 'shop' as the 'database' parameter in show_tables, describe_table, and execute_query tools.\"\n}"
        }])
    );
}

#[tokio::test]
async fn resources_read_rejects_other_scheme() {
    let reply = post_mcp(
        "localhost",
        json!({"jsonrpc":"2.0","id":5,"method":"resources/read","params":{"uri":"http://shop"}}),
    )
    .await;
    assert_eq!(
        reply.body["error"],
        json!({"code": -32602, "message": "URI must start with mysql://"})
    );
}

#[tokio::test]
async fn resources_read_rejects_unknown_database() {
    let reply = post_mcp(
        "localhost",
        json!({"jsonrpc":"2.0","id":6,"method":"resources/read","params":{"uri":"mysql://nope"}}),
    )
    .await;
    assert_eq!(
        reply.body["error"],
        json!({"code": -32602, "message": "Unknown database 'nope'. Available: [\"shop\"]"})
    );
}
