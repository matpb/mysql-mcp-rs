//! Live checks for the log and activity tools against the MySQL servers in docker-compose.live.yml.
//! Run through scripts/live-test.sh. Drives the real router in-process, as tests/mcp_http.rs does.

use std::process::ExitCode;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::Router;
use axum::body::Body;
use axum::http::Request;
use chrono::{DateTime, NaiveDateTime, Utc};
use http_body_util::BodyExt;
use mysql_mcp::build_router;
use mysql_mcp::config::{Config, DatabaseConfig, SslMode};
use mysql_mcp::db::PoolManager;
use serde_json::{Value, json};
use sqlx::Connection;
use sqlx::mysql::{MySqlConnectOptions, MySqlConnection, MySqlSslMode};
use tower::ServiceExt;

/// Test-only password, matching docker/live-init/01-live-fixtures.sql.
const PASSWORD: &str = "live-test-only";
const HOST: &str = "127.0.0.1";
/// Name the tools address the database by; the schema behind it is `app`.
const DB: &str = "live";
const DEFAULT_PORTS: &str = "13306,13307,13308";
/// Largest allowed gap between a started_at value and the host clock.
const CLOCK_TOLERANCE_MS: i64 = 10_000;
const SOURCES: [&str; 6] = [
    "error_log",
    "slow_log",
    "general_log",
    "recent_statements",
    "top_queries",
    "error_summary",
];

struct Server {
    version: &'static str,
    port: u16,
}

const SERVERS: [Server; 3] = [
    Server {
        version: "8.4",
        port: 13306,
    },
    Server {
        version: "8.0",
        port: 13307,
    },
    Server {
        version: "5.7",
        port: 13308,
    },
];

type Verdict = Result<(), String>;

#[derive(Default)]
struct Report {
    failures: usize,
}

impl Report {
    fn record(&mut self, name: &str, verdict: Verdict) {
        match verdict {
            Ok(()) => println!("PASS {name}"),
            Err(detail) => {
                self.failures += 1;
                println!("FAIL {name}: {detail}");
            }
        }
    }
}

fn want(cond: bool, detail: impl FnOnce() -> String) -> Verdict {
    if cond { Ok(()) } else { Err(detail()) }
}

fn label(tag: &str, check: &str) -> String {
    format!("{tag} {check}")
}

/// A JSON-RPC error, or a tool result flagged isError (code 0).
#[derive(Debug)]
struct RpcError {
    code: i64,
    message: String,
}

type Reply = Result<Value, RpcError>;

/// One router over one database config, driven like tests/mcp_http.rs.
struct Client {
    app: Router,
    next_id: u64,
}

impl Client {
    async fn connect(port: u16, user: &str) -> Self {
        let db = DatabaseConfig {
            name: DB.into(),
            host: HOST.into(),
            port,
            user: user.into(),
            password: PASSWORD.into(),
            database: "app".into(),
            max_connections: 5,
            query_timeout_secs: 30,
            ssl_mode: SslMode::Required,
        };
        let cfg = Config::from_parts(HOST, 0, vec![db], 1000);
        let pools = Arc::new(PoolManager::new(&cfg).await);
        Self {
            app: build_router(&cfg, pools),
            next_id: 0,
        }
    }

    async fn call(&mut self, tool: &str, arguments: Value) -> Reply {
        self.next_id += 1;
        let body = json!({
            "jsonrpc": "2.0",
            "id": self.next_id,
            "method": "tools/call",
            "params": {"name": tool, "arguments": arguments},
        });
        let req = Request::post("/mcp")
            .header("content-type", "application/json")
            .header("accept", "application/json, text/event-stream")
            .header("host", "localhost")
            .body(Body::from(body.to_string()))
            .expect("static request parts are valid");
        let resp = self
            .app
            .clone()
            .oneshot(req)
            .await
            .expect("router is infallible");
        let bytes = resp
            .into_body()
            .collect()
            .await
            .expect("body is readable")
            .to_bytes();
        let reply: Value = match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(e) => {
                return Err(RpcError {
                    code: 0,
                    message: format!("reply is not JSON: {e}"),
                });
            }
        };
        if let Some(err) = reply.get("error") {
            return Err(RpcError {
                code: err["code"].as_i64().unwrap_or_default(),
                message: err["message"].as_str().unwrap_or_default().to_owned(),
            });
        }
        let text = reply["result"]["content"][0]["text"]
            .as_str()
            .unwrap_or_default();
        if reply["result"]["isError"] == true {
            return Err(RpcError {
                code: 0,
                message: text.to_owned(),
            });
        }
        serde_json::from_str(text).map_err(|e| RpcError {
            code: 0,
            message: format!("tool output is not JSON ({e}): {text}"),
        })
    }
}

fn payload(reply: &Reply) -> Result<&Value, String> {
    reply
        .as_ref()
        .map_err(|e| format!("JSON-RPC error {}: {}", e.code, e.message))
}

/// Tool payload with success:true.
fn success_of(reply: &Reply) -> Result<&Value, String> {
    let v = payload(reply)?;
    want(v["success"] == true, || format!("success is not true: {v}"))?;
    Ok(v)
}

/// Tool payload with success:false.
fn failure_of(reply: &Reply) -> Result<&Value, String> {
    let v = payload(reply)?;
    want(v["success"] == false, || {
        format!("expected success:false, got: {v}")
    })?;
    Ok(v)
}

/// invalid_params (-32602) whose message names `param`; an unknown tool is also -32602.
fn expect_invalid_params(reply: &Reply, param: &str) -> Verdict {
    match reply {
        Err(e) => {
            want(e.code == -32602, || {
                format!(
                    "JSON-RPC code {} ({}), want -32602 invalid_params",
                    e.code, e.message
                )
            })?;
            want(e.message.contains(param), || {
                format!(
                    "invalid_params message does not name `{param}`: {}",
                    e.message
                )
            })
        }
        Ok(v) => Err(format!("expected invalid_params, got a result: {v}")),
    }
}

fn rows(v: &Value) -> Result<&Vec<Value>, String> {
    v["data"]
        .as_array()
        .ok_or_else(|| format!("no data array in {v}"))
}

fn first_row(v: &Value) -> Result<&Value, String> {
    rows(v)?.first().ok_or_else(|| "no rows".to_owned())
}

fn row_count(v: &Value) -> i64 {
    v["rowCount"].as_i64().unwrap_or(-1)
}

fn sources(v: &Value) -> Result<&Vec<Value>, String> {
    v["sources"]
        .as_array()
        .ok_or_else(|| format!("no sources array in {v}"))
}

fn source<'a>(v: &'a Value, name: &str) -> Result<&'a Value, String> {
    sources(v)?
        .iter()
        .find(|s| s["source"] == name)
        .ok_or_else(|| format!("no {name} entry"))
}

fn non_empty(v: &Value) -> bool {
    v.as_array().is_some_and(|a| !a.is_empty())
}

/// True when a string, or any string in a list, contains `needle`.
fn mentions(v: &Value, needle: &str) -> bool {
    match v {
        Value::String(s) => s.contains(needle),
        Value::Array(items) => items
            .iter()
            .any(|i| i.as_str().is_some_and(|s| s.contains(needle))),
        _ => false,
    }
}

/// DECIMAL and big integers arrive as strings; accept both forms.
fn as_number(v: &Value) -> Option<f64> {
    v.as_f64().or_else(|| v.as_str()?.parse().ok())
}

fn as_integer(v: &Value) -> Option<i64> {
    v.as_i64().or_else(|| v.as_str()?.parse().ok())
}

fn number_field(row: &Value, key: &str) -> Result<f64, String> {
    as_number(&row[key]).ok_or_else(|| format!("{key} is not a number: {}", row[key]))
}

fn non_increasing(values: &[f64]) -> bool {
    values.windows(2).all(|w| w[0] >= w[1])
}

fn started_at(row: &Value) -> Result<&str, String> {
    row["started_at"]
        .as_str()
        .ok_or_else(|| format!("started_at is not a string: {}", row["started_at"]))
}

/// Accepts the DATE_FORMAT string (UTC, trailing Z) or RFC 3339, and compares with host UTC.
fn within_clock_tolerance(raw: &str) -> Verdict {
    let parsed = DateTime::parse_from_rfc3339(raw)
        .map(|d| d.with_timezone(&Utc).naive_utc())
        .or_else(|_| {
            NaiveDateTime::parse_from_str(
                raw.strip_suffix('Z').unwrap_or(raw),
                "%Y-%m-%d %H:%M:%S%.f",
            )
        })
        .map_err(|e| format!("started_at `{raw}` is not a timestamp: {e}"))?;
    let drift_ms = (Utc::now().naive_utc() - parsed).num_milliseconds().abs();
    want(drift_ms <= CLOCK_TOLERANCE_MS, || {
        format!("started_at `{raw}` is {drift_ms} ms from host UTC time")
    })
}

/// Two distinct tokens: `livecheck-<a>` for the marker statement, `livecheck_missing_<b>` for the table.
fn fresh_tokens() -> (String, String) {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos() as u64);
    // An odd multiplier is a bijection on u64, so the second token never equals the first.
    let second = nanos.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (
        format!("livecheck-{nanos:016x}"),
        format!("livecheck_missing_{second:016x}"),
    )
}

/// The marker SELECT, run as mcp_full. Its statement text contains `marker`.
async fn run_marker(client: &mut Client, marker: &str) -> Reply {
    client
        .call(
            "execute_query",
            json!({"database": DB, "query": format!("SELECT '{marker}' AS m")}),
        )
        .await
}

/// The failing SELECT (MySQL error 1146), run as mcp_full.
async fn run_missing(client: &mut Client, missing: &str) -> Reply {
    client
        .call(
            "execute_query",
            json!({"database": DB, "query": format!("SELECT * FROM {missing}")}),
        )
        .await
}

/// A session of its own as mcp_full, like application traffic. Pool traffic from the tools
/// would evict its rows from the 10-row per-thread history.
async fn open_session(port: u16) -> Result<MySqlConnection, String> {
    let opts = MySqlConnectOptions::new()
        .host(HOST)
        .port(port)
        .username("mcp_full")
        .password(PASSWORD)
        .database("app")
        .ssl_mode(MySqlSslMode::Required);
    MySqlConnection::connect_with(&opts)
        .await
        .map_err(|e| format!("cannot open an mcp_full session on port {port}: {e}"))
}

/// Runs one statement on the session. Its outcome is not inspected: the activity check reads
/// the history row, and the failing statement is expected to error.
async fn run_on_session(session: &mut MySqlConnection, sql: &str) {
    let _ = sqlx::query(sql).execute(session).await;
}

async fn close_session(session: MySqlConnection) {
    if let Err(e) = session.close().await {
        println!("INFO closing the mcp_full session failed: {e}");
    }
}

fn verdict_marker(reply: &Reply, marker: &str) -> Verdict {
    let v = success_of(reply)?;
    want(first_row(v)?["m"] == marker, || {
        format!(
            "m is {}",
            first_row(v).map_or(Value::Null, |r| r["m"].clone())
        )
    })
}

fn verdict_missing_table(reply: &Reply) -> Verdict {
    let v = failure_of(reply)?;
    want(mentions(&v["error"], "MySQL error 1146"), || {
        format!("error is {}", v["error"])
    })
}

fn verdict_widgets(reply: &Reply) -> Verdict {
    let v = success_of(reply)?;
    want(
        v["data"]
            == json!([
                {"id": 1, "name": "alpha"},
                {"id": 2, "name": "beta"},
                {"id": 3, "name": "gamma"}
            ]),
        || format!("data is {}", v["data"]),
    )
}

fn verdict_log_sources_ready(reply: &Reply) -> Verdict {
    let v = success_of(reply)?;
    let names: Vec<&str> = sources(v)?
        .iter()
        .filter_map(|s| s["source"].as_str())
        .collect();
    want(names == SOURCES, || format!("sources are {names:?}"))?;
    for s in sources(v)? {
        want(s["available"] == true && s["readable"] == true, || {
            format!(
                "{} available={} readable={}",
                s["source"], s["available"], s["readable"]
            )
        })?;
    }
    for name in ["slow_log", "general_log"] {
        let entry = source(v, name)?;
        want(entry["enabled"] == true, || {
            format!("{name} enabled={}", entry["enabled"])
        })?;
    }
    let error_log = source(v, "error_log")?;
    want(
        error_log["how_to_enable"]
            .as_array()
            .is_some_and(|a| a.is_empty()),
        || format!("error_log how_to_enable is {}", error_log["how_to_enable"]),
    )
}

fn verdict_error_log_columns(reply: &Reply) -> Verdict {
    let v = success_of(reply)?;
    want(row_count(v) >= 1, || {
        format!("rowCount is {}", row_count(v))
    })?;
    let columns: Vec<&str> = v["columns"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|c| c["name"].as_str())
        .collect();
    for needed in ["LOGGED", "PRIO", "DATA"] {
        want(columns.contains(&needed), || {
            format!("columns are {columns:?}, missing {needed}")
        })?;
    }
    Ok(())
}

fn verdict_all_prio_system(reply: &Reply) -> Verdict {
    let v = success_of(reply)?;
    want(row_count(v) >= 1, || {
        format!("rowCount is {}", row_count(v))
    })?;
    for row in rows(v)? {
        want(row["PRIO"] == "System", || {
            format!("PRIO is {}", row["PRIO"])
        })?;
    }
    Ok(())
}

/// rowCount >= 1 and some row's `field` contains the marker.
fn verdict_has_rows(reply: &Reply) -> Verdict {
    let v = success_of(reply)?;
    want(row_count(v) >= 1, || {
        format!("rowCount is {}", row_count(v))
    })
}

fn verdict_contains(reply: &Reply, field: &str, marker: &str) -> Verdict {
    let v = success_of(reply)?;
    want(row_count(v) >= 1, || {
        format!("rowCount is {}", row_count(v))
    })?;
    want(
        rows(v)?
            .iter()
            .any(|row| row[field].as_str().is_some_and(|s| s.contains(marker))),
        || format!("no {field} contains the marker"),
    )
}

/// Some row's SQL_TEXT contains the marker, and that row started within the clock tolerance.
fn verdict_marker_row(reply: &Reply, marker: &str) -> Verdict {
    let v = success_of(reply)?;
    let row = rows(v)?
        .iter()
        .find(|r| r["SQL_TEXT"].as_str().is_some_and(|s| s.contains(marker)))
        .ok_or_else(|| {
            format!(
                "no row of {} has SQL_TEXT containing the marker",
                row_count(v)
            )
        })?;
    within_clock_tolerance(started_at(row)?)
}

/// Some row has MYSQL_ERRNO 1146.
fn verdict_errno_1146(reply: &Reply) -> Verdict {
    let v = success_of(reply)?;
    want(
        rows(v)?
            .iter()
            .any(|row| as_integer(&row["MYSQL_ERRNO"]) == Some(1146)),
        || format!("no row of {} has MYSQL_ERRNO 1146", row_count(v)),
    )
}

fn verdict_top_queries_limit(reply: &Reply) -> Verdict {
    let v = success_of(reply)?;
    want(row_count(v) <= 5, || {
        format!("rowCount is {}", row_count(v))
    })?;
    let totals = rows(v)?
        .iter()
        .map(|row| number_field(row, "total_ms"))
        .collect::<Result<Vec<f64>, String>>()?;
    want(non_increasing(&totals), || {
        format!("total_ms is not non-increasing: {totals:?}")
    })
}

fn verdict_exec_count_sorted(reply: &Reply) -> Verdict {
    let v = success_of(reply)?;
    let counts = rows(v)?
        .iter()
        .map(|row| number_field(row, "exec_count"))
        .collect::<Result<Vec<f64>, String>>()?;
    want(non_increasing(&counts), || {
        format!("exec_count is not non-increasing: {counts:?}")
    })
}

fn verdict_error_summary(reply: &Reply) -> Verdict {
    let v = success_of(reply)?;
    want(
        rows(v)?
            .iter()
            .any(|row| row["ERROR_NAME"] == "ER_NO_SUCH_TABLE"),
        || "no ER_NO_SUCH_TABLE row".to_owned(),
    )
}

fn verdict_limit_one(reply: &Reply) -> Verdict {
    let v = success_of(reply)?;
    want(row_count(v) == 1, || {
        format!("rowCount is {}", row_count(v))
    })?;
    want(v["truncated"] == true, || {
        format!("truncated is {}", v["truncated"])
    })
}

fn verdict_min_log_sources(reply: &Reply) -> Verdict {
    let v = success_of(reply)?;
    let error_log = source(v, "error_log")?;
    want(error_log["readable"] == false, || {
        format!("error_log readable is {}", error_log["readable"])
    })?;
    want(
        mentions(
            &error_log["how_to_enable"],
            "GRANT SELECT ON performance_schema.error_log TO 'mcp_min'@'%'",
        ),
        || format!("how_to_enable is {}", error_log["how_to_enable"]),
    )
}

fn verdict_min_error_log(reply: &Reply) -> Verdict {
    let v = failure_of(reply)?;
    want(non_empty(&v["how_to_enable"]), || {
        format!("how_to_enable is empty: {v}")
    })
}

fn verdict_min_top_queries(reply: &Reply) -> Verdict {
    let v = failure_of(reply)?;
    want(
        mentions(&v["how_to_enable"], "events_statements_summary_by_digest"),
        || format!("how_to_enable is {}", v["how_to_enable"]),
    )
}

fn verdict_57_log_sources(reply: &Reply) -> Verdict {
    let v = success_of(reply)?;
    let error_log = source(v, "error_log")?;
    want(error_log["available"] == false, || {
        format!("error_log available is {}", error_log["available"])
    })?;
    want(mentions(&error_log["detail"], "8.0.22"), || {
        format!("error_log detail is {}", error_log["detail"])
    })?;
    let summary = source(v, "error_summary")?;
    want(summary["available"] == false, || {
        format!("error_summary available is {}", summary["available"])
    })
}

fn verdict_57_error_log(reply: &Reply) -> Verdict {
    let v = failure_of(reply)?;
    want(mentions(&v["error"], "8.0.22"), || {
        format!("error is {}", v["error"])
    })
}

fn verdict_57_error_summary(reply: &Reply) -> Verdict {
    let v = failure_of(reply)?;
    want(mentions(&v["error"], "8.0"), || {
        format!("error is {}", v["error"])
    })
}

/// 5.7 records no text for binary-protocol statements, so only success and the approximate note are asserted.
fn verdict_recent_57(reply: &Reply) -> Verdict {
    let v = success_of(reply)?;
    want(mentions(&v["note"], "approximate"), || {
        format!("note lacks 'approximate': {}", v["note"])
    })
}

async fn check_modern(
    r: &mut Report,
    tag: &str,
    c: &mut Client,
    port: u16,
    marker: &str,
    missing: &str,
) {
    let reply = c.call("log_sources", json!({"database": DB})).await;
    r.record(
        &label(
            tag,
            "1 log_sources: all sources available and readable, logs enabled",
        ),
        verdict_log_sources_ready(&reply),
    );

    let reply = c
        .call(
            "read_server_log",
            json!({"database": DB, "source": "error"}),
        )
        .await;
    r.record(
        &label(
            tag,
            "2 read_server_log error: rows and LOGGED/PRIO/DATA columns",
        ),
        verdict_error_log_columns(&reply),
    );

    let reply = c
        .call(
            "read_server_log",
            json!({"database": DB, "source": "error", "min_priority": "System"}),
        )
        .await;
    r.record(
        &label(
            tag,
            "3 read_server_log error min_priority=System: every PRIO is System",
        ),
        verdict_all_prio_system(&reply),
    );

    let reply = c
        .call(
            "read_server_log",
            json!({"database": DB, "source": "general", "contains": marker}),
        )
        .await;
    r.record(
        &label(tag, "4 read_server_log general contains marker"),
        verdict_contains(&reply, "argument", marker),
    );

    let reply = c
        .call(
            "read_server_log",
            json!({"database": DB, "source": "slow", "contains": marker}),
        )
        .await;
    r.record(
        &label(tag, "5 read_server_log slow contains marker"),
        verdict_contains(&reply, "sql_text", marker),
    );

    let since = c
        .call(
            "read_server_log",
            json!({"database": DB, "source": "general", "since": "5m"}),
        )
        .await;
    r.record(
        &label(tag, "6a read_server_log general since=5m succeeds"),
        success_of(&since).map(|_| ()),
    );
    let inverted = c
        .call(
            "read_server_log",
            json!({"database": DB, "source": "general", "since": "5m", "until": "10m"}),
        )
        .await;
    r.record(
        &label(
            tag,
            "6b read_server_log until earlier than since is invalid_params",
        ),
        expect_invalid_params(&inverted, "until"),
    );

    let by_user = c
        .call(
            "read_server_log",
            json!({"database": DB, "source": "general", "user": "mcp_full"}),
        )
        .await;
    r.record(
        &label(tag, "6c read_server_log general user=mcp_full has rows"),
        verdict_has_rows(&by_user),
    );

    let zero = c
        .call(
            "read_server_log",
            json!({"database": DB, "source": "error", "limit": 0}),
        )
        .await;
    r.record(
        &label(tag, "6d read_server_log limit=0 is invalid_params"),
        expect_invalid_params(&zero, "limit"),
    );

    // Checks 7 and 8 share one session, held open until both have run.
    match open_session(port).await {
        Ok(mut session) => {
            run_on_session(&mut session, &format!("SELECT '{marker}' AS m")).await;
            let reply = c
                .call(
                    "query_activity",
                    json!({"database": DB, "view": "recent_statements", "contains": marker}),
                )
                .await;
            r.record(
                &label(
                    tag,
                    "7 query_activity recent_statements: a row contains the marker, started within 10 s",
                ),
                verdict_marker_row(&reply, marker),
            );

            run_on_session(&mut session, &format!("SELECT * FROM {missing}")).await;
            let reply = c
                .call(
                    "query_activity",
                    json!({"database": DB, "view": "recent_statements", "errors_only": true, "contains": missing}),
                )
                .await;
            r.record(
                &label(
                    tag,
                    "8 query_activity recent_statements errors_only: a row has MYSQL_ERRNO 1146",
                ),
                verdict_errno_1146(&reply),
            );
            close_session(session).await;
        }
        Err(e) => {
            r.record(
                &label(tag, "7 query_activity recent_statements: session"),
                Err(e.clone()),
            );
            r.record(
                &label(tag, "8 query_activity recent_statements: session"),
                Err(e),
            );
        }
    }

    let reply = c
        .call(
            "query_activity",
            json!({"database": DB, "view": "top_queries", "limit": 5}),
        )
        .await;
    r.record(
        &label(
            tag,
            "9 query_activity top_queries limit=5: at most 5 rows, total_ms sorted",
        ),
        verdict_top_queries_limit(&reply),
    );

    let reply = c
        .call(
            "query_activity",
            json!({"database": DB, "view": "top_queries", "order_by": "count"}),
        )
        .await;
    r.record(
        &label(
            tag,
            "10 query_activity top_queries order_by=count: exec_count sorted",
        ),
        verdict_exec_count_sorted(&reply),
    );

    let reply = c
        .call(
            "query_activity",
            json!({"database": DB, "view": "error_summary"}),
        )
        .await;
    r.record(
        &label(
            tag,
            "11 query_activity error_summary: ER_NO_SUCH_TABLE present",
        ),
        verdict_error_summary(&reply),
    );

    let reply = c
        .call(
            "read_server_log",
            json!({"database": DB, "source": "error", "limit": 1}),
        )
        .await;
    r.record(
        &label(tag, "12 read_server_log error limit=1: one row, truncated"),
        verdict_limit_one(&reply),
    );

    let reply = c
        .call(
            "read_server_log",
            json!({"database": DB, "source": "slow", "min_priority": "Error"}),
        )
        .await;
    r.record(
        &label(
            tag,
            "13a read_server_log slow min_priority=Error is invalid_params",
        ),
        expect_invalid_params(&reply, "min_priority"),
    );

    let reply = c
        .call(
            "query_activity",
            json!({"database": DB, "view": "recent_statements", "order_by": "count"}),
        )
        .await;
    r.record(
        &label(
            tag,
            "13b query_activity recent_statements order_by=count is invalid_params",
        ),
        expect_invalid_params(&reply, "order_by"),
    );
}

async fn check_min(r: &mut Report, tag: &str, c: &mut Client) {
    let reply = c.call("log_sources", json!({"database": DB})).await;
    r.record(
        &label(
            tag,
            "14 mcp_min log_sources: error_log unreadable, GRANT suggested",
        ),
        verdict_min_log_sources(&reply),
    );

    let reply = c
        .call(
            "read_server_log",
            json!({"database": DB, "source": "error"}),
        )
        .await;
    r.record(
        &label(
            tag,
            "15 mcp_min read_server_log error: success:false with how_to_enable",
        ),
        verdict_min_error_log(&reply),
    );

    let reply = c
        .call(
            "query_activity",
            json!({"database": DB, "view": "top_queries"}),
        )
        .await;
    r.record(
        &label(
            tag,
            "16 mcp_min query_activity top_queries: how_to_enable names the digest table",
        ),
        verdict_min_top_queries(&reply),
    );
}

async fn check_57(r: &mut Report, tag: &str, c: &mut Client, port: u16, marker: &str) {
    let by_user = c
        .call(
            "read_server_log",
            json!({"database": DB, "source": "general", "user": "mcp_full"}),
        )
        .await;
    r.record(
        &label(tag, "6c read_server_log general user=mcp_full has rows"),
        verdict_has_rows(&by_user),
    );

    let reply = c.call("log_sources", json!({"database": DB})).await;
    r.record(
        &label(
            tag,
            "17 log_sources: error_log and error_summary unavailable, 8.0.22 cited",
        ),
        verdict_57_log_sources(&reply),
    );

    let reply = c
        .call(
            "read_server_log",
            json!({"database": DB, "source": "error"}),
        )
        .await;
    r.record(
        &label(tag, "18 read_server_log error: success:false citing 8.0.22"),
        verdict_57_error_log(&reply),
    );

    let reply = c
        .call(
            "read_server_log",
            json!({"database": DB, "source": "general", "contains": marker}),
        )
        .await;
    r.record(
        &label(tag, "19 read_server_log general contains marker"),
        verdict_contains(&reply, "argument", marker),
    );

    let session = open_session(port).await;
    match session {
        Ok(mut session) => {
            run_on_session(&mut session, &format!("SELECT '{marker}' AS m")).await;
            let reply = c
                .call(
                    "query_activity",
                    json!({"database": DB, "view": "recent_statements", "contains": marker}),
                )
                .await;
            match payload(&reply) {
                Ok(v) => println!("INFO {tag} recent_statements rowCount={}", row_count(v)),
                Err(e) => println!("INFO {tag} recent_statements: {e}"),
            }
            r.record(
                &label(
                    tag,
                    "20 query_activity recent_statements: success with approximate note",
                ),
                verdict_recent_57(&reply),
            );
            close_session(session).await;
        }
        Err(e) => r.record(
            &label(tag, "20 query_activity recent_statements: session"),
            Err(e),
        ),
    }

    let reply = c
        .call(
            "query_activity",
            json!({"database": DB, "view": "error_summary"}),
        )
        .await;
    r.record(
        &label(
            tag,
            "21 query_activity error_summary: success:false citing 8.0",
        ),
        verdict_57_error_summary(&reply),
    );
}

async fn run_server(r: &mut Report, server: &Server) {
    let tag = format!("MySQL {}", server.version);
    let mut full = Client::connect(server.port, "mcp_full").await;
    let (marker, missing) = fresh_tokens();

    let marker_reply = run_marker(&mut full, &marker).await;
    let missing_reply = run_missing(&mut full, &missing).await;
    r.record(
        &label(&tag, "setup marker SELECT runs through execute_query"),
        verdict_marker(&marker_reply, &marker),
    );
    r.record(
        &label(&tag, "setup failing SELECT is MySQL error 1146"),
        verdict_missing_table(&missing_reply),
    );

    let widgets = full
        .call(
            "execute_query",
            json!({"database": DB, "query": "SELECT id, name FROM widgets ORDER BY id"}),
        )
        .await;
    r.record(
        &label(&tag, "setup execute_query returns the widgets rows"),
        verdict_widgets(&widgets),
    );

    match server.version {
        "5.7" => check_57(r, &tag, &mut full, server.port, &marker).await,
        version => {
            check_modern(r, &tag, &mut full, server.port, &marker, &missing).await;
            if version == "8.4" {
                let mut min = Client::connect(server.port, "mcp_min").await;
                check_min(r, &tag, &mut min).await;
            }
        }
    }
}

/// LIVE_PORTS (comma list) selects servers; each entry must be one of the rig ports.
fn selected_ports() -> Result<Vec<u16>, String> {
    let raw = match std::env::var("LIVE_PORTS") {
        Ok(v) => v,
        Err(std::env::VarError::NotPresent) => DEFAULT_PORTS.to_owned(),
        Err(e) => return Err(format!("LIVE_PORTS: {e}")),
    };
    raw.split(',')
        .map(|entry| {
            let port: u16 = entry
                .trim()
                .parse()
                .map_err(|e| format!("LIVE_PORTS entry `{entry}`: {e}"))?;
            if SERVERS.iter().any(|s| s.port == port) {
                Ok(port)
            } else {
                Err(format!("LIVE_PORTS entry {port} is not a rig port"))
            }
        })
        .collect()
}

#[tokio::main]
async fn main() -> ExitCode {
    let ports = match selected_ports() {
        Ok(ports) => ports,
        Err(e) => {
            eprintln!("live_check: {e}");
            return ExitCode::from(2);
        }
    };

    let mut report = Report::default();
    for server in &SERVERS {
        if ports.contains(&server.port) {
            run_server(&mut report, server).await;
        } else {
            println!(
                "INFO skip MySQL {} (port {} not in LIVE_PORTS)",
                server.version, server.port
            );
        }
    }

    if report.failures == 0 {
        println!("RESULT all checks passed");
        ExitCode::SUCCESS
    } else {
        println!("RESULT {} check(s) failed", report.failures);
        ExitCode::FAILURE
    }
}
