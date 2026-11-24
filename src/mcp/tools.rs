use std::sync::Arc;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    Implementation, ListResourcesResult, PaginatedRequestParams, ReadResourceRequestParams,
    ReadResourceResponse, ReadResourceResult, Resource, ResourceContents, ServerCapabilities,
    ServerConfig,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::{Value, json};
use sqlx::Row;
use sqlx::mysql::{MySqlPool, MySqlRow};

use crate::config::Config;
use crate::db::{PoolManager, ValueLimits, column_descriptors, encode_row, row_to_json};
use crate::logs::{
    ActivityQuery, ActivityView, Bind, LogQuery, LogSource, MinPriority, OrderBy, ServerFacts,
    activity_note, check_activity_query, check_log_query, error_log_sql, error_summary_sql,
    general_log_sql, grant_line, grant_target, parse_time_bound, parse_version,
    recent_statements_sql, set_line, slow_log_sql, top_queries_sql,
};
use crate::sanitizer;

/// MySQL's own error text, so a caller can correct the query without reading server logs.
fn db_error_message(e: &sqlx::Error) -> String {
    let Some(db_err) = e.as_database_error() else {
        return e.to_string();
    };
    let code = db_err
        .downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
        .number();
    match db_err.code() {
        Some(sqlstate) => format!("MySQL error {code} ({sqlstate}): {}", db_err.message()),
        None => format!("MySQL error {code}: {}", db_err.message()),
    }
}

/// MySQL error number, when the error came from the server.
fn mysql_errno(e: &sqlx::Error) -> Option<u16> {
    let db_err = e.as_database_error()?;
    Some(
        db_err
            .try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()?
            .number(),
    )
}

/// The MySQL error numbers that mean "this account lacks SELECT on the table".
fn is_missing_grant(e: &sqlx::Error) -> bool {
    matches!(mysql_errno(e), Some(1142 | 1143))
}

/// Success envelope for a row result, shared by execute_query and the log tools.
/// `rows` may hold one row past `cap`; that extra row is how truncation is seen.
fn rows_envelope(
    database: &str,
    mut rows: Vec<MySqlRow>,
    cap: u32,
    limits: &ValueLimits,
    elapsed: Duration,
) -> Value {
    // Also catches a caller-supplied LIMIT above the cap, and SHOW/EXPLAIN results,
    // which never get a LIMIT injected.
    let truncated = rows.len() > cap as usize;
    if truncated {
        rows.truncate(cap as usize);
    }

    let columns = rows.first().map(column_descriptors).unwrap_or_default();

    let mut decode_errors = 0usize;
    let mut truncated_values = 0usize;
    let data: Vec<Value> = rows
        .iter()
        .map(|row| {
            let encoded = encode_row(row, limits);
            decode_errors += encoded.decode_errors;
            truncated_values += encoded.truncated_values;
            Value::Object(encoded.values)
        })
        .collect();

    let row_count = data.len();
    let mut message = if truncated {
        format!("Query executed successfully. Capped at {cap} row(s); more rows matched.")
    } else {
        format!("Query executed successfully. {row_count} row(s) returned.")
    };
    if decode_errors > 0 {
        message.push_str(&format!(
            " {decode_errors} value(s) failed to decode and carry an __error__ object."
        ));
    }
    if truncated_values > 0 {
        message.push_str(&format!(
            " {truncated_values} value(s) exceeded the size cap and were truncated."
        ));
    }

    json!({
        "success": true,
        "database": database,
        "columns": columns,
        "data": data,
        "rowCount": row_count,
        "truncated": truncated,
        "decodeErrors": decode_errors,
        "truncatedValues": truncated_values,
        "executionTime": format!("{}ms", elapsed.as_millis()),
        "message": message,
    })
}

/// Why a log or activity query did not produce rows.
enum QueryFailure {
    Timeout,
    Db(sqlx::Error),
}

/// Runs one built query with its binds in order, under the database's timeout.
async fn fetch_rows(
    pool: &MySqlPool,
    timeout_secs: u64,
    sql: &str,
    binds: &[Bind],
) -> Result<Vec<MySqlRow>, QueryFailure> {
    let mut query = sqlx::query(sql);
    for bind in binds {
        query = match bind {
            Bind::Str(v) => query.bind(v.as_str()),
            Bind::U64(v) => query.bind(*v),
        };
    }
    match tokio::time::timeout(Duration::from_secs(timeout_secs), query.fetch_all(pool)).await {
        Ok(Ok(rows)) => Ok(rows),
        Ok(Err(e)) => Err(QueryFailure::Db(e)),
        Err(_) => Err(QueryFailure::Timeout),
    }
}

/// Failure envelope for the log and activity tools. `grants` are the GRANT lines for the
/// source; `missing` is the version message for a table this server does not have (1146).
fn failure_envelope(
    database: &str,
    failure: QueryFailure,
    timeout_secs: u64,
    grants: &[String],
    missing: Option<&str>,
) -> Value {
    match failure {
        QueryFailure::Timeout => json!({
            "success": false,
            "database": database,
            "error": format!("Query timed out after {timeout_secs}s"),
            "message": "Query execution timed out.",
        }),
        QueryFailure::Db(e) => {
            if let (Some(1146), Some(text)) = (mysql_errno(&e), missing) {
                return unsupported(database, text);
            }
            tracing::error!(database = %database, error = %e, "Log or activity query failed");
            let detail = db_error_message(&e);
            let mut out = json!({
                "success": false,
                "database": database,
                "error": detail,
                "message": format!("Query execution failed: {detail}"),
            });
            if is_missing_grant(&e) && !grants.is_empty() {
                out["how_to_enable"] = json!(grants);
            }
            out
        }
    }
}

/// Failure envelope for a source this server cannot read at all, with no query run.
fn unsupported(database: &str, text: &str) -> Value {
    json!({
        "success": false,
        "database": database,
        "error": text,
        "message": text,
    })
}

const PERFORMANCE_SCHEMA_OFF: &str = "performance_schema is OFF: set performance_schema=ON and restart (Cloud SQL: the performance_schema flag, off by default below 12 GB RAM).";

fn version_text(version: (u32, u32, u32)) -> String {
    format!("{}.{}.{}", version.0, version.1, version.2)
}

fn error_log_version_text(version: (u32, u32, u32)) -> String {
    format!(
        "performance_schema.error_log requires MySQL 8.0.22 or later; this server is {}.",
        version_text(version)
    )
}

fn error_summary_version_text(version: (u32, u32, u32)) -> String {
    format!(
        "events_errors_summary_global_by_error requires MySQL 8.0 or later; this server is {}.",
        version_text(version)
    )
}

/// Probes the server facts in one query, then the consumer switches. Nothing is cached.
async fn probe_facts(pool: &MySqlPool, timeout_secs: u64) -> Result<ServerFacts, QueryFailure> {
    let rows = fetch_rows(
        pool,
        timeout_secs,
        "SELECT VERSION(), CAST(@@performance_schema AS CHAR), CAST(@@log_output AS CHAR), CAST(@@slow_query_log AS CHAR), CAST(@@general_log AS CHAR), CURRENT_USER()",
        &[],
    )
    .await?;
    let row = rows
        .first()
        .ok_or(QueryFailure::Db(sqlx::Error::RowNotFound))?;
    let text = |idx: usize| -> Result<Option<String>, QueryFailure> {
        row.try_get_unchecked::<Option<String>, _>(idx)
            .map_err(QueryFailure::Db)
    };
    let flag =
        |idx: usize| -> Result<bool, QueryFailure> { Ok(text(idx)?.as_deref() == Some("1")) };
    let version_raw = text(0)?.unwrap_or_default();
    let version = parse_version(&version_raw).unwrap_or((0, 0, 0));

    Ok(ServerFacts {
        version,
        current_user: text(5)?.unwrap_or_default(),
        performance_schema: flag(1)?,
        slow_query_log: flag(3)?,
        general_log: flag(4)?,
        log_output: text(2)?.unwrap_or_default(),
        consumers: read_consumers(pool, timeout_secs).await,
        prepared_statements: probe_table(
            pool,
            timeout_secs,
            "performance_schema.prepared_statements_instances",
        )
        .await
        .1,
    })
}

/// Consumer switches; an unreadable setup_consumers leaves every field None (unknown, not off).
async fn read_consumers(pool: &MySqlPool, timeout_secs: u64) -> crate::logs::Consumers {
    let mut consumers = crate::logs::Consumers::default();
    let rows = match fetch_rows(
        pool,
        timeout_secs,
        "SELECT NAME, ENABLED FROM performance_schema.setup_consumers WHERE NAME IN ('events_statements_current', 'events_statements_history', 'events_statements_history_long', 'statements_digest')",
        &[],
    )
    .await
    {
        Ok(rows) => rows,
        Err(QueryFailure::Db(e)) => {
            tracing::warn!(error = %e, "setup_consumers is not readable");
            return consumers;
        }
        Err(QueryFailure::Timeout) => return consumers,
    };
    for row in &rows {
        let name = row.try_get_unchecked::<Option<String>, _>(0);
        let enabled = row.try_get_unchecked::<Option<String>, _>(1);
        let (Ok(Some(name)), Ok(enabled)) = (name, enabled) else {
            continue;
        };
        let on = enabled.as_deref() == Some("YES");
        match name.as_str() {
            "events_statements_current" => consumers.statements_current = Some(on),
            "events_statements_history" => consumers.statements_history = Some(on),
            "events_statements_history_long" => consumers.statements_history_long = Some(on),
            "statements_digest" => consumers.statements_digest = Some(on),
            _ => {}
        }
    }
    consumers
}

/// `(available, readable)` from a direct `SELECT ... LIMIT 0`: 1146 is missing, 1142/1143 is no grant.
/// information_schema is not used because it hides tables the account cannot see.
async fn probe_table(pool: &MySqlPool, timeout_secs: u64, table: &str) -> (bool, Option<bool>) {
    let sql = ["SELECT 1 FROM ", table, " LIMIT 0"].concat();
    match fetch_rows(pool, timeout_secs, &sql, &[]).await {
        Ok(_) => (true, Some(true)),
        Err(QueryFailure::Db(e)) if is_missing_grant(&e) => (true, Some(false)),
        Err(QueryFailure::Db(e)) if mysql_errno(&e) == Some(1146) => (false, None),
        Err(QueryFailure::Db(e)) => {
            tracing::warn!(table = %table, error = %e, "table probe failed");
            (false, None)
        }
        Err(QueryFailure::Timeout) => {
            tracing::warn!(table = %table, "table probe timed out");
            (false, None)
        }
    }
}

/// One entry of log_sources, in the shape the tool description documents.
fn source_entry(
    source: &str,
    table: &str,
    available: bool,
    readable: Option<bool>,
    enabled: Option<bool>,
    detail: &str,
    how_to_enable: &[String],
) -> Value {
    json!({
        "source": source,
        "table": table,
        "available": available,
        "readable": readable,
        "enabled": enabled,
        "detail": detail,
        "how_to_enable": how_to_enable,
    })
}

fn consumer_update(consumer: &str) -> String {
    [
        "UPDATE performance_schema.setup_consumers SET ENABLED = 'YES' WHERE NAME = '",
        consumer,
        "';",
    ]
    .concat()
}

const SET_GLOBAL_LOST: &str = "SET GLOBAL is lost on restart; also set it in my.cnf";
const CLOUD_SQL_LOG_NOTE: &str = "On Cloud SQL set these as database flags; Google notes TABLE output is not rotated and is not shown in Logs Explorer.";

const ERROR_SUMMARY_TABLE: &str = "performance_schema.events_errors_summary_global_by_error";

fn resolve_bound(raw: Option<&str>, now: DateTime<Utc>) -> Result<Option<String>, ErrorData> {
    raw.map(|text| parse_time_bound(text, now).map_err(|e| ErrorData::invalid_params(e, None)))
        .transpose()
}

/// The caller's limit (default 100), clamped to the server row cap; 0 is rejected.
fn row_limit(requested: Option<u32>, cap: u32) -> Result<u32, ErrorData> {
    match requested {
        Some(0) => Err(ErrorData::invalid_params("limit must be at least 1", None)),
        Some(n) => Ok(n.min(cap)),
        None => Ok(100u32.min(cap)),
    }
}

fn timeout_for(pool_manager: &PoolManager, database: &str) -> u64 {
    pool_manager
        .get_config(database)
        .map(|c| c.query_timeout_secs)
        .unwrap_or(30)
}

fn on_off(on: bool) -> &'static str {
    if on { "ON" } else { "OFF" }
}

/// Note for slow and general while the log is not receiving rows.
fn file_log_note(table: &str, variable: &str, on: bool, log_output: &str) -> String {
    format!(
        "{table} receives rows only when {variable}=ON and log_output includes TABLE; this server has {variable}={}, log_output={log_output}.",
        on_off(on)
    )
}

/// A query the tools chose and built, plus what to report if it fails or is partial.
struct BuiltQuery {
    sql: String,
    binds: Vec<Bind>,
    grants: Vec<String>,
    note: Option<String>,
    /// Version message for a 1146 (table missing) on a server too old to have the table.
    missing: Option<String>,
}

/// Runs a built query into the shared envelope; `(key, label)` names the source or view.
async fn run_built(
    pool: &MySqlPool,
    database: &str,
    timeout_secs: u64,
    limit: u32,
    limits: &ValueLimits,
    built: BuiltQuery,
    (key, label): (&str, &str),
) -> String {
    let started = Instant::now();
    match fetch_rows(pool, timeout_secs, &built.sql, &built.binds).await {
        Ok(rows) => {
            let mut out = rows_envelope(database, rows, limit, limits, started.elapsed());
            out[key] = json!(label);
            if let Some(note) = built.note {
                out["note"] = json!(note);
            }
            out.to_string()
        }
        Err(failure) => failure_envelope(
            database,
            failure,
            timeout_secs,
            &built.grants,
            built.missing.as_deref(),
        )
        .to_string(),
    }
}

/// One entry per source for log_sources, in the order the tool documents.
async fn log_source_entries(pool: &MySqlPool, timeout: u64, facts: &ServerFacts) -> Vec<Value> {
    let target = grant_target(&facts.current_user);
    let grant = |table: &str| grant_line(table, &target);
    let ps_on = facts.performance_schema;
    let version = facts.version;
    let consumers = facts.consumers;
    let setup_consumers_grant = grant("performance_schema.setup_consumers");
    let mut entries = Vec::new();

    // Version gates come first: an unprivileged account gets 1142 even for a missing table.
    let (available, can_read) = if version < (8, 0, 22) || !ps_on {
        (false, None)
    } else {
        probe_table(pool, timeout, "performance_schema.error_log").await
    };
    let detail = if version < (8, 0, 22) {
        format!(
            "requires MySQL 8.0.22+ (error_log); this server is {}.",
            version_text(version)
        )
    } else if !ps_on {
        PERFORMANCE_SCHEMA_OFF.to_string()
    } else {
        String::new()
    };
    let how = if can_read == Some(false) {
        vec![grant("performance_schema.error_log")]
    } else {
        Vec::new()
    };
    entries.push(source_entry(
        "error_log",
        "performance_schema.error_log",
        available,
        can_read,
        Some(available),
        &detail,
        &how,
    ));

    let (available, can_read) = probe_table(pool, timeout, "mysql.slow_log").await;
    let enabled = facts.slow_query_log && facts.table_output();
    let mut how = Vec::new();
    if can_read == Some(false) {
        how.push(grant("mysql.slow_log"));
    }
    if !facts.slow_query_log {
        how.push(set_line("slow_query_log", "ON", version));
    }
    if !facts.table_output() {
        how.push(set_line("log_output", "'FILE,TABLE'", version));
    }
    let detail = if enabled {
        ""
    } else if version < (8, 0, 0) {
        SET_GLOBAL_LOST
    } else {
        CLOUD_SQL_LOG_NOTE
    };
    entries.push(source_entry(
        "slow_log",
        "mysql.slow_log",
        available,
        can_read,
        Some(enabled),
        detail,
        &how,
    ));

    let (available, can_read) = probe_table(pool, timeout, "mysql.general_log").await;
    let enabled = facts.general_log && facts.table_output();
    let mut how = Vec::new();
    if can_read == Some(false) {
        how.push(grant("mysql.general_log"));
    }
    if !facts.general_log {
        how.push(set_line("general_log", "ON", version));
    }
    if !facts.table_output() {
        how.push(set_line("log_output", "'FILE,TABLE'", version));
    }
    let detail = if enabled {
        ""
    } else if version < (8, 0, 0) {
        SET_GLOBAL_LOST
    } else {
        CLOUD_SQL_LOG_NOTE
    };
    entries.push(source_entry(
        "general_log",
        "mysql.general_log",
        available,
        can_read,
        Some(enabled),
        detail,
        &how,
    ));

    let long_on = facts.history_long_on();
    let table = if long_on {
        "performance_schema.events_statements_history_long"
    } else {
        "performance_schema.events_statements_history"
    };
    let can_read = if ps_on {
        probe_table(pool, timeout, table).await.1
    } else {
        None
    };
    let enabled = if long_on {
        Some(true)
    } else {
        consumers.statements_history
    };
    let mut how = Vec::new();
    if can_read == Some(false) {
        how.push(grant("performance_schema.events_statements_history"));
        how.push(grant("performance_schema.events_statements_history_long"));
        how.push(grant("performance_schema.events_statements_current"));
    }
    if facts.prepared_statements == Some(false) {
        how.push(grant("performance_schema.prepared_statements_instances"));
    }
    if enabled.is_none() {
        how.push(setup_consumers_grant.clone());
    }
    if enabled == Some(false) {
        how.push(consumer_update("events_statements_history"));
    }
    let detail = if !ps_on {
        PERFORMANCE_SCHEMA_OFF
    } else if enabled.is_none() {
        "setup_consumers is not readable, so whether statement history is recorded is unknown."
    } else if enabled == Some(false) {
        "the events_statements_history consumer is disabled, so no statements are recorded. The UPDATE does not survive a restart; set performance-schema-consumer-events-statements-history=ON in my.cnf to keep it."
    } else {
        ""
    };
    entries.push(source_entry(
        "recent_statements",
        table,
        ps_on,
        can_read,
        enabled,
        detail,
        &how,
    ));

    let table = "performance_schema.events_statements_summary_by_digest";
    let can_read = if ps_on {
        probe_table(pool, timeout, table).await.1
    } else {
        None
    };
    let enabled = consumers.statements_digest;
    let mut how = Vec::new();
    if can_read == Some(false) {
        how.push(grant(table));
    }
    if enabled.is_none() {
        how.push(setup_consumers_grant.clone());
    }
    if enabled == Some(false) {
        how.push(consumer_update("statements_digest"));
    }
    let detail = if !ps_on {
        PERFORMANCE_SCHEMA_OFF
    } else if enabled.is_none() {
        "setup_consumers is not readable, so whether statement digests are recorded is unknown."
    } else if enabled == Some(false) {
        "the statements_digest consumer is disabled, so no digests are recorded. The UPDATE does not survive a restart; set performance-schema-consumer-statements-digest=ON in my.cnf to keep it."
    } else {
        ""
    };
    entries.push(source_entry(
        "top_queries",
        table,
        ps_on,
        can_read,
        enabled,
        detail,
        &how,
    ));

    let (available, can_read) = if version.0 < 8 || !ps_on {
        (false, None)
    } else {
        probe_table(pool, timeout, ERROR_SUMMARY_TABLE).await
    };
    let detail = if version.0 < 8 {
        format!(
            "requires MySQL 8.0+ (error_summary); this server is {}.",
            version_text(version)
        )
    } else if !ps_on {
        PERFORMANCE_SCHEMA_OFF.to_string()
    } else {
        String::new()
    };
    let how = if can_read == Some(false) {
        vec![grant(ERROR_SUMMARY_TABLE)]
    } else {
        Vec::new()
    };
    entries.push(source_entry(
        "error_summary",
        ERROR_SUMMARY_TABLE,
        available,
        can_read,
        Some(available),
        &detail,
        &how,
    ));

    entries
}

/// Rejects rather than strips: filtering turned "app.users" into "appusers", which either
/// 404s opaquely or describes a different real table.
fn safe_table_ident(raw: &str) -> Result<String, rmcp::ErrorData> {
    let valid = !raw.is_empty()
        && raw.chars().count() <= 64
        && raw
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_' || c == '$');

    if !valid {
        return Err(rmcp::ErrorData::invalid_params(
            format!(
                "Invalid table name {raw:?}: expected a bare identifier of letters, digits, '_' or '$' (max 64 characters). \
                 Select the schema with the 'database' parameter rather than a qualified 'schema.table'."
            ),
            None,
        ));
    }
    Ok(raw.to_string())
}

fn json_map_str(obj: Option<&serde_json::Map<String, serde_json::Value>>, key: &str) -> String {
    obj.and_then(|o| o.get(key))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string()
}

fn json_map_clone(
    obj: Option<&serde_json::Map<String, serde_json::Value>>,
    key: &str,
) -> serde_json::Value {
    obj.and_then(|o| o.get(key))
        .cloned()
        .unwrap_or(serde_json::Value::Null)
}

/// Integer out of a decoded row value, accepting both the JSON number and the decimal-string
/// form the encoder uses for values past 2^53-1. `None` means "unknown", never a default.
fn json_map_int(
    obj: Option<&serde_json::Map<String, serde_json::Value>>,
    key: &str,
) -> Option<i64> {
    match obj.and_then(|o| o.get(key))? {
        serde_json::Value::Number(n) => n.as_i64(),
        serde_json::Value::String(s) => s.trim().parse().ok(),
        _ => None,
    }
}

fn describe_column_json(raw: &serde_json::Value) -> serde_json::Value {
    let obj = raw.as_object();
    serde_json::json!({
        "field": json_map_str(obj, "Field"),
        "type": json_map_str(obj, "Type"),
        "nullable": json_map_str(obj, "Null") == "YES",
        "key": json_map_str(obj, "Key"),
        "default": json_map_clone(obj, "Default"),
        "extra": json_map_str(obj, "Extra"),
    })
}

/// Keys match the aliases in the information_schema.TABLES query.
fn table_metadata_json(raw: &serde_json::Value) -> serde_json::Value {
    let obj = raw.as_object();
    serde_json::json!({
        "engine": json_map_clone(obj, "engine"),
        "rows_estimate": json_map_clone(obj, "rows_estimate"),
        "collation": json_map_clone(obj, "collation"),
        "comment": json_map_clone(obj, "comment"),
    })
}

struct IndexAcc {
    name: String,
    unique: Option<bool>,
    columns: Vec<(i64, String)>,
}

/// Groups SHOW INDEX rows into one entry per index, PRIMARY first and the rest in the order
/// the server returned them; columns are ordered by Seq_in_index, not by row arrival.
fn formatted_indexes(rows: &[serde_json::Value]) -> Vec<serde_json::Value> {
    let mut order: Vec<String> = Vec::new();
    let mut acc: std::collections::HashMap<String, IndexAcc> = std::collections::HashMap::new();

    for raw in rows {
        let obj = raw.as_object();
        let key_name = json_map_str(obj, "Key_name");
        let col_name = json_map_str(obj, "Column_name");

        let entry = acc.entry(key_name.clone()).or_insert_with(|| {
            order.push(key_name.clone());
            IndexAcc {
                name: key_name.clone(),
                // Unknown stays null: a missing Non_unique must not read as "not unique".
                unique: json_map_int(obj, "Non_unique").map(|v| v == 0),
                columns: Vec::new(),
            }
        });

        let seq = json_map_int(obj, "Seq_in_index").unwrap_or(entry.columns.len() as i64 + 1);
        entry.columns.push((seq, col_name));
    }

    order.sort_by_key(|name| name != "PRIMARY");

    order
        .into_iter()
        .filter_map(|name| acc.remove(&name))
        .map(|idx| {
            let mut columns = idx.columns;
            columns.sort_by_key(|(seq, _)| *seq);
            serde_json::json!({
                "name": idx.name,
                "unique": idx.unique,
                "columns": columns.into_iter().map(|(_, c)| c).collect::<Vec<String>>(),
            })
        })
        .collect()
}

#[derive(Clone)]
pub struct MysqlMcp {
    pool_manager: Arc<PoolManager>,
    config: Config,
    tool_router: rmcp::handler::server::tool::ToolRouter<Self>,
}

impl MysqlMcp {
    pub fn new(pool_manager: Arc<PoolManager>, config: Config) -> Self {
        let tool_router = Self::tool_router();
        Self {
            pool_manager,
            config,
            tool_router,
        }
    }
}

// --- Parameter types ---

#[derive(Debug, Deserialize, JsonSchema)]
struct ShowTablesParams {
    /// Database name as configured in MYSQL_DATABASES (e.g. "my-db")
    database: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct DescribeTableParams {
    /// Database name as configured in MYSQL_DATABASES (e.g. "my-db")
    database: String,
    /// Bare table name, unqualified: letters, digits, '_' or '$' only. "schema.table" is rejected —
    /// pick the schema with the 'database' parameter.
    table: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ExecuteQueryParams {
    /// Database name as configured in MYSQL_DATABASES (e.g. "my-db")
    database: String,
    /// SQL query. Read-only: must start with SELECT, WITH, TABLE, VALUES, SHOW, DESCRIBE/DESC,
    /// EXPLAIN, or a parenthesized SELECT/TABLE/VALUES/WITH. Write statements (anywhere, not just
    /// first), SET in every form, INTO OUTFILE/DUMPFILE, FOR UPDATE/SHARE, LOCK IN SHARE MODE,
    /// LOAD_FILE(), GET_LOCK(), /*! ... */ comments and multiple statements are rejected.
    /// A missing LIMIT is added and a larger one clamped to the server row cap (default 1000);
    /// "truncated" reports whether more rows matched.
    /// Result encoding: data[] is one flat {column: value} object per row; columns[] carries the true
    /// column order and MySQL type; duplicate names become "id__2" with originalName kept; integers
    /// past 2^53-1 and every DECIMAL are strings; DATETIME has no zone, TIMESTAMP ends in "Z" (UTC),
    /// both gain ".ffffff"; TIME is "[-]HH:MM:SS"; oversized text is cut with
    /// "…[truncated: X of Y bytes]"; an undecodable value is {"__error__":"decode_failed",...}.
    /// The tool description carries the full table.
    query: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct LogSourcesParams {
    /// Database name as configured in MYSQL_DATABASES (e.g. "my-db")
    database: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ReadServerLogParams {
    /// Database name as configured in MYSQL_DATABASES (e.g. "my-db")
    database: String,
    /// "error" = performance_schema.error_log, "slow" = mysql.slow_log, "general" = mysql.general_log.
    source: LogSource,
    /// Window start. Relative "<n>s|m|h|d|w" (n > 0, counted back from now, e.g. "15m"), or
    /// "YYYY-MM-DD HH:MM:SS[.ffffff]" (UTC), or RFC 3339 (e.g. "2026-11-24T10:00:00-05:00").
    since: Option<String>,
    /// Window end, exclusive. Same forms as since.
    until: Option<String>,
    /// Substring match (INSTR) on the message (error), the SQL text (slow) or the argument (general).
    contains: Option<String>,
    /// error only: System, Error, Warning or Note. Error keeps System and Error; Note means no filter.
    min_priority: Option<MinPriority>,
    /// slow only: keep statements with query_time of at least this many milliseconds.
    min_query_time_ms: Option<u64>,
    /// slow and general only: substring match on user_host, e.g. "app_user" (user_host reads like "app_user[app_user] @ localhost").
    user: Option<String>,
    /// Rows to return, newest first. Default 100, at least 1, clamped to the server row cap.
    limit: Option<u32>,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct QueryActivityParams {
    /// Database name as configured in MYSQL_DATABASES (e.g. "my-db")
    database: String,
    /// "recent_statements" = statement history, "top_queries" = statement digests,
    /// "error_summary" = server errors by error number.
    view: ActivityView,
    /// Substring match (INSTR) on the SQL text (recent_statements), the digest text (top_queries)
    /// or the error name (error_summary).
    contains: Option<String>,
    /// recent_statements and top_queries only: exact match on the current schema name.
    schema: Option<String>,
    /// recent_statements and top_queries only: true keeps only statements that raised an error.
    errors_only: Option<bool>,
    /// recent_statements only: keep statements of at least this many milliseconds.
    min_duration_ms: Option<u64>,
    /// top_queries only: sort key, default total_time (total time, milliseconds, descending).
    order_by: Option<OrderBy>,
    /// Rows to return. Default 100, at least 1, clamped to the server row cap.
    limit: Option<u32>,
}

// --- Tool implementations ---

#[tool_router]
impl MysqlMcp {
    #[tool(
        name = "list_databases",
        description = "List all available database connections and their details"
    )]
    async fn list_databases(&self) -> Result<String, rmcp::ErrorData> {
        let databases: Vec<serde_json::Value> = self
            .config
            .databases
            .iter()
            .map(|db| {
                serde_json::json!({
                    "name": db.name,
                    "host": db.host,
                    "port": db.port,
                    "database": db.database,
                    "user": db.user,
                })
            })
            .collect();

        Ok(serde_json::json!({
            "databases": databases,
            "count": databases.len(),
            "message": format!("Available databases: {}", self.config.database_names().join(", "))
        })
        .to_string())
    }

    #[tool(name = "show_tables", description = "List all tables in a database")]
    async fn show_tables(
        &self,
        Parameters(p): Parameters<ShowTablesParams>,
    ) -> Result<String, rmcp::ErrorData> {
        let pool = self
            .pool_manager
            .get_pool(&p.database)
            .await
            .map_err(|e| rmcp::ErrorData::invalid_params(e, None))?;

        let rows = sqlx::query("SHOW TABLES")
            .fetch_all(&pool)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "SHOW TABLES query failed");
                rmcp::ErrorData::internal_error(
                    format!("Failed to list tables: {}", db_error_message(&e)),
                    None,
                )
            })?;

        if rows.is_empty() {
            return Ok(serde_json::json!({
                "tables": [],
                "count": 0,
                "database": p.database,
                "message": "No tables found"
            })
            .to_string());
        }

        let tables: Vec<String> = rows
            .iter()
            .map(|row| {
                // SHOW TABLES returns a single column with a dynamic name like "Tables_in_<db>",
                // so take the first value rather than looking it up by name.
                let json = row_to_json(row);
                json.as_object()
                    .and_then(|obj| obj.values().next())
                    .and_then(|v| v.as_str())
                    .unwrap_or_default()
                    .to_string()
            })
            .collect();

        Ok(serde_json::json!({
            "tables": tables,
            "count": tables.len(),
            "database": p.database,
            "message": format!("Found {} table(s)", tables.len())
        })
        .to_string())
    }

    #[tool(
        name = "describe_table",
        description = "Schema of one table: columns, indexes (PRIMARY first, index columns in \
                       Seq_in_index order) and metadata. 'table' must be a bare unqualified name. \
                       metadata.rows_estimate is the engine's row ESTIMATE, not a count — use \
                       SELECT COUNT(*) for an exact number. If the index or metadata lookup fails, \
                       that field is null and an 'indexesError'/'metadataError' explains why: null \
                       means unknown, never 'none'."
    )]
    async fn describe_table(
        &self,
        Parameters(p): Parameters<DescribeTableParams>,
    ) -> Result<String, rmcp::ErrorData> {
        let pool = self
            .pool_manager
            .get_pool(&p.database)
            .await
            .map_err(|e| rmcp::ErrorData::invalid_params(e, None))?;

        let table_name = safe_table_ident(&p.table)?;

        let columns = sqlx::query(&format!("DESCRIBE `{table_name}`"))
            .fetch_all(&pool)
            .await
            .map_err(|e| {
                tracing::error!(table = %table_name, error = %e, "DESCRIBE query failed");
                rmcp::ErrorData::internal_error(
                    format!(
                        "Failed to describe table '{table_name}': {}",
                        db_error_message(&e)
                    ),
                    None,
                )
            })?;

        let formatted_columns: Vec<serde_json::Value> = columns
            .iter()
            .map(|row| describe_column_json(&row_to_json(row)))
            .collect();

        let (indexes, indexes_error) = match sqlx::query(&format!("SHOW INDEX FROM `{table_name}`"))
            .fetch_all(&pool)
            .await
        {
            Ok(rows) => {
                let raws: Vec<serde_json::Value> = rows.iter().map(row_to_json).collect();
                (
                    serde_json::Value::Array(formatted_indexes(&raws)),
                    None::<String>,
                )
            }
            Err(e) => {
                tracing::error!(table = %table_name, error = %e, "SHOW INDEX query failed");
                (serde_json::Value::Null, Some(db_error_message(&e)))
            }
        };

        // Bound lookup, not SHOW TABLE STATUS LIKE: '_' is a LIKE wildcard, so "user_roles"
        // could match and describe "userXroles".
        let status = sqlx::query(
            "SELECT ENGINE AS engine, TABLE_ROWS AS rows_estimate, \
             TABLE_COLLATION AS collation, TABLE_COMMENT AS comment \
             FROM information_schema.TABLES \
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
        )
        .bind(table_name.as_str())
        .fetch_optional(&pool)
        .await;

        let (metadata, metadata_error) = match status {
            Ok(Some(row)) => (table_metadata_json(&row_to_json(&row)), None::<String>),
            Ok(None) => (serde_json::Value::Null, None),
            Err(e) => {
                tracing::error!(table = %table_name, error = %e, "table metadata query failed");
                (serde_json::Value::Null, Some(db_error_message(&e)))
            }
        };

        let summary = match indexes.as_array() {
            Some(list) => format!(
                "Table '{table_name}' has {} columns and {} indexes",
                formatted_columns.len(),
                list.len()
            ),
            None => format!(
                "Table '{table_name}' has {} columns; index information is unavailable (see indexesError)",
                formatted_columns.len()
            ),
        };

        let mut payload = serde_json::json!({
            "table": table_name,
            "database": p.database,
            "columns": formatted_columns,
            "indexes": indexes,
            "metadata": metadata,
            "summary": summary,
        });

        if let Some(obj) = payload.as_object_mut() {
            if let Some(err) = indexes_error {
                obj.insert("indexesError".into(), serde_json::Value::String(err));
            }
            if let Some(err) = metadata_error {
                obj.insert("metadataError".into(), serde_json::Value::String(err));
            }
        }

        Ok(payload.to_string())
    }

    #[tool(
        name = "execute_query",
        description = "Run ONE read-only SQL query.\n\
            ACCEPTED: a statement starting with SELECT, WITH, TABLE, VALUES, SHOW, DESCRIBE/DESC, \
            EXPLAIN, or a parenthesized SELECT/TABLE/VALUES/WITH.\n\
            REJECTED, at the start or nested anywhere: INSERT, UPDATE, DELETE, DROP, ALTER, RENAME, \
            GRANT, REVOKE, CREATE (SHOW CREATE is fine), REPLACE ... INTO, TRUNCATE TABLE, LOAD \
            DATA/XML; FLUSH, LOCK, UNLOCK, CALL and transaction control (BEGIN, START TRANSACTION, \
            COMMIT, ROLLBACK, SAVEPOINT); INTO OUTFILE, INTO DUMPFILE, FOR UPDATE, FOR SHARE, LOCK \
            IN SHARE MODE, LOAD_FILE(), GET_LOCK()/RELEASE_LOCK(); SET in EVERY form, including \
            SET @var, SET @@var and any @@variable assignment, since each mutates state on a pooled \
            connection the next caller inherits; DO, PREPARE/EXECUTE, HANDLER, XA, RESET, INSTALL; more \
            than one statement (one trailing ';' is fine); and /*! version-gated */ comments, whose \
            body MySQL would execute. The keyword scan ignores anything inside '...', \"...\" and \
            `...`, so REPLACE()/INSERT()/TRUNCATE() as functions and a column named `delete` pass. \
            Ordinary comments (--, #, /* */) are stripped; optimizer hints (/*+ ... */) are kept.\n\
            Reading @@variables is fine (SELECT @@version); assigning one is not. Inline values or \
            use a CTE instead of a session variable.\n\
            ROWS: SELECT/WITH/TABLE/VALUES get a LIMIT appended, and a larger LIMIT of your own is \
            clamped, to the server row cap (DEFAULT_MAX_ROWS, default 1000); SHOW and EXPLAIN results \
            are cut to the same cap afterwards. \"truncated\": true means more rows matched than were \
            returned.\n\
            RESULT ENCODING — {success, database, columns, data, rowCount, truncated, decodeErrors, \
            truncatedValues, executionTime, message}:\n\
            - data: one flat {column: value} object per row. columns: emitted once, in true select \
            order, as [{name, type, originalName?}] — read it for column order and for each value's \
            MySQL type. No rows means \"columns\": [].\n\
            - Duplicate column names are de-duplicated left to right: a second \"id\" becomes \
            \"id__2\", a third \"id__3\", keyed identically in columns and data, with originalName on \
            the renamed entry. Nothing is dropped.\n\
            - Integers (TINYINT..BIGINT, BIT, YEAR, COUNT(), window functions): a JSON number while \
            |v| <= 9007199254740991 (2^53-1), otherwise a decimal STRING. A BIGINT UNSIGNED id can be \
            either, so compare loosely.\n\
            - DECIMAL/NUMERIC, and SUM()/AVG() over them: ALWAYS a string with exact digits and scale \
            (\"829\", \"1.50\"). Parse it before doing arithmetic.\n\
            - FLOAT/DOUBLE: numbers; NaN and infinities are the strings \"NaN\", \"Infinity\", \
            \"-Infinity\".\n\
            - tinyint(1)/BOOLEAN: true/false only for exactly 0/1; any other value is a number. \
            SELECT col + 0 forces a number.\n\
            - DATE \"YYYY-MM-DD\". DATETIME \"YYYY-MM-DD HH:MM:SS\" with NO zone marker: wall clock \
            exactly as stored. TIMESTAMP \"YYYY-MM-DD HH:MM:SSZ\", UTC (the session runs at +00:00). \
            Both gain \".ffffff\" when microseconds are non-zero. TIME is \"[-]HH:MM:SS[.ffffff]\", \
            signed and not capped at 24h (\"838:59:59\"). Zero dates are null.\n\
            - JSON columns are inlined as real JSON values, not encoded strings.\n\
            - BINARY/VARBINARY and the BLOB family come back as text when the bytes are valid \
            UTF-8, otherwise \"0x\"-prefixed lowercase hex capped near 256 source bytes. GEOMETRY \
            is always hex; select ST_AsText(col) or ST_AsGeoJSON(col) for a readable shape.\n\
            - Oversized text/JSON is cut on a character boundary and suffixed \
            \"…[truncated: X of Y bytes]\"; truncatedValues counts them. Re-query with SUBSTRING() \
            for the rest.\n\
            - A value that fails to decode becomes {\"__error__\": \"decode_failed\", \"type\": ..., \
            \"message\": ...} and increments decodeErrors. A SQL NULL is a plain null."
    )]
    async fn execute_query(
        &self,
        Parameters(p): Parameters<ExecuteQueryParams>,
    ) -> Result<String, rmcp::ErrorData> {
        let pool = self
            .pool_manager
            .get_pool(&p.database)
            .await
            .map_err(|e| rmcp::ErrorData::invalid_params(e, None))?;

        let result = sanitizer::sanitize(&p.query);
        if !result.is_valid {
            let reason = result
                .error
                .unwrap_or_else(|| "Query is not read-only".to_string());
            return Ok(serde_json::json!({
                "success": false,
                "database": p.database,
                "error": reason,
                "message": format!("Query rejected: {reason}")
            })
            .to_string());
        }

        // Fetch one row past the cap so truncation is observed rather than guessed from
        // row_count == max_rows, which false-positives on an exactly-max_rows result.
        let max_rows = self.config.default_max_rows;
        let final_query =
            sanitizer::apply_limit(&result.sanitized_query, max_rows.saturating_add(1));

        let timeout_secs = self
            .pool_manager
            .get_config(&p.database)
            .map(|c| c.query_timeout_secs)
            .unwrap_or(30);

        let start = Instant::now();
        let query_result = tokio::time::timeout(
            std::time::Duration::from_secs(timeout_secs),
            sqlx::query(&final_query).fetch_all(&pool),
        )
        .await;

        let elapsed = start.elapsed();

        match query_result {
            Ok(Ok(rows)) => {
                let limits = ValueLimits::from(&self.config);
                Ok(rows_envelope(&p.database, rows, max_rows, &limits, elapsed).to_string())
            }
            Ok(Err(e)) => {
                tracing::error!(database = %p.database, error = %e, "Query execution failed");
                let detail = db_error_message(&e);
                Ok(serde_json::json!({
                    "success": false,
                    "database": p.database,
                    "error": detail,
                    "message": format!("Query execution failed: {detail}")
                })
                .to_string())
            }
            Err(_) => Ok(serde_json::json!({
                "success": false,
                "database": p.database,
                "error": format!("Query timed out after {timeout_secs}s"),
                "message": "Query execution timed out."
            })
            .to_string()),
        }
    }

    #[tool(
        name = "log_sources",
        description = "Which server log and activity sources this database can read, and what to switch on for each. \
            Returns {success, database, server_version, current_user, performance_schema, log_output, sources[]}, \
            one entry per source: {source, table, available, readable, enabled, detail, how_to_enable[]}. \
            available: error_log needs performance_schema ON and MySQL 8.0.22+; error_summary needs performance_schema \
            ON and MySQL 8.0+; slow_log and general_log need the table to exist; recent_statements and top_queries need \
            performance_schema ON. readable: true when a SELECT on the table works, false when the account lacks the \
            grant (the GRANT is in how_to_enable), null when the table is missing. enabled: error_log and error_summary \
            follow available; slow_log and general_log need the log ON and log_output including TABLE; recent_statements \
            needs the history consumer ENABLED; top_queries needs the statements_digest consumer ENABLED; null when \
            setup_consumers is unreadable. how_to_enable lists the exact GRANT, SET PERSIST (SET GLOBAL before MySQL 8.0) \
            or UPDATE statements a DBA would run; this server never runs them, and SET GLOBAL and the UPDATE do not \
            survive a restart. Takes only 'database'. Values use the same encoding as execute_query."
    )]
    async fn log_sources(
        &self,
        Parameters(p): Parameters<LogSourcesParams>,
    ) -> Result<String, rmcp::ErrorData> {
        let pool = self
            .pool_manager
            .get_pool(&p.database)
            .await
            .map_err(|e| rmcp::ErrorData::invalid_params(e, None))?;
        let timeout = timeout_for(&self.pool_manager, &p.database);

        let facts = match probe_facts(&pool, timeout).await {
            Ok(facts) => facts,
            Err(failure) => {
                return Ok(failure_envelope(&p.database, failure, timeout, &[], None).to_string());
            }
        };
        let sources = log_source_entries(&pool, timeout, &facts).await;

        Ok(json!({
            "success": true,
            "database": p.database,
            "server_version": version_text(facts.version),
            "current_user": facts.current_user,
            "performance_schema": facts.performance_schema,
            "log_output": facts.log_output,
            "sources": sources,
        })
        .to_string())
    }

    #[tool(
        name = "read_server_log",
        description = "Rows from one server log, newest first. source 'error' reads performance_schema.error_log \
            (MySQL 8.0.22+), 'slow' reads mysql.slow_log, 'general' reads mysql.general_log. Params: database; \
            source (error|slow|general); since and until, the window: '<n>s|m|h|d|w' counted back from now (e.g. \
            '15m', n > 0), 'YYYY-MM-DD HH:MM:SS[.ffffff]' in UTC, or RFC 3339 (e.g. '2026-11-24T10:00:00-05:00'); \
            since is inclusive, until exclusive; contains (substring match); min_priority (error only: System, \
            Error, Warning or Note; Error keeps System and Error, Note means no filter); min_query_time_ms (slow \
            only, milliseconds); user (slow and general only, substring of user_host); limit (default 100, clamped \
            to the server row cap). Returns the execute_query envelope {success, database, columns, data, rowCount, \
            truncated, decodeErrors, truncatedValues, executionTime, message} plus 'source' and, when relevant, \
            'note'. Values use the same encoding as execute_query. A missing SELECT grant returns success:false with \
            the GRANT line in how_to_enable. slow and general hold rows only while the log is ON and log_output \
            includes TABLE; 'note' says so when it is not. Parameters that do not apply to the source are \
            invalid_params."
    )]
    async fn read_server_log(
        &self,
        Parameters(p): Parameters<ReadServerLogParams>,
    ) -> Result<String, rmcp::ErrorData> {
        let now = Utc::now();
        let since = resolve_bound(p.since.as_deref(), now)?;
        let until = resolve_bound(p.until.as_deref(), now)?;
        let limit = row_limit(p.limit, self.config.default_max_rows)?;
        let query = LogQuery {
            since,
            until,
            contains: p.contains,
            min_priority: p.min_priority,
            min_query_time_ms: p.min_query_time_ms,
            user: p.user,
            limit: u64::from(limit) + 1,
        };
        check_log_query(p.source, &query).map_err(|e| rmcp::ErrorData::invalid_params(e, None))?;

        let pool = self
            .pool_manager
            .get_pool(&p.database)
            .await
            .map_err(|e| rmcp::ErrorData::invalid_params(e, None))?;
        let timeout = timeout_for(&self.pool_manager, &p.database);
        let facts = match probe_facts(&pool, timeout).await {
            Ok(facts) => facts,
            Err(failure) => {
                return Ok(failure_envelope(&p.database, failure, timeout, &[], None).to_string());
            }
        };
        let target = grant_target(&facts.current_user);

        let built = match p.source {
            LogSource::Error => {
                if !facts.performance_schema {
                    return Ok(unsupported(&p.database, PERFORMANCE_SCHEMA_OFF).to_string());
                }
                if facts.version < (8, 0, 22) {
                    let text = error_log_version_text(facts.version);
                    return Ok(unsupported(&p.database, &text).to_string());
                }
                let (sql, binds) = error_log_sql(&query);
                BuiltQuery {
                    sql,
                    binds,
                    grants: vec![grant_line("performance_schema.error_log", &target)],
                    note: None,
                    missing: Some(error_log_version_text(facts.version)),
                }
            }
            LogSource::Slow => {
                let (sql, binds) = slow_log_sql(&query);
                let enabled = facts.slow_query_log && facts.table_output();
                BuiltQuery {
                    sql,
                    binds,
                    grants: vec![grant_line("mysql.slow_log", &target)],
                    note: (!enabled).then(|| {
                        file_log_note(
                            "mysql.slow_log",
                            "slow_query_log",
                            facts.slow_query_log,
                            &facts.log_output,
                        )
                    }),
                    missing: None,
                }
            }
            LogSource::General => {
                let (sql, binds) = general_log_sql(&query);
                let enabled = facts.general_log && facts.table_output();
                BuiltQuery {
                    sql,
                    binds,
                    grants: vec![grant_line("mysql.general_log", &target)],
                    note: (!enabled).then(|| {
                        file_log_note(
                            "mysql.general_log",
                            "general_log",
                            facts.general_log,
                            &facts.log_output,
                        )
                    }),
                    missing: None,
                }
            }
        };

        let limits = ValueLimits::from(&self.config);
        Ok(run_built(
            &pool,
            &p.database,
            timeout,
            limit,
            &limits,
            built,
            ("source", p.source.as_str()),
        )
        .await)
    }

    #[tool(
        name = "query_activity",
        description = "Activity recorded by performance_schema. view 'recent_statements' returns statement history, \
            newest first, with started_at computed from the server clock. 'top_queries' returns statement digests \
            aggregated since the server started or the digest tables were truncated, with durations in milliseconds, \
            ordered by order_by (total_time, avg_time, max_time, count, errors or rows_examined; default total_time, \
            descending). 'error_summary' returns server errors by error number (MySQL 8.0+). Params: database; view \
            (recent_statements|top_queries|error_summary); contains (substring of the SQL text, digest text or error \
            name); schema (exact current schema; recent_statements and top_queries); errors_only (true keeps only \
            statements or digests with errors; recent_statements and top_queries); min_duration_ms (recent_statements \
            only, milliseconds); order_by (top_queries only); limit (default 100, clamped to the server row cap). \
            Returns the execute_query envelope {success, database, columns, data, rowCount, truncated, decodeErrors, \
            truncatedValues, executionTime, message} plus 'view' and, when relevant, 'note'. Values use the same \
            encoding as execute_query. recent_statements and top_queries include other sessions' SQL text, literals \
            included. Statements sent through the binary prepared-statement protocol show their text on the \
            statement/com/Prepare row, joined from performance_schema.prepared_statements_instances; statement/com/Execute \
            rows carry no text. Without SELECT on that table they show no SQL text. History keeps the last statements per connection \
            (performance_schema_events_statements_history_size, default 10), so this tool's own probe queries can \
            push out earlier queries made through this server; other sessions' history is unaffected. A missing SELECT grant returns success:false with the GRANT line in how_to_enable. Parameters \
            that do not apply to the view are invalid_params."
    )]
    async fn query_activity(
        &self,
        Parameters(p): Parameters<QueryActivityParams>,
    ) -> Result<String, rmcp::ErrorData> {
        let limit = row_limit(p.limit, self.config.default_max_rows)?;
        let query = ActivityQuery {
            contains: p.contains,
            schema: p.schema,
            errors_only: p.errors_only,
            min_duration_ms: p.min_duration_ms,
            order_by: p.order_by,
            limit: u64::from(limit) + 1,
        };
        check_activity_query(p.view, &query)
            .map_err(|e| rmcp::ErrorData::invalid_params(e, None))?;

        let pool = self
            .pool_manager
            .get_pool(&p.database)
            .await
            .map_err(|e| rmcp::ErrorData::invalid_params(e, None))?;
        let timeout = timeout_for(&self.pool_manager, &p.database);
        let facts = match probe_facts(&pool, timeout).await {
            Ok(facts) => facts,
            Err(failure) => {
                return Ok(failure_envelope(&p.database, failure, timeout, &[], None).to_string());
            }
        };
        let target = grant_target(&facts.current_user);
        let grant = |table: &str| grant_line(table, &target);

        let built = match p.view {
            ActivityView::RecentStatements => {
                if !facts.performance_schema {
                    return Ok(unsupported(&p.database, PERFORMANCE_SCHEMA_OFF).to_string());
                }
                let (sql, binds) = recent_statements_sql(&query, &facts);
                let mut grants = vec![
                    grant("performance_schema.events_statements_history"),
                    grant("performance_schema.events_statements_history_long"),
                    grant("performance_schema.events_statements_current"),
                ];
                if facts.prepared_statements.is_some() {
                    grants.push(grant("performance_schema.prepared_statements_instances"));
                }
                BuiltQuery {
                    sql,
                    binds,
                    grants,
                    note: activity_note(p.view, &facts),
                    missing: None,
                }
            }
            ActivityView::TopQueries => {
                if !facts.performance_schema {
                    return Ok(unsupported(&p.database, PERFORMANCE_SCHEMA_OFF).to_string());
                }
                let (sql, binds) = top_queries_sql(&query, &facts);
                BuiltQuery {
                    sql,
                    binds,
                    grants: vec![grant(
                        "performance_schema.events_statements_summary_by_digest",
                    )],
                    note: activity_note(p.view, &facts),
                    missing: None,
                }
            }
            ActivityView::ErrorSummary => {
                if !facts.performance_schema {
                    return Ok(unsupported(&p.database, PERFORMANCE_SCHEMA_OFF).to_string());
                }
                if facts.version.0 < 8 {
                    let text = error_summary_version_text(facts.version);
                    return Ok(unsupported(&p.database, &text).to_string());
                }
                let (sql, binds) = error_summary_sql(&query);
                BuiltQuery {
                    sql,
                    binds,
                    grants: vec![grant(ERROR_SUMMARY_TABLE)],
                    note: None,
                    missing: Some(error_summary_version_text(facts.version)),
                }
            }
        };

        let limits = ValueLimits::from(&self.config);
        Ok(run_built(
            &pool,
            &p.database,
            timeout,
            limit,
            &limits,
            built,
            ("view", p.view.as_str()),
        )
        .await)
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for MysqlMcp {
    fn get_info(&self) -> ServerConfig {
        let db_names = self.pool_manager.database_names();
        let mut info = ServerConfig::default();
        info.server_info = Implementation::new(env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION"));
        info.capabilities = ServerCapabilities::builder()
            .enable_tools()
            .enable_resources()
            .build();
        info.instructions = Some(format!(
            "MySQL MCP Server — read-only access to {} database(s): {}. \
             Use list_databases to discover available databases. \
             All query tools require a 'database' parameter. \
             Use show_tables to list tables, describe_table for schema, execute_query for SQL. \
             execute_query's description documents how values are encoded (integers past 2^53-1 and \
             DECIMALs are strings, TIMESTAMPs end in Z, column order lives in columns[]); read it \
             before interpreting a result. \
             Server logs and activity: log_sources, read_server_log and query_activity read the server's \
             error, slow and general logs and performance_schema activity; a missing SELECT grant comes back \
             in how_to_enable. \
             Resources are available at mysql://<database-name> for each configured database.",
            db_names.len(),
            db_names.join(", ")
        ));
        info
    }

    async fn list_resources(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        let resources: Vec<_> = self
            .config
            .databases
            .iter()
            .map(|db| {
                Resource::new(format!("mysql://{}", db.name), db.name.clone())
                    .with_description(format!(
                        "MySQL database '{}' (schema: {})",
                        db.name, db.database
                    ))
                    .with_mime_type("application/json")
            })
            .collect();

        Ok(ListResourcesResult {
            resources,
            ..Default::default()
        })
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        let db_name = request
            .uri
            .strip_prefix("mysql://")
            .ok_or_else(|| ErrorData::invalid_params("URI must start with mysql://", None))?;

        let db_config = self
            .config
            .databases
            .iter()
            .find(|d| d.name == db_name)
            .ok_or_else(|| {
                let available: Vec<&str> = self
                    .config
                    .databases
                    .iter()
                    .map(|d| d.name.as_str())
                    .collect();
                ErrorData::invalid_params(
                    format!("Unknown database '{db_name}'. Available: {available:?}"),
                    None,
                )
            })?;

        let info = serde_json::json!({
            "name": db_config.name,
            "database": db_config.database,
            "query_timeout_secs": db_config.query_timeout_secs,
            "usage": format!(
                "Use this database name '{}' as the 'database' parameter in show_tables, describe_table, and execute_query tools.",
                db_config.name
            )
        });

        Ok(ReadResourceResult::new(vec![ResourceContents::text(
            serde_json::to_string_pretty(&info).unwrap_or_default(),
            request.uri,
        )])
        .into())
    }
}

#[cfg(test)]
mod tool_helpers_tests {
    use super::*;

    #[test]
    fn safe_table_ident_rejects_empty_or_non_ident() {
        assert!(safe_table_ident("").is_err());
        assert!(safe_table_ident("!!!").is_err());
        assert!(safe_table_ident(&"a".repeat(65)).is_err());
    }

    #[test]
    fn safe_table_ident_rejects_instead_of_mangling() {
        // "app.users" used to be filtered down to "appusers", "my-table" to "mytable".
        for raw in ["app.users", "my-table", "a`drop`--", "users users"] {
            let err = safe_table_ident(raw).unwrap_err();
            assert!(
                err.message.contains(raw),
                "{raw} not echoed: {}",
                err.message
            );
        }
    }

    #[test]
    fn safe_table_ident_keeps_bare_identifiers() {
        assert_eq!(safe_table_ident("users_1").unwrap(), "users_1");
        assert_eq!(safe_table_ident("wp$posts").unwrap(), "wp$posts");
    }

    #[test]
    fn describe_column_json_maps_fields() {
        let raw = serde_json::json!({
            "Field": "id",
            "Type": "int",
            "Null": "NO",
            "Key": "PRI",
            "Default": null,
            "Extra": "auto_increment"
        });
        let out = describe_column_json(&raw);
        assert_eq!(out["field"], "id");
        assert_eq!(out["nullable"], false);
    }

    #[test]
    fn json_map_int_accepts_numbers_and_strings() {
        let obj = serde_json::json!({"n": 0, "s": "1", "big": "9007199254740993", "junk": true});
        let map = obj.as_object();
        assert_eq!(json_map_int(map, "n"), Some(0));
        assert_eq!(json_map_int(map, "s"), Some(1));
        assert_eq!(json_map_int(map, "big"), Some(9007199254740993));
        assert_eq!(json_map_int(map, "junk"), None);
        assert_eq!(json_map_int(map, "missing"), None);
    }

    fn index_row(
        key: &str,
        col: &str,
        seq: i64,
        non_unique: serde_json::Value,
    ) -> serde_json::Value {
        serde_json::json!({
            "Key_name": key,
            "Column_name": col,
            "Seq_in_index": seq,
            "Non_unique": non_unique,
        })
    }

    #[test]
    fn formatted_indexes_puts_primary_first_and_orders_columns() {
        let rows = vec![
            index_row("idx_b", "b2", 2, serde_json::json!(1)),
            index_row("idx_b", "b1", 1, serde_json::json!(1)),
            index_row("PRIMARY", "id", 1, serde_json::json!(0)),
            index_row("idx_a", "a1", 1, serde_json::json!("0")),
        ];
        let out = formatted_indexes(&rows);
        assert_eq!(out[0]["name"], "PRIMARY");
        assert_eq!(out[0]["unique"], true);
        assert_eq!(out[1]["name"], "idx_b");
        assert_eq!(out[1]["columns"], serde_json::json!(["b1", "b2"]));
        assert_eq!(out[1]["unique"], false);
        // Non_unique as a string still resolves.
        assert_eq!(out[2]["unique"], true);
    }

    #[test]
    fn formatted_indexes_reports_unknown_uniqueness_as_null() {
        let rows = vec![serde_json::json!({"Key_name": "idx", "Column_name": "c"})];
        let out = formatted_indexes(&rows);
        assert_eq!(out[0]["unique"], serde_json::Value::Null);
        assert_eq!(out[0]["columns"], serde_json::json!(["c"]));
    }

    #[test]
    fn row_limit_defaults_clamps_and_rejects_zero() {
        assert_eq!(row_limit(None, 1000).unwrap(), 100);
        assert_eq!(row_limit(None, 50).unwrap(), 50);
        assert_eq!(row_limit(Some(7), 1000).unwrap(), 7);
        assert_eq!(row_limit(Some(5000), 1000).unwrap(), 1000);
        let err = row_limit(Some(0), 1000).unwrap_err();
        assert_eq!(err.code.0, -32602);
        assert_eq!(err.message, "limit must be at least 1");
    }
}
