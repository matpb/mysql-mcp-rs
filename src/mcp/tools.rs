use std::sync::Arc;
use std::time::Instant;

use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{
    Annotated, ListResourcesResult, PaginatedRequestParams, RawResource, ReadResourceRequestParams,
    ReadResourceResult, ResourceContents, ServerCapabilities, ServerInfo,
};
use rmcp::service::RequestContext;
use rmcp::{ErrorData, RoleServer, ServerHandler, tool, tool_handler, tool_router};
use schemars::JsonSchema;
use serde::Deserialize;

use crate::config::Config;
use crate::db::{PoolManager, ValueLimits, column_descriptors, encode_row, row_to_json};
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

/// Rejects rather than strips: filtering turned "siku.users" into "sikuusers", which either
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
    /// Database name (e.g. "siku-local", "siku-dev", "siku-prod")
    database: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct DescribeTableParams {
    /// Database name (e.g. "siku-local", "siku-dev", "siku-prod")
    database: String,
    /// Bare table name, unqualified: letters, digits, '_' or '$' only. "schema.table" is rejected —
    /// pick the schema with the 'database' parameter.
    table: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ExecuteQueryParams {
    /// Database name (e.g. "siku-local", "siku-dev", "siku-prod")
    database: String,
    /// SQL query. Read-only: must start with SELECT, WITH, TABLE, VALUES, SHOW, DESCRIBE/DESC,
    /// EXPLAIN, SET @var, or a parenthesized SELECT/TABLE/VALUES/WITH. Write statements (anywhere,
    /// not just first), INTO OUTFILE/DUMPFILE, FOR UPDATE/SHARE, LOCK IN SHARE MODE, LOAD_FILE(),
    /// SET GLOBAL/SESSION, /*! ... */ comments and multiple statements are rejected.
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
                // SHOW TABLES returns a single column with a dynamic name like "Tables_in_siku",
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
            EXPLAIN, SET @var, or a parenthesized SELECT/TABLE/VALUES/WITH.\n\
            REJECTED, at the start or nested anywhere: INSERT, UPDATE, DELETE, DROP, ALTER, RENAME, \
            GRANT, REVOKE, CREATE (SHOW CREATE is fine), REPLACE ... INTO, TRUNCATE TABLE, LOAD \
            DATA/XML; FLUSH, LOCK, UNLOCK, CALL and transaction control (BEGIN, START TRANSACTION, \
            COMMIT, ROLLBACK, SAVEPOINT); INTO OUTFILE, INTO DUMPFILE, FOR UPDATE, FOR SHARE, LOCK \
            IN SHARE MODE, LOAD_FILE(); SET GLOBAL/SESSION/PERSIST (only SET @var is allowed); more \
            than one statement (one trailing ';' is fine); and /*! version-gated */ comments, whose \
            body MySQL would execute. The keyword scan ignores anything inside '...', \"...\" and \
            `...`, so REPLACE()/INSERT()/TRUNCATE() as functions and a column named `delete` pass. \
            Ordinary comments (--, #, /* */) are stripped; optimizer hints (/*+ ... */) are kept.\n\
            SET @var is accepted but nearly useless here: multi-statement is blocked, so it cannot be \
            set and read in one call, and the next call may land on a different pooled connection. \
            Use a subquery or a CTE instead.\n\
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
            Ok(Ok(mut rows)) => {
                // Also catches a caller-supplied LIMIT above the cap, and SHOW/EXPLAIN results,
                // which never get a LIMIT injected.
                let cap = max_rows as usize;
                let truncated = rows.len() > cap;
                if truncated {
                    rows.truncate(cap);
                }

                let columns = rows.first().map(column_descriptors).unwrap_or_default();

                let limits = ValueLimits::from(&self.config);
                let mut decode_errors = 0usize;
                let mut truncated_values = 0usize;
                let data: Vec<serde_json::Value> = rows
                    .iter()
                    .map(|row| {
                        let encoded = encode_row(row, &limits);
                        decode_errors += encoded.decode_errors;
                        truncated_values += encoded.truncated_values;
                        serde_json::Value::Object(encoded.values)
                    })
                    .collect();

                let row_count = data.len();
                let mut message = if truncated {
                    format!(
                        "Query executed successfully. Capped at {max_rows} row(s); more rows matched."
                    )
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

                Ok(serde_json::json!({
                    "success": true,
                    "database": p.database,
                    "columns": columns,
                    "data": data,
                    "rowCount": row_count,
                    "truncated": truncated,
                    "decodeErrors": decode_errors,
                    "truncatedValues": truncated_values,
                    "executionTime": format!("{}ms", elapsed.as_millis()),
                    "message": message,
                })
                .to_string())
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
}

#[tool_handler]
impl ServerHandler for MysqlMcp {
    fn get_info(&self) -> ServerInfo {
        let db_names = self.pool_manager.database_names();
        let mut info = ServerInfo::default();
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
                Annotated::new(
                    RawResource::new(format!("mysql://{}", db.name), db.name.clone())
                        .with_description(format!(
                            "MySQL database '{}' (schema: {})",
                            db.name, db.database
                        ))
                        .with_mime_type("application/json"),
                    None,
                )
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
    ) -> Result<ReadResourceResult, ErrorData> {
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
        )]))
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
        // "siku.users" used to be filtered down to "sikuusers", "my-table" to "mytable".
        for raw in ["siku.users", "my-table", "a`drop`--", "users users"] {
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
}
