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
use crate::db::{PoolManager, row_to_json};
use crate::sanitizer;

fn safe_table_ident(raw: &str) -> Result<String, rmcp::ErrorData> {
    let table_name: String = raw
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect();
    if table_name.is_empty() {
        return Err(rmcp::ErrorData::invalid_params(
            "Invalid table name".to_string(),
            None,
        ));
    }
    Ok(table_name)
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

fn json_map_i64(obj: Option<&serde_json::Map<String, serde_json::Value>>, key: &str) -> i64 {
    obj.and_then(|o| o.get(key))
        .and_then(|v| v.as_i64())
        .unwrap_or(1)
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

fn table_status_metadata_json(raw: &serde_json::Value) -> serde_json::Value {
    let obj = raw.as_object();
    serde_json::json!({
        "engine": json_map_clone(obj, "Engine"),
        "rows": json_map_clone(obj, "Rows"),
        "collation": json_map_clone(obj, "Collation"),
        "comment": json_map_clone(obj, "Comment"),
    })
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
    /// Name of the table to describe
    table: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
struct ExecuteQueryParams {
    /// Database name (e.g. "siku-local", "siku-dev", "siku-prod")
    database: String,
    /// SQL query to execute (read-only operations only: SELECT, SHOW, DESCRIBE, EXPLAIN, WITH)
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
                    "Failed to list tables. Check server logs for details.".to_string(),
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
                // SHOW TABLES returns a single column with a dynamic name like "Tables_in_siku"
                // Use row_to_json to handle type conversion properly, then extract the first value
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
        description = "Get detailed schema information for a specific table including columns, indexes, and metadata"
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

        // Get columns
        let columns = sqlx::query(&format!("DESCRIBE `{table_name}`"))
            .fetch_all(&pool)
            .await
            .map_err(|e| {
                tracing::error!(table = %table_name, error = %e, "DESCRIBE query failed");
                rmcp::ErrorData::internal_error(
                    format!(
                        "Failed to describe table '{table_name}'. Check server logs for details."
                    ),
                    None,
                )
            })?;

        let formatted_columns: Vec<serde_json::Value> = columns
            .iter()
            .map(|row| describe_column_json(&row_to_json(row)))
            .collect();

        // Get indexes
        let indexes = sqlx::query(&format!("SHOW INDEX FROM `{table_name}`"))
            .fetch_all(&pool)
            .await
            .unwrap_or_default();

        let mut index_map: std::collections::HashMap<String, serde_json::Value> =
            std::collections::HashMap::new();

        for row in &indexes {
            let raw = row_to_json(row);
            let obj = raw.as_object();
            let key_name = json_map_str(obj, "Key_name");
            let col_name = json_map_str(obj, "Column_name");
            let non_unique = json_map_i64(obj, "Non_unique");

            let entry = index_map.entry(key_name.clone()).or_insert_with(|| {
                serde_json::json!({
                    "name": key_name,
                    "unique": non_unique == 0,
                    "columns": []
                })
            });

            if let Some(cols) = entry.get_mut("columns").and_then(|c| c.as_array_mut()) {
                cols.push(serde_json::Value::String(col_name));
            }
        }

        let formatted_indexes: Vec<serde_json::Value> = index_map.into_values().collect();

        // Get table status
        let status = sqlx::query(&format!("SHOW TABLE STATUS LIKE '{table_name}'"))
            .fetch_optional(&pool)
            .await
            .unwrap_or(None);

        let metadata = status.map(|row| table_status_metadata_json(&row_to_json(&row)));

        Ok(serde_json::json!({
            "table": table_name,
            "database": p.database,
            "columns": formatted_columns,
            "indexes": formatted_indexes,
            "metadata": metadata,
            "summary": format!("Table '{}' has {} columns and {} indexes", table_name, formatted_columns.len(), formatted_indexes.len())
        })
        .to_string())
    }

    #[tool(
        name = "execute_query",
        description = "Execute a read-only SQL query against a database. Only SELECT, SHOW, DESCRIBE, EXPLAIN, WITH, and SET @ are allowed."
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

        // Sanitize the query
        let result = sanitizer::sanitize(&p.query);
        if !result.is_valid {
            return Ok(serde_json::json!({
                "success": false,
                "error": result.error,
                "message": "Query rejected: only read-only queries are allowed."
            })
            .to_string());
        }

        // Apply row limit
        let max_rows = self.config.default_max_rows;
        let final_query = sanitizer::apply_limit(&result.sanitized_query, max_rows);

        // Get query timeout from database config
        let timeout_secs = self
            .pool_manager
            .get_config(&p.database)
            .map(|c| c.query_timeout_secs)
            .unwrap_or(30);

        // Execute with timeout
        let start = Instant::now();
        let query_result = tokio::time::timeout(
            std::time::Duration::from_secs(timeout_secs),
            sqlx::query(&final_query).fetch_all(&pool),
        )
        .await;

        let elapsed = start.elapsed();

        match query_result {
            Ok(Ok(rows)) => {
                let data: Vec<serde_json::Value> = rows.iter().map(row_to_json).collect();
                let row_count = data.len();
                let truncated = row_count as u32 == max_rows;

                Ok(serde_json::json!({
                    "success": true,
                    "database": p.database,
                    "data": data,
                    "rowCount": row_count,
                    "truncated": truncated,
                    "executionTime": format!("{}ms", elapsed.as_millis()),
                    "message": if truncated {
                        format!("Query executed successfully. Showing first {max_rows} rows.")
                    } else {
                        format!("Query executed successfully. {row_count} row(s) returned.")
                    }
                })
                .to_string())
            }
            Ok(Err(e)) => {
                tracing::error!(database = %p.database, error = %e, "Query execution failed");
                Ok(serde_json::json!({
                    "success": false,
                    "database": p.database,
                    "error": "Query execution failed",
                    "message": "Query execution failed. Check server logs for details."
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
    }

    #[test]
    fn safe_table_ident_keeps_alphanumeric_and_underscore() {
        assert_eq!(safe_table_ident("users_1").unwrap(), "users_1");
        assert_eq!(safe_table_ident("a`drop`--").unwrap(), "adrop");
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
}
