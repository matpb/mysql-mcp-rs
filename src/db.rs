use std::collections::HashMap;
use std::time::Duration;

use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::{Column, Row, TypeInfo};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use rust_decimal::Decimal;

use crate::config::{Config, DatabaseConfig};

pub struct PoolManager {
    pools: HashMap<String, MySqlPool>,
    configs: HashMap<String, DatabaseConfig>,
}

impl PoolManager {
    pub async fn new(config: &Config) -> Self {
        let mut pools = HashMap::new();
        let mut configs = HashMap::new();

        for db_config in &config.databases {
            let opts = MySqlConnectOptions::new()
                .host(&db_config.host)
                .port(db_config.port)
                .username(&db_config.user)
                .password(&db_config.password)
                .database(&db_config.database);

            let pool = MySqlPoolOptions::new()
                .max_connections(db_config.max_connections)
                .acquire_timeout(Duration::from_secs(10))
                .idle_timeout(Duration::from_secs(300))
                .connect_with(opts)
                .await
                .unwrap_or_else(|e| {
                    panic!("Failed to connect to database '{}': {e}", db_config.name)
                });

            // Validate connection
            sqlx::query("SELECT 1")
                .execute(&pool)
                .await
                .unwrap_or_else(|e| {
                    panic!("Failed to ping database '{}': {e}", db_config.name)
                });

            tracing::info!("Connected to database '{}' at {}:{}", db_config.name, db_config.host, db_config.port);
            pools.insert(db_config.name.clone(), pool);
            configs.insert(db_config.name.clone(), db_config.clone());
        }

        Self { pools, configs }
    }

    pub fn get_pool(&self, name: &str) -> Result<&MySqlPool, String> {
        self.pools
            .get(name)
            .ok_or_else(|| {
                let available: Vec<&str> = self.pools.keys().map(|s| s.as_str()).collect();
                format!("Unknown database '{name}'. Available: {available:?}")
            })
    }

    pub fn get_config(&self, name: &str) -> Option<&DatabaseConfig> {
        self.configs.get(name)
    }

    pub fn database_names(&self) -> Vec<&str> {
        self.pools.keys().map(|s| s.as_str()).collect()
    }

    pub async fn close_all(&self) {
        for (name, pool) in &self.pools {
            pool.close().await;
            tracing::info!("Closed connection pool for '{name}'");
        }
    }
}

/// Convert a MySQL row to a serde_json::Value object.
///
/// sqlx reports MySQL type names with length suffixes (e.g. "VARCHAR(64)", "INT UNSIGNED"),
/// so we normalize to uppercase and match on prefixes rather than exact strings.
///
/// All extractions use `Option<T>` to handle NULL values cleanly — sqlx's `try_get::<T>`
/// returns an error (not Ok(null)) when the column is NULL, which would otherwise mask
/// the difference between "wrong type" and "null value".
pub fn row_to_json(row: &MySqlRow) -> serde_json::Value {
    let mut map = serde_json::Map::new();

    for col in row.columns() {
        let name = col.name().to_string();
        let type_name = col.type_info().name().to_uppercase();

        let value: serde_json::Value = if type_name == "NULL" {
            serde_json::Value::Null
        } else if type_name == "BOOLEAN" || type_name == "BOOL" || type_name == "TINYINT(1)" {
            row.try_get::<Option<bool>, _>(name.as_str())
                .ok()
                .flatten()
                .map(serde_json::Value::Bool)
                .unwrap_or(serde_json::Value::Null)
        } else if type_name == "JSON" {
            row.try_get::<Option<serde_json::Value>, _>(name.as_str())
                .ok()
                .flatten()
                .unwrap_or(serde_json::Value::Null)
        } else if type_name.starts_with("BIGINT UNSIGNED") {
            row.try_get::<Option<u64>, _>(name.as_str())
                .ok()
                .flatten()
                .map(|v| serde_json::Value::String(v.to_string()))
                .unwrap_or(serde_json::Value::Null)
        } else if type_name.starts_with("BIGINT")
            || type_name.contains("UNSIGNED")
        {
            row.try_get::<Option<i64>, _>(name.as_str())
                .ok()
                .flatten()
                .map(|v| serde_json::Value::Number(v.into()))
                .unwrap_or(serde_json::Value::Null)
        } else if type_name.starts_with("TINYINT")
            || type_name.starts_with("SMALLINT")
            || type_name.starts_with("MEDIUMINT")
            || type_name.starts_with("INT")
            || type_name.starts_with("INTEGER")
        {
            row.try_get::<Option<i32>, _>(name.as_str())
                .ok()
                .flatten()
                .map(|v| serde_json::Value::Number(v.into()))
                .unwrap_or(serde_json::Value::Null)
        } else if type_name.starts_with("FLOAT") {
            row.try_get::<Option<f32>, _>(name.as_str())
                .ok()
                .flatten()
                .and_then(|v| serde_json::Number::from_f64(v as f64))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        } else if type_name.starts_with("DOUBLE") {
            row.try_get::<Option<f64>, _>(name.as_str())
                .ok()
                .flatten()
                .and_then(|v| serde_json::Number::from_f64(v))
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        } else if type_name.starts_with("DECIMAL") || type_name.starts_with("NUMERIC") {
            // Use rust_decimal::Decimal for proper extraction, then convert to JSON number
            row.try_get::<Option<Decimal>, _>(name.as_str())
                .ok()
                .flatten()
                .map(|d| {
                    // Try to represent as a JSON number (f64), fall back to string
                    use rust_decimal::prelude::ToPrimitive;
                    d.to_f64()
                        .and_then(|v| serde_json::Number::from_f64(v))
                        .map(serde_json::Value::Number)
                        .unwrap_or_else(|| serde_json::Value::String(d.to_string()))
                })
                .unwrap_or(serde_json::Value::Null)
        } else if type_name.starts_with("DATETIME") || type_name.starts_with("TIMESTAMP") {
            row.try_get::<Option<NaiveDateTime>, _>(name.as_str())
                .ok()
                .flatten()
                .map(|dt| serde_json::Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()))
                .unwrap_or(serde_json::Value::Null)
        } else if type_name.starts_with("DATE") {
            row.try_get::<Option<NaiveDate>, _>(name.as_str())
                .ok()
                .flatten()
                .map(|d| serde_json::Value::String(d.format("%Y-%m-%d").to_string()))
                .unwrap_or(serde_json::Value::Null)
        } else if type_name.starts_with("TIME") {
            row.try_get::<Option<NaiveTime>, _>(name.as_str())
                .ok()
                .flatten()
                .map(|t| serde_json::Value::String(t.format("%H:%M:%S").to_string()))
                .unwrap_or(serde_json::Value::Null)
        } else {
            // Default: try String first, then bytes with UTF-8 decode, then hex.
            // This covers VARCHAR, TEXT, CHAR, ENUM, SET, and also BLOB/BINARY
            // columns that MySQL uses for metadata results (e.g. DESCRIBE, SHOW INDEX,
            // information_schema) which contain text despite being typed as binary.
            //
            // We must distinguish "value is NULL" (Ok(None)) from "wrong type" (Err)
            // to avoid falling through to the [binary:...] placeholder for NULLs.
            match row.try_get::<Option<String>, _>(name.as_str()) {
                Ok(Some(s)) => serde_json::Value::String(s),
                Ok(None) => serde_json::Value::Null,
                Err(_) => {
                    // String extraction failed — try bytes
                    match row.try_get::<Option<Vec<u8>>, _>(name.as_str()) {
                        Ok(Some(v)) => {
                            // Try UTF-8 first — many "BLOB" columns are actually text
                            match String::from_utf8(v.clone()) {
                                Ok(s) => serde_json::Value::String(s),
                                Err(_) => {
                                    use std::fmt::Write;
                                    let mut hex = String::with_capacity(v.len() * 2 + 2);
                                    hex.push_str("0x");
                                    for byte in &v[..v.len().min(100)] {
                                        let _ = write!(hex, "{byte:02x}");
                                    }
                                    if v.len() > 100 {
                                        hex.push_str("...");
                                    }
                                    serde_json::Value::String(hex)
                                }
                            }
                        }
                        Ok(None) => serde_json::Value::Null,
                        Err(_) => {
                            // Neither String nor Vec<u8> worked — this is a type sqlx
                            // can't decode generically (e.g. GEOMETRY, POINT, POLYGON).
                            let hint = type_name.to_lowercase();
                            serde_json::Value::String(format!("[binary:{hint}]"))
                        }
                    }
                }
            }
        };

        map.insert(name, value);
    }

    serde_json::Value::Object(map)
}
