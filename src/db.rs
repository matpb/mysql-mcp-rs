use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use rust_decimal::Decimal;
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::{Column, Row, TypeInfo};

use crate::config::{Config, DatabaseConfig};

pub struct PoolManager {
    pools: tokio::sync::RwLock<HashMap<String, MySqlPool>>,
    configs: HashMap<String, DatabaseConfig>,
}

impl PoolManager {
    pub async fn new(config: &Config) -> Self {
        let mut pools = HashMap::new();
        let mut configs = HashMap::new();

        for db_config in &config.databases {
            configs.insert(db_config.name.clone(), db_config.clone());

            match Self::try_connect(db_config).await {
                Ok(pool) => {
                    tracing::info!(
                        "Connected to database '{}' at {}:{}",
                        db_config.name,
                        db_config.host,
                        db_config.port
                    );
                    pools.insert(db_config.name.clone(), pool);
                }
                Err(e) => {
                    tracing::warn!(
                        "Database '{}' unavailable at startup (will retry on access): {e}",
                        db_config.name
                    );
                }
            }
        }

        Self {
            pools: tokio::sync::RwLock::new(pools),
            configs,
        }
    }

    async fn try_connect(db_config: &DatabaseConfig) -> Result<MySqlPool, String> {
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
            .map_err(|e| format!("{e}"))?;

        sqlx::query("SELECT 1")
            .execute(&pool)
            .await
            .map_err(|e| format!("ping failed: {e}"))?;

        Ok(pool)
    }

    pub async fn get_pool(&self, name: &str) -> Result<MySqlPool, String> {
        // Fast path: pool already exists
        {
            let pools = self.pools.read().await;
            if let Some(pool) = pools.get(name) {
                return Ok(pool.clone());
            }
        }

        // Check if it's a known database
        let db_config = self.configs.get(name).ok_or_else(|| {
            let available: Vec<&str> = self.configs.keys().map(|s| s.as_str()).collect();
            format!("Unknown database '{name}'. Configured: {available:?}")
        })?;

        // Try to connect
        tracing::info!("Attempting to connect to database '{name}'...");
        let pool = Self::try_connect(db_config)
            .await
            .map_err(|e| format!("Database '{name}' is currently unavailable: {e}"))?;

        tracing::info!(
            "Connected to database '{}' at {}:{}",
            db_config.name,
            db_config.host,
            db_config.port
        );

        let mut pools = self.pools.write().await;
        pools.insert(name.to_string(), pool.clone());
        Ok(pool)
    }

    pub fn get_config(&self, name: &str) -> Option<&DatabaseConfig> {
        self.configs.get(name)
    }

    pub fn database_names(&self) -> Vec<&str> {
        self.configs.keys().map(|s| s.as_str()).collect()
    }

    pub async fn close_all(&self) {
        let pools = self.pools.read().await;
        for (name, pool) in pools.iter() {
            pool.close().await;
            tracing::info!("Closed connection pool for '{name}'");
        }
    }
}

/// Convert a MySQL row to a JSON object keyed by column name.
///
/// sqlx reports MySQL type names with length suffixes (e.g. `VARCHAR(64)`, `INT UNSIGNED`),
/// so we normalize to uppercase and match on prefixes rather than exact strings.
///
/// Extractions use `Option<T>` so NULL becomes JSON `null` without conflating it with type errors.
pub fn row_to_json(row: &MySqlRow) -> serde_json::Value {
    let mut map = serde_json::Map::new();

    for col in row.columns() {
        let name = col.name().to_string();
        let type_name = col.type_info().name().to_uppercase();
        tracing::debug!(
            "Column '{name}' type_info='{}' normalized='{type_name}'",
            col.type_info().name()
        );

        let value = mysql_value_to_json(row, &name, &type_name);
        map.insert(name, value);
    }

    serde_json::Value::Object(map)
}

/// JSON number for MySQL unsigned integer columns (sqlx uses u32/u16/u8 depending on width).
fn unsigned_integer_to_json(row: &MySqlRow, name: &str) -> serde_json::Value {
    if let Ok(Some(v)) = row.try_get::<Option<u32>, _>(name) {
        return serde_json::Value::Number(v.into());
    }
    if let Ok(Some(v)) = row.try_get::<Option<u16>, _>(name) {
        return serde_json::Value::Number(v.into());
    }
    if let Ok(Some(v)) = row.try_get::<Option<u8>, _>(name) {
        return serde_json::Value::Number(v.into());
    }
    serde_json::Value::Null
}

fn mysql_value_to_json(row: &MySqlRow, name: &str, type_name: &str) -> serde_json::Value {
    if type_name == "NULL" {
        return serde_json::Value::Null;
    }

    if type_name == "BOOLEAN" || type_name == "BOOL" || type_name == "TINYINT(1)" {
        return row
            .try_get::<Option<bool>, _>(name)
            .ok()
            .flatten()
            .map(serde_json::Value::Bool)
            .unwrap_or(serde_json::Value::Null);
    }

    if type_name == "JSON" {
        return row
            .try_get::<Option<serde_json::Value>, _>(name)
            .ok()
            .flatten()
            .unwrap_or(serde_json::Value::Null);
    }

    if type_name.starts_with("BIGINT UNSIGNED") {
        return row
            .try_get::<Option<u64>, _>(name)
            .ok()
            .flatten()
            .map(|v| serde_json::Value::String(v.to_string()))
            .unwrap_or(serde_json::Value::Null);
    }

    // Signed BIGINT only. Do not use `contains("UNSIGNED")` here — INT UNSIGNED is decoded as
    // u32 by sqlx, not i64, and would incorrectly fall through as null.
    if type_name.starts_with("BIGINT") {
        return row
            .try_get::<Option<i64>, _>(name)
            .ok()
            .flatten()
            .map(|v| serde_json::Value::Number(v.into()))
            .unwrap_or(serde_json::Value::Null);
    }

    // TINYINT/SMALLINT/MEDIUMINT/INT UNSIGNED (and INTEGER UNSIGNED): sqlx uses u8/u16/u32.
    if type_name.contains("UNSIGNED")
        && (type_name.starts_with("TINYINT")
            || type_name.starts_with("SMALLINT")
            || type_name.starts_with("MEDIUMINT")
            || type_name.starts_with("INT")
            || type_name.starts_with("INTEGER"))
    {
        return unsigned_integer_to_json(row, name);
    }

    if type_name.starts_with("TINYINT")
        || type_name.starts_with("SMALLINT")
        || type_name.starts_with("MEDIUMINT")
        || type_name.starts_with("INT")
        || type_name.starts_with("INTEGER")
    {
        return row
            .try_get::<Option<i32>, _>(name)
            .ok()
            .flatten()
            .map(|v| serde_json::Value::Number(v.into()))
            .unwrap_or(serde_json::Value::Null);
    }

    if type_name.starts_with("FLOAT") {
        return row
            .try_get::<Option<f32>, _>(name)
            .ok()
            .flatten()
            .and_then(|v| serde_json::Number::from_f64(f64::from(v)))
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null);
    }

    if type_name.starts_with("DOUBLE") {
        return row
            .try_get::<Option<f64>, _>(name)
            .ok()
            .flatten()
            .and_then(serde_json::Number::from_f64)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null);
    }

    if type_name.starts_with("DECIMAL") || type_name.starts_with("NUMERIC") {
        use rust_decimal::prelude::ToPrimitive;
        return row
            .try_get::<Option<Decimal>, _>(name)
            .ok()
            .flatten()
            .map(|d| {
                d.to_f64()
                    .and_then(serde_json::Number::from_f64)
                    .map(serde_json::Value::Number)
                    .unwrap_or_else(|| serde_json::Value::String(d.to_string()))
            })
            .unwrap_or(serde_json::Value::Null);
    }

    if type_name.starts_with("TIMESTAMP") {
        return match row.try_get::<Option<DateTime<Utc>>, _>(name) {
            Ok(opt) => opt
                .map(|dt| serde_json::Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()))
                .unwrap_or(serde_json::Value::Null),
            Err(e) => {
                tracing::warn!("Failed to decode {name} (type={type_name}): {e:?}");
                serde_json::Value::Null
            }
        };
    }

    if type_name.starts_with("DATETIME") {
        return match row.try_get::<Option<NaiveDateTime>, _>(name) {
            Ok(opt) => opt
                .map(|dt| serde_json::Value::String(dt.format("%Y-%m-%d %H:%M:%S").to_string()))
                .unwrap_or(serde_json::Value::Null),
            Err(e) => {
                tracing::warn!("Failed to decode {name} (type={type_name}): {e:?}");
                serde_json::Value::Null
            }
        };
    }

    if type_name.starts_with("DATE") {
        return row
            .try_get::<Option<NaiveDate>, _>(name)
            .ok()
            .flatten()
            .map(|d| serde_json::Value::String(d.format("%Y-%m-%d").to_string()))
            .unwrap_or(serde_json::Value::Null);
    }

    if type_name.starts_with("TIME") {
        return row
            .try_get::<Option<NaiveTime>, _>(name)
            .ok()
            .flatten()
            .map(|t| serde_json::Value::String(t.format("%H:%M:%S").to_string()))
            .unwrap_or(serde_json::Value::Null);
    }

    string_or_binary_fallback(row, name, type_name)
}

/// VARCHAR/TEXT/ENUM/BLOB and unknown types: prefer UTF-8 string, then hex, then a placeholder.
fn string_or_binary_fallback(row: &MySqlRow, name: &str, type_name: &str) -> serde_json::Value {
    match row.try_get::<Option<String>, _>(name) {
        Ok(Some(s)) => serde_json::Value::String(s),
        Ok(None) => serde_json::Value::Null,
        Err(_) => match row.try_get::<Option<Vec<u8>>, _>(name) {
            Ok(Some(v)) => match String::from_utf8(v.clone()) {
                Ok(s) => serde_json::Value::String(s),
                Err(_) => hex_prefix_string(&v),
            },
            Ok(None) => serde_json::Value::Null,
            Err(_) => {
                let hint = type_name.to_lowercase();
                serde_json::Value::String(format!("[binary:{hint}]"))
            }
        },
    }
}

fn hex_prefix_string(v: &[u8]) -> serde_json::Value {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_prefix_truncates_long_binary() {
        let bytes: Vec<u8> = (0..150).collect();
        let v = hex_prefix_string(&bytes);
        let s = v.as_str().unwrap_or("");
        assert!(s.ends_with("..."));
        assert!(s.starts_with("0x"));
    }
}
