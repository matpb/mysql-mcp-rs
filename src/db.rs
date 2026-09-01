use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, NaiveDateTime, Timelike, Utc};
use serde_json::{Map, Number, Value};
use sqlx::mysql::types::{MySqlTime, MySqlTimeSign};
use sqlx::mysql::{MySqlConnectOptions, MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::{Column, Executor, Row, TypeInfo};

use crate::config::{Config, DatabaseConfig};

/// Largest integer a JSON consumer can hold in an f64 without loss (2^53 - 1).
const MAX_SAFE_INTEGER: i128 = 9_007_199_254_740_991;

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

        let max_execution_ms = db_config.query_timeout_secs.saturating_mul(1000);

        let pool = MySqlPoolOptions::new()
            .max_connections(db_config.max_connections)
            .acquire_timeout(Duration::from_secs(10))
            .idle_timeout(Duration::from_secs(300))
            .after_connect(move |conn, _meta| {
                let ceiling = format!("SET SESSION MAX_EXECUTION_TIME = {max_execution_ms}");
                Box::pin(async move {
                    // Server-side ceiling: the client timeout only abandons the future, MySQL
                    // keeps executing. SELECT-only, and absent on MariaDB, so failure is a warning.
                    if let Err(e) = (&mut *conn).execute(ceiling.as_str()).await {
                        tracing::warn!("MAX_EXECUTION_TIME unsupported on this server: {e}");
                    }
                    (&mut *conn)
                        .execute("SET SESSION TRANSACTION READ ONLY")
                        .await?;
                    Ok::<(), sqlx::Error>(())
                })
            })
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

/// Byte caps applied to a single encoded value.
#[derive(Debug, Clone, Copy)]
pub struct ValueLimits {
    pub max_value_bytes: usize,
    pub max_binary_preview_bytes: usize,
}

impl Default for ValueLimits {
    fn default() -> Self {
        Self {
            max_value_bytes: 4096,
            max_binary_preview_bytes: 256,
        }
    }
}

impl From<&Config> for ValueLimits {
    fn from(config: &Config) -> Self {
        Self {
            max_value_bytes: config.max_value_bytes,
            max_binary_preview_bytes: config.max_binary_preview_bytes,
        }
    }
}

/// One encoded row plus the counters the response envelope reports.
pub struct EncodedRow {
    pub values: Map<String, Value>,
    pub decode_errors: usize,
    pub truncated_values: usize,
}

struct Encoded {
    value: Value,
    error: Option<String>,
    truncated: bool,
}

impl Encoded {
    fn plain(value: Value) -> Self {
        Self {
            value,
            error: None,
            truncated: false,
        }
    }

    fn cut(value: Value) -> Self {
        Self {
            value,
            error: None,
            truncated: true,
        }
    }

    fn failed(type_name: &str, message: String) -> Self {
        Self {
            value: decode_error_value(type_name, &message),
            error: Some(message),
            truncated: false,
        }
    }
}

/// Convert a MySQL row to a JSON object keyed by de-duplicated column name.
///
/// Thin wrapper over [`encode_row`] for callers that do not report the counters.
pub fn row_to_json(row: &MySqlRow) -> Value {
    Value::Object(encode_row(row, &ValueLimits::default()).values)
}

/// Encode every column of a row, counting decode failures and truncated values.
pub fn encode_row(row: &MySqlRow, limits: &ValueLimits) -> EncodedRow {
    let names = column_names(row);
    let mut values = Map::new();
    let mut decode_errors = 0;
    let mut truncated_values = 0;

    for (idx, col) in row.columns().iter().enumerate() {
        let type_name = col.type_info().name();
        // Look up by ordinal, not name: sqlx cannot resolve EXPLAIN column names, which made
        // every field decode as the "[binary:...]" placeholder.
        let encoded = encode_value(row, idx, type_name, limits);

        if let Some(error) = &encoded.error {
            decode_errors += 1;
            tracing::warn!(
                column = %names[idx],
                ordinal = idx,
                sql_type = %type_name,
                error = %error,
                "column value failed to decode"
            );
        }
        if encoded.truncated {
            truncated_values += 1;
        }

        values.insert(names[idx].clone(), encoded.value);
    }

    EncodedRow {
        values,
        decode_errors,
        truncated_values,
    }
}

/// `[{name, type, originalName?}]` in true ordinal order, emitted once per result.
pub fn column_descriptors(row: &MySqlRow) -> Vec<Value> {
    let names = column_names(row);
    row.columns()
        .iter()
        .enumerate()
        .map(|(idx, col)| column_descriptor(&names[idx], col.name(), col.type_info().name()))
        .collect()
}

fn column_descriptor(name: &str, original: &str, type_name: &str) -> Value {
    let mut descriptor = Map::new();
    descriptor.insert("name".into(), Value::String(name.to_string()));
    descriptor.insert("type".into(), Value::String(type_name.to_string()));
    if name != original {
        descriptor.insert("originalName".into(), Value::String(original.to_string()));
    }
    Value::Object(descriptor)
}

/// The single de-duplication authority: row keys and column descriptors both come from here.
fn column_names(row: &MySqlRow) -> Vec<String> {
    let raw: Vec<&str> = row.columns().iter().map(|c| c.name()).collect();
    dedup_column_names(&raw)
}

/// Left-to-right de-duplication: the Nth occurrence of a name becomes `{name}__{N}`,
/// bumping the suffix until it no longer collides with a name already taken.
fn dedup_column_names(raw: &[&str]) -> Vec<String> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut used: HashSet<String> = HashSet::new();
    let mut names = Vec::with_capacity(raw.len());

    for (idx, name) in raw.iter().enumerate() {
        let base = if name.is_empty() {
            format!("column_{idx}")
        } else {
            (*name).to_string()
        };

        let mut n = counts.get(&base).copied().unwrap_or(0) + 1;
        let mut candidate = if n == 1 {
            base.clone()
        } else {
            format!("{base}__{n}")
        };
        while used.contains(&candidate) {
            n += 1;
            candidate = format!("{base}__{n}");
        }

        counts.insert(base, n);
        used.insert(candidate.clone());
        names.push(candidate);
    }

    names
}

fn encode_value(row: &MySqlRow, idx: usize, type_name: &str, limits: &ValueLimits) -> Encoded {
    match type_name {
        "NULL" => Encoded::plain(Value::Null),

        // tinyint(1): Bool only for exactly 0/1, so a small-int use of the type stays honest.
        "BOOLEAN" => match row.try_get_unchecked::<Option<i64>, _>(idx) {
            Ok(None) => Encoded::plain(Value::Null),
            Ok(Some(0)) => Encoded::plain(Value::Bool(false)),
            Ok(Some(1)) => Encoded::plain(Value::Bool(true)),
            Ok(Some(v)) => Encoded::plain(int_to_json(v.into())),
            Err(e) => Encoded::failed(type_name, decode_error_message(&e)),
        },

        "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" => {
            match row.try_get::<Option<i64>, _>(idx) {
                Ok(opt) => Encoded::plain(opt.map_or(Value::Null, |v| int_to_json(v.into()))),
                Err(e) => Encoded::failed(type_name, decode_error_message(&e)),
            }
        }

        "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "MEDIUMINT UNSIGNED" | "INT UNSIGNED"
        | "BIGINT UNSIGNED" => match row.try_get::<Option<u64>, _>(idx) {
            Ok(opt) => Encoded::plain(opt.map_or(Value::Null, |v| int_to_json(v.into()))),
            Err(e) => Encoded::failed(type_name, decode_error_message(&e)),
        },

        // YEAR and BIT have no checked path: neither str nor bytes accept them.
        "YEAR" | "BIT" => match row.try_get_unchecked::<Option<u64>, _>(idx) {
            Ok(opt) => Encoded::plain(opt.map_or(Value::Null, |v| int_to_json(v.into()))),
            Err(e) => Encoded::failed(type_name, decode_error_message(&e)),
        },

        "FLOAT" => match row.try_get::<Option<f32>, _>(idx) {
            Ok(opt) => Encoded::plain(opt.map_or(Value::Null, f32_to_json)),
            Err(e) => Encoded::failed(type_name, decode_error_message(&e)),
        },

        "DOUBLE" => match row.try_get::<Option<f64>, _>(idx) {
            Ok(opt) => Encoded::plain(opt.map_or(Value::Null, f64_to_json)),
            Err(e) => Encoded::failed(type_name, decode_error_message(&e)),
        },

        "DECIMAL" => match row.try_get_unchecked::<Option<String>, _>(idx) {
            Ok(opt) => Encoded::plain(decimal_to_json(opt)),
            Err(e) => Encoded::failed(type_name, decode_error_message(&e)),
        },

        "DATE" => match row.try_get::<Option<NaiveDate>, _>(idx) {
            Ok(opt) => Encoded::plain(opt.map_or(Value::Null, |d| {
                Value::String(d.format("%Y-%m-%d").to_string())
            })),
            Err(e) => zero_or_failed(row, idx, type_name, &e),
        },

        "DATETIME" => match row.try_get::<Option<NaiveDateTime>, _>(idx) {
            Ok(opt) => Encoded::plain(
                opt.map_or(Value::Null, |dt| Value::String(format_naive_datetime(&dt))),
            ),
            Err(e) => zero_or_failed(row, idx, type_name, &e),
        },

        // The session runs at +00:00 (MySqlConnectOptions default), so this really is UTC.
        "TIMESTAMP" => match row.try_get::<Option<DateTime<Utc>>, _>(idx) {
            Ok(opt) => {
                Encoded::plain(opt.map_or(Value::Null, |dt| Value::String(format_timestamp(&dt))))
            }
            Err(e) => zero_or_failed(row, idx, type_name, &e),
        },

        // NaiveTime would drop the sign and reject anything past 23:59:59.
        "TIME" => match row.try_get::<Option<MySqlTime>, _>(idx) {
            Ok(opt) => Encoded::plain(opt.map_or(Value::Null, |t| {
                Value::String(format_mysql_time(
                    t.sign() == MySqlTimeSign::Negative,
                    t.hours(),
                    t.minutes(),
                    t.seconds(),
                    t.microseconds(),
                ))
            })),
            Err(e) => Encoded::failed(type_name, decode_error_message(&e)),
        },

        "CHAR" | "VARCHAR" | "ENUM" | "TINYTEXT" | "TEXT" | "MEDIUMTEXT" | "LONGTEXT" => {
            match row.try_get_unchecked::<Option<String>, _>(idx) {
                Ok(None) => Encoded::plain(Value::Null),
                Ok(Some(s)) => encode_text(s, limits.max_value_bytes),
                Err(e) => Encoded::failed(type_name, decode_error_message(&e)),
            }
        }

        "JSON" => match row.try_get_unchecked::<Option<String>, _>(idx) {
            Ok(None) => Encoded::plain(Value::Null),
            Ok(Some(raw)) => encode_json_text(raw, type_name, limits.max_value_bytes),
            Err(e) => Encoded::failed(type_name, decode_error_message(&e)),
        },

        // Internal WKB, never text: sniffing it would emit mojibake for a readable-looking prefix.
        "GEOMETRY" => match row.try_get_unchecked::<Option<Vec<u8>>, _>(idx) {
            Ok(None) => Encoded::plain(Value::Null),
            Ok(Some(bytes)) => encode_hex(&bytes, limits.max_binary_preview_bytes),
            Err(e) => Encoded::failed(type_name, decode_error_message(&e)),
        },

        // UTF-8 wins when valid: BLOBs are routinely mis-declared to hold text, and MySQL types
        // its own SHOW/DESCRIBE result values BINARY, so always-hex here would break every one.
        "BINARY" | "VARBINARY" | "TINYBLOB" | "BLOB" | "MEDIUMBLOB" | "LONGBLOB" => {
            match row.try_get_unchecked::<Option<Vec<u8>>, _>(idx) {
                Ok(None) => Encoded::plain(Value::Null),
                Ok(Some(bytes)) => match String::from_utf8(bytes) {
                    Ok(s) => encode_text(s, limits.max_value_bytes),
                    Err(e) => encode_hex(e.as_bytes(), limits.max_binary_preview_bytes),
                },
                Err(e) => Encoded::failed(type_name, decode_error_message(&e)),
            }
        }

        // Only a type sqlx does not name (MariaDB) reaches here.
        _ => match row.try_get_unchecked::<Option<String>, _>(idx) {
            Ok(None) => Encoded::plain(Value::Null),
            Ok(Some(s)) => encode_text(s, limits.max_value_bytes),
            Err(_) => match row.try_get_unchecked::<Option<Vec<u8>>, _>(idx) {
                Ok(None) => Encoded::plain(Value::Null),
                Ok(Some(bytes)) => encode_hex(&bytes, limits.max_binary_preview_bytes),
                Err(e) => Encoded::failed(type_name, decode_error_message(&e)),
            },
        },
    }
}

/// MySQL's zero date is a value, not a failure; it must not become a decode sentinel.
fn zero_or_failed(row: &MySqlRow, idx: usize, type_name: &str, e: &sqlx::Error) -> Encoded {
    if let Ok(Some(raw)) = row.try_get_unchecked::<Option<String>, _>(idx)
        && is_zero_temporal(&raw)
    {
        return Encoded::plain(Value::Null);
    }
    Encoded::failed(type_name, decode_error_message(e))
}

fn is_zero_temporal(raw: &str) -> bool {
    !raw.is_empty()
        && raw.contains('0')
        && raw
            .chars()
            .all(|c| matches!(c, '0' | '-' | ':' | ' ' | '.'))
}

fn decode_error_value(type_name: &str, message: &str) -> Value {
    serde_json::json!({
        "__error__": "decode_failed",
        "type": type_name,
        "message": message,
    })
}

fn decode_error_message(e: &sqlx::Error) -> String {
    match e {
        sqlx::Error::ColumnDecode { source, .. } => source.to_string(),
        other => other.to_string(),
    }
}

/// The wire already carries the exact decimal literal; every float hop loses digits and
/// scale, so the text goes through verbatim.
fn decimal_to_json(raw: Option<String>) -> Value {
    raw.map_or(Value::Null, Value::String)
}

/// The one integer rule: exact below 2^53, exact decimal digits as a string above it.
fn int_to_json(v: i128) -> Value {
    if v.abs() <= MAX_SAFE_INTEGER {
        Value::Number(Number::from(v as i64))
    } else {
        Value::String(v.to_string())
    }
}

fn f32_to_json(v: f32) -> Value {
    if !v.is_finite() {
        return non_finite_to_json(f64::from(v));
    }
    // Display for f32 is the shortest round-tripping decimal, so no f32->f64 widening noise.
    let shortest = format!("{v}");
    match shortest.parse::<f64>().ok().and_then(Number::from_f64) {
        Some(n) => Value::Number(n),
        None => Value::String(shortest),
    }
}

fn f64_to_json(v: f64) -> Value {
    match Number::from_f64(v) {
        Some(n) => Value::Number(n),
        None => non_finite_to_json(v),
    }
}

fn non_finite_to_json(v: f64) -> Value {
    if v.is_nan() {
        Value::String("NaN".into())
    } else if v > 0.0 {
        Value::String("Infinity".into())
    } else {
        Value::String("-Infinity".into())
    }
}

fn format_naive_datetime(dt: &NaiveDateTime) -> String {
    let base = dt.format("%Y-%m-%d %H:%M:%S").to_string();
    match dt.time().nanosecond() / 1_000 {
        0 => base,
        micros => format!("{base}.{micros:06}"),
    }
}

fn format_timestamp(dt: &DateTime<Utc>) -> String {
    format!("{}Z", format_naive_datetime(&dt.naive_utc()))
}

/// MySQL TIME spans -838:59:59..838:59:59, so hours are not capped at 24 and the sign is kept.
fn format_mysql_time(negative: bool, hours: u32, minutes: u8, seconds: u8, micros: u32) -> String {
    let sign = if negative { "-" } else { "" };
    let base = format!("{sign}{hours:02}:{minutes:02}:{seconds:02}");
    match micros {
        0 => base,
        micros => format!("{base}.{micros:06}"),
    }
}

fn encode_text(s: String, max_bytes: usize) -> Encoded {
    match truncate_text(&s, max_bytes) {
        Some(cut) => Encoded::cut(Value::String(cut)),
        None => Encoded::plain(Value::String(s)),
    }
}

/// A parsed JSON document cannot be truncated without becoming invalid, so an oversized
/// one degrades to marked raw text instead.
fn encode_json_text(raw: String, type_name: &str, max_bytes: usize) -> Encoded {
    if let Some(cut) = truncate_text(&raw, max_bytes) {
        return Encoded::cut(Value::String(cut));
    }
    match serde_json::from_str::<Value>(&raw) {
        Ok(value) => Encoded::plain(value),
        Err(e) => Encoded::failed(type_name, e.to_string()),
    }
}

fn encode_hex(bytes: &[u8], max_bytes: usize) -> Encoded {
    let shown = bytes.len().min(max_bytes);
    let mut hex = String::with_capacity(shown * 2 + 2);
    hex.push_str("0x");
    for byte in &bytes[..shown] {
        let _ = write!(hex, "{byte:02x}");
    }
    if shown < bytes.len() {
        hex.push_str(&truncation_marker(shown, bytes.len()));
        return Encoded::cut(Value::String(hex));
    }
    Encoded::plain(Value::String(hex))
}

/// `None` when the value fits; otherwise the value cut on a UTF-8 boundary plus the marker.
fn truncate_text(s: &str, max_bytes: usize) -> Option<String> {
    if s.len() <= max_bytes {
        return None;
    }
    let mut end = max_bytes;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    Some(format!("{}{}", &s[..end], truncation_marker(end, s.len())))
}

fn truncation_marker(shown: usize, total: usize) -> String {
    format!("…[truncated: {shown} of {total} bytes]")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_keeps_first_occurrence_bare() {
        let names = dedup_column_names(&["id", "id", "name"]);
        assert_eq!(names, vec!["id", "id__2", "name"]);
    }

    #[test]
    fn dedup_bumps_suffix_past_an_existing_name() {
        let names = dedup_column_names(&["id", "id", "id__2"]);
        assert_eq!(names, vec!["id", "id__2", "id__2__2"]);
        let unique: HashSet<&String> = names.iter().collect();
        assert_eq!(unique.len(), 3);
    }

    #[test]
    fn dedup_names_empty_columns_by_ordinal() {
        assert_eq!(dedup_column_names(&["", ""]), vec!["column_0", "column_1"]);
    }

    #[test]
    fn dedup_preserves_order_and_arity() {
        let names = dedup_column_names(&["a", "b", "a", "b", "a"]);
        assert_eq!(names, vec!["a", "b", "a__2", "b__2", "a__3"]);
    }

    #[test]
    fn descriptor_records_original_name_only_when_renamed() {
        let bare = column_descriptor("id", "id", "BIGINT UNSIGNED");
        assert_eq!(bare["name"], "id");
        assert_eq!(bare["type"], "BIGINT UNSIGNED");
        assert!(bare.get("originalName").is_none());

        let renamed = column_descriptor("id__2", "id", "BIGINT UNSIGNED");
        assert_eq!(renamed["originalName"], "id");
    }

    #[test]
    fn integers_stay_numbers_through_max_safe_integer() {
        assert_eq!(int_to_json(0), serde_json::json!(0));
        assert_eq!(
            int_to_json(9_007_199_254_740_991),
            serde_json::json!(9_007_199_254_740_991i64)
        );
        assert_eq!(
            int_to_json(-9_007_199_254_740_991),
            serde_json::json!(-9_007_199_254_740_991i64)
        );
    }

    #[test]
    fn integers_past_max_safe_integer_become_exact_strings() {
        assert_eq!(
            int_to_json(9_007_199_254_740_992),
            Value::String("9007199254740992".into())
        );
        assert_eq!(
            int_to_json(i128::from(u64::MAX)),
            Value::String("18446744073709551615".into())
        );
        assert_eq!(
            int_to_json(i128::from(i64::MIN)),
            Value::String("-9223372036854775808".into())
        );
    }

    #[test]
    fn float_keeps_the_shortest_round_tripping_decimal() {
        assert_eq!(f32_to_json(0.1f32), serde_json::json!(0.1));
        assert_eq!(f64_to_json(0.1f64), serde_json::json!(0.1));
    }

    #[test]
    fn non_finite_floats_become_named_strings() {
        assert_eq!(f32_to_json(f32::NAN), Value::String("NaN".into()));
        assert_eq!(f64_to_json(f64::INFINITY), Value::String("Infinity".into()));
        assert_eq!(
            f64_to_json(f64::NEG_INFINITY),
            Value::String("-Infinity".into())
        );
    }

    #[test]
    fn negative_and_overlong_times_survive() {
        assert_eq!(format_mysql_time(true, 10, 0, 0, 0), "-10:00:00");
        assert_eq!(format_mysql_time(false, 838, 59, 59, 0), "838:59:59");
        assert_eq!(format_mysql_time(false, 48, 0, 0, 0), "48:00:00");
        assert_eq!(format_mysql_time(false, 9, 0, 0, 0), "09:00:00");
        assert_eq!(
            format_mysql_time(true, 1, 2, 3, 400_000),
            "-01:02:03.400000"
        );
    }

    #[test]
    fn datetime_keeps_fractional_seconds_and_timestamp_marks_utc() {
        let dt = NaiveDateTime::parse_from_str("2026-09-01 17:10:18.123", "%Y-%m-%d %H:%M:%S%.f")
            .unwrap();
        assert_eq!(format_naive_datetime(&dt), "2026-09-01 17:10:18.123000");

        let whole =
            NaiveDateTime::parse_from_str("2026-09-01 17:10:18", "%Y-%m-%d %H:%M:%S").unwrap();
        assert_eq!(format_naive_datetime(&whole), "2026-09-01 17:10:18");
        assert_eq!(format_timestamp(&whole.and_utc()), "2026-09-01 17:10:18Z");
        assert_eq!(
            format_timestamp(&dt.and_utc()),
            "2026-09-01 17:10:18.123000Z"
        );
    }

    #[test]
    fn zero_dates_are_recognised_but_real_ones_are_not() {
        assert!(is_zero_temporal("0000-00-00"));
        assert!(is_zero_temporal("0000-00-00 00:00:00"));
        assert!(!is_zero_temporal("2026-09-01"));
        assert!(!is_zero_temporal(""));
        assert!(!is_zero_temporal("not a date"));
    }

    #[test]
    fn decode_failure_sentinel_names_the_type_and_reason() {
        let v = decode_error_value("DECIMAL", "invalid digit found in string");
        assert_eq!(v["__error__"], "decode_failed");
        assert_eq!(v["type"], "DECIMAL");
        assert_eq!(v["message"], "invalid digit found in string");
    }

    #[test]
    fn decimal_keeps_every_digit_and_its_scale() {
        for raw in ["829", "1.50", "12345678901234567890", "-0.00000001"] {
            assert_eq!(
                decimal_to_json(Some(raw.to_string())),
                Value::String(raw.to_string())
            );
        }
        assert_eq!(decimal_to_json(None), Value::Null);
    }

    #[test]
    fn text_under_the_cap_is_untouched() {
        let enc = encode_text("hello".into(), 4096);
        assert_eq!(enc.value, serde_json::json!("hello"));
        assert!(!enc.truncated);
    }

    #[test]
    fn text_over_the_cap_is_cut_on_a_char_boundary() {
        let enc = encode_text("é".repeat(10), 5); // 20 bytes, cap lands mid-char
        assert!(enc.truncated);
        let out = enc.value.as_str().unwrap().to_string();
        assert!(out.starts_with("éé"));
        assert!(out.contains("[truncated: 4 of 20 bytes]"));
    }

    #[test]
    fn json_documents_are_inlined_but_oversized_ones_degrade_to_text() {
        let inlined = encode_json_text(r#"{"a":[1,2]}"#.into(), "JSON", 4096);
        assert_eq!(inlined.value, serde_json::json!({"a": [1, 2]}));
        assert!(!inlined.truncated);

        let big = format!(r#"{{"a":"{}"}}"#, "x".repeat(100));
        let cut = encode_json_text(big, "JSON", 32);
        assert!(cut.truncated);
        assert!(cut.value.is_string());

        let broken = encode_json_text("{not json".into(), "JSON", 4096);
        assert_eq!(broken.value["__error__"], "decode_failed");
        assert!(broken.error.is_some());
    }

    #[test]
    fn hex_preview_marks_what_it_cut() {
        let bytes: Vec<u8> = (0..=255u8).collect();
        let enc = encode_hex(&bytes, 256);
        assert!(!enc.truncated);
        assert!(enc.value.as_str().unwrap().starts_with("0x000102"));

        let enc = encode_hex(&bytes, 4);
        assert!(enc.truncated);
        assert_eq!(
            enc.value.as_str().unwrap(),
            "0x00010203…[truncated: 4 of 256 bytes]"
        );
    }
}
