use serde::Deserialize;
use std::env;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SslMode {
    Disabled,
    Preferred,
    Required,
}

impl std::str::FromStr for SslMode {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "disabled" => Ok(SslMode::Disabled),
            "preferred" => Ok(SslMode::Preferred),
            "required" => Ok(SslMode::Required),
            other => Err(format!(
                "invalid ssl_mode `{other}`: expected disabled, preferred, or required"
            )),
        }
    }
}

fn default_ssl_mode() -> SslMode {
    SslMode::Required
}

#[derive(Clone, Deserialize)]
pub struct DatabaseConfig {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub database: String,
    #[serde(default = "default_pool_size")]
    pub max_connections: u32,
    #[serde(default = "default_query_timeout")]
    pub query_timeout_secs: u64,
    #[serde(default = "default_ssl_mode")]
    pub ssl_mode: SslMode,
}

impl fmt::Debug for DatabaseConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatabaseConfig")
            .field("name", &self.name)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("user", &self.user)
            .field("password", &"[REDACTED]")
            .field("database", &self.database)
            .field("max_connections", &self.max_connections)
            .field("query_timeout_secs", &self.query_timeout_secs)
            .field("ssl_mode", &self.ssl_mode)
            .finish()
    }
}

fn default_pool_size() -> u32 {
    5
}

fn default_query_timeout() -> u64 {
    30
}

fn default_max_value_bytes() -> usize {
    4096
}

fn default_max_binary_preview_bytes() -> usize {
    256
}

#[derive(Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub databases: Vec<DatabaseConfig>,
    pub default_max_rows: u32,
    /// Byte cap on a single text/JSON value before it is truncated with a marker.
    pub max_value_bytes: usize,
    /// Source-byte cap on a binary value before its hex preview is truncated.
    pub max_binary_preview_bytes: usize,
    /// HTTP API keys; empty means auth is disabled. Ignored in stdio mode.
    pub api_keys: Vec<String>,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("databases", &self.databases)
            .field("default_max_rows", &self.default_max_rows)
            .field("max_value_bytes", &self.max_value_bytes)
            .field("max_binary_preview_bytes", &self.max_binary_preview_bytes)
            .field("api_keys", &format!("[{} key(s)]", self.api_keys.len()))
            .finish()
    }
}

/// Splits `API_KEYS` on `,`, trims each entry, and drops empties.
pub fn parse_api_keys(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

/// Parses the JSON array used for the `MYSQL_DATABASES` environment variable.
///
/// Returns an error if the JSON is invalid or the array is empty.
pub fn load_databases_from_json(json: &str) -> Result<Vec<DatabaseConfig>, String> {
    let databases: Vec<DatabaseConfig> = serde_json::from_str(json)
        .map_err(|e| format!("MYSQL_DATABASES must be a valid JSON array: {e}"))?;
    if databases.is_empty() {
        return Err("MYSQL_DATABASES must contain at least one database config".into());
    }
    Ok(databases)
}

/// Builds a one-database config from flat `MYSQL_*` vars, the ergonomic form for
/// a stdio client whose config file holds a plain env map rather than JSON.
pub fn load_single_database_from_env() -> Option<DatabaseConfig> {
    let database = env::var("MYSQL_DATABASE").ok()?;
    let ssl_mode = match env::var("MYSQL_SSL_MODE") {
        Ok(raw) => raw
            .parse()
            .unwrap_or_else(|e| panic!("MYSQL_SSL_MODE: {e}")),
        Err(_) => default_ssl_mode(),
    };
    Some(DatabaseConfig {
        name: env::var("MYSQL_NAME").unwrap_or_else(|_| database.clone()),
        host: env::var("MYSQL_HOST").unwrap_or_else(|_| "localhost".into()),
        port: env::var("MYSQL_PORT")
            .ok()
            .and_then(|p| p.parse().ok())
            .unwrap_or(3306),
        user: env::var("MYSQL_USER").unwrap_or_else(|_| "root".into()),
        password: env::var("MYSQL_PASSWORD").unwrap_or_default(),
        database,
        max_connections: env::var("MYSQL_MAX_CONNECTIONS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(default_pool_size),
        query_timeout_secs: env::var("MYSQL_QUERY_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or_else(default_query_timeout),
        ssl_mode,
    })
}

impl Config {
    pub fn from_env() -> Self {
        let databases = match env::var("MYSQL_DATABASES") {
            Ok(json) => load_databases_from_json(&json).unwrap_or_else(|e| panic!("{e}")),
            Err(_) => vec![load_single_database_from_env().expect(
                "set MYSQL_DATABASES (JSON array) or MYSQL_DATABASE (+ MYSQL_HOST/PORT/USER/PASSWORD)",
            )],
        };

        // Loopback by default: the server has no authentication and permissive CORS.
        // docker-compose sets MCP_HOST=0.0.0.0 explicitly to reach the published port.
        let cfg = Self::from_parts(
            env::var("MCP_HOST").unwrap_or_else(|_| "127.0.0.1".into()),
            env::var("MCP_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(8431),
            databases,
            env::var("DEFAULT_MAX_ROWS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000),
        );

        Self {
            max_value_bytes: env::var("MAX_VALUE_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_max_value_bytes),
            max_binary_preview_bytes: env::var("MAX_BINARY_PREVIEW_BYTES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or_else(default_max_binary_preview_bytes),
            api_keys: env::var("API_KEYS")
                .ok()
                .map(|raw| parse_api_keys(&raw))
                .unwrap_or_default(),
            ..cfg
        }
    }

    /// Builds a [`Config`] without reading the environment (tests and tooling).
    pub fn from_parts(
        host: impl Into<String>,
        port: u16,
        databases: Vec<DatabaseConfig>,
        default_max_rows: u32,
    ) -> Self {
        Self {
            host: host.into(),
            port,
            databases,
            default_max_rows,
            max_value_bytes: default_max_value_bytes(),
            max_binary_preview_bytes: default_max_binary_preview_bytes(),
            api_keys: vec![],
        }
    }

    pub fn database_names(&self) -> Vec<&str> {
        self.databases.iter().map(|d| d.name.as_str()).collect()
    }

    pub fn auth_enabled(&self) -> bool {
        !self.api_keys.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn one_db_json(name: &str) -> String {
        format!(
            r#"[{{"name":"{name}","host":"localhost","port":3306,"user":"u","password":"p","database":"d"}}]"#
        )
    }

    #[test]
    fn load_databases_defaults_optional_fields() {
        let json = one_db_json("primary");
        let dbs = load_databases_from_json(&json).unwrap();
        assert_eq!(dbs.len(), 1);
        assert_eq!(dbs[0].max_connections, 5);
        assert_eq!(dbs[0].query_timeout_secs, 30);
    }

    #[test]
    fn load_databases_parses_overrides() {
        let json = r#"[{"name":"x","host":"h","port":3307,"user":"u","password":"p","database":"db","max_connections":10,"query_timeout_secs":60}]"#;
        let dbs = load_databases_from_json(json).unwrap();
        assert_eq!(dbs[0].max_connections, 10);
        assert_eq!(dbs[0].query_timeout_secs, 60);
    }

    #[test]
    fn load_databases_rejects_empty() {
        assert_eq!(
            load_databases_from_json("[]").unwrap_err(),
            "MYSQL_DATABASES must contain at least one database config"
        );
    }

    #[test]
    fn load_databases_rejects_invalid_json() {
        let err = load_databases_from_json("not json").unwrap_err();
        assert!(err.starts_with("MYSQL_DATABASES must be a valid JSON array: "));
        assert!(err.contains("at line 1 column"), "{err}");
    }

    #[test]
    fn load_databases_error_names_the_missing_field() {
        let err = load_databases_from_json(
            r#"[{"name":"x","host":"h","user":"u","password":"p","database":"d"}]"#,
        )
        .unwrap_err();
        assert!(err.contains("missing field `port`"), "{err}");
    }

    #[test]
    fn value_limits_have_defaults() {
        let cfg = Config::from_parts("127.0.0.1", 8431, vec![], 1000);
        assert_eq!(cfg.max_value_bytes, 4096);
        assert_eq!(cfg.max_binary_preview_bytes, 256);
    }

    #[test]
    fn database_names_order_matches_vec() {
        let cfg = Config::from_parts(
            "0.0.0.0",
            8431,
            vec![
                DatabaseConfig {
                    name: "a".into(),
                    host: "h".into(),
                    port: 3306,
                    user: "u".into(),
                    password: "p".into(),
                    database: "d".into(),
                    max_connections: 5,
                    query_timeout_secs: 30,
                    ssl_mode: SslMode::Required,
                },
                DatabaseConfig {
                    name: "b".into(),
                    host: "h".into(),
                    port: 3306,
                    user: "u".into(),
                    password: "p".into(),
                    database: "d".into(),
                    max_connections: 5,
                    query_timeout_secs: 30,
                    ssl_mode: SslMode::Required,
                },
            ],
            1000,
        );
        assert_eq!(cfg.database_names(), vec!["a", "b"]);
    }

    #[test]
    fn debug_redacts_password() {
        let db = DatabaseConfig {
            name: "n".into(),
            host: "h".into(),
            port: 3306,
            user: "u".into(),
            password: "secret".into(),
            database: "d".into(),
            max_connections: 5,
            query_timeout_secs: 30,
            ssl_mode: SslMode::Required,
        };
        let s = format!("{db:?}");
        assert!(!s.contains("secret"));
        assert!(s.contains("[REDACTED]"));
    }

    #[test]
    fn parse_api_keys_trims_and_drops_empties() {
        let keys = parse_api_keys(" a , b,,  c ,");
        assert_eq!(keys, vec!["a", "b", "c"]);
    }

    #[test]
    fn parse_api_keys_empty_string_gives_empty_vec() {
        assert!(parse_api_keys("").is_empty());
    }

    #[test]
    fn config_debug_does_not_leak_configured_key() {
        let mut cfg = Config::from_parts("127.0.0.1", 8431, vec![], 1000);
        cfg.api_keys = vec!["super-secret-key".into()];
        let s = format!("{cfg:?}");
        assert!(!s.contains("super-secret-key"));
        assert!(s.contains("[1 key(s)]"));
    }

    #[test]
    fn ssl_mode_defaults_to_required() {
        let json = one_db_json("primary");
        let dbs = load_databases_from_json(&json).unwrap();
        assert_eq!(dbs[0].ssl_mode, SslMode::Required);
    }

    #[test]
    fn ssl_mode_parses_disabled_from_json() {
        let json = r#"[{"name":"x","host":"h","port":3306,"user":"u","password":"p","database":"d","ssl_mode":"disabled"}]"#;
        let dbs = load_databases_from_json(json).unwrap();
        assert_eq!(dbs[0].ssl_mode, SslMode::Disabled);
    }

    #[test]
    fn ssl_mode_rejects_unknown_value_in_json() {
        let json = r#"[{"name":"x","host":"h","port":3306,"user":"u","password":"p","database":"d","ssl_mode":"sometimes"}]"#;
        assert!(load_databases_from_json(json).is_err());
    }

    #[test]
    fn ssl_mode_from_str_is_case_insensitive() {
        assert_eq!("Preferred".parse::<SslMode>().unwrap(), SslMode::Preferred);
        assert_eq!("DISABLED".parse::<SslMode>().unwrap(), SslMode::Disabled);
        assert_eq!("required".parse::<SslMode>().unwrap(), SslMode::Required);
        assert!("nope".parse::<SslMode>().is_err());
    }
}
