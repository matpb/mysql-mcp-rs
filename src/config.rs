use serde::Deserialize;
use std::env;
use std::fmt;

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
            .finish()
    }
}

fn default_pool_size() -> u32 {
    5
}

fn default_query_timeout() -> u64 {
    30
}

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    pub port: u16,
    pub databases: Vec<DatabaseConfig>,
    pub default_max_rows: u32,
}

/// Parses the JSON array used for the `MYSQL_DATABASES` environment variable.
///
/// Returns an error if the JSON is invalid or the array is empty.
pub fn load_databases_from_json(json: &str) -> Result<Vec<DatabaseConfig>, String> {
    let databases: Vec<DatabaseConfig> = serde_json::from_str(json)
        .map_err(|_| "MYSQL_DATABASES must be valid JSON array".to_string())?;
    if databases.is_empty() {
        return Err("MYSQL_DATABASES must contain at least one database config".into());
    }
    Ok(databases)
}

impl Config {
    pub fn from_env() -> Self {
        let databases_json = env::var("MYSQL_DATABASES")
            .expect("MYSQL_DATABASES env var is required (JSON array of database configs)");

        let databases = load_databases_from_json(&databases_json).unwrap_or_else(|e| panic!("{e}"));

        Self::from_parts(
            env::var("MCP_HOST").unwrap_or_else(|_| "0.0.0.0".into()),
            env::var("MCP_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(8431),
            databases,
            env::var("DEFAULT_MAX_ROWS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000),
        )
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
        }
    }

    pub fn database_names(&self) -> Vec<&str> {
        self.databases.iter().map(|d| d.name.as_str()).collect()
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
        assert_eq!(
            load_databases_from_json("not json").unwrap_err(),
            "MYSQL_DATABASES must be valid JSON array"
        );
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
        };
        let s = format!("{db:?}");
        assert!(!s.contains("secret"));
        assert!(s.contains("[REDACTED]"));
    }
}
