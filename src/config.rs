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

impl Config {
    pub fn from_env() -> Self {
        let databases_json = env::var("MYSQL_DATABASES")
            .expect("MYSQL_DATABASES env var is required (JSON array of database configs)");

        let databases: Vec<DatabaseConfig> = serde_json::from_str(&databases_json)
            .expect("MYSQL_DATABASES must be valid JSON array");

        if databases.is_empty() {
            panic!("MYSQL_DATABASES must contain at least one database config");
        }

        Self {
            host: env::var("MCP_HOST").unwrap_or_else(|_| "0.0.0.0".into()),
            port: env::var("MCP_PORT")
                .ok()
                .and_then(|p| p.parse().ok())
                .unwrap_or(8431),
            databases,
            default_max_rows: env::var("DEFAULT_MAX_ROWS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(1000),
        }
    }

    pub fn database_names(&self) -> Vec<&str> {
        self.databases.iter().map(|d| d.name.as_str()).collect()
    }
}
