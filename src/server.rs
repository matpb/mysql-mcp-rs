use std::net::SocketAddr;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::db::PoolManager;
use crate::mcp::tools::MysqlMcp;

/// Starts tracing, loads config, connects to MySQL, and serves MCP over HTTP until shutdown.
pub async fn run() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("mysql_mcp=info")),
        )
        .init();

    let cfg = Config::from_env();
    tracing::info!(
        "Connecting to {} database(s): {:?}",
        cfg.databases.len(),
        cfg.database_names()
    );

    let pool_manager = Arc::new(PoolManager::new(&cfg).await);
    tracing::info!("All database connections established");

    let mcp_pool = pool_manager.clone();
    let mcp_cfg = cfg.clone();
    let mcp_service = StreamableHttpService::new(
        move || Ok(MysqlMcp::new(mcp_pool.clone(), mcp_cfg.clone())),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default(),
    );

    // Permissive CORS: MCP clients (IDEs, CLI tools, web UIs) use varied origins;
    // the server is intended to run locally or behind a firewall.
    let app = axum::Router::new()
        .route("/health", axum::routing::get(health))
        .route("/mcp", axum::routing::any_service(mcp_service))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    let addr: SocketAddr = format!("{}:{}", cfg.host, cfg.port).parse().unwrap();
    let listener = TcpListener::bind(addr).await.unwrap();
    tracing::info!("MySQL MCP server listening on {addr}");

    let shutdown = async {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Shutting down...");
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .unwrap();

    pool_manager.close_all().await;
    tracing::info!("All connections closed");
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}
