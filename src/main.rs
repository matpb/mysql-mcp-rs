mod config;
mod db;
mod mcp;
mod sanitizer;

use std::net::SocketAddr;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService,
    session::local::LocalSessionManager,
};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use mcp::tools::MysqlMcp;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("mysql_mcp=info")),
        )
        .init();

    let cfg = config::Config::from_env();
    tracing::info!(
        "Connecting to {} database(s): {:?}",
        cfg.databases.len(),
        cfg.database_names()
    );

    let pool_manager = Arc::new(db::PoolManager::new(&cfg).await);
    tracing::info!("All database connections established");

    // MCP streamable HTTP service
    let mcp_pool = pool_manager.clone();
    let mcp_cfg = cfg.clone();
    let mcp_service = StreamableHttpService::new(
        move || Ok(MysqlMcp::new(mcp_pool.clone(), mcp_cfg.clone())),
        Arc::new(LocalSessionManager::default()),
        StreamableHttpServerConfig::default(),
    );

    let app = axum::Router::new()
        .route("/health", axum::routing::get(health))
        .route("/mcp", axum::routing::any_service(mcp_service))
        // Permissive CORS: MCP clients (IDEs, CLI tools, web UIs) connect from
        // various origins. The server is intended to run locally or behind a firewall.
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http());

    let addr: SocketAddr = format!("{}:{}", cfg.host, cfg.port).parse().unwrap();
    let listener = TcpListener::bind(addr).await.unwrap();
    tracing::info!("MySQL MCP server listening on {addr}");

    // Graceful shutdown on SIGTERM/SIGINT
    let shutdown = async {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Shutting down...");
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
        .unwrap();

    // Close all database connection pools after the server stops accepting requests
    pool_manager.close_all().await;
    tracing::info!("All connections closed");
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}
