use std::net::SocketAddr;
use std::sync::Arc;

use axum::http::StatusCode;
use axum::response::IntoResponse;
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::never::NeverSessionManager,
};
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::EnvFilter;

use crate::config::Config;
use crate::db::PoolManager;
use crate::mcp::tools::MysqlMcp;

const USAGE: &str = "usage: mysql-mcp [http|stdio]\n\n  http   serve MCP over streamable HTTP (default)\n  stdio  serve MCP over stdin/stdout for a local client";

/// Dispatches on the first CLI argument; no argument keeps the historical HTTP behaviour.
pub async fn run() {
    match std::env::args().nth(1).as_deref() {
        None | Some("http") => run_http().await,
        Some("stdio") => run_stdio().await,
        Some("-h" | "--help") => println!("{USAGE}"),
        Some(other) => {
            eprintln!("unknown subcommand `{other}`\n{USAGE}");
            std::process::exit(2);
        }
    }
}

/// In stdio mode stdout carries the JSON-RPC framing, so logs must go to stderr.
fn init_tracing(to_stderr: bool) {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("mysql_mcp=info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if to_stderr {
        builder.with_writer(std::io::stderr).init();
    } else {
        builder.init();
    }
}

async fn connect(cfg: &Config) -> Arc<PoolManager> {
    tracing::info!(
        "Connecting to {} database(s): {:?}",
        cfg.databases.len(),
        cfg.database_names()
    );
    Arc::new(PoolManager::new(cfg).await)
}

/// Starts tracing, loads config, connects to MySQL, and serves MCP over HTTP until shutdown.
async fn run_http() {
    init_tracing(false);

    let cfg = Config::from_env();
    let pool_manager = connect(&cfg).await;

    let mcp_pool = pool_manager.clone();
    let mcp_cfg = cfg.clone();
    let mcp_service = StreamableHttpService::new(
        move || Ok(MysqlMcp::new(mcp_pool.clone(), mcp_cfg.clone())),
        Arc::new(NeverSessionManager::default()),
        StreamableHttpServerConfig {
            stateful_mode: false,
            json_response: true,
            ..Default::default()
        },
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

/// Serves a single MCP session over stdin/stdout, for clients that spawn the binary directly.
async fn run_stdio() {
    init_tracing(true);

    let cfg = Config::from_env();
    let pool_manager = connect(&cfg).await;
    let mcp = MysqlMcp::new(pool_manager.clone(), cfg);

    tracing::info!("Serving MySQL MCP over stdio");
    let running = match rmcp::serve_server(mcp, (tokio::io::stdin(), tokio::io::stdout())).await {
        Ok(running) => running,
        Err(e) => {
            tracing::error!("stdio serve init failed: {e}");
            std::process::exit(1);
        }
    };
    if let Err(e) = running.waiting().await {
        tracing::error!("stdio session ended with error: {e}");
    }

    pool_manager.close_all().await;
    tracing::info!("All connections closed");
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "ok")
}
