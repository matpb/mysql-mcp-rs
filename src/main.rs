//! Binary entrypoint for the MySQL MCP server.

#[tokio::main]
async fn main() {
    mysql_mcp::run().await;
}
