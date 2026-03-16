# mysql-mcp-rs

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.85+-orange.svg)](https://www.rust-lang.org/)
[![MCP](https://img.shields.io/badge/MCP-2025--03--26-green.svg)](https://modelcontextprotocol.io/)

A lightweight Rust [MCP](https://modelcontextprotocol.io/) server for **read-only** MySQL access with multi-database support.

Connect your AI tools (Claude Code, Cursor, Windsurf, etc.) to MySQL databases via the Model Context Protocol. The server enforces read-only access with query sanitization, automatic row limits, and timeouts.

## Features

- **Multi-database** — connect to multiple MySQL databases simultaneously
- **Read-only enforcement** — query sanitizer blocks all mutation operations (INSERT, UPDATE, DELETE, DROP, etc.)
- **SQL injection protection** — comment stripping, multi-statement blocking, DML keyword scanning, dangerous keyword detection
- **Automatic LIMIT** — SELECT queries without a LIMIT get one applied automatically (default: 1000 rows)
- **Query timeouts** — configurable per-database timeout (default: 30s)
- **Connection pooling** — configurable pool size per database
- **Credential safety** — passwords are never logged or exposed in API responses
- **MCP tools** — `list_databases`, `show_tables`, `describe_table`, `execute_query`
- **MCP resources** — each database is exposed as a `mysql://<name>` resource
- **Streamable HTTP transport** — serves MCP over HTTP at `/mcp`

## Quick Start

### 1. Configure databases

Copy the example environment file and fill in your database credentials:

```bash
cp .env.example .env
```

Edit `.env` with your database connection details:

```env
RUST_LOG=mysql_mcp=info

MYSQL_DATABASES='[
  {"name": "my-db", "host": "localhost", "port": 3306, "user": "readonly", "password": "secret", "database": "myapp"}
]'
```

Each database entry supports:

| Field | Required | Default | Description |
|-------|----------|---------|-------------|
| `name` | yes | — | Friendly name used in MCP tool calls |
| `host` | yes | — | MySQL host |
| `port` | yes | — | MySQL port |
| `user` | yes | — | MySQL username |
| `password` | yes | — | MySQL password |
| `database` | yes | — | MySQL schema/database name |
| `max_connections` | no | `5` | Connection pool size |
| `query_timeout_secs` | no | `30` | Query timeout in seconds |

### 2. Run with Docker (recommended)

```bash
docker compose up -d
```

### 3. Or build from source

```bash
cargo build --release
./target/release/mysql-mcp
```

The server starts on `http://0.0.0.0:8431` by default.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MYSQL_DATABASES` | *required* | JSON array of database configs |
| `MCP_HOST` | `0.0.0.0` | Server bind address |
| `MCP_PORT` | `8431` | Server port |
| `DEFAULT_MAX_ROWS` | `1000` | Max rows returned per query |
| `RUST_LOG` | `mysql_mcp=info` | Log level filter |

## MCP Tools

### `list_databases`

Lists all configured database connections and their details (names, schemas).

### `show_tables`

Lists all tables in a database.

- **Parameters:** `database` — the database name from your config

### `describe_table`

Returns detailed schema info: columns, types, indexes, and table metadata.

- **Parameters:** `database`, `table`

### `execute_query`

Executes a read-only SQL query. Only `SELECT`, `SHOW`, `DESCRIBE`, `DESC`, `EXPLAIN`, `WITH`, and `SET @` are allowed.

- **Parameters:** `database`, `query`

## Security Model

This server is designed for **read-only database access** and enforces this at multiple layers:

1. **Query validation** — only whitelisted statement types are allowed
2. **Mutation blocking** — INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, and other DML/DDL keywords are blocked both at query start and anywhere in the query (outside string literals)
3. **Dangerous keyword detection** — blocks `INTO OUTFILE`, `INTO DUMPFILE`, `FOR UPDATE`, `LOCK IN SHARE MODE`
4. **Comment stripping** — removes `--`, `#`, and `/* */` comments (respecting string literals) to prevent bypass attempts
5. **Multi-statement prevention** — semicolons that separate multiple statements are rejected
6. **Automatic LIMIT** — prevents unbounded result sets
7. **Query timeouts** — prevents long-running queries from consuming resources
8. **Credential protection** — passwords are redacted in debug output and never included in API responses or logs

**Recommendations for production use:**

- Use a **read-only MySQL user** for each database connection
- Run the server behind a **firewall or VPN** — there is no built-in authentication
- Set `RUST_LOG=mysql_mcp=warn` to minimize log output

## Endpoints

| Path | Description |
|------|-------------|
| `GET /health` | Health check (returns `ok`) |
| `/mcp` | MCP streamable HTTP endpoint |

## License

[MIT](LICENSE)
