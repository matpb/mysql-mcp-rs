# mysql-mcp-rs

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-1.85+-orange.svg)](https://www.rust-lang.org/)
[![MCP](https://img.shields.io/badge/MCP-2025--03--26-green.svg)](https://modelcontextprotocol.io/)

A lightweight Rust [MCP](https://modelcontextprotocol.io/) server for **read-only** MySQL access with multi-database support.

Connect your AI tools (Claude Code, Cursor, Windsurf, etc.) to MySQL databases via the Model Context Protocol. The server ships a read-only query gate, a row cap that cannot be talked out of, per-database timeouts, and a JSON result encoding built so a model reads values correctly instead of plausibly.

## Features

- **Multi-database** — connect to multiple MySQL databases simultaneously
- **Read-only query gate** — mutation statements are rejected before they reach MySQL
- **Row cap that holds** — every limitable statement is bounded, including CTEs; an oversized user `LIMIT` is clamped down
- **Lossless result encoding** — exact integers and decimals, real datetime/time formats, typed column metadata, explicit decode errors
- **Value size caps** — oversized text, JSON and binary values are truncated with a self-describing marker instead of flooding the caller's context
- **Query timeouts** — configurable per-database timeout (default: 30s)
- **Connection pooling** — configurable pool size per database, connected lazily
- **Credential safety** — passwords are never logged or exposed in API responses
- **MCP tools** — `list_databases`, `show_tables`, `describe_table`, `execute_query`
- **MCP resources** — each database is exposed as a `mysql://<name>` resource
- **Two transports** — streamable HTTP at `/mcp`, or stdio for a locally spawned binary

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

If `MYSQL_DATABASES` is malformed, startup fails with serde's own message rather than a generic "invalid JSON":

```
MYSQL_DATABASES must be a valid JSON array: missing field `port` at line 1 column 84
```

#### Single-database shorthand

For one database — typically a stdio client whose config file holds a plain env map rather than embedded JSON — set the flat variables instead of `MYSQL_DATABASES`:

| Variable | Default | Description |
|----------|---------|-------------|
| `MYSQL_DATABASE` | *required* | Schema name; its presence selects this form |
| `MYSQL_HOST` | `localhost` | MySQL host |
| `MYSQL_PORT` | `3306` | MySQL port |
| `MYSQL_USER` | `root` | MySQL username |
| `MYSQL_PASSWORD` | *(empty)* | MySQL password |
| `MYSQL_NAME` | value of `MYSQL_DATABASE` | Friendly name used in MCP tool calls |
| `MYSQL_MAX_CONNECTIONS` | `5` | Connection pool size |
| `MYSQL_QUERY_TIMEOUT_SECS` | `30` | Query timeout in seconds |

`MYSQL_DATABASES` wins when both are set.

### 2. Run with Docker (recommended)

```bash
docker compose up -d
```

The compose file publishes the port on `127.0.0.1` only and sets `MCP_HOST=0.0.0.0` inside the container so the process is reachable from the published port.

### 3. Or build from source

```bash
cargo build --release
./target/release/mysql-mcp          # http transport (default)
./target/release/mysql-mcp stdio    # stdio transport
```

The HTTP server starts on `http://127.0.0.1:8431` by default. `mysql-mcp --help` prints the subcommands.

## Transports

### HTTP (default)

`mysql-mcp` or `mysql-mcp http` serves streamable HTTP at `/mcp`, stateless, with a `GET /health` probe. Logs go to stdout.

### stdio

`mysql-mcp stdio` serves one MCP session over stdin/stdout, for clients that spawn the binary themselves. **Logs go to stderr** — stdout carries the JSON-RPC framing and nothing else.

```json
{
  "mcpServers": {
    "mysql": {
      "command": "/path/to/mysql-mcp",
      "args": ["stdio"],
      "env": {
        "MYSQL_DATABASE": "myapp",
        "MYSQL_HOST": "127.0.0.1",
        "MYSQL_PORT": "3306",
        "MYSQL_USER": "readonly",
        "MYSQL_PASSWORD": "secret"
      }
    }
  }
}
```

`MCP_HOST` and `MCP_PORT` are ignored in stdio mode.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MYSQL_DATABASES` | *required* | JSON array of database configs (or use the flat `MYSQL_*` form above) |
| `MCP_HOST` | `127.0.0.1` | HTTP bind address. There is no authentication — see [Security Model](#security-model) |
| `MCP_PORT` | `8431` | HTTP port |
| `DEFAULT_MAX_ROWS` | `1000` | Row cap applied to every limitable query |
| `MAX_VALUE_BYTES` | `4096` | Byte cap on a single text or JSON value before truncation |
| `MAX_BINARY_PREVIEW_BYTES` | `256` | Source-byte cap on a binary value's hex preview |
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

Executes a read-only SQL query.

- **Parameters:** `database`, `query`

## Query Rules

### What is allowed

A statement must begin with `SELECT`, `WITH`, `TABLE`, `VALUES`, `SHOW`, `DESCRIBE`, `DESC`, `EXPLAIN`, `SET @var`, or a parenthesized `SELECT`/`TABLE`/`VALUES`/`WITH` — so `(SELECT a FROM t1) UNION (SELECT b FROM t2)` is accepted.

Only one statement per call; a single trailing `;` is fine.

`SHOW CREATE TABLE` / `VIEW` / `DATABASE` / `PROCEDURE` / `FUNCTION` are allowed. So are the built-in functions `REPLACE()`, `INSERT()` and `TRUNCATE()` — only their statement forms (`REPLACE INTO`, `TRUNCATE TABLE`) are blocked.

Rejected: mutation statements anywhere in the query, `INTO OUTFILE`, `INTO DUMPFILE`, `FOR UPDATE`, `FOR SHARE`, `LOCK IN SHARE MODE`, and `LOAD_FILE()`. Rejection messages name the offending construct and the way forward — they never hand back a regex.

### `SET @var` is single-statement-scoped, and therefore near-useless

`SET @var = ...` passes the gate, but each `execute_query` call runs on a pooled connection that is not pinned to your session and the next call may land on a different one. A variable set in one call is **not** visible to the next, and because only one statement is allowed per call you cannot set and read it together. Treat it as accepted-but-inert; inline the value or use a CTE instead.

`SET` may only assign user variables. `GLOBAL`, `SESSION`, `LOCAL`, `PERSIST` and `PERSIST_ONLY` scopes are rejected, including in a mixed assignment list such as `SET @a = 1, GLOBAL general_log = 1`.

### Comments

`--` (followed by whitespace or end of input), `#`, and `/* */` comments are stripped before the query is inspected, respecting string and backtick-quoted identifiers. `1--2` is arithmetic, not a comment, and survives intact.

Two exceptions:

- `/*! ... */` and `/*!80000 ... */` **version-gated comments are rejected.** MySQL *executes* their contents, so stripping them would change what the server runs — the classic gate-bypass shape.
- `/*+ ... */` optimizer hints are **preserved verbatim** and reach MySQL.

### Automatic LIMIT

Every `SELECT`, `WITH` (CTE), `TABLE` and parenthesized set operation is bounded by `DEFAULT_MAX_ROWS`. `SHOW`, `DESCRIBE`, `EXPLAIN`, `VALUES` and `SET @` are deliberately excluded — most `SHOW` forms reject a `LIMIT` clause, and on `EXPLAIN` a `LIMIT` would bind to the inner `SELECT` and change the plan being explained.

The cap is applied against a copy of the query with quoted text blanked out, and only a `LIMIT` that is **trailing and at paren depth 0** counts as yours:

- No trailing top-level `LIMIT` → ` LIMIT <max>` is appended (after trimming any trailing `;`).
- Trailing top-level `LIMIT n` where `n` is larger than the cap → **`n` is clamped down to the cap**. `LIMIT 5000000` becomes `LIMIT 1000`.
- Both offset forms clamp the row count only: `LIMIT 100, 5000000` → `LIMIT 100, 1000`; `LIMIT 5000000 OFFSET 20` → `LIMIT 1000 OFFSET 20`.
- Trailing `LIMIT n` at or below the cap → untouched.
- A `LIMIT` inside a subquery, a derived table, or a string literal no longer suppresses the outer cap. `WITH c AS (SELECT ... LIMIT 1) SELECT * FROM c` still gets capped.
- A non-numeric limit (`LIMIT ?`, `LIMIT @n`) is returned verbatim and **is not capped** — appending a second `LIMIT` would guarantee a syntax error.

When the result comes back at the cap, `truncated` is `true` and `message` says so.

## Result Encoding

`execute_query` returns one envelope per call:

```json
{
  "success": true,
  "database": "my-db",
  "columns": [
    {"name": "id",    "type": "BIGINT UNSIGNED"},
    {"name": "id__2", "type": "BIGINT UNSIGNED", "originalName": "id"},
    {"name": "name",  "type": "VARCHAR"}
  ],
  "data": [{"id": 1, "id__2": 2, "name": "x"}],
  "rowCount": 1,
  "truncated": false,
  "decodeErrors": 0,
  "truncatedValues": 0,
  "executionTime": "2ms",
  "message": "Query executed successfully. 1 row(s) returned."
}
```

`data` is an array of flat `{"column": value}` objects. `columns` is emitted once per result, in **true SELECT order**, with the MySQL type of each column; `columns[i].name` is exactly the key used in every row object. A query returning zero rows carries no column metadata over the wire, so `columns` is `[]` — nothing is fabricated.

`decodeErrors` and `truncatedValues` count affected values across all rows, so a caller can tell at a glance whether anything in the payload is lossy.

### Duplicate column names

`SELECT a.id, b.id, a.name` used to lose a column: the object had two keys and the second `id` overwrote the first. Names are now de-duplicated left to right — the first occurrence keeps its bare name, the *N*th becomes `name__N` (incrementing past any collision with a name selected elsewhere) — so that query yields three keys: `id`, `id__2`, `name`. Every renamed entry carries `originalName` in `columns`. An empty column name becomes `column_<ordinal>`.

### Values

| MySQL type | JSON |
|------------|------|
| `TINYINT(1)` / `BOOLEAN` | `true` / `false` for exactly 1 / 0; any other value is a number (the column is a small int, not a boolean) |
| `TINYINT` … `INT`, signed and unsigned | Number |
| `BIGINT`, `BIGINT UNSIGNED`, `COUNT()`, window functions | Number when the magnitude is at most 2^53-1, otherwise the exact decimal digits as a **String** |
| `BIT(M)`, `YEAR` | Number (same integer rule) |
| `DECIMAL` / `NUMERIC`, `SUM()`, `AVG()` | **String**, always — MySQL's exact literal with its scale intact: `"829"`, `"1.50"`, `"12345678901234567890"` |
| `FLOAT`, `DOUBLE` | Number; `NaN` and infinities become the strings `"NaN"`, `"Infinity"`, `"-Infinity"` |
| `DATE` | `"YYYY-MM-DD"` |
| `DATETIME` | `"YYYY-MM-DD HH:MM:SS"`, plus `.ffffff` when non-zero. **No timezone marker, by design** |
| `TIMESTAMP` | `"YYYY-MM-DD HH:MM:SSZ"`, plus `.ffffff` before the `Z` when non-zero |
| `TIME`, `TIMEDIFF()` | `"[-]HH:MM:SS"` (`.ffffff` when non-zero); signed and uncapped, so `"-10:00:00"` and `"838:59:59"` are returned as-is |
| `CHAR`, `VARCHAR`, `TEXT` family, `ENUM`, `SET` | String, verbatim, truncated past `MAX_VALUE_BYTES` |
| `JSON` | The parsed document, inlined — object, array or scalar |
| `BINARY`, `VARBINARY`, `BLOB` family | String when the bytes are valid UTF-8, otherwise `"0x…"` lowercase hex capped at `MAX_BINARY_PREVIEW_BYTES` |
| `GEOMETRY` | Always `"0x…"` lowercase hex of MySQL's internal SRID+WKB, capped at `MAX_BINARY_PREVIEW_BYTES` |
| SQL `NULL`, zero dates (`0000-00-00`) | `null` |

**Integers.** A JSON number is only exact up to 2^53-1, and JavaScript clients round silently past it. Anything larger is returned as a string of its exact digits, so a 19-digit id survives the trip. Below that boundary ids are plain numbers — including `BIGINT UNSIGNED`, which previously came back quoted at every magnitude.

**Datetimes and timezones.** A `DATETIME` has no zone: MySQL stores and returns it verbatim, so the absence of a marker is the contract's statement that the value is wall-clock in whatever zone wrote it. The connection sets `time_zone = '+00:00'`, so a `TIMESTAMP` is converted to UTC on the way out and carries a trailing `Z`. Fractional seconds are no longer dropped. Space separator rather than RFC 3339 `T`, so the two families still eyeball-align.

**Truncation.** A text or JSON value longer than `MAX_VALUE_BYTES`, or a binary value longer than `MAX_BINARY_PREVIEW_BYTES`, is cut (on a UTF-8 character boundary for text) and suffixed with `…[truncated: <shown> of <total> bytes]`. A truncated value stays a JSON string, so nothing changes shape. An oversized JSON document is *not* parsed — it degrades to that marked string, because a truncated JSON value cannot be valid. Re-query with `SUBSTRING()` to page through a large cell.

**Decode failures are visible.** A value that cannot be decoded is no longer laundered into `null`. It becomes a sentinel object and increments `decodeErrors`:

```json
{"__error__": "decode_failed", "type": "DECIMAL", "message": "invalid digit found in string"}
```

SQL `NULL` and MySQL's zero dates remain plain `null` — those are real values, not failures.

**Escape hatches.** `SELECT col + 0` re-types a `TINYINT(1)` to an integer. `CAST(col AS CHAR)` forces a `BINARY` column to text when its bytes are not valid UTF-8. `ST_AsText(col)` or `ST_AsGeoJSON(col)` returns readable geometry instead of the hex-encoded internal format.

## Security Model

**The security boundary is the MySQL grant, not the query gate.**

The sanitizer is a *usability* guard: it catches obvious writes early and returns a clear, cheap error instead of a MySQL permission failure. It cannot be complete — nothing in it can stop `SELECT some_writing_stored_function()`, for one. **Connect with a MySQL account that holds `SELECT`-only grants**, and treat everything below as defence in depth on top of that.

Layers, in order of application:

1. **Comment handling** — comments are stripped (respecting strings and backticked identifiers) so they cannot hide a keyword; version-gated `/*! */` comments are rejected outright rather than silently stripped
2. **Statement allow-list** — only the read-only statement forms listed under [Query Rules](#what-is-allowed) may lead
3. **Dangerous constructs** — `INTO OUTFILE`, `INTO DUMPFILE`, `FOR UPDATE`, `FOR SHARE`, `LOCK IN SHARE MODE`, `LOAD_FILE()`, checked with quoted text masked so `WHERE note LIKE '%for update%'` is not a false positive
4. **Embedded write statements** — `INSERT`/`UPDATE`/`DELETE`/`DROP`/`ALTER`/`RENAME`/`GRANT`/`REVOKE`, `REPLACE INTO`, `TRUNCATE TABLE`, `LOAD DATA|XML`, anywhere in the statement
5. **Multi-statement prevention** — a `;` followed by anything but whitespace is rejected
6. **Row cap and value caps** — bound the result set and each value
7. **Query timeouts** — bound execution time
8. **Credential protection** — passwords are redacted in debug output and never appear in responses or logs

### No authentication

**This server has no authentication and permissive CORS.** Anyone who can reach the port can run any query the MySQL account allows.

- `MCP_HOST` defaults to `127.0.0.1`. Keep it there unless something else is enforcing access control; the process logs a warning when it binds anywhere else.
- The Docker setup sets `MCP_HOST=0.0.0.0` *inside the container* and publishes the port on `127.0.0.1` only — the container boundary is what keeps it loopback-only. Do not widen the `ports:` mapping.
- To reach it from another machine, front it with an authenticating reverse proxy, a VPN, or an SSH tunnel. Do not simply set `MCP_HOST=0.0.0.0` on the host.
- Use a **read-only MySQL user** per database connection.
- Set `RUST_LOG=mysql_mcp=warn` to minimize log output.

## Endpoints

| Path | Description |
|------|-------------|
| `GET /health` | Health check (returns `ok`) |
| `/mcp` | MCP streamable HTTP endpoint |

## License

[MIT](LICENSE)
