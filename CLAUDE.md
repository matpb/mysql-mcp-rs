# mysql-mcp: read-only MySQL access over MCP, for AI clients that query your databases

This codebase is yours: what it's for and how it's built are your call, and you'll keep building on it.

## Ground rules
- Never let a query change connection or session state. Connections are pooled and shared, so a `SET`, an `@@variable` assignment or a session tweak leaks into the next caller's results. The gate rejects them on purpose; do not add a path around it.
- Never widen exposure. There is no authentication unless `API_KEYS` is set, and CORS is permissive. Keep the `MCP_HOST` default of `127.0.0.1`, keep the compose port published on `127.0.0.1`, and never bind `0.0.0.0` on the host.
- In stdio mode stdout is the JSON-RPC channel. Log through `tracing`, which writes to stderr; never `println!` there.
- The MySQL account's read-only grant is the security boundary. The sanitizer is a usability guard that returns early, clear errors. Do not weaken either on the theory that the other covers it.
- A change to a tool's behaviour or result shape updates `README.md` and the tool description in `src/mcp/tools.rs` in the same change.

## Commands
Run from the repo root. None of them needs a MySQL server.
- Build: `cargo build`
- Full test suite: `cargo test`
- One test file: `cargo test --test sanitizer` (or `--test auth`)
- One test: `cargo test --test sanitizer set_global_blocked`
- Format check: `cargo fmt --check`
- Lint: `cargo clippy --all-targets -- -D warnings`
- Binary usage: `cargo run -- --help`

The tests never connect to MySQL. Keep it that way: do not point a test at a real server.
Subcommands are `http` (default) and `stdio`; `README.md` covers how to run each one.

## Layout worth knowing
- `src/server.rs` holds the transports, the router and the API key wiring. `src/main.rs` only calls `run()`.
- `src/mcp/tools.rs` holds every MCP tool and the resources. Nothing else lives in `src/mcp/`.
- Value encoding (what a caller sees for each MySQL type) lives in `src/db.rs` (`encode_row`, `row_to_json`), not in `tools.rs`, even though the tool descriptions there document it. Change both together.
- `src/sanitizer.rs` is the query gate, and `apply_limit` in the same file applies the row cap.
- Unit tests are inline `#[cfg(test)]` modules in `src/`. `tests/` holds only the sanitizer and auth suites.

## Conventions
- Reject, don't repair. A query or identifier that fails the gate is refused with a message that names the way forward. The one rewrite is the `LIMIT` clamp, and that is intentional.
- Errors reaching the caller carry MySQL's own error text (`db_error_message`), not "check the logs".
- Secrets stay out of output: passwords are redacted in `Debug`, and API keys appear only as a count.
- Configuration comes from environment variables only. `MYSQL_DATABASES` wins over the flat `MYSQL_*` form.
- Connections are lazy. A database that is down at startup is retried on first use, and startup does not fail for it.
- Values follow the encoding contract in `README.md`: integers past 2^53-1 and every DECIMAL are strings. Do not simplify them to JSON numbers.
- Read nullable columns as `Option<T>`. A bare `T` turns SQL NULL into a decode error.

## Landmines
- Don't switch TLS back to rustls: it implements only ECDHE key exchange, and MySQL 5.7 offers only DHE and static RSA, so every 5.7 connection dies at the handshake. The crate uses `tls-native-tls` for this reason.
- Don't pass `timezone(None)` to the sqlx connect options or add a `SET time_zone`: TIMESTAMP values end in `Z` on the assumption that the session runs at `+00:00`, which sqlx sets by default.
- Don't enable stateful sessions in the rmcp HTTP service: the server is stateless on purpose (`NeverSessionManager`, `stateful_mode: false`), and per-request credentials make sessions pointless.
- Don't read result values by column name: sqlx cannot resolve some names (EXPLAIN is one), so the lookup fails. Read by ordinal.
- Don't drop the trailing-`;` trim in `apply_limit`: `SELECT ...; LIMIT 1000` is a MySQL syntax error.
- Don't strip `/*! */` version-gated comments instead of rejecting them: MySQL executes their contents, so stripping changes what runs.
- Don't remove the per-connection `MAX_EXECUTION_TIME` and `READ ONLY` settings: a client-side timeout only abandons the future, and MySQL keeps running the query.

## Deeper docs
- `README.md`: the user-facing contract. Query rules, result encoding, environment variables, security model.
- `.env.example`: every configuration variable, with a comment on each.
- `Dockerfile` and `docker-compose.yml`: container build and the loopback-only port mapping.
