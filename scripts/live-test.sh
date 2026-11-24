#!/usr/bin/env bash
# Runs examples/live_check against the MySQL 8.4, 8.0 and 5.7 containers, then tears them down.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

compose=(docker compose -f docker-compose.live.yml)

cleanup() {
  local rc=$?
  if ! "${compose[@]}" down -v; then
    echo "live-test: teardown failed; check 'docker ps' for mysqlmcp-live containers" >&2
    [ "$rc" -eq 0 ] && rc=1
  fi
  exit "$rc"
}
trap cleanup EXIT

"${compose[@]}" up -d --wait
cargo run --quiet --example live_check
