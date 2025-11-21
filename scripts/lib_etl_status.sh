#!/usr/bin/env bash
set -euo pipefail

# Корень репозитория: один уровень выше папки scripts/
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STATE_DIR="$ROOT_DIR/state"
ETL_STATUS_FILE="$STATE_DIR/etl_status.env"

mkdir -p "$STATE_DIR"

etl_set_status() {
  local component="${1:-unknown}"
  local status="${2:-unknown}"
  local message="${3:-}"

  local ts_utc
  ts_utc="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

  cat > "$ETL_STATUS_FILE" <<EOF2
ETL_LAST_COMPONENT="$component"
ETL_LAST_STATUS="$status"
ETL_LAST_MESSAGE="$message"
ETL_LAST_UPDATED_AT="$ts_utc"
EOF2
}

etl_load_status() {
  if [ -f "$ETL_STATUS_FILE" ]; then
    # shellcheck disable=SC1090
    . "$ETL_STATUS_FILE"
  fi
}

etl_is_ok() {
  etl_load_status
  if [ "${ETL_LAST_STATUS:-}" = "ok" ]; then
    return 0
  fi
  return 1
}
