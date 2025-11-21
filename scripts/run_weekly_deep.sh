#!/usr/bin/env bash
set -euo pipefail
: "${ENV_FILE:=.env.server}"

# shellcheck disable=SC1091
. "$(dirname "$0")/lib_etl_status.sh"

# 1) RAW: глубокий рефреш за N дней
if ! docker compose run --rm app python -m src.raw.raw_orchestrator --mode weekly-deep --force-weekly-deep; then
  etl_set_status "raw" "failed" "raw_orchestrator failed (mode=weekly-deep)"
  docker compose run --rm app \
    python -m src.monitoring.notify_etl_failure \
      --component raw \
      --stage raw_orchestrator \
      --message "src.raw.raw_orchestrator --mode weekly-deep --force-weekly-deep failed"
  exit 1
fi

# 2) CORE: пересобрать поверх обновлённого RAW
if ! docker compose run --rm app python -m src.core.core_etl --mode weekly-deep --force-weekly-deep; then
  etl_set_status "core" "failed" "core_etl failed (mode=weekly-deep)"
  docker compose run --rm app \
    python -m src.monitoring.notify_etl_failure \
      --component core \
      --stage core_etl \
      --message "src.core.core_etl --mode weekly-deep --force-weekly-deep failed"
  exit 1
fi

etl_set_status "raw+core" "ok" "weekly deep ETL ok (raw+core)"

exit 0
