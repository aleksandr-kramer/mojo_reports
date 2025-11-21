#!/usr/bin/env bash
set -euo pipefail
: "${ENV_FILE:=.env.server}"

# Общие функции для статуса ETL
# путь относительно текущего файла: scripts/lib_etl_status.sh
# shellcheck disable=SC1091
. "$(dirname "$0")/lib_etl_status.sh"

# Запуск RAW ETL с обработкой ошибок
if ! docker compose run --rm app python -m src.raw.raw_orchestrator --mode auto; then
  # Фиксируем сбой RAW
  etl_set_status "raw" "failed" "raw_orchestrator failed (mode=auto)"

  # Пытаемся отправить уведомление по email
  docker compose run --rm app \
    python -m src.monitoring.notify_etl_failure \
      --component raw \
      --stage raw_orchestrator \
      --message "src.raw.raw_orchestrator --mode auto failed"

  exit 1
fi

exit 0
