#!/usr/bin/env bash
set -euo pipefail
: "${ENV_FILE:=.env.server}"

# Общие функции для статуса ETL
# shellcheck disable=SC1091
. "$(dirname "$0")/lib_etl_status.sh"

# Запуск CORE ETL с обработкой ошибок
if ! docker compose run --rm app python -m src.core.core_etl --mode auto; then
  # Фиксируем сбой CORE
  etl_set_status "core" "failed" "core_etl failed (mode=auto)"

  # Пытаемся отправить уведомление по email
  docker compose run --rm app \
    python -m src.monitoring.notify_etl_failure \
      --component core \
      --stage core_etl \
      --message "src.core.core_etl --mode auto failed"

  exit 1
fi

# Если до сюда дошли, и RAW раньше тоже прошёл (иначе cron не вызвал бы этот скрипт),
# считаем, что ежедневный ETL (RAW+CORE) завершился успешно.
etl_set_status "raw+core" "ok" "daily ETL ok (raw+core)"

exit 0
