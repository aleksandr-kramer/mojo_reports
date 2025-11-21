#!/usr/bin/env bash
set -euo pipefail
: "${ENV_FILE:=.env.server}"

# shellcheck disable=SC1091
. "$(dirname "$0")/lib_etl_status.sh"

# Если ETL не в состоянии ok — не запускаем отчёты
if ! etl_is_ok; then
  etl_load_status
  echo "[guard] ETL status is not ok (status=${ETL_LAST_STATUS:-unset}, component=${ETL_LAST_COMPONENT:-unset})."
  echo "[guard] Coordinator daily attendance report will not be generated."
  exit 0
fi

# без --date: сам возьмёт «вчерашний учебный день»
docker compose run --rm app python -m src.reports.coordinator_daily_attendance_report
