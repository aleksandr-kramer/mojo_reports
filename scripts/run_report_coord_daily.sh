#!/usr/bin/env bash
set -euo pipefail
: "${ENV_FILE:=.env.server}"
# без --date: сам возьмёт «вчерашний учебный день»
docker compose run --rm app python -m src.reports.coordinator_daily_attendance_report
