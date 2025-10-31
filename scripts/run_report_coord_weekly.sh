#!/usr/bin/env bash
set -euo pipefail
: "${ENV_FILE:=.env.server}"
# без аргументов: скрипт сам возьмёт прошлую неделю (пн–пт) в локальной TZ
docker compose run --rm app python -m src.reports.coordinator_weekly_report
