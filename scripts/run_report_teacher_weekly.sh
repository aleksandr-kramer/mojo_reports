#!/usr/bin/env bash
set -euo pipefail
: "${ENV_FILE:=.env.server}"

docker compose run --rm app python -m src.reports.teacher_weekly_report