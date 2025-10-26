#!/usr/bin/env bash
set -euo pipefail
: "${ENV_FILE:=.env.server}"
# 1) RAW: глубокий рефреш за N дней
docker compose run --rm app python -m src.raw.raw_orchestrator --mode weekly-deep --force-weekly-deep
# 2) CORE: пересобрать поверх обновлённого RAW
docker compose run --rm app python -m src.core.core_etl --mode weekly-deep --force-weekly-deep
