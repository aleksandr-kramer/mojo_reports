# src/core/core_etl.py
from __future__ import annotations

import argparse
from datetime import date, datetime

from .core_common import log, validate_window_or_throw
from .core_load_attendance import run_attendance
from .core_load_classes import run_classes
from .core_load_groups import run_groups
from .core_load_marks import run_marks
from .core_load_people import run_people
from .core_load_refs import run_refs
from .core_load_schedule import run_schedule


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser("CORE ETL")
    p.add_argument("--mode", choices=["init", "daily", "backfill"], required=True)
    p.add_argument("--date-from", dest="d_from")
    p.add_argument("--date-to", dest="d_to")
    return p.parse_args()


def main():
    args = parse_args()
    d_from = date.fromisoformat(args.d_from) if args.d_from else None
    d_to = date.fromisoformat(args.d_to) if args.d_to else None
    if args.mode == "backfill":
        if not (d_from and d_to):
            raise SystemExit("--date-from/--date-to обязательны в backfill")
        validate_window_or_throw(d_from, d_to)

    log(f"[core] start mode={args.mode} window={d_from}..{d_to}")

    # 1) Справочники
    run_refs(mode=args.mode, d_from=d_from, d_to=d_to)

    # 2) Люди
    run_people(mode=args.mode, d_from=d_from, d_to=d_to)

    # 3) Классы
    run_classes(mode=args.mode, d_from=d_from, d_to=d_to)

    # 4) Расписание
    run_schedule(mode=args.mode, d_from=d_from, d_to=d_to)

    # 5) Посещаемость
    run_attendance(mode=args.mode, d_from=d_from, d_to=d_to)

    # 6) Оценки
    run_marks(mode=args.mode, d_from=d_from, d_to=d_to)

    run_groups()

    log("[core] done.")


if __name__ == "__main__":
    main()
