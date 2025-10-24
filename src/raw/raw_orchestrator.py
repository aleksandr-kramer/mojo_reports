# src/raw/raw_orchestrator.py
from __future__ import annotations

import argparse
from datetime import date, timedelta
from typing import List, Optional, Tuple

from ..db import advisory_lock, get_conn
from ..settings import CONFIG, settings

# RAW loaders
from . import load_attendance as att
from . import load_classes_excel as xl_classes
from . import load_marks_current as mc
from . import load_marks_final as mf
from . import load_parents_excel as xl_parents
from . import load_schedule as sch
from . import load_staff_excel as xl_staff
from . import load_students_excel as xl_students
from . import load_subjects as subj
from . import load_work_forms as wf

# ───────────────────────────── helpers ─────────────────────────────


def _today() -> date:
    return date.today()


def _monday_of(d: date) -> date:
    return d - timedelta(days=d.weekday())


def _mondays_between(d_from: date, d_to: date) -> List[date]:
    cur = _monday_of(d_from)
    end = _monday_of(d_to)
    out = []
    while cur <= end:
        out.append(cur)
        cur += timedelta(days=7)
    return out


def _last_window_to(endpoint: str) -> Optional[date]:
    """
    Берём последнее window_to из core.sync_state по данному endpoint.
    """
    sql = """
      SELECT window_to::date
      FROM core.sync_state
      WHERE endpoint = %s
      ORDER BY last_successful_sync_at DESC
      LIMIT 1;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (endpoint,))
        row = cur.fetchone()
        return row[0] if row and row[0] else None


def _has_any_rows(schema_table: str) -> bool:
    schema, _, table = schema_table.partition(".")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s);", (f"{schema}.{table}",))
        if cur.fetchone()[0] is None:
            return False
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM {schema}.{table} LIMIT 1);")
        return bool(cur.fetchone()[0])


def _date_range(d_from: date, d_to: date) -> List[date]:
    cur = d_from
    out = []
    while cur <= d_to:
        out.append(cur)
        cur += timedelta(days=1)
    return out


# ───────────────────────── strategy ─────────────────────────


def _run_snapshots_daily() -> None:
    """
    Снэпшоты без дат: запускаем каждый день.
    """
    xl_students.run()
    xl_staff.run()
    xl_classes.run()
    xl_parents.run()

    wf.run_load(mode="daily")
    subj.run_load(mode="daily")


def _init_if_empty() -> None:
    """
    Первый запуск: если таблицы пустые и/или нет sync_state — делаем init.
    Диапазон: today - weekly_deep_days .. today (для schedule — неделями + вперед).
    """
    weekly_deep_days = int(
        ((CONFIG or {}).get("load") or {}).get("weekly_deep_days", 60)
    )
    schedule_forward = int(
        ((CONFIG or {}).get("api") or {})
        .get("windows", {})
        .get("schedule_days_forward", 7)
    )

    today = _today()
    d_from = today - timedelta(days=max(weekly_deep_days, 0))
    d_to = today

    # attendance
    if not _has_any_rows("raw.attendance"):
        att.run_init(d_from=d_from, d_to=d_to)

    # marks/current
    if not _has_any_rows("raw.marks_current"):
        mc.run_init(d_from=d_from, d_to=d_to)

    # marks/final (init — фильтр по created_date внутри загрузчика)
    if not _has_any_rows("raw.marks_final"):
        mf.run_init(d_from=d_from, d_to=d_to)

    # schedule: неделями от monday(d_from) до (today + forward)
    if not _has_any_rows("raw.schedule_lessons"):
        sch.run_init(
            d_from=_monday_of(d_from), d_to=(today + timedelta(days=schedule_forward))
        )


def _run_daily_windows_and_recovery() -> None:
    """
    Ежедневная загрузка окон + попытка «донабрать» пропуски (recovery) по sync_state.
    """
    # 1) ежедневные окна
    att.run_daily()
    mc.run_daily()
    mf.run_daily()  # у final — «забрать всё»; внутри — идемпотентный upsert
    sch.run_daily()

    # 2) recovery, если видим «большую дыру» между последним window_to и сегодня
    today = _today()
    attendance_back = int(
        ((CONFIG or {}).get("api") or {})
        .get("windows", {})
        .get("attendance_days_back", 2)
    )
    safety_gap = max(attendance_back, 2)

    def recover_days(endpoint: str):
        last_to = _last_window_to(endpoint)
        if not last_to:
            return
        if last_to <= (today - timedelta(days=safety_gap + 1)):
            miss_from = last_to + timedelta(days=1)
            days = _date_range(miss_from, today)
            if not days:
                return
            if endpoint == att.ENDPOINT:
                att.run_backfill(days)
            elif endpoint == mc.ENDPOINT:
                mc.run_backfill(days)
            elif endpoint == mf.ENDPOINT:
                mf.run_backfill(days)
            # schedule — неделями:
            elif endpoint == sch.ENDPOINT:
                sch.run_backfill(_mondays_between(miss_from, today))

    for ep in (att.ENDPOINT, mc.ENDPOINT, mf.ENDPOINT, sch.ENDPOINT):
        recover_days(ep)


def _run_weekly_deep_if_due(force: bool = False) -> None:
    """
    Раз в неделю (или по флагу) — глубокий рефреш последних weekly_deep_days.
    """
    weekly_deep_days = int(
        ((CONFIG or {}).get("load") or {}).get("weekly_deep_days", 60)
    )
    if weekly_deep_days <= 0:
        return

    today = _today()
    is_weekly_slot = today.weekday() == 0  # понедельник
    if not (is_weekly_slot or force):
        return

    d_from = today - timedelta(days=weekly_deep_days)
    d_to = today
    days = _date_range(d_from, d_to)

    att.run_backfill(days)
    mc.run_backfill(days)
    mf.run_backfill(days)
    sch.run_backfill(_mondays_between(d_from, d_to))


# ───────────────────────── entrypoint ─────────────────────────


def main():
    parser = argparse.ArgumentParser(description="RAW orchestrator")
    parser.add_argument(
        "--mode",
        choices=["auto", "daily", "weekly-deep", "init-if-empty"],
        default="auto",
    )
    parser.add_argument(
        "--force-weekly-deep",
        action="store_true",
        help="выполнить weekly-deep независимо от дня недели",
    )
    args = parser.parse_args()

    # защита от параллельных запусков RAW
    with advisory_lock(1001):
        # снэпшоты — каждый день до дата-эндпоинтов
        _run_snapshots_daily()

        if args.mode == "init-if-empty":
            _init_if_empty()
            return

        if args.mode == "daily":
            _run_daily_windows_and_recovery()
            return

        if args.mode == "weekly-deep":
            _run_weekly_deep_if_due(force=True)
            return

        # auto: делаем всё по стратегии
        _init_if_empty()
        _run_daily_windows_and_recovery()
        _run_weekly_deep_if_due(force=args.force_weekly_deep)


if __name__ == "__main__":
    main()
