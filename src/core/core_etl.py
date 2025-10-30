# src/core/core_etl.py
from __future__ import annotations

import argparse
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

from ..db import advisory_lock, get_conn
from ..settings import CONFIG
from .core_common import (
    get_core_checkpoint,
    log,
    set_core_checkpoint,
    validate_window_or_throw,
)
from .core_load_attendance import run_attendance
from .core_load_classes import run_classes
from .core_load_groups import run_groups
from .core_load_marks import run_marks
from .core_load_people import run_people
from .core_load_refs import run_refs
from .core_load_schedule import run_schedule

# ──────────────────────────────────────────────────────────────────────────────
# Константы/настройки

CORE_LOCK_KEY = 1002

# ВАЖНО: endpoints — РОВНО как их пишет RAW в core.sync_state
RAW_ENDPOINTS = ("/attendance", "/marks/current", "/marks/final", "/schedule")

# ──────────────────────────────────────────────────────────────────────────────
# Хелперы


def _today() -> date:
    return date.today()


def _table_exists(schema_table: str) -> bool:
    # schema_table вида 'core.attendance_event'
    schema, _, table = schema_table.partition(".")
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s);", (f"{schema}.{table}",))
        return cur.fetchone()[0] is not None


def _has_any_rows(schema_table: str) -> bool:
    if not _table_exists(schema_table):
        return False
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT EXISTS (SELECT 1 FROM {schema_table} LIMIT 1);")
        return bool(cur.fetchone()[0])


def _monday_of(d: date) -> date:
    return d - timedelta(days=d.weekday())


def _read_recent_raw_windows(since: Optional[date]) -> Dict[str, Tuple[date, date]]:
    """
    Читаем из core.sync_state окна RAW по нужным endpoint'ам,
    которые были записаны ПОСЛЕ последнего чекпойнта CORE.
    Возвращаем endpoint -> (min_from, max_to).
    """
    where_since = ""
    params: tuple = (list(RAW_ENDPOINTS),)
    if since:
        where_since = "AND last_successful_sync_at > %s"
        params = (list(RAW_ENDPOINTS), since)

    sql = f"""
      SELECT endpoint,
             min(window_from)::date AS min_from,
             max(window_to)::date   AS max_to
      FROM core.sync_state
      WHERE endpoint = ANY(%s)
        {where_since}
      GROUP BY endpoint
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()

    out: Dict[str, Tuple[date, date]] = {}
    for ep, f, t in rows:
        if f and t and f <= t:
            out[ep] = (f, t)
    return out


# ──────────────────────────────────────────────────────────────────────────────
# Режимы


def core_init_if_empty() -> None:
    """
    Первый проход CORE: если таблицы пустые — делаем init на широкий диапазон,
    синхронно с политикой RAW (weekly_deep_days).
    """
    deep_days = int(((CONFIG or {}).get("load") or {}).get("weekly_deep_days", 60))
    today = _today()
    d_from = today - timedelta(days=max(deep_days, 0))
    d_to = today

    log(f"[core:init-if-empty] window={d_from}..{d_to}")

    # Справочники/люди/классы — idempotent снапшоты
    run_refs(mode="init", d_from=d_from, d_to=d_to)
    run_people(mode="init", d_from=d_from, d_to=d_to)
    run_classes(mode="init", d_from=d_from, d_to=d_to)

    # Датные сущности
    run_schedule(mode="init", d_from=_monday_of(d_from), d_to=d_to)
    run_attendance(mode="init", d_from=d_from, d_to=d_to)
    run_marks(mode="init", d_from=d_from, d_to=d_to)

    # Производные группы (если витрины есть)
    run_groups()

    # Чекпойнт
    set_core_checkpoint(d_to)
    log("[core:init-if-empty] done")


def core_run_auto() -> None:
    """
    Ежедневный проход CORE:
      - читаем и нормализуем чекпойнт CORE (не дальше «сегодня»);
      - собираем изменившиеся окна RAW после чекпойнта;
      - обновляем CORE только в этих диапазонах (с обрезкой верхней границы до «сегодня»);
      - обновляем чекпойнт CORE (не дальше «сегодня»).
    """
    today = _today()

    # 1) Читаем чекпойнт и страхуемся от «будущей» даты
    last_cp = get_core_checkpoint()
    if last_cp and last_cp > today:
        log(f"[core:auto] future checkpoint detected ({last_cp}) → clamp to {today}")
        last_cp = today

    # 2) Ищем изменившиеся окна RAW, начиная с (нормализованного) чекпойнта
    changed = _read_recent_raw_windows(since=last_cp)

    log(
        f"[core:auto] last_checkpoint={last_cp} changed_endpoints={list(changed.keys()) or '∅'}"
    )

    # 3) «Снапшоты» обновляются всегда
    run_refs(mode="daily", d_from=None, d_to=None)
    run_people(mode="daily", d_from=None, d_to=None)
    run_classes(mode="daily", d_from=None, d_to=None)

    # 4) Если RAW-изменений нет — только производные и чекпойнт = сегодня
    if not changed:
        run_groups()
        set_core_checkpoint(today)
        log("[core:auto] no RAW changes → snapshots only; done")
        return

    # Вспомогательная функция: обрезаем верхнюю границу окна до «сегодня»
    def _clamp_to_today(f, t):
        return (f, t if t <= today else today)

    # 5) Обрабатываем изменившиеся окна по эндпоинтам
    if "/schedule" in changed:
        f, t = changed["/schedule"]
        f, t = _clamp_to_today(f, t)
        validate_window_or_throw(f, t)
        run_schedule(mode="backfill", d_from=f, d_to=t)

    if "/attendance" in changed:
        f, t = changed["/attendance"]
        f, t = _clamp_to_today(f, t)
        validate_window_or_throw(f, t)
        run_attendance(mode="backfill", d_from=f, d_to=t)

    if ("/marks/current" in changed) or ("/marks/final" in changed):
        windows = []
        if "/marks/current" in changed:
            windows.append(changed["/marks/current"])
        if "/marks/final" in changed:
            windows.append(changed["/marks/final"])
        f = min(w[0] for w in windows)
        t = max(w[1] for w in windows)
        f, t = _clamp_to_today(f, t)
        validate_window_or_throw(f, t)
        run_marks(mode="backfill", d_from=f, d_to=t)

    # 6) Производные витрины
    run_groups()

    # 7) Новый чекпойнт: не дальше «сегодня», чтобы не «терять» дни
    #    (берём max(window_to) по всем изменившимся окнам, но с обрезкой до today)
    _clamped_to_list = []
    for _, (wf, wt) in changed.items():
        cf, ct = _clamp_to_today(wf, wt)
        _clamped_to_list.append(ct)

    max_to = max(_clamped_to_list)
    safe_max_to = min(max_to, today)
    set_core_checkpoint(safe_max_to)
    log(f"[core:auto] done; checkpoint={safe_max_to}")


def core_weekly_deep(force: bool = False) -> None:
    """
    Раз в неделю — глубокая перепроверка последних weekly_deep_days.
    """
    deep_days = int(((CONFIG or {}).get("load") or {}).get("weekly_deep_days", 60))
    if deep_days <= 0:
        log("[core:weekly-deep] skipped (deep_days<=0)")
        return

    today = _today()
    is_monday = today.weekday() == 0
    if not (is_monday or force):
        log("[core:weekly-deep] skipped (not Monday, no --force-weekly-deep)")
        return

    d_from = today - timedelta(days=deep_days)
    d_to = today
    validate_window_or_throw(d_from, d_to)
    log(f"[core:weekly-deep] window={d_from}..{d_to} (force={force})")

    run_refs(mode="backfill", d_from=d_from, d_to=d_to)
    run_people(mode="backfill", d_from=d_from, d_to=d_to)
    run_classes(mode="backfill", d_from=d_from, d_to=d_to)

    # сначала расписание/уроки, затем посещаемость
    run_schedule(mode="backfill", d_from=d_from, d_to=d_to)
    run_attendance(mode="backfill", d_from=d_from, d_to=d_to)
    run_marks(mode="backfill", d_from=d_from, d_to=d_to)

    run_groups()

    set_core_checkpoint(d_to)
    log(f"[core:weekly-deep] done; checkpoint={d_to}")


# ──────────────────────────────────────────────────────────────────────────────
# CLI


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser("CORE ETL")
    p.add_argument(
        "--mode",
        choices=["auto", "init-if-empty", "daily", "weekly-deep", "init", "backfill"],
        default="auto",
    )
    p.add_argument("--date-from", dest="d_from")
    p.add_argument("--date-to", dest="d_to")
    p.add_argument("--force-weekly-deep", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()

    # Разбираем явные окна для legacy-режимов
    d_from = date.fromisoformat(args.d_from) if args.d_from else None
    d_to = date.fromisoformat(args.d_to) if args.d_to else None
    if args.mode == "backfill":
        if not (d_from and d_to):
            raise SystemExit("--date-from/--date-to обязательны в backfill")
        validate_window_or_throw(d_from, d_to)

    with advisory_lock(CORE_LOCK_KEY):
        log(f"[core] start mode={args.mode} window={d_from}..{d_to}")

        # 1) Legacy-режимы (на случай прямых вызовов)
        if args.mode == "init":
            # если окно не передали — берём weekly_deep_days
            if not (d_from and d_to):
                deep_days = int(
                    ((CONFIG or {}).get("load") or {}).get("weekly_deep_days", 60)
                )
                today = _today()
                d_from = today - timedelta(days=max(deep_days, 0))
                d_to = today
            run_refs(mode="init", d_from=d_from, d_to=d_to)
            run_people(mode="init", d_from=d_from, d_to=d_to)
            run_classes(mode="init", d_from=d_from, d_to=d_to)
            run_schedule(mode="init", d_from=_monday_of(d_from), d_to=d_to)
            run_attendance(mode="init", d_from=d_from, d_to=d_to)
            run_marks(mode="init", d_from=d_from, d_to=d_to)
            run_groups()
            set_core_checkpoint(d_to)
            log("[core] done (legacy init).")
            return

        if args.mode == "backfill":
            run_refs(mode="backfill", d_from=d_from, d_to=d_to)
            run_people(mode="backfill", d_from=d_from, d_to=d_to)
            run_classes(mode="backfill", d_from=d_from, d_to=d_to)
            run_schedule(mode="backfill", d_from=d_from, d_to=d_to)
            run_attendance(mode="backfill", d_from=d_from, d_to=d_to)
            run_marks(mode="backfill", d_from=d_from, d_to=d_to)
            run_groups()
            set_core_checkpoint(d_to)
            log("[core] done (legacy backfill).")
            return

        # 2) Новые режимы
        if args.mode in ("auto", "init-if-empty"):
            core_empty = (
                not _has_any_rows("core.attendance_event")
                or not _has_any_rows("core.lesson")
                or not _has_any_rows("core.mark_current")
            )

            if core_empty:
                core_init_if_empty()
                if args.mode == "init-if-empty":
                    log("[core] done (init-if-empty).")
                    return

        if args.mode in ("auto", "daily"):
            core_run_auto()
            if args.mode == "daily":
                log("[core] done (daily).")
                return

        if args.mode in ("auto", "weekly-deep"):
            core_weekly_deep(force=args.force_weekly_deep)
            log("[core] done (weekly-deep).")
            return

        log("[core] done.")


if __name__ == "__main__":
    main()
