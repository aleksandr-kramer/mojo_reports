# src/core/core_load_groups.py
from __future__ import annotations

from ..db import get_conn
from ..settings import CONFIG
from .core_common import log


def _merge_gap_days() -> int:
    try:
        return int((CONFIG or {}).get("groups", {}).get("merge_gap_days", 14))
    except Exception:
        return 14


# ──────────────────────────────────────────────────────────────────────────────
# Алгоритм «острова и проливы» (без вложенных окон):
# 1) строим "точечные" интервалы: start_date = end_date = дата факта;
# 2) prev_run_max = MAX(end_date) OVER (... ROWS UNBOUNDED PRECEDING TO 1 PRECEDING)
# 3) новый сегмент, если start_date > prev_run_max + :merge_gap_days;
# 4) по (ключи, seg_id) берём MIN(start_date) .. MAX(end_date).
# ──────────────────────────────────────────────────────────────────────────────

SQL_BUILD_GROUP_STAFF = """
WITH base AS (
    SELECT DISTINCT
        ts.group_id,
        ls.staff_id,
        l.lesson_date::date AS d
    FROM core.lesson l
    JOIN core.lesson_staff       ls ON ls.lesson_id = l.lesson_id
    JOIN core.timetable_schedule ts ON ts.schedule_id = l.schedule_id
    WHERE COALESCE(l.is_replacement, FALSE) = FALSE
      AND ts.group_id   IS NOT NULL
      AND ls.staff_id   IS NOT NULL
      AND l.lesson_date IS NOT NULL
),
points AS (
    SELECT group_id, staff_id, d AS start_date, d AS end_date
    FROM base
),
prevmax AS (
    SELECT
        group_id, staff_id, start_date, end_date,
        MAX(end_date) OVER (
            PARTITION BY group_id, staff_id
            ORDER BY start_date, end_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_run_max
    FROM points
),
marked AS (
    SELECT
        group_id, staff_id, start_date, end_date,
        CASE
          WHEN prev_run_max IS NULL THEN 1
          WHEN start_date > ((prev_run_max + make_interval(days => %s))::date) THEN 1
          ELSE 0
        END AS is_new
    FROM prevmax
),
seg AS (
    SELECT
        group_id, staff_id, start_date, end_date,
        SUM(is_new) OVER (
            PARTITION BY group_id, staff_id
            ORDER BY start_date, end_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS seg_id
    FROM marked
)
SELECT
    group_id,
    staff_id,
    MIN(start_date) AS valid_from,
    MAX(end_date)   AS valid_to
FROM seg
GROUP BY group_id, staff_id, seg_id
"""

SQL_BUILD_GROUP_STUDENTS = """
-- Источник: полный список пар урок–ученик из RAW attendance (включая status=0)
WITH att AS (
    SELECT DISTINCT
        ts.group_id,
        ra.student_id,
        l.lesson_date::date AS d
    FROM raw.attendance ra
    JOIN core.lesson              l  ON l.lesson_id   = ra.lesson_id
    JOIN core.timetable_schedule  ts ON ts.schedule_id = l.schedule_id
    JOIN core.student             s  ON s.student_id   = ra.student_id
    WHERE ra.student_id  IS NOT NULL
      AND ts.group_id    IS NOT NULL
      AND l.lesson_date  IS NOT NULL
),
points AS (
    SELECT group_id, student_id, d AS start_date, d AS end_date
    FROM att
),
prevmax AS (
    SELECT
        group_id, student_id, start_date, end_date,
        MAX(end_date) OVER (
            PARTITION BY group_id, student_id
            ORDER BY start_date, end_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_run_max
    FROM points
),
marked AS (
    SELECT
        group_id, student_id, start_date, end_date,
        CASE
          WHEN prev_run_max IS NULL THEN 1
          WHEN start_date > ((prev_run_max + make_interval(days => %s))::date) THEN 1
          ELSE 0
        END AS is_new
    FROM prevmax
),
seg AS (
    SELECT
        group_id, student_id, start_date, end_date,
        SUM(is_new) OVER (
            PARTITION BY group_id, student_id
            ORDER BY start_date, end_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS seg_id
    FROM marked
)
SELECT
    group_id,
    student_id,
    MIN(start_date) AS valid_from,
    MAX(end_date)   AS valid_to
FROM seg
GROUP BY group_id, student_id, seg_id
"""


def run_groups() -> None:
    """
    Полная пересборка витрин:
      - core.group_staff_assignment  ← фактические уроки (без замен)
      - core.group_student_membership ← пары урок–ученик из RAW attendance (включая status=0)
    Интервалы: склейка с допуском по разрыву (merge_gap_days), строка появляется
    только при реальном «разрыве» участия. EXCLUDE-индексы удовлетворяются.
    """
    gap = _merge_gap_days()
    with get_conn() as conn, conn.cursor() as cur:
        log(f"[core][groups] rebuild (merge_gap_days={gap}) …")

        # Преподаватели
        log("[core][groups]   staff …")
        cur.execute("TRUNCATE TABLE core.group_staff_assignment")
        cur.execute(
            "INSERT INTO core.group_staff_assignment (group_id, staff_id, valid_from, valid_to) "
            + SQL_BUILD_GROUP_STAFF,
            (gap,),
        )
        log(f"[core][groups]     +rows: {cur.rowcount or 0}")

        # Ученики
        log("[core][groups]   students …")
        cur.execute("TRUNCATE TABLE core.group_student_membership")
        cur.execute(
            "INSERT INTO core.group_student_membership (group_id, student_id, valid_from, valid_to) "
            + SQL_BUILD_GROUP_STUDENTS,
            (gap,),
        )
        log(f"[core][groups]     +rows: {cur.rowcount or 0}")

        conn.commit()
        log("[core][groups] done.")
