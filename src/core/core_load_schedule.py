# src/core/core_load_schedule.py
from __future__ import annotations

from datetime import date
from typing import Optional, Tuple

from ..db import get_conn
from .core_common import log


def _window_clause(d_from: Optional[date], d_to: Optional[date]) -> Tuple[str, tuple]:
    """
    Если окно задано — фильтруем по дате урока (lesson_date).
    Для init/daily без окна — грузим идемпотентно весь snapshot RAW.
    """
    if d_from and d_to:
        return "WHERE rl.lesson_date BETWEEN %s AND %s", (d_from, d_to)
    return "", tuple()


def _upsert_teaching_groups(cur, d_from: Optional[date], d_to: Optional[date]) -> int:
    """
    raw.schedule_lessons -> core.teaching_group
    group_id, group_name, subject_id (по subject_title).
    """
    where_sql, params = _window_clause(d_from, d_to)
    sql = f"""
    WITH src AS (
      SELECT DISTINCT
        rl.group_id::bigint                 AS group_id,
        NULLIF(TRIM(rl.group_name),'')      AS group_name,
        rs.subject_id
      FROM raw.schedule_lessons rl
      LEFT JOIN core.ref_subject rs
        ON rs.subject_title = NULLIF(TRIM(rl.subject_name),'')
      {where_sql}
    )
    INSERT INTO core.teaching_group (group_id, group_name, subject_id, active)
    SELECT s.group_id, s.group_name, s.subject_id, TRUE
    FROM src s
    WHERE s.group_id IS NOT NULL AND s.group_name IS NOT NULL
    ON CONFLICT (group_id) DO UPDATE
      SET group_name = EXCLUDED.group_name,
          subject_id = EXCLUDED.subject_id,
          active     = TRUE;
    """
    cur.execute(sql, params)
    return cur.rowcount or 0


def _upsert_timetable_schedule(
    cur, d_from: Optional[date], d_to: Optional[date]
) -> int:
    """
    raw.schedule_lessons -> core.timetable_schedule
    schedule_id, group_id, subject_id, room, replaced_schedule_id, schedule_start/finish.
    """
    where_sql, params = _window_clause(d_from, d_to)
    sql = f"""
    WITH src AS (
      SELECT DISTINCT
        rl.schedule_id::bigint             AS schedule_id,
        rl.group_id::bigint                AS group_id,
        rs.subject_id,
        NULLIF(TRIM(rl.room),'')           AS room,
        rl.replaced_schedule_id::bigint    AS replaced_schedule_id,
        rl.schedule_start::date            AS schedule_start,
        rl.schedule_finish::date           AS schedule_finish
      FROM raw.schedule_lessons rl
      LEFT JOIN core.ref_subject rs
        ON rs.subject_title = NULLIF(TRIM(rl.subject_name),'')
      {where_sql}
    )
    INSERT INTO core.timetable_schedule
      (schedule_id, group_id, subject_id, room, replaced_schedule_id, schedule_start, schedule_finish)
    SELECT s.schedule_id, s.group_id, s.subject_id, s.room, s.replaced_schedule_id, s.schedule_start, s.schedule_finish
    FROM src s
    WHERE s.schedule_id IS NOT NULL
      AND s.group_id IS NOT NULL
      AND s.schedule_start IS NOT NULL
    ON CONFLICT (schedule_id) DO UPDATE
      SET group_id             = EXCLUDED.group_id,
          subject_id           = EXCLUDED.subject_id,
          room                 = EXCLUDED.room,
          replaced_schedule_id = EXCLUDED.replaced_schedule_id,
          schedule_start       = EXCLUDED.schedule_start,
          schedule_finish      = EXCLUDED.schedule_finish;
    """
    cur.execute(sql, params)
    return cur.rowcount or 0


def _upsert_lessons(cur, d_from: Optional[date], d_to: Optional[date]) -> int:
    """
    raw.schedule_lessons -> core.lesson
    lesson_id, schedule_id, lesson_date, day_number, lesson_start/finish,
    is_replacement (0/1 -> boolean), replaced_schedule_id.
    """
    where_sql, params = _window_clause(d_from, d_to)
    sql = f"""
    WITH src AS (
      SELECT DISTINCT
        rl.lesson_id::bigint               AS lesson_id,
        rl.schedule_id::bigint             AS schedule_id,
        rl.lesson_date::date               AS lesson_date,
        rl.day_number::smallint            AS day_number,
        rl.lesson_start::time              AS lesson_start,
        rl.lesson_finish::time             AS lesson_finish,
        CASE WHEN COALESCE(rl.is_replacement,0) = 1 THEN TRUE ELSE FALSE END AS is_replacement,
        rl.replaced_schedule_id::bigint    AS replaced_schedule_id
      FROM raw.schedule_lessons rl
      {where_sql}
    )
    INSERT INTO core.lesson
      (lesson_id, schedule_id, lesson_date, day_number, lesson_start, lesson_finish, is_replacement, replaced_schedule_id)
    SELECT s.lesson_id, s.schedule_id, s.lesson_date, s.day_number, s.lesson_start, s.lesson_finish, s.is_replacement, s.replaced_schedule_id
    FROM src s
    WHERE s.lesson_id IS NOT NULL
      AND s.schedule_id IS NOT NULL
      AND s.lesson_date IS NOT NULL
    ON CONFLICT (lesson_id) DO UPDATE
      SET schedule_id          = EXCLUDED.schedule_id,
          lesson_date          = EXCLUDED.lesson_date,
          day_number           = EXCLUDED.day_number,
          lesson_start         = EXCLUDED.lesson_start,
          lesson_finish        = EXCLUDED.lesson_finish,
          is_replacement       = EXCLUDED.is_replacement,
          replaced_schedule_id = EXCLUDED.replaced_schedule_id;
    """
    cur.execute(sql, params)
    return cur.rowcount or 0


def _upsert_lesson_staff(cur, d_from: Optional[date], d_to: Optional[date]) -> int:
    """
    raw.schedule_lessons.staff_json -> core.lesson_staff (lesson_id, staff_id)
    Ключи staff_json — внешние ID сотрудников (строки). Фильтруем только числовые ключи.
    """
    where_sql, params = _window_clause(d_from, d_to)
    sql = f"""
    WITH src AS (
      SELECT
        rl.lesson_id::bigint AS lesson_id,
        CASE WHEN kv.key ~ '^[0-9]+$' THEN kv.key::bigint ELSE NULL END AS external_staff_id
      FROM raw.schedule_lessons rl
      JOIN LATERAL jsonb_each_text(COALESCE(rl.staff_json, '{{}}'::jsonb)) AS kv(key, val) ON TRUE
      {where_sql}
    ),
    resolved AS (
      SELECT s.lesson_id, st.staff_id
      FROM src s
      JOIN core.staff st ON st.external_staff_id = s.external_staff_id
      WHERE s.external_staff_id IS NOT NULL
    )
    INSERT INTO core.lesson_staff (lesson_id, staff_id, is_primary)
    SELECT r.lesson_id, r.staff_id, TRUE
    FROM resolved r
    ON CONFLICT (lesson_id, staff_id) DO NOTHING;
    """
    cur.execute(sql, params)
    return cur.rowcount or 0


def run_schedule(mode: str, d_from: Optional[date], d_to: Optional[date]) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            log("[core][schedule] upsert teaching_group ...")
            g = _upsert_teaching_groups(cur, d_from, d_to)
            log(f"[core][schedule]   +groups: {g}")

            log("[core][schedule] upsert timetable_schedule ...")
            ts = _upsert_timetable_schedule(cur, d_from, d_to)
            log(f"[core][schedule]   +timetable: {ts}")

            log("[core][schedule] upsert lessons ...")
            le = _upsert_lessons(cur, d_from, d_to)
            log(f"[core][schedule]   +lessons: {le}")

            log("[core][schedule] upsert lesson_staff ...")
            ls = _upsert_lesson_staff(cur, d_from, d_to)
            log(f"[core][schedule]   +lesson_staff: {ls}")

        conn.commit()
    log("[core][schedule] done.")
