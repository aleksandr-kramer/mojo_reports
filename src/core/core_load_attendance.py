# src/core/core_load_attendance.py
from __future__ import annotations

from datetime import date
from typing import Optional, Tuple

from ..db import get_conn
from .core_common import compute_daily_window, log, upsert_sync_state

ENDPOINT = "core_attendance"  # имя в core.sync_state


def _window_for_mode(
    mode: str, d_from: Optional[date], d_to: Optional[date]
) -> Tuple[Optional[date], Optional[date]]:
    """
    Определяем окно загрузки.
    - backfill: строго заданное пользователем (оба края обязательны на уровне оркестратора)
    - daily: консервативное окно последних 14 дней
    - init: без окна (вся RAW-таблица), парт. по дате прикроет стоимость
    """
    if mode == "backfill" and d_from and d_to:
        return d_from, d_to
    if mode == "daily":
        return compute_daily_window(days_back=14)
    return None, None  # init


def _where_clause(d_from: Optional[date], d_to: Optional[date]) -> Tuple[str, tuple]:
    if d_from and d_to:
        return "WHERE ra.attendance_date BETWEEN %s AND %s", (d_from, d_to)
    return "", tuple()


def _upsert_attendance(cur, d_from: Optional[date], d_to: Optional[date]) -> int:
    """
    raw.attendance -> core.attendance_event

    Маппинги:
      - status (SMALLINT 0/1/2/3/6/7) -> status_code (BOOLEAN не нужен, используем как есть)
      - period_id: по попаданию attendance_date в ref_academic_period
      - subject_id: по совпадению названия в ref_subject.subject_title
      - grade_cohort: берём из raw.grade (INT), можно NULL
      - student_name_src: raw.student (текст)
    Учитываем FK: вставляем только если есть core.student и core.lesson.
    """
    where_sql, params = _where_clause(d_from, d_to)
    sql = f"""
    WITH src AS (
      SELECT DISTINCT
        ra.id::bigint            AS attendance_id,
        ra.student_id::bigint    AS student_id,
        ra.lesson_id::bigint     AS lesson_id,
        ra.attendance_date::date AS attendance_date,
        CASE WHEN ra.status IN (0,1,2,3,6,7) THEN ra.status ELSE 0 END AS status_code,
        ap.period_id,
        rs.subject_id,
        ra.grade                  AS grade_cohort,
        NULLIF(TRIM(ra.student),'') AS student_name_src
      FROM raw.attendance ra
      LEFT JOIN core.ref_academic_period ap
        ON ra.attendance_date BETWEEN ap.start_date AND ap.end_date
      LEFT JOIN core.ref_subject rs
        ON rs.subject_title = NULLIF(TRIM(ra.subject_name),'')
      {where_sql}
    ),
    valid AS (
      SELECT s.*
      FROM src s
      JOIN core.student st ON st.student_id = s.student_id
      JOIN core.lesson  l  ON l.lesson_id   = s.lesson_id
      WHERE s.attendance_id IS NOT NULL
        AND s.attendance_date IS NOT NULL
    )
    INSERT INTO core.attendance_event
      (attendance_id, student_id, lesson_id, attendance_date, status_code,
       period_id, subject_id, grade_cohort, student_name_src)
    SELECT
      v.attendance_id, v.student_id, v.lesson_id, v.attendance_date, v.status_code,
      v.period_id, v.subject_id, v.grade_cohort, v.student_name_src
    FROM valid v
    ON CONFLICT (attendance_id) DO UPDATE
      SET student_id       = EXCLUDED.student_id,
          lesson_id        = EXCLUDED.lesson_id,
          attendance_date  = EXCLUDED.attendance_date,
          status_code      = EXCLUDED.status_code,
          period_id        = EXCLUDED.period_id,
          subject_id       = EXCLUDED.subject_id,
          grade_cohort     = EXCLUDED.grade_cohort,
          student_name_src = EXCLUDED.student_name_src;
    """
    cur.execute(sql, params)
    return cur.rowcount or 0


def run_attendance(mode: str, d_from: Optional[date], d_to: Optional[date]) -> None:
    w_from, w_to = _window_for_mode(mode, d_from, d_to)

    with get_conn() as conn:
        with conn.cursor() as cur:
            log(f"[core][attendance] window: {w_from}..{w_to}")
            cnt = _upsert_attendance(cur, w_from, w_to)
            log(f"[core][attendance]   +events: {cnt}")

        conn.commit()

    # фиксируем прохождение
    upsert_sync_state(
        endpoint=ENDPOINT,
        window_from=w_from,
        window_to=w_to,
        last_seen_updated_at=None,  # не используем updated_at для FACT; окно по дате урока
        params={"mode": mode, "rows": cnt},
        notes="attendance upsert",
    )
    log("[core][attendance] done.")
