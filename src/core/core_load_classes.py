# src/core/core_load_classes.py
from __future__ import annotations

from datetime import date

from ..db import get_conn
from .core_common import log

# По договорённости: дата начала действия записей по умолчанию
VALID_FROM_DEFAULT = date(2025, 9, 1)


def _upsert_classes(cur) -> int:
    """
    raw.classes_ref -> core.class
    class_code = title, cohort -> int (если цифры)
    """
    sql = """
    WITH src AS (
      SELECT DISTINCT
        NULLIF(TRIM(title),'')                         AS class_code,
        CASE WHEN cohort ~ '^[0-9]+$' THEN cohort::int ELSE NULL END AS cohort_int
      FROM raw.classes_ref
    )
    INSERT INTO core.class (class_code, cohort)
    SELECT s.class_code, s.cohort_int
    FROM src s
    WHERE s.class_code IS NOT NULL
    ON CONFLICT (class_code) DO UPDATE
      SET cohort = EXCLUDED.cohort;
    """
    cur.execute(sql)
    return cur.rowcount or 0


def _upsert_class_teachers(cur) -> int:
    """
    raw.classes_ref (homeroom_email / homeroom_staff_id) -> core.class_teacher
    valid_from = 2025-09-01 (по умолчанию), valid_to = NULL.
    Пробуем связать по email, иначе по external_staff_id.
    """
    sql = """
    WITH src AS (
      SELECT
        c.class_id,
        COALESCE(st_e.staff_id, st_x.staff_id) AS staff_id,
        %s::date AS valid_from
      FROM raw.classes_ref r
      JOIN core.class c
        ON c.class_code = r.title
      LEFT JOIN core.staff st_e
        ON st_e.email = NULLIF(LOWER(TRIM(r.homeroom_email)), '')
      LEFT JOIN core.staff st_x
        ON st_x.external_staff_id = CASE
                                       WHEN r.homeroom_staff_id IS NULL THEN NULL
                                       ELSE r.homeroom_staff_id::bigint
                                     END
    )
    INSERT INTO core.class_teacher (class_id, staff_id, valid_from)
    SELECT class_id, staff_id, valid_from
    FROM src
    WHERE class_id IS NOT NULL AND staff_id IS NOT NULL
    ON CONFLICT (class_id, valid_from) DO UPDATE
      SET staff_id = EXCLUDED.staff_id;
    """
    cur.execute(sql, (VALID_FROM_DEFAULT,))
    return cur.rowcount or 0


def _upsert_student_enrolments(cur) -> int:
    """
    raw.students_ref.class_name -> core.student_class_enrolment
    valid_from = 2025-09-01, valid_to = NULL.
    Вставляем только если есть и student, и class.
    """
    sql = """
    WITH src AS (
      SELECT DISTINCT
        st.student_id::bigint AS student_id,
        c.class_id            AS class_id,
        %s::date              AS valid_from
      FROM raw.students_ref st
      JOIN core.student cs
        ON cs.student_id = st.student_id::bigint
      JOIN core.class c
        ON c.class_code = NULLIF(TRIM(st.class_name),'')
      WHERE st.student_id IS NOT NULL
        AND NULLIF(TRIM(st.class_name),'') IS NOT NULL
    )
    INSERT INTO core.student_class_enrolment (student_id, class_id, valid_from)
    SELECT s.student_id, s.class_id, s.valid_from
    FROM src s
    ON CONFLICT (student_id, class_id, valid_from) DO NOTHING;
    """
    cur.execute(sql, (VALID_FROM_DEFAULT,))
    return cur.rowcount or 0


def run_classes(mode: str, d_from: date | None, d_to: date | None) -> None:
    """
    Окно дат здесь не используется — наполнение справочных/связочных таблиц идемпотентное.
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            log("[core][classes] upsert classes ...")
            c = _upsert_classes(cur)
            log(f"[core][classes]   +classes: {c}")

            log("[core][classes] upsert class_teachers ...")
            t = _upsert_class_teachers(cur)
            log(f"[core][classes]   +class_teachers: {t}")

            log("[core][classes] upsert student enrolments ...")
            e = _upsert_student_enrolments(cur)
            log(f"[core][classes]   +enrolments: {e}")

        conn.commit()
    log("[core][classes] done.")
