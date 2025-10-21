# src/core/core_load_people.py
from __future__ import annotations

from datetime import date

import psycopg2.extras

from ..db import get_conn
from .core_common import log

VALID_FROM_DEFAULT = date(2025, 9, 1)


def _table_exists(cur, fqname: str) -> bool:
    # fqname вроде 'core.parent'
    cur.execute("SELECT to_regclass(%s)", (fqname,))
    return cur.fetchone()[0] is not None


def _upsert_students(cur) -> int:
    sql = """
    WITH s AS (
      SELECT DISTINCT
        student_id::bigint                              AS student_id,
        NULLIF(TRIM(first_name),'')                     AS first_name,
        NULLIF(TRIM(last_name),'')                      AS last_name,
        NULLIF(TRIM(gender),'')                         AS gender,
        dob::date                                       AS dob,
        NULLIF(TRIM(email),'')                          AS email,
        CASE WHEN cohort ~ '^[0-9]+$' THEN cohort::int ELSE NULL END AS cohort,
        -- RAW: program (текст). Маппим в коды core.ref_programme
        CASE
          WHEN program ILIKE 'IB%%' OR program ILIKE '%%baccalaureate%%' THEN 'IB'
          WHEN program ILIKE 'IPC%%' OR program ILIKE '%%primary curriculum%%' THEN 'IPC'
          WHEN program ILIKE 'PEARSON%%' OR program ILIKE '%%pearson%%' THEN 'PEARSON'
          WHEN program ILIKE 'STATE%%' OR program ILIKE '%%state%%' OR program ILIKE '%%national%%' THEN 'STATE'
          ELSE NULL
        END AS programme_code
      FROM raw.students_ref
    )
    INSERT INTO core.student (student_id, first_name, last_name, gender, dob, email, programme_code, cohort, active)
    SELECT s.student_id, s.first_name, s.last_name, s.gender, s.dob, s.email, s.programme_code, s.cohort, TRUE
    FROM s
    WHERE s.student_id IS NOT NULL
      AND s.first_name IS NOT NULL
      AND s.last_name  IS NOT NULL
    ON CONFLICT (student_id) DO UPDATE
    SET first_name     = EXCLUDED.first_name,
        last_name      = EXCLUDED.last_name,
        gender         = EXCLUDED.gender,
        dob            = EXCLUDED.dob,
        email          = EXCLUDED.email,
        programme_code = EXCLUDED.programme_code,
        cohort         = EXCLUDED.cohort,
        active         = EXCLUDED.active;
    """
    cur.execute(sql)
    return cur.rowcount or 0


def _upsert_parents_and_links(cur) -> tuple[int, int]:
    # Выполняем только если таблицы существуют
    if not _table_exists(cur, "core.parent"):
        log("[core][people]   core.parent отсутствует — пропускаю родителей.")
        return 0, 0
    if not _table_exists(cur, "core.student_parent"):
        log(
            "[core][people]   core.student_parent отсутствует — пропускаю связи родитель↔ученик."
        )
        # всё равно можно вставить самих родителей
        only_parents = True
    else:
        only_parents = False

    # 1) Родители (RAW: parents_ref)
    sql_parent = """
    WITH p AS (
      SELECT DISTINCT
        NULLIF(TRIM(parent_email),'') AS email,
        NULLIF(TRIM(parent_name),'')  AS parent_name
      FROM raw.parents_ref
      WHERE parent_email IS NOT NULL
    )
    INSERT INTO core.parent (email, parent_name, active)
    SELECT p.email, COALESCE(p.parent_name, ''), TRUE
    FROM p
    WHERE p.email IS NOT NULL
      AND NOT EXISTS (SELECT 1 FROM core.parent cp WHERE cp.email = p.email);
    """
    cur.execute(sql_parent)
    ins_parents = cur.rowcount or 0

    if only_parents:
        return ins_parents, 0

    # 2) Связи студент-родитель (RAW: student_parent_links ↔ parents_ref)
    sql_links = """
    WITH l AS (
      SELECT DISTINCT
        spl.student_id::bigint AS student_id,
        NULLIF(TRIM(spl.parent_email),'') AS parent_email
      FROM raw.student_parent_links spl
      WHERE spl.student_id IS NOT NULL
    ),
    rel AS (
      SELECT l.student_id, pa.parent_id
      FROM l
      JOIN core.parent pa  ON pa.email = l.parent_email
      JOIN core.student st ON st.student_id = l.student_id
    )
    INSERT INTO core.student_parent (student_id, parent_id)
    SELECT rel.student_id, rel.parent_id
    FROM rel
    WHERE NOT EXISTS (
      SELECT 1 FROM core.student_parent sp
      WHERE sp.student_id = rel.student_id AND sp.parent_id = rel.parent_id
    );
    """

    cur.execute(sql_links)
    ins_links = cur.rowcount or 0
    return ins_parents, ins_links


def _upsert_staff_and_departments(cur) -> tuple[int, int]:
    # staff: из RAW staff_ref -> core.staff
    # raw.staff_ref: staff_email (PK, lower), staff_id (Mojo ID), staff_name (ФИО), gender
    # core.staff: email (UNIQUE), staff_name, gender, external_staff_id, active
    sql_staff = """
    WITH s AS (
      SELECT DISTINCT
        NULLIF(TRIM(staff_email),'') AS email,
        NULLIF(TRIM(staff_name),'')  AS staff_name,
        NULLIF(TRIM(gender),'')      AS gender,
        CASE WHEN staff_id IS NOT NULL THEN staff_id::bigint ELSE NULL END AS external_staff_id,
        TRUE AS active
      FROM raw.staff_ref
    )
    INSERT INTO core.staff (email, staff_name, gender, external_staff_id, active)
    SELECT s.email, s.staff_name, s.gender, s.external_staff_id, s.active
    FROM s
    WHERE s.email IS NOT NULL
      AND s.staff_name IS NOT NULL
    ON CONFLICT (email) DO UPDATE
    SET staff_name        = EXCLUDED.staff_name,
        gender            = EXCLUDED.gender,
        -- не затираем осознанно существующий external_staff_id пустым:
        external_staff_id = COALESCE(EXCLUDED.external_staff_id, core.staff.external_staff_id),
        active            = EXCLUDED.active;
    """
    cur.execute(sql_staff)
    ins_staff = cur.rowcount or 0

    # staff_department: raw.staff_positions -> core.staff_department
    # raw.staff_positions: staff_email, department, position, ... (department_key/position_key уже нормализованы в RAW)
    # core.staff_department: (staff_id, department_id) PK, position_title
    if not _table_exists(cur, "core.staff_department"):
        return ins_staff, 0

    sql_deps = """
    WITH sp AS (
      SELECT DISTINCT
        NULLIF(TRIM(p.staff_email),'') AS email,
        NULLIF(TRIM(p.department),'')  AS department_name,
        NULLIF(TRIM(p.position),'')    AS position_title
      FROM raw.staff_positions p
      WHERE p.staff_email IS NOT NULL
    ),
    rel AS (
      SELECT st.staff_id, d.department_id, sp.position_title
      FROM sp
      JOIN core.staff st         ON st.email = sp.email
      JOIN core.ref_department d ON d.department_name = sp.department_name
    )
    INSERT INTO core.staff_department (staff_id, department_id, position_title)
    SELECT rel.staff_id, rel.department_id, rel.position_title
    FROM rel
    ON CONFLICT (staff_id, department_id) DO UPDATE
      SET position_title = EXCLUDED.position_title;
    """
    cur.execute(sql_deps)
    ins_staff_deps = cur.rowcount or 0
    return ins_staff, ins_staff_deps


def run_people(mode: str, d_from: date | None, d_to: date | None) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            log("[core][people] upsert students ...")
            s = _upsert_students(cur)
            log(f"[core][people]   +students: {s}")

            log("[core][people] upsert parents + links ...")
            p, l = _upsert_parents_and_links(cur)
            log(f"[core][people]   +parents: {p}, +links: {l}")

            log("[core][people] upsert staff + departments ...")
            st, sd = _upsert_staff_and_departments(cur)
            log(f"[core][people]   +staff: {st}, +staff_deps: {sd}")

        conn.commit()
    log("[core][people] done.")
