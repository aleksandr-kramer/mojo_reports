# src/core/core_load_refs.py
from __future__ import annotations

from datetime import date

import psycopg2.extras

from ..db import get_conn
from .core_common import log

ENDPOINT = "core_refs"  # для core.sync_state


def _upsert_departments(cur) -> int:
    sql = """
    WITH src AS (
      SELECT DISTINCT NULLIF(TRIM(department), '') AS department_name
      FROM raw.subjects
      WHERE department IS NOT NULL
      UNION
      SELECT DISTINCT NULLIF(TRIM(department), '') AS department_name
      FROM raw.staff_positions
      WHERE department IS NOT NULL
    )
    INSERT INTO core.ref_department (department_name)
    SELECT s.department_name
    FROM src s
    WHERE s.department_name IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM core.ref_department d
        WHERE d.department_name = s.department_name
      );
    """
    cur.execute(sql)
    return cur.rowcount or 0


def _upsert_subjects(cur) -> int:
    # raw.subjects: id, title, in_curriculum (smallint 0/1), in_olymp (smallint 0/1), department (text), closed (smallint 0/1)
    # core.ref_subject: subject_id, subject_title, in_curriculum (bool), in_olymp (bool), department_id, is_closed (bool)
    sql = """
    WITH s AS (
      SELECT DISTINCT
        id::bigint                  AS subject_id,
        NULLIF(TRIM(title), '')     AS subject_title,
        CASE WHEN in_curriculum = 1 THEN TRUE ELSE FALSE END AS in_curriculum,
        CASE WHEN in_olymp      = 1 THEN TRUE ELSE FALSE END AS in_olymp,
        NULLIF(TRIM(department), '') AS department_name,
        CASE WHEN closed         = 1 THEN TRUE ELSE FALSE END AS is_closed
      FROM raw.subjects
    )
    INSERT INTO core.ref_subject
      (subject_id, subject_title, in_curriculum, in_olymp, department_id, is_closed)
    SELECT
      s.subject_id,
      s.subject_title,
      s.in_curriculum,
      s.in_olymp,
      d.department_id,
      s.is_closed
    FROM s
    LEFT JOIN core.ref_department d ON d.department_name = s.department_name
    WHERE s.subject_id IS NOT NULL
    ON CONFLICT (subject_id) DO UPDATE
    SET subject_title = EXCLUDED.subject_title,
        in_curriculum = EXCLUDED.in_curriculum,
        in_olymp      = EXCLUDED.in_olymp,
        department_id = EXCLUDED.department_id,
        is_closed     = EXCLUDED.is_closed;
    """
    cur.execute(sql)
    return cur.rowcount or 0


def _upsert_work_forms(cur) -> int:
    # raw.work_forms:
    #   id_form BIGINT (PK), form_name TEXT, form_description TEXT,
    #   form_area SMALLINT, form_control SMALLINT (0/1), form_weight NUMERIC(6,2),
    #   form_percent SMALLINT (0/1), form_created/archived/deleted TIMESTAMPTZ
    #
    # core.ref_work_form:
    #   form_id BIGINT (PK), form_name TEXT UNIQUE, form_description TEXT,
    #   is_control BOOLEAN, weight_pct INT 0..100, form_percent_raw INT,
    #   created_at_src/archived_at_src/deleted_at_src TIMESTAMPTZ
    sql = """
    WITH wf AS (
      SELECT DISTINCT
        id_form::bigint                    AS form_id,
        NULLIF(TRIM(form_name), '')        AS form_name,
        NULLIF(TRIM(form_description), '') AS form_description,

        -- SMALLINT (0/1) -> BOOLEAN
        CASE WHEN COALESCE(form_control, 0) = 1 THEN TRUE ELSE FALSE END AS is_control,

        -- NUMERIC(6,2) -> INT [0..100], с округлением и «зажимом»
        CASE
          WHEN form_weight IS NULL THEN 0
          ELSE LEAST(GREATEST(ROUND(form_weight)::int, 0), 100)
        END AS weight_pct,

        -- SMALLINT (0/1) -> INT (как есть, может быть NULL)
        CASE WHEN form_percent IS NULL THEN NULL ELSE form_percent::int END AS form_percent_raw,

        form_created  AS created_at_src,
        form_archived AS archived_at_src,
        form_deleted  AS deleted_at_src
      FROM raw.work_forms
    )
    INSERT INTO core.ref_work_form
      (form_id, form_name, form_description, is_control, weight_pct, form_percent_raw,
       created_at_src, archived_at_src, deleted_at_src)
    SELECT
      wf.form_id,
      wf.form_name,
      wf.form_description,
      wf.is_control,
      wf.weight_pct,
      wf.form_percent_raw,
      wf.created_at_src,
      wf.archived_at_src,
      wf.deleted_at_src
    FROM wf
    WHERE wf.form_id IS NOT NULL
    ON CONFLICT (form_id) DO UPDATE
    SET form_name        = EXCLUDED.form_name,
        form_description = EXCLUDED.form_description,
        is_control       = EXCLUDED.is_control,
        weight_pct       = EXCLUDED.weight_pct,
        form_percent_raw = EXCLUDED.form_percent_raw,
        created_at_src   = COALESCE(EXCLUDED.created_at_src, core.ref_work_form.created_at_src),
        archived_at_src  = EXCLUDED.archived_at_src,
        deleted_at_src   = EXCLUDED.deleted_at_src;
    """
    cur.execute(sql)
    return cur.rowcount or 0


def run_refs(mode: str, d_from: date | None, d_to: date | None) -> None:
    # Для справочников окно не принципиально — грузим идемпотентно весь снимок.
    with get_conn() as conn:
        with conn.cursor() as cur:
            log("[core][refs] upsert departments ...")
            ins1 = _upsert_departments(cur)
            log(f"[core][refs]   +{ins1}")

            log("[core][refs] upsert subjects ...")
            ins2 = _upsert_subjects(cur)
            log(f"[core][refs]   +{ins2}")

            log("[core][refs] upsert work_forms ...")
            ins3 = _upsert_work_forms(cur)
            log(f"[core][refs]   +{ins3}")

        conn.commit()
    log("[core][refs] done.")
