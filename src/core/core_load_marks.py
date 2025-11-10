# src/core/core_load_marks.py
from __future__ import annotations

from datetime import date, timedelta

import psycopg2.extras

from ..db import get_conn
from ..settings import CONFIG
from .core_common import log, today_utc_date, upsert_sync_state

ENDPOINT = "core_marks"  # для core.sync_state


def _window_for_daily() -> tuple[date, date]:
    # берём такой же «хвост» дней, как для attendance (по умолчанию 2)
    back = int(
        ((CONFIG or {}).get("api") or {})
        .get("windows", {})
        .get("attendance_days_back", 2)
    )
    d_to = today_utc_date()
    d_from = d_to - timedelta(days=max(back, 0))
    return d_from, d_to


def _upsert_marks_current(cur, d_from: date, d_to: date) -> int:
    """
    Переливка RAW -> CORE для /marks/current в окне дат [d_from..d_to] по mark_date.
    """
    ng_en = (
        CONFIG.get("marks", {}).get("non_grade_tokens", {}).get("en", "Non-grade (En)")
    )
    ng_ru = (
        CONFIG.get("marks", {}).get("non_grade_tokens", {}).get("ru", "Non-grade (Ru)")
    )
    sql = """
    WITH src AS (
      SELECT *
      FROM raw.marks_current
      WHERE mark_date BETWEEN %(d_from)s AND %(d_to)s
    ),
    n AS (
      SELECT
        mc.id::bigint                                      AS mark_id,
        st.student_id                                      AS student_id,
        tg.group_id                                        AS group_id,

        -- период по дате оценки
        ap.period_id                                       AS period_id,
        NULLIF(TRIM(mc.period), '')                        AS period_label_raw,

        -- снапшот названия группы как пришло
        NULLIF(TRIM(mc.group_name), '')                    AS group_name_snapshot,

        mc.mark_date                                       AS lesson_date,
        mc.created                                         AS created_at_src,
        mc.value                                           AS value,

        -- оценочная схема из RAW (в Swagger это поле названо 'assesment')
        NULLIF(TRIM(mc.assesment), '')                     AS assessment_scheme,

        -- текстовая оценка по правилам Non-grade En/Ru; иначе копируем value в текст
        CASE
          WHEN NULLIF(TRIM(mc.assesment), '') = %(ng_en)s THEN
            CASE ROUND(COALESCE(mc.value, 0))::int
              WHEN 1 THEN 'Could do better'
              WHEN 2 THEN 'Tried'
              WHEN 3 THEN 'Progress made'
              WHEN 4 THEN 'Quite a progress'
              WHEN 5 THEN 'Good job!'
              WHEN 6 THEN 'Excellent job!'
              ELSE NULL
            END
          WHEN NULLIF(TRIM(mc.assesment), '') = %(ng_ru)s THEN
            CASE ROUND(COALESCE(mc.value, 0))::int
              WHEN 1 THEN 'Включайся'
              WHEN 2 THEN 'Попытался'
              WHEN 3 THEN 'Поработал!'
              WHEN 4 THEN 'Постарался!'
              WHEN 5 THEN 'Молодец!'
              WHEN 6 THEN 'Умница!'
              ELSE NULL
            END
          ELSE
            CASE WHEN mc.value IS NULL THEN NULL ELSE mc.value::text END
        END                                               AS assessment,

        CASE WHEN COALESCE(mc.control, 0) = 1 THEN TRUE ELSE FALSE END AS is_control,

        -- форма
        CASE WHEN mc.form ~ '^[0-9]+$' THEN mc.form::bigint ELSE NULL END AS form_id,
        CASE WHEN mc.form ~ '^[0-9]+$' THEN NULL ELSE NULLIF(TRIM(mc.form), '') END AS form_name_raw,

        mc.weight                                          AS weight_raw,
        CASE
          WHEN mc.weight IS NOT NULL THEN LEAST(GREATEST(ROUND(mc.weight)::int, 0), 100)
          WHEN mc.form ~ '^[0-9]+$' AND wf.weight_pct IS NOT NULL THEN wf.weight_pct
          ELSE NULL
        END                                                AS weight_pct
        FROM src mc
        JOIN core.student st
          ON st.student_id = mc.id_student
        LEFT JOIN core.teaching_group tg
          ON tg.group_name = mc.group_name
        LEFT JOIN core.ref_academic_period ap
          ON mc.mark_date BETWEEN ap.start_date AND ap.end_date
        LEFT JOIN core.ref_work_form wf
          ON (CASE WHEN mc.form ~ '^[0-9]+$' THEN mc.form::bigint ELSE NULL END) = wf.form_id
    )  

    INSERT INTO core.mark_current
      (mark_id, student_id, group_id, period_id, period_label_raw, group_name_snapshot,
      lesson_date, created_at_src, value, assessment, assessment_scheme, is_control,
      form_id, form_name_raw, weight_raw, weight_pct)
    SELECT
      n.mark_id, n.student_id, n.group_id, n.period_id, n.period_label_raw, n.group_name_snapshot,
      n.lesson_date, n.created_at_src, n.value, n.assessment, n.assessment_scheme, n.is_control,
      n.form_id, n.form_name_raw, n.weight_raw, n.weight_pct

    FROM n
    ON CONFLICT (mark_id) DO UPDATE
      SET student_id        = EXCLUDED.student_id,
          group_id          = EXCLUDED.group_id,
          period_id         = EXCLUDED.period_id,
          period_label_raw  = EXCLUDED.period_label_raw,
          group_name_snapshot = EXCLUDED.group_name_snapshot,
          lesson_date       = EXCLUDED.lesson_date,
          created_at_src    = EXCLUDED.created_at_src,
          value             = EXCLUDED.value,
          assessment        = EXCLUDED.assessment,
          assessment_scheme = EXCLUDED.assessment_scheme,
          is_control        = EXCLUDED.is_control,
          form_id           = EXCLUDED.form_id,
          form_name_raw     = EXCLUDED.form_name_raw,
          weight_raw        = EXCLUDED.weight_raw,
          weight_pct        = EXCLUDED.weight_pct
      WHERE
          core.mark_current.student_id        IS DISTINCT FROM EXCLUDED.student_id OR
          core.mark_current.group_id          IS DISTINCT FROM EXCLUDED.group_id OR
          core.mark_current.period_id         IS DISTINCT FROM EXCLUDED.period_id OR
          core.mark_current.period_label_raw  IS DISTINCT FROM EXCLUDED.period_label_raw OR
          core.mark_current.group_name_snapshot IS DISTINCT FROM EXCLUDED.group_name_snapshot OR
          core.mark_current.lesson_date       IS DISTINCT FROM EXCLUDED.lesson_date OR
          core.mark_current.created_at_src    IS DISTINCT FROM EXCLUDED.created_at_src OR
          core.mark_current.value             IS DISTINCT FROM EXCLUDED.value OR
          core.mark_current.assessment        IS DISTINCT FROM EXCLUDED.assessment OR
          core.mark_current.assessment_scheme IS DISTINCT FROM EXCLUDED.assessment_scheme OR
          core.mark_current.is_control        IS DISTINCT FROM EXCLUDED.is_control OR
          core.mark_current.form_id           IS DISTINCT FROM EXCLUDED.form_id OR
          core.mark_current.form_name_raw     IS DISTINCT FROM EXCLUDED.form_name_raw OR
          core.mark_current.weight_raw        IS DISTINCT FROM EXCLUDED.weight_raw OR
          core.mark_current.weight_pct        IS DISTINCT FROM EXCLUDED.weight_pct;

    """
    # Полная пересборка CORE за окно: сперва удаляем, потом вставляем заново из RAW
    cur.execute(
        "DELETE FROM core.mark_current WHERE lesson_date BETWEEN %(d_from)s AND %(d_to)s",
        {"d_from": d_from, "d_to": d_to},
    )

    cur.execute(sql, {"d_from": d_from, "d_to": d_to, "ng_en": ng_en, "ng_ru": ng_ru})
    return cur.rowcount or 0


def _upsert_marks_final(cur, d_from: date, d_to: date) -> int:
    """
    Переливка RAW -> CORE для /marks/final в окне дат [d_from..d_to] по created_date.
    """
    sql = """
    WITH src AS (
      SELECT *
      FROM raw.marks_final
      WHERE created_date BETWEEN %(d_from)s AND %(d_to)s
    ),
    n AS (
      SELECT
        mf.id::bigint                                      AS final_mark_id,
        st.student_id                                      AS student_id,
        tg.group_id                                        AS group_id,
        COALESCE(mf.subject_id, rs.subject_id)            AS subject_id,

        -- период по дате создания финальной оценки
        ap.period_id                                       AS period_id,
        NULLIF(TRIM(mf.period), '')                        AS period_label_raw,

        NULLIF(TRIM(mf.group_name), '')                    AS group_name_snapshot,

        -- final не привязан к конкретному уроку — кладём дату создания (удобно для отчётов)
        mf.created_date                                    AS lesson_date,

        -- RAW.value шире; приводим к precision core (6,2)
        CASE WHEN mf.value IS NULL THEN NULL ELSE ROUND(mf.value::numeric, 2) END AS value,

        NULLIF(TRIM(mf.final_criterion), '')               AS final_criterion_raw,
        NULLIF(TRIM(mf.assesment), '')                     AS assessment_scheme,
        mf.created                                         AS created_at_src
      FROM src mf
      JOIN core.student st
        ON st.student_id = mf.id_student
      LEFT JOIN core.teaching_group tg
        ON tg.group_name = mf.group_name
      LEFT JOIN core.ref_subject rs
        ON rs.subject_title = mf.subject
      LEFT JOIN core.ref_academic_period ap
        ON mf.created_date BETWEEN ap.start_date AND ap.end_date
    )
    INSERT INTO core.mark_final
      (final_mark_id, student_id, group_id, subject_id, period_id, period_label_raw,
       group_name_snapshot, lesson_date, value, final_criterion_raw, assessment_scheme, created_at_src)
    SELECT
      n.final_mark_id, n.student_id, n.group_id, n.subject_id, n.period_id, n.period_label_raw,
      n.group_name_snapshot, n.lesson_date, n.value, n.final_criterion_raw, n.assessment_scheme, n.created_at_src
    FROM n
    ON CONFLICT (final_mark_id) DO UPDATE
      SET student_id          = EXCLUDED.student_id,
          group_id            = EXCLUDED.group_id,
          subject_id          = EXCLUDED.subject_id,
          period_id           = EXCLUDED.period_id,
          period_label_raw    = EXCLUDED.period_label_raw,
          group_name_snapshot = EXCLUDED.group_name_snapshot,
          lesson_date         = EXCLUDED.lesson_date,
          value               = EXCLUDED.value,
          final_criterion_raw = EXCLUDED.final_criterion_raw,
          assessment_scheme   = EXCLUDED.assessment_scheme,
          created_at_src      = EXCLUDED.created_at_src;
    """
    cur.execute(sql, {"d_from": d_from, "d_to": d_to})
    return cur.rowcount or 0


def run_marks(mode: str, d_from: date | None, d_to: date | None) -> None:
    # окно
    if mode == "daily":
        d_from2, d_to2 = _window_for_daily()
    else:
        # init/backfill — используем переданное окно; если его нет — весь год (с 1 сент)
        if d_from and d_to:
            d_from2, d_to2 = d_from, d_to
        else:
            # безопасный дефолт под наш учебный год: с 1 сентября текущего года до сегодня
            today = today_utc_date()
            d_from2 = date(today.year if today.month >= 9 else today.year - 1, 9, 1)
            d_to2 = today

    with get_conn() as conn:
        with conn.cursor() as cur:
            log("[core][marks] upsert mark_current ...")
            c = _upsert_marks_current(cur, d_from2, d_to2)
            log(f"[core][marks]   +current: {c}")

            log("[core][marks] upsert mark_final ...")
            f = _upsert_marks_final(cur, d_from2, d_to2)
            log(f"[core][marks]   +final: {f}")

        conn.commit()

    upsert_sync_state(
        endpoint=ENDPOINT,
        window_from=d_from2,
        window_to=d_to2,
        last_seen_updated_at=None,
        params={"mode": mode, "notes": "core marks upsert"},
        notes=f"CORE marks upsert {mode} window {d_from2}..{d_to2}",
    )
    log("[core][marks] done.")
