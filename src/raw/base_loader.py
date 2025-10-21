# src/raw/base_loader.py
from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import psycopg2.extras

from ..db import get_conn


def insert_attendance_rows(rows: List[Dict[str, Any]]) -> int:
    """
    Вставка пачки строк в raw.attendance.
    Ожидается, что каждая строка уже содержит все целевые колонки.
    ON CONFLICT(id) DO NOTHING — дубль мы тихо пропускаем.
    """
    if not rows:
        return 0

    cols = [
        "id",
        "student_id",
        "lesson_id",
        "student",
        "grade",
        "attendance_date",
        "status",
        "period_name",
        "subject_name",
        "src_day",
        "source_system",
        "endpoint",
        "raw_json",
        "ingested_at",
        "source_hash",
        "batch_id",
    ]

    values = []
    for r in rows:
        row_vals = []
        for c in cols:
            v = r.get(c)
            if c == "raw_json":
                # корректная передача jsonb
                v = psycopg2.extras.Json(
                    v, dumps=lambda x: json.dumps(x, ensure_ascii=False)
                )
            row_vals.append(v)
        values.append(tuple(row_vals))

    sql = f"""
    INSERT INTO raw.attendance ({", ".join(cols)})
    VALUES %s
    ON CONFLICT (id, attendance_date) DO UPDATE
    SET student_id     = EXCLUDED.student_id,
        lesson_id      = EXCLUDED.lesson_id,
        student        = EXCLUDED.student,
        grade          = EXCLUDED.grade,
        status         = EXCLUDED.status,
        period_name    = EXCLUDED.period_name,
        subject_name   = EXCLUDED.subject_name,
        src_day        = EXCLUDED.src_day,
        source_system  = EXCLUDED.source_system,
        endpoint       = EXCLUDED.endpoint,
        raw_json       = EXCLUDED.raw_json,
        ingested_at    = EXCLUDED.ingested_at,
        source_hash    = EXCLUDED.source_hash,
        batch_id       = EXCLUDED.batch_id
    WHERE raw.attendance.source_hash <> EXCLUDED.source_hash
    """

    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
        inserted = cur.rowcount
        conn.commit()
    return inserted


def upsert_sync_state(
    endpoint: str,
    window_from: Optional[date],
    window_to: Optional[date],
    last_seen_updated_at: Optional[datetime],
    params: Optional[dict] = None,
    notes: Optional[str] = None,
) -> None:
    """
    Обновляет служебную таблицу core.sync_state по эндпоинту.
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO core.sync_state (
              endpoint, last_successful_sync_at, last_seen_updated_at,
              window_from, window_to, next_cursor, params, notes
            )
            VALUES (%s, now(), %s, %s, %s, NULL, %s, %s)
            ON CONFLICT (endpoint) DO UPDATE
               SET last_successful_sync_at = EXCLUDED.last_successful_sync_at,
                   last_seen_updated_at    = EXCLUDED.last_seen_updated_at,
                   window_from             = EXCLUDED.window_from,
                   window_to               = EXCLUDED.window_to,
                   params                  = EXCLUDED.params,
                   notes                   = EXCLUDED.notes;
            """,
            (
                endpoint,
                last_seen_updated_at,
                window_from,
                window_to,
                psycopg2.extras.Json(params or {}),
                notes,
            ),
        )
        conn.commit()


def insert_marks_current_rows(rows):
    if not rows:
        return 0

    cols = [
        "id",
        "period",
        "mark_date",
        "subject",
        "group_name",
        "id_student",
        "value",
        "created",
        "assesment",
        "control",
        "flex",
        "weight",
        "form",
        "grade",
        "student",
        "src_day",
        "source_system",
        "endpoint",
        "raw_json",
        "ingested_at",
        "source_hash",
        "batch_id",
    ]

    values = []
    for r in rows:
        row_vals = []
        for c in cols:
            v = r.get(c)
            if c == "raw_json":
                v = psycopg2.extras.Json(
                    v, dumps=lambda x: json.dumps(x, ensure_ascii=False)
                )
            row_vals.append(v)
        values.append(tuple(row_vals))

    sql = f"""
        INSERT INTO raw.marks_current ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (id, mark_date) DO UPDATE
        SET period        = EXCLUDED.period,
            subject       = EXCLUDED.subject,
            group_name    = EXCLUDED.group_name,
            id_student    = EXCLUDED.id_student,
            value         = EXCLUDED.value,
            created       = EXCLUDED.created,
            assesment     = EXCLUDED.assesment,
            control       = EXCLUDED.control,
            flex          = EXCLUDED.flex,
            weight        = EXCLUDED.weight,
            form          = EXCLUDED.form,
            grade         = EXCLUDED.grade,
            student       = EXCLUDED.student,
            src_day       = EXCLUDED.src_day,
            source_system = EXCLUDED.source_system,
            endpoint      = EXCLUDED.endpoint,
            raw_json      = EXCLUDED.raw_json,
            ingested_at   = EXCLUDED.ingested_at,
            source_hash   = EXCLUDED.source_hash,
            batch_id      = EXCLUDED.batch_id
        WHERE raw.marks_current.source_hash <> EXCLUDED.source_hash
        """

    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
        inserted = cur.rowcount
        conn.commit()
    return inserted


# вверху файла должны быть:
# import json
# import psycopg2.extras


def insert_marks_final_rows(rows):
    if not rows:
        return 0

    cols = [
        "id",
        "period",
        "created_date",
        "subject",
        "subject_id",
        "group_name",
        "id_student",
        "value",
        "final_criterion",
        "assesment",
        "created",
        "grade",
        "student",
        "src_day",
        "source_system",
        "endpoint",
        "raw_json",
        "ingested_at",
        "source_hash",
        "batch_id",
    ]

    values = []
    for r in rows:
        row_vals = []
        for c in cols:
            v = r.get(c)
            if c == "raw_json":
                v = psycopg2.extras.Json(
                    v, dumps=lambda x: json.dumps(x, ensure_ascii=False)
                )
            row_vals.append(v)
        values.append(tuple(row_vals))

    sql = f"""
        INSERT INTO raw.marks_final ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (id, created_date) DO UPDATE
        SET period          = EXCLUDED.period,
            subject         = EXCLUDED.subject,
            subject_id      = EXCLUDED.subject_id,
            group_name      = EXCLUDED.group_name,
            id_student      = EXCLUDED.id_student,
            value           = EXCLUDED.value,
            final_criterion = EXCLUDED.final_criterion,
            assesment       = EXCLUDED.assesment,
            created         = EXCLUDED.created,
            grade           = EXCLUDED.grade,
            student         = EXCLUDED.student,
            src_day         = EXCLUDED.src_day,
            source_system   = EXCLUDED.source_system,
            endpoint        = EXCLUDED.endpoint,
            raw_json        = EXCLUDED.raw_json,
            ingested_at     = EXCLUDED.ingested_at,
            source_hash     = EXCLUDED.source_hash,
            batch_id        = EXCLUDED.batch_id
        WHERE raw.marks_final.source_hash <> EXCLUDED.source_hash

    """

    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
        inserted = cur.rowcount
        conn.commit()
    return inserted


# вверху: import json; import psycopg2.extras
def insert_schedule_lessons_rows(rows):
    if not rows:
        return 0

    cols = [
        "schedule_id",
        "schedule_start",
        "schedule_finish",
        "group_id",
        "building_id",
        "group_name",
        "subject_name",
        "room",
        "is_replacement",
        "replaced_schedule_id",
        "lesson_id",
        "lesson_date",
        "day_number",
        "lesson_start",
        "lesson_finish",
        "staff_json",
        "src_day",
        "source_system",
        "endpoint",
        "raw_json",
        "ingested_at",
        "source_hash",
        "batch_id",
    ]

    values = []
    for r in rows:
        row_vals = []
        for c in cols:
            v = r.get(c)
            if c in ("raw_json", "staff_json"):
                v = psycopg2.extras.Json(
                    v, dumps=lambda x: json.dumps(x, ensure_ascii=False)
                )
            row_vals.append(v)
        values.append(tuple(row_vals))

    sql = f"""
        INSERT INTO raw.schedule_lessons ({", ".join(cols)})
        VALUES %s
        -- Обновляем строку, если в источнике её поменяли (по хэшу)
        ON CONFLICT (lesson_id, lesson_date) DO UPDATE
        SET schedule_id          = EXCLUDED.schedule_id,
            schedule_start       = EXCLUDED.schedule_start,
            schedule_finish      = EXCLUDED.schedule_finish,
            group_id             = EXCLUDED.group_id,
            building_id          = EXCLUDED.building_id,
            group_name           = EXCLUDED.group_name,
            subject_name         = EXCLUDED.subject_name,
            room                 = EXCLUDED.room,
            is_replacement       = EXCLUDED.is_replacement,
            replaced_schedule_id = EXCLUDED.replaced_schedule_id,
            day_number           = EXCLUDED.day_number,
            lesson_start         = EXCLUDED.lesson_start,
            lesson_finish        = EXCLUDED.lesson_finish,
            staff_json           = EXCLUDED.staff_json,
            src_day              = EXCLUDED.src_day,
            source_system        = EXCLUDED.source_system,
            endpoint             = EXCLUDED.endpoint,
            raw_json             = EXCLUDED.raw_json,
            ingested_at          = EXCLUDED.ingested_at,
            source_hash          = EXCLUDED.source_hash,
            batch_id             = EXCLUDED.batch_id
        WHERE raw.schedule_lessons.source_hash <> EXCLUDED.source_hash
    """

    from ..db import get_conn

    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
        inserted = cur.rowcount
        conn.commit()
    return inserted


def insert_subjects_rows(rows):
    """
    rows: список словарей с готовыми полями под таблицу raw.subjects.
    Поведение: upsert по id. Обновляем поля ТОЛЬКО если изменился source_hash.
    Всегда обновляем last_seen_src_day и src_day текущим днём.
    """
    if not rows:
        return 0

    cols = [
        "id",
        "title",
        "in_curriculum",
        "in_olymp",
        "department",
        "closed",
        "first_seen_src_day",
        "last_seen_src_day",
        "src_day",
        "source_system",
        "endpoint",
        "raw_json",
        "ingested_at",
        "source_hash",
        "batch_id",
    ]

    values = []
    for r in rows:
        row_vals = []
        for c in cols:
            v = r.get(c)
            if c == "raw_json":
                v = psycopg2.extras.Json(
                    v, dumps=lambda x: json.dumps(x, ensure_ascii=False)
                )
            row_vals.append(v)
        values.append(tuple(row_vals))

    sql = f"""
        INSERT INTO raw.subjects ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (id) DO UPDATE
        SET title          = CASE WHEN raw.subjects.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.title ELSE raw.subjects.title END,
            in_curriculum  = CASE WHEN raw.subjects.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.in_curriculum ELSE raw.subjects.in_curriculum END,
            in_olymp       = CASE WHEN raw.subjects.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.in_olymp ELSE raw.subjects.in_olymp END,
            department     = CASE WHEN raw.subjects.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.department ELSE raw.subjects.department END,
            closed         = CASE WHEN raw.subjects.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.closed ELSE raw.subjects.closed END,
            -- метки видимости и служебное
            last_seen_src_day = EXCLUDED.last_seen_src_day,
            src_day           = EXCLUDED.src_day,
            raw_json          = CASE WHEN raw.subjects.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.raw_json ELSE raw.subjects.raw_json END,
            ingested_at       = EXCLUDED.ingested_at,
            source_hash       = EXCLUDED.source_hash,
            batch_id          = EXCLUDED.batch_id,
            first_seen_src_day = LEAST(raw.subjects.first_seen_src_day, EXCLUDED.first_seen_src_day)
    """

    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
        inserted = cur.rowcount
        conn.commit()
    return inserted


def insert_work_forms_rows(rows):
    """
    Upsert по id_form. Поля меняем только при изменении source_hash.
    Всегда обновляем last_seen_src_day и src_day текущим днём.
    """
    if not rows:
        return 0

    cols = [
        "id_form",
        "form_name",
        "form_description",
        "form_area",
        "form_control",
        "form_weight",
        "form_percent",
        "form_created",
        "form_archived",
        "form_deleted",
        "first_seen_src_day",
        "last_seen_src_day",
        "src_day",
        "source_system",
        "endpoint",
        "raw_json",
        "ingested_at",
        "source_hash",
        "batch_id",
    ]

    values = []
    for r in rows:
        row_vals = []
        for c in cols:
            v = r.get(c)
            if c == "raw_json":
                v = psycopg2.extras.Json(
                    v, dumps=lambda x: json.dumps(x, ensure_ascii=False)
                )
            row_vals.append(v)
        values.append(tuple(row_vals))

    sql = f"""
        INSERT INTO raw.work_forms ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (id_form) DO UPDATE
        SET form_name        = CASE WHEN raw.work_forms.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.form_name        ELSE raw.work_forms.form_name END,
            form_description = CASE WHEN raw.work_forms.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.form_description ELSE raw.work_forms.form_description END,
            form_area        = CASE WHEN raw.work_forms.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.form_area        ELSE raw.work_forms.form_area END,
            form_control     = CASE WHEN raw.work_forms.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.form_control     ELSE raw.work_forms.form_control END,
            form_weight      = CASE WHEN raw.work_forms.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.form_weight      ELSE raw.work_forms.form_weight END,
            form_percent     = CASE WHEN raw.work_forms.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.form_percent     ELSE raw.work_forms.form_percent END,
            form_created     = CASE WHEN raw.work_forms.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.form_created     ELSE raw.work_forms.form_created END,
            form_archived    = CASE WHEN raw.work_forms.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.form_archived    ELSE raw.work_forms.form_archived END,
            form_deleted     = CASE WHEN raw.work_forms.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.form_deleted     ELSE raw.work_forms.form_deleted END,
            last_seen_src_day = EXCLUDED.last_seen_src_day,
            src_day           = EXCLUDED.src_day,
            raw_json          = CASE WHEN raw.work_forms.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.raw_json ELSE raw.work_forms.raw_json END,
            ingested_at       = EXCLUDED.ingested_at,
            source_hash       = EXCLUDED.source_hash,
            batch_id          = EXCLUDED.batch_id,
            first_seen_src_day = LEAST(raw.work_forms.first_seen_src_day, EXCLUDED.first_seen_src_day)
    """

    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
        inserted = cur.rowcount
        conn.commit()
    return inserted


import json
from datetime import date

# --- students_ref ------------------------------------------------------------
import psycopg2.extras

from ..db import get_conn


def insert_students_rows(rows):
    """
    Upsert по student_id. Поля меняем, только если изменился source_hash.
    Всегда обновляем last_seen_src_day и src_day текущим днём.
    """
    if not rows:
        return 0

    cols = [
        "student_id",
        "first_name",
        "last_name",
        "gender",
        "dob",
        "email",
        "cohort",
        "class_name",
        "program",
        "parents_raw",
        "first_seen_src_day",
        "last_seen_src_day",
        "src_day",
        "source_system",
        "endpoint",
        "raw_json",
        "ingested_at",
        "source_hash",
        "batch_id",
    ]

    values = []
    for r in rows:
        row_vals = []
        for c in cols:
            v = r.get(c)
            if c == "raw_json":
                v = psycopg2.extras.Json(
                    v, dumps=lambda x: json.dumps(x, ensure_ascii=False)
                )
            row_vals.append(v)
        values.append(tuple(row_vals))

    sql = f"""
        INSERT INTO raw.students_ref ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (student_id) DO UPDATE
        SET first_name        = CASE WHEN raw.students_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.first_name  ELSE raw.students_ref.first_name END,
            last_name         = CASE WHEN raw.students_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.last_name   ELSE raw.students_ref.last_name END,
            gender            = CASE WHEN raw.students_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.gender      ELSE raw.students_ref.gender END,
            dob               = CASE WHEN raw.students_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.dob         ELSE raw.students_ref.dob END,
            email             = CASE WHEN raw.students_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.email       ELSE raw.students_ref.email END,
            cohort            = CASE WHEN raw.students_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.cohort      ELSE raw.students_ref.cohort END,
            class_name        = CASE WHEN raw.students_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.class_name  ELSE raw.students_ref.class_name END,
            program           = CASE WHEN raw.students_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.program     ELSE raw.students_ref.program END,
            parents_raw       = CASE WHEN raw.students_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.parents_raw ELSE raw.students_ref.parents_raw END,

            last_seen_src_day = EXCLUDED.last_seen_src_day,
            src_day           = EXCLUDED.src_day,
            raw_json          = CASE WHEN raw.students_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.raw_json ELSE raw.students_ref.raw_json END,
            ingested_at       = EXCLUDED.ingested_at,
            source_hash       = EXCLUDED.source_hash,
            batch_id          = EXCLUDED.batch_id,
            first_seen_src_day = LEAST(raw.students_ref.first_seen_src_day, EXCLUDED.first_seen_src_day)

    """

    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
        inserted = cur.rowcount
        conn.commit()
    return inserted


# --- parents_ref -------------------------------------------------------------
import json

import psycopg2.extras

from ..db import get_conn


def insert_parents_rows(rows):
    """
    Upsert по parent_email (в нижнем регистре).
    Поля меняем, только если изменился source_hash.
    Всегда обновляем last_seen_src_day и src_day.
    """
    if not rows:
        return 0

    cols = [
        "parent_email",
        "parent_id",
        "parent_name",
        "first_seen_src_day",
        "last_seen_src_day",
        "src_day",
        "source_system",
        "endpoint",
        "raw_json",
        "ingested_at",
        "source_hash",
        "batch_id",
    ]

    values = []
    for r in rows:
        row_vals = []
        for c in cols:
            v = r.get(c)
            if c == "raw_json":
                v = psycopg2.extras.Json(
                    v, dumps=lambda x: json.dumps(x, ensure_ascii=False)
                )
            row_vals.append(v)
        values.append(tuple(row_vals))

    sql = f"""
        INSERT INTO raw.parents_ref ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (parent_email) DO UPDATE
        SET parent_id         = CASE WHEN raw.parents_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.parent_id   ELSE raw.parents_ref.parent_id END,
            parent_name       = CASE WHEN raw.parents_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.parent_name ELSE raw.parents_ref.parent_name END,
            last_seen_src_day = EXCLUDED.last_seen_src_day,
            src_day           = EXCLUDED.src_day,
            raw_json          = CASE WHEN raw.parents_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.raw_json ELSE raw.parents_ref.raw_json END,
            ingested_at       = EXCLUDED.ingested_at,
            source_hash       = EXCLUDED.source_hash,
            batch_id          = EXCLUDED.batch_id,
            first_seen_src_day = LEAST(raw.parents_ref.first_seen_src_day, EXCLUDED.first_seen_src_day);
    """

    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
        inserted = cur.rowcount
        conn.commit()
    return inserted


# --- student_parent_links ----------------------------------------------------
def insert_parent_links_rows(rows):
    """
    Upsert связей по (parent_email, student_name, grade).
    Обновляем student_id, если он стал известен; parent_id подставляем, если ранее был NULL.
    """
    if not rows:
        return 0

    cols = [
        "parent_email",
        "student_name",
        "grade",
        "student_id",
        "parent_id",
        "first_seen_src_day",
        "last_seen_src_day",
        "src_day",
        "source_system",
        "endpoint",
        "raw_json",
        "ingested_at",
        "source_hash",
        "batch_id",
    ]

    values = []
    for r in rows:
        row_vals = []
        for c in cols:
            v = r.get(c)
            if c == "raw_json":
                v = psycopg2.extras.Json(
                    v, dumps=lambda x: json.dumps(x, ensure_ascii=False)
                )
            row_vals.append(v)
        values.append(tuple(row_vals))

    sql = f"""
        INSERT INTO raw.student_parent_links ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (parent_email, student_name, grade) DO UPDATE
        SET student_id        = COALESCE(EXCLUDED.student_id, raw.student_parent_links.student_id),
            parent_id         = COALESCE(raw.student_parent_links.parent_id, EXCLUDED.parent_id),
            last_seen_src_day = EXCLUDED.last_seen_src_day,
            src_day           = EXCLUDED.src_day,
            raw_json          = CASE WHEN raw.student_parent_links.source_hash <> EXCLUDED.source_hash
                                     THEN EXCLUDED.raw_json ELSE raw.student_parent_links.raw_json END,
            ingested_at       = EXCLUDED.ingested_at,
            source_hash       = EXCLUDED.source_hash,
            batch_id          = EXCLUDED.batch_id,
            first_seen_src_day = LEAST(raw.student_parent_links.first_seen_src_day, EXCLUDED.first_seen_src_day);
    """

    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
        inserted = cur.rowcount
        conn.commit()
    return inserted


# --- staff_ref ----------------------------------------------------------------
import json

import psycopg2.extras

from ..db import get_conn


def insert_staff_rows(rows):
    if not rows:
        return 0

    cols = [
        "staff_email",
        "staff_id",
        "staff_name",
        "gender",
        "first_seen_src_day",
        "last_seen_src_day",
        "src_day",
        "source_system",
        "endpoint",
        "raw_json",
        "ingested_at",
        "source_hash",
        "batch_id",
    ]

    values = []
    for r in rows:
        row_vals = []
        for c in cols:
            v = r.get(c)
            if c == "raw_json":
                v = psycopg2.extras.Json(
                    v, dumps=lambda x: json.dumps(x, ensure_ascii=False)
                )
            row_vals.append(v)
        values.append(tuple(row_vals))

    sql = f"""
        INSERT INTO raw.staff_ref ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (staff_email) DO UPDATE
        SET staff_id          = CASE WHEN raw.staff_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.staff_id   ELSE raw.staff_ref.staff_id END,
            staff_name        = CASE WHEN raw.staff_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.staff_name ELSE raw.staff_ref.staff_name END,
            gender            = CASE WHEN raw.staff_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.gender     ELSE raw.staff_ref.gender END,
            last_seen_src_day = EXCLUDED.last_seen_src_day,
            src_day           = EXCLUDED.src_day,
            raw_json          = CASE WHEN raw.staff_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.raw_json ELSE raw.staff_ref.raw_json END,
            ingested_at       = EXCLUDED.ingested_at,
            source_hash       = EXCLUDED.source_hash,
            batch_id          = EXCLUDED.batch_id,
            first_seen_src_day = LEAST(raw.staff_ref.first_seen_src_day, EXCLUDED.first_seen_src_day);
    """

    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
        cnt = cur.rowcount
        conn.commit()
    return cnt


# --- staff_positions ----------------------------------------------------------
def insert_staff_positions_rows(rows):
    """
    Upsert по (staff_email, department_key, position_key).
    На конфликте: подтягиваем не заполненные ранее поля и обновляем служебные метки.
    """
    if not rows:
        return 0

    cols = [
        "staff_email",
        "department",
        "position",
        "department_key",
        "position_key",
        "first_seen_src_day",
        "last_seen_src_day",
        "src_day",
        "source_system",
        "endpoint",
        "raw_json",
        "ingested_at",
        "source_hash",
        "batch_id",
    ]

    values = []
    for r in rows:
        row_vals = []
        for c in cols:
            v = r.get(c)
            if c == "raw_json":
                v = psycopg2.extras.Json(
                    v, dumps=lambda x: json.dumps(x, ensure_ascii=False)
                )
            row_vals.append(v)
        values.append(tuple(row_vals))

    sql = f"""
        INSERT INTO raw.staff_positions ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (staff_email, department_key, position_key) DO UPDATE
        SET department        = COALESCE(raw.staff_positions.department, EXCLUDED.department),
            position          = COALESCE(raw.staff_positions.position,   EXCLUDED.position),
            last_seen_src_day = EXCLUDED.last_seen_src_day,
            src_day           = EXCLUDED.src_day,
            raw_json          = CASE WHEN raw.staff_positions.source_hash <> EXCLUDED.source_hash
                                     THEN EXCLUDED.raw_json ELSE raw.staff_positions.raw_json END,
            ingested_at       = EXCLUDED.ingested_at,
            source_hash       = EXCLUDED.source_hash,
            batch_id          = EXCLUDED.batch_id,
            first_seen_src_day = LEAST(raw.staff_positions.first_seen_src_day, EXCLUDED.first_seen_src_day);
    """

    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
        cnt = cur.rowcount
        conn.commit()
    return cnt


# --- classes_ref -------------------------------------------------------------
import json

import psycopg2.extras

from ..db import get_conn


def insert_classes_rows(rows):
    if not rows:
        return 0

    cols = [
        "title",
        "cohort",
        "homeroom_short",
        "students_count",
        "homeroom_email",
        "homeroom_staff_id",
        "match_status",
        "match_method",
        "first_seen_src_day",
        "last_seen_src_day",
        "src_day",
        "source_system",
        "endpoint",
        "raw_json",
        "ingested_at",
        "source_hash",
        "batch_id",
    ]

    values = []
    for r in rows:
        row_vals = []
        for c in cols:
            v = r.get(c)
            if c == "raw_json":
                v = psycopg2.extras.Json(
                    v, dumps=lambda x: json.dumps(x, ensure_ascii=False)
                )
            row_vals.append(v)
        values.append(tuple(row_vals))

    sql = f"""
        INSERT INTO raw.classes_ref ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (title) DO UPDATE
        SET cohort            = CASE WHEN raw.classes_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.cohort          ELSE raw.classes_ref.cohort END,
            homeroom_short    = CASE WHEN raw.classes_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.homeroom_short  ELSE raw.classes_ref.homeroom_short END,
            students_count    = CASE WHEN raw.classes_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.students_count  ELSE raw.classes_ref.students_count END,
            homeroom_email    = EXCLUDED.homeroom_email,
            homeroom_staff_id = EXCLUDED.homeroom_staff_id,
            match_status      = EXCLUDED.match_status,
            match_method      = EXCLUDED.match_method,
            last_seen_src_day = EXCLUDED.last_seen_src_day,
            src_day           = EXCLUDED.src_day,
            raw_json          = CASE WHEN raw.classes_ref.source_hash <> EXCLUDED.source_hash THEN EXCLUDED.raw_json ELSE raw.classes_ref.raw_json END,
            ingested_at       = EXCLUDED.ingested_at,
            source_hash       = EXCLUDED.source_hash,
            batch_id          = EXCLUDED.batch_id,
            first_seen_src_day = LEAST(raw.classes_ref.first_seen_src_day, EXCLUDED.first_seen_src_day);
    """

    with get_conn() as conn, conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)
        inserted = cur.rowcount
        conn.commit()
    return inserted
