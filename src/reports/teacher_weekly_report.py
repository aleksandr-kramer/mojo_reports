# src/reports/teacher_weekly_report.py
from __future__ import annotations

"""
Еженедельный отчёт для УЧИТЕЛЕЙ:
- Блок 1: уроки с неполной регистрацией посещаемости за прошедшую неделю (пн–пт), PDF.
- Блок 2: уроки с оценками БЕЗ выбора формы работ за ВЕСЬ учебный период от reports.weekly_assessment_period_start, PDF.
- Письмо на e-mail КАЖДОМУ учителю, у кого есть записи в любом блоке. CC не используется.
- Если оба блока пусты — письмо и файлы для данного учителя НЕ формируются.

Папки Google Drive:
  mojo_reports/teacher_weekly_report/{Teacher Name}/{MMYYYY}/{date2}_{teacher}_teacher_weekly_{attendance|assessment}.pdf

Шаблоны Slides:
  Блок 1: id="1qeysx5BEqRg-ZRpcMbkdZd55UEN7WZGu" (teacher_weekly_attendance_report.pptx)
    Плейсхолдеры шапки:
      {{date1}}, {{date2}}, {{fullname}}, {{allcount}}, {{unregcount}}, {{regcount}}
    Плейсхолдеры строк (X=1..26 на слайд):
      {{predmet_X}} = group_name
      {{AX}}        = programme_name
      {{BX}}        = дата урока
      {{CX}}        = время урока (HH:MM-HH:MM)
  Блок 2: id="1t0SyVB8_9N0y-TWzcoqKOM2gqn2osCRb" (teacher_weekly_assessment_report.pptx)
    Только строки (X=1..30 на слайд):
      {{teacher_X}} = staff_name
      {{BX}}        = lesson_date
      {{CX}}        = group_name
"""

import io
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Sequence, Tuple

import pytz
from googleapiclient.http import MediaIoBaseUpload

from ..db import advisory_lock, get_conn
from ..google.clients import build_services
from ..google.gmail_sender import send_email_with_attachments
from ..google.retry import with_retries
from ..google.slides_export import (
    delete_file,
    ensure_subfolder,
    prepare_presentation_from_template,
    render_and_export_pdf,
)
from ..settings import CONFIG, settings

# ─────────────────────────────────────────────────────────────────────────────
# Константы
# ─────────────────────────────────────────────────────────────────────────────

ADVISORY_LOCK_KEY = 1008
REPORT_KEY_ATT = "teacher_weekly_attendance"
REPORT_KEY_ASM = "teacher_weekly_assessment"


# ─────────────────────────────────────────────────────────────────────────────
# Таймзона и неделя
# ─────────────────────────────────────────────────────────────────────────────


def _tz() -> pytz.BaseTzInfo:
    tz_name = (CONFIG.get("reports", {}) or {}).get("timezone", settings.timezone)
    return pytz.timezone(tz_name or "Europe/Podgorica")


def compute_week_range() -> tuple[date, date, date]:
    """
    Возвращает прошлую учебную неделю (пн–пт) в локальной TZ:
      date1 = прошлый понедельник
      date2 = прошлый пятница
      run_date = текущая локальная дата (для шаблонов, если требуется)
    """
    now_local = datetime.now(_tz()).date()
    cur_mon = now_local - timedelta(days=now_local.weekday())
    date1 = cur_mon - timedelta(days=7)
    date2 = date1 + timedelta(days=4)
    return date1, date2, now_local


def month_partition_folder(d: date) -> str:
    """MMYYYY: для подкаталога месяца."""
    return d.strftime("%m%Y")


def fmt_hhmm_span(start: Optional[datetime], finish: Optional[datetime]) -> str:
    if not start or not finish:
        return ""
    return f"{start.strftime('%H:%M')}-{finish.strftime('%H:%M')}"


# ─────────────────────────────────────────────────────────────────────────────
# SQL
# ─────────────────────────────────────────────────────────────────────────────

SQL_TEACHERS_WEEKLY_CANDIDATES = """
SELECT DISTINCT staff_id, staff_name, staff_email
FROM (
  SELECT staff_id, staff_name, staff_email
  FROM rep.v_coord_daily_attendance_src
  WHERE report_date BETWEEN %s AND %s AND staff_id IS NOT NULL

  UNION

  SELECT staff_id, staff_name, staff_email
  FROM rep.v_teacher_unweighted_marks
  WHERE report_date >= %s AND staff_id IS NOT NULL
) t
ORDER BY staff_name;
"""

SQL_WEEKLY_ATT_SUMMARY = """
SELECT lessons_total_week, lessons_bad_week
FROM rep.v_teacher_weekly_attendance_summary
WHERE week_start = %s AND staff_id = %s
"""

SQL_WEEKLY_ATT_DETAIL = """
SELECT report_date, group_name, programme_name, lesson_start, lesson_finish
FROM rep.v_teacher_weekly_attendance_detail
WHERE week_start = %s AND staff_id = %s
ORDER BY report_date, lesson_start, group_name
"""

SQL_UNWEIGHTED_BY_TEACHER_PERIOD = """
SELECT lesson_date, staff_name, group_name
FROM rep.v_teacher_unweighted_marks
WHERE staff_id = %s AND report_date >= %s
ORDER BY lesson_date, group_name
"""

SQL_INSERT_RUN = """
INSERT INTO rep.report_run
  (report_key, report_date, programme_code, programme_name,
   pdf_drive_id, pdf_drive_path, page_count, row_count)
VALUES (%s, %s, NULL, %s, %s, %s, %s, %s)
RETURNING run_id
"""

SQL_INSERT_DELIVERY = """
INSERT INTO rep.report_delivery_log
  (run_id, email_from, email_to, email_cc, subject, message_id, success, details)
VALUES (%s, %s, %s, %s::text[], %s, %s, %s, %s)
"""


# ─────────────────────────────────────────────────────────────────────────────
# Модели и загрузка данных
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class Teacher:
    staff_id: int
    staff_name: str
    staff_email: Optional[str]


def load_teachers(conn, date1: date, date2: date, period_start: date) -> List[Teacher]:
    with conn.cursor() as cur:
        cur.execute(SQL_TEACHERS_WEEKLY_CANDIDATES, (date1, date2, period_start))
        rows = cur.fetchall()
    return [
        Teacher(staff_id=r[0], staff_name=r[1] or "", staff_email=r[2]) for r in rows
    ]


def load_attendance_summary(conn, week_start: date, staff_id: int) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(SQL_WEEKLY_ATT_SUMMARY, (week_start, staff_id))
        row = cur.fetchone()
    if not row:
        return 0, 0
    return int(row[0] or 0), int(row[1] or 0)


def load_attendance_detail(
    conn, week_start: date, staff_id: int
) -> List[Tuple[date, str, str, Optional[datetime], Optional[datetime]]]:
    with conn.cursor() as cur:
        cur.execute(SQL_WEEKLY_ATT_DETAIL, (week_start, staff_id))
        rows = cur.fetchall()
    return (
        rows  # (report_date, group_name, programme_name, lesson_start, lesson_finish)
    )


def load_unweighted_detail(
    conn, staff_id: int, period_start: date
) -> List[Tuple[date, str]]:
    with conn.cursor() as cur:
        cur.execute(SQL_UNWEIGHTED_BY_TEACHER_PERIOD, (staff_id, period_start))
        rows = cur.fetchall()
    out: List[Tuple[date, str]] = []
    for lesson_date, _staff_name, group_name in rows:
        out.append((lesson_date, group_name or ""))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Построение e-mail (на основе daily; замена дат на диапазон)
# Основа и верстка — как в teacher_daily_report.py, но: заголовки = диапазон дат, текст = weekly. :contentReference[oaicite:4]{index=4}
# ─────────────────────────────────────────────────────────────────────────────


def extract_first_name(full_name: str) -> str:
    if not full_name:
        return ""
    parts = full_name.strip().split()
    if len(parts) >= 2:
        return parts[1]
    return parts[0]


def build_email_html_weekly(
    teacher_name: str,
    date1_str_slash: str,  # DD/MM/YYYY
    date2_str_slash: str,  # DD/MM/YYYY
    rows_bad: List[Tuple[str, str]],  # [(date_time_str, group_name)]
    rows_unweighted: List[Tuple[str, str]],  # [(lesson_date_str, group_name)]
) -> str:
    first_name = extract_first_name(teacher_name)
    date_range = f"{date1_str_slash} - {date2_str_slash}"

    # Блок 1
    if rows_bad:
        block1_items = "".join(
            f'<li style="margin:0 0 6px 0;">{dts} — {g}</li>' for dts, g in rows_bad
        )
        block1_list_html = f"""
        <ul style="margin:0 0 16px 18px;padding:0;">
          {block1_items}
        </ul>
        """
        block1_note = ""
    else:
        block1_list_html = ""
        block1_note = (
            '<p style="margin:0 0 4px 0;color:#555;">Attendance has been recorded correctly.</p>'
            '<p style="margin:0 0 16px 0;color:#555;">Посещаемость на уроках отмечена корректно.</p>'
        )

    block1_html = f"""
      <p style="margin:0 0 8px 0;"><strong>List of lessons on {date_range} with incomplete marking of present and absent students.</strong></p>
      {block1_list_html}
      {block1_note}
      <div style="background:#f5f5f5;border:1px solid #eee;border-radius:6px;padding:12px 14px;margin:8px 0 16px 0;color:#444;font-size:13px;line-height:1.5;">
        <p style="margin:0 0 6px 0;">All students present and absent in the class must be marked.</p>
        <p style="margin:0 0 6px 0;">If lessons are double, attendance must be recorded for each lesson separately.</p>
        <p style="margin:6px 0;">&nbsp;</p>
        <p style="margin:0 0 6px 0;">Отмечать необходимо всех присутствующих и отсутствующих учеников в классе.</p>
        <p style="margin:0;">Если уроки сдвоенные, регистрацию присутствия/отсутствия нужно проводить на каждом уроке.</p>
      </div>
    """

    # Блок 2
    if rows_unweighted:
        block2_items = "".join(
            f'<li style="margin:0 0 6px 0;">{d} — {g}</li>' for d, g in rows_unweighted
        )
        block2_list_html = f"""
        <ul style="margin:0 0 16px 18px;padding:0;">
          {block2_items}
        </ul>
        """
        block2_note = ""
    else:
        block2_list_html = ""
        block2_note = (
            '<p style="margin:0 0 16px 0;color:#555;">You have no lessons with marks entered without selecting an assessment type</p>'
            '<p style="margin:0 0 16px 0;color:#555;">У Вас нет уроков с выставленными оценками без выбора формы работ.</p>'
        )

    policy_url_ru = "https://adriaticcollege.com/ru/policies/assessment-policy"
    policy_url_en = "https://adriaticcollege.com/en/policies/assessment-policy"

    block2_html = f"""
      <p style="margin:8px 0 8px 0;"><strong>List of lessons in which marks have been entered without selecting an assessment type for the entire academic period</strong></p>
      {block2_list_html}
      {block2_note}
      <div style="background:#f5f5f5;border:1px solid #eee;border-radius:6px;padding:12px 14px;margin:8px 0 16px 0;color:#444;font-size:13px;line-height:1.5;">
        <p style="margin:0 0 6px 0;">According to the <a href="{policy_url_en}" target="_blank" rel="noopener noreferrer">school’s Assessment Policy</a>, marks may be awarded only for specific types of work (selected from the preset list) that include an assessment type, criterion, and weight.</p>
        <p style="margin:0 0 6px 0;">Marks entered without selecting an assessment type will distort the final marks seen by students and parents.</p>
        <p style="margin:0 0 10px 0;">If your list contains lessons with marks entered without selecting an assessment type, please make the necessary corrections in the electronic gradebook.</p>
        <p style="margin:6px 0;">&nbsp;</p>
        <p style="margin:0 0 6px 0;">Согласно <a href="{policy_url_ru}" target="_blank" rel="noopener noreferrer">школьной политики оценивания</a>, оценки могут выставляться только за конкретные виды работ (выбираются из готового списка), которые включают форму работы, критерий и вес.</p>
        <p style="margin:0 0 6px 0;">Оценки без выбора формы работы будут искажать итоговые оценки, которые видят ученики и родители.</p>
        <p style="margin:0;">Если в Вашем списке есть уроки с оценками без выбора формы работы, пожалуйста, в электронном журнале внесите корректировки.</p>
      </div>
    """

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8"><meta name="viewport" content="width=device-width">
  <title>Weekly teacher report</title>
</head>
<body style="margin:0;padding:0;background:#ffffff;">
  <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="background:#ffffff;">
    <tr><td>
      <table role="presentation" cellpadding="0" cellspacing="0" width="800" style="width:800px;max-width:100%;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;color:#111;line-height:1.55;">
        <tr>
          <td style="padding:20px 24px 6px 24px;">
            <p style="margin:0 0 10px 0;font-size:16px;">Dear <strong>{first_name}</strong>,</p>
            <p style="margin:0;color:#555;">This email is your weekly report on your entries in the school’s electronic gradebook.</p>
            <p style="margin:0;color:#555;">Данное письмо является еженедельным отчётом по заполнению Вами электронных журналов школы.</p>
          </td>
        </tr>

        <tr><td style="padding:8px 24px;"><hr style="border:0;border-top:1px solid #eaeaea;margin:0;"></td></tr>

        <tr>
          <td style="padding:12px 24px 6px 24px;">
            <p style="margin:0 0 0 0;font-size:16px;"><strong>Report for {date_range}</strong></p>
          </td>
        </tr>

        <tr><td style="padding:8px 24px 0 24px;">{block1_html}</td></tr>
        <tr><td style="padding:0 24px 12px 24px;">{block2_html}</td></tr>

        <tr>
          <td style="padding:4px 24px 24px 24px;color:#777;font-size:12px;">
            <p style="margin:0;">If you have any questions, please contact your Programme Coordinator.</p>
          </td>
        </tr>
      </table>
    </td></tr>
  </table>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# Slides helpers
# ─────────────────────────────────────────────────────────────────────────────


def _upload_pdf_to_drive(drive, parent_id: str, filename: str, pdf_bytes: bytes) -> str:
    media = MediaIoBaseUpload(
        io.BytesIO(pdf_bytes), mimetype="application/pdf", resumable=False
    )
    meta = {"name": filename, "parents": [parent_id], "mimeType": "application/pdf"}
    created = with_retries(
        lambda: drive.files().create(body=meta, media_body=media, fields="id").execute()
    )
    return created["id"]


def _chunk(lst: Sequence, size: int) -> List[List]:
    return [list(lst[i : i + size]) for i in range(0, len(lst), size)] or [[]]


def make_maps_attendance(
    header: Dict[str, Optional[str]],
    rows: List[Tuple[str, str, str, str]],
    per_slide_max: int,
) -> List[Dict[str, Optional[str]]]:
    """
    rows: [(predmet, AX_programme, BX_date, CX_time)]
    """
    out: List[Dict[str, Optional[str]]] = []
    for pack in _chunk(rows, per_slide_max):
        m = dict(header)
        for idx in range(1, per_slide_max + 1):
            if idx <= len(pack):
                predmet, ax, bx, cx = pack[idx - 1]
                m[f"predmet_{idx}"] = predmet
                m[f"A{idx}"] = ax
                m[f"B{idx}"] = bx
                m[f"C{idx}"] = cx
            else:
                m[f"predmet_{idx}"] = None
                m[f"A{idx}"] = None
                m[f"B{idx}"] = None
                m[f"C{idx}"] = None
        out.append(m)
    return out


def make_maps_assessment(
    rows: List[Tuple[str, str, str]],
    per_slide_max: int,
) -> List[Dict[str, Optional[str]]]:
    """
    rows: [(teacher, BX_date, CX_group)]
    (Шапки нет — по требованию шаблона второго блока)
    """
    out: List[Dict[str, Optional[str]]] = []
    for pack in _chunk(rows, per_slide_max):
        m: Dict[str, Optional[str]] = {}
        for idx in range(1, per_slide_max + 1):
            if idx <= len(pack):
                teacher, bx, cx = pack[idx - 1]
                m[f"teacher_{idx}"] = teacher
                m[f"B{idx}"] = bx
                m[f"C{idx}"] = cx
            else:
                m[f"teacher_{idx}"] = None
                m[f"B{idx}"] = None
                m[f"C{idx}"] = None
        out.append(m)
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Главный сценарий
# ─────────────────────────────────────────────────────────────────────────────


def main():
    with advisory_lock(ADVISORY_LOCK_KEY):
        date1, date2, _run_date = compute_week_range()
        # Для отображения в письме:
        date1_slash = date1.strftime("%d/%m/%Y")
        date2_slash = date2.strftime("%d/%m/%Y")
        # Для имени файла и путей:
        date2_file = date2.strftime("%Y-%m-%d")
        month_folder = month_partition_folder(date2)

        reports_cfg = CONFIG.get("reports", {}) or {}
        sender = (reports_cfg.get("email", {}) or {}).get("sender")
        if not sender:
            raise RuntimeError("Missing reports.email.sender in config.yaml")

        # Конфиг Блок 1 (посещаемость)
        att_cfg = reports_cfg.get("teacher_weekly_attendance", {}) or {}
        att_template_id = att_cfg.get("template_id")
        att_parent_folder_id = att_cfg.get("parent_folder_id")
        att_per_slide_max = int(att_cfg.get("per_slide_max_rows", 26))
        att_filename_pattern = att_cfg.get(
            "filename_pattern", "{date2}_{teacher}_teacher_weekly_attendance.pdf"
        )
        if not (att_template_id and att_parent_folder_id):
            raise RuntimeError(
                "Missing reports.teacher_weekly_attendance.* in config.yaml"
            )

        # Конфиг Блок 2 (оценки без форм)
        asm_cfg = reports_cfg.get("teacher_weekly_assessment", {}) or {}
        asm_template_id = asm_cfg.get("template_id")
        asm_parent_folder_id = asm_cfg.get("parent_folder_id") or att_parent_folder_id
        asm_per_slide_max = int(asm_cfg.get("per_slide_max_rows", 30))
        asm_filename_pattern = asm_cfg.get(
            "filename_pattern", "{date2}_{teacher}_teacher_weekly_assessment.pdf"
        )
        if not (asm_template_id and asm_parent_folder_id):
            raise RuntimeError(
                "Missing reports.teacher_weekly_assessment.* in config.yaml"
            )

        # Период для Блока 2
        period_start_str = (
            reports_cfg.get("weekly_assessment_period_start") or ""
        ).strip()
        if not period_start_str:
            raise RuntimeError(
                "Missing reports.weekly_assessment_period_start in config.yaml"
            )
        period_start = datetime.strptime(period_start_str, "%Y-%m-%d").date()

        # Лимиты отправки Gmail
        rl = ((CONFIG.get("google", {}) or {}).get("rate_limits", {}) or {}).get(
            "gmail", {}
        ) or {}
        min_gap = int(rl.get("min_seconds_between_sends", 0))

        drive, slides, gmail = build_services()

        subject = f"{date2_file} Mojo weekly teacher report"

        with get_conn() as conn:
            teachers = load_teachers(conn, date1, date2, period_start)

            for t in teachers:
                # Пропуск без e-mail
                if not t.staff_email:
                    # Лог (без run_id)
                    with conn.cursor() as cur:
                        cur.execute(
                            SQL_INSERT_DELIVERY,
                            (
                                None,
                                sender,
                                "",
                                [],
                                subject,
                                "",
                                False,
                                f"No email for teacher staff_id={t.staff_id}",
                            ),
                        )
                    conn.commit()
                    continue

                # Свод и детализация по посещаемости за неделю (пн–пт)
                allcount, badcount = load_attendance_summary(conn, date1, t.staff_id)
                att_rows_db = load_attendance_detail(conn, date1, t.staff_id)
                # Приводим к строковым полям шаблона и письма
                att_rows: List[Tuple[str, str, str, str]] = []  # predmet, AX, BX, CX
                email_rows_bad: List[Tuple[str, str]] = (
                    []
                )  # (date_time_str, group_name)
                for (
                    rep_date,
                    group_name,
                    programme_name,
                    l_start,
                    l_finish,
                ) in att_rows_db:
                    predmet = group_name or ""
                    ax = programme_name or ""
                    bx = rep_date.strftime("%Y-%m-%d")
                    cx = fmt_hhmm_span(l_start, l_finish)
                    att_rows.append((predmet, ax, bx, cx))
                    email_rows_bad.append(
                        (f"{rep_date.strftime('%d/%m')} {cx}".strip(), predmet)
                    )

                regcount = max(allcount - badcount, 0)

                # Детализация по оценкам без форм, за весь учебный период
                uw_rows_db = load_unweighted_detail(conn, t.staff_id, period_start)
                asm_rows: List[Tuple[str, str, str]] = []  # teacher, BX, CX
                email_rows_unw: List[Tuple[str, str]] = (
                    []
                )  # (lesson_date_str, group_name)
                for lesson_date, group_name in uw_rows_db:
                    dstr = lesson_date.strftime("%Y-%m-%d") if lesson_date else ""
                    asm_rows.append((t.staff_name, dstr, group_name))
                    email_rows_unw.append((dstr, group_name))

                # Ничего не отправляем, если оба блока пусты
                if badcount == 0 and not asm_rows:
                    continue

                # Папки Drive: {parent}/Teacher Name/MMYYYY
                teacher_folder_id = ensure_subfolder(
                    drive, att_parent_folder_id, t.staff_name
                )
                month_folder_id = ensure_subfolder(
                    drive, teacher_folder_id, month_folder
                )

                attachments: List[Tuple[bytes, str]] = []
                run_id_to_log: Optional[int] = None

                # Блок 1: PDF посещаемости (только при наличии проблемных уроков)
                if badcount > 0 and att_template_id:
                    header = {
                        "date1": date1_slash,
                        "date2": date2_slash,
                        "fullname": t.staff_name,
                        "allcount": str(allcount),
                        "unregcount": str(badcount),
                        "regcount": str(regcount),
                    }
                    maps_att = make_maps_attendance(header, att_rows, att_per_slide_max)
                    # Базовый слайд (если в шаблоне >= 2, используем второй)
                    pres_title = f"tmp_{REPORT_KEY_ATT}_{t.staff_id}_{date2_file}"
                    pres_id, pages = prepare_presentation_from_template(
                        att_template_id, pres_title, month_folder_id
                    )
                    try:
                        base_idx = 1 if (pages and len(pages) >= 2) else 0
                        pdf_bytes_att = render_and_export_pdf(
                            pres_id, maps_att, base_slide_index=base_idx
                        )
                        filename_att = att_filename_pattern.format(
                            date2=date2_file, teacher=t.staff_name.replace("/", "-")
                        )
                        file_id_att = _upload_pdf_to_drive(
                            drive, month_folder_id, filename_att, pdf_bytes_att
                        )
                        attachments.append((pdf_bytes_att, filename_att))
                        # Лог генерации PDF в report_run (programme_name используем для teacher name)
                        with conn.cursor() as cur:
                            cur.execute(
                                SQL_INSERT_RUN,
                                (
                                    REPORT_KEY_ATT,
                                    date2,
                                    t.staff_name,
                                    file_id_att,
                                    f"mojo_reports/teacher_weekly_report/{t.staff_name}/{month_folder}/{filename_att}",
                                    len(maps_att),
                                    len(att_rows),
                                ),
                            )
                            run_id_to_log = cur.fetchone()[0]
                        conn.commit()
                    finally:
                        try:
                            delete_file(drive, pres_id)
                        except Exception:
                            pass

                # Блок 2: PDF по оценкам без форм (если есть такие записи)
                if asm_rows and asm_template_id:
                    maps_asm = make_maps_assessment(asm_rows, asm_per_slide_max)
                    pres_title2 = f"tmp_{REPORT_KEY_ASM}_{t.staff_id}_{date2_file}"
                    pres2_id, pages2 = prepare_presentation_from_template(
                        asm_template_id, pres_title2, month_folder_id
                    )
                    try:
                        base_idx2 = 1 if (pages2 and len(pages2) >= 2) else 0
                        pdf_bytes_asm = render_and_export_pdf(
                            pres2_id, maps_asm, base_slide_index=base_idx2
                        )
                        filename_asm = asm_filename_pattern.format(
                            date2=date2_file, teacher=t.staff_name.replace("/", "-")
                        )
                        file_id_asm = _upload_pdf_to_drive(
                            drive, month_folder_id, filename_asm, pdf_bytes_asm
                        )
                        attachments.append((pdf_bytes_asm, filename_asm))
                        # Лог генерации PDF
                        with conn.cursor() as cur:
                            cur.execute(
                                SQL_INSERT_RUN,
                                (
                                    REPORT_KEY_ASM,
                                    date2,
                                    t.staff_name,
                                    file_id_asm,
                                    f"mojo_reports/teacher_weekly_report/{t.staff_name}/{month_folder}/{filename_asm}",
                                    len(maps_asm),
                                    len(asm_rows),
                                ),
                            )
                            run_id_asm = cur.fetchone()[0]
                            if run_id_to_log is None:
                                run_id_to_log = run_id_asm
                        conn.commit()
                    finally:
                        try:
                            delete_file(drive, pres2_id)
                        except Exception:
                            pass

                # Сборка письма (HTML, как в daily; заголовки → диапазон)
                html_body = build_email_html_weekly(
                    teacher_name=t.staff_name,
                    date1_str_slash=date1_slash,
                    date2_str_slash=date2_slash,
                    rows_bad=email_rows_bad,
                    rows_unweighted=email_rows_unw,
                )

                # Отправка
                message_id = ""
                ok = False
                err = None
                try:
                    message_id = (
                        send_email_with_attachments(
                            gmail=gmail,
                            sender=sender,
                            to=[t.staff_email],
                            cc=None,
                            subject=subject,
                            html_body=html_body,
                            attachments=attachments,
                        )
                        or ""
                    )
                    ok = True
                except Exception as e:
                    ok = False
                    err = str(e)

                with conn.cursor() as cur:
                    cur.execute(
                        SQL_INSERT_DELIVERY,
                        (
                            run_id_to_log,  # может быть None, если не было PDF по блоку 1
                            sender,
                            t.staff_email,
                            [],  # CC пустой
                            subject,
                            message_id,
                            ok,
                            err,
                        ),
                    )
                conn.commit()

                if min_gap > 0:
                    time.sleep(min_gap)


if __name__ == "__main__":
    main()
