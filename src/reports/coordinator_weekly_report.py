from __future__ import annotations

import io
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

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

"""
Недельный отчёт координатора (weekly) — за прошедшую учебную неделю (пн–пт).

Запуск:
  python -m src.reports.coordinator_weekly_report
(в любой день недели скрипт сам возьмёт прошлую неделю в таймзоне reports.timezone)

Результат по каждой программе:
  - копия weekly-шаблона Slides -> заполнение -> экспорт PDF -> загрузка PDF в Drive
  - письмо с PDF ко всем координаторам программы (cc академдиректор)
  - запись в rep.report_run + rep.report_delivery_log
"""

REPORT_KEY = "coord_weekly_attendance"
REPORT_KEY2 = "coord_weekly_assessment"


# ─────────────────────────────────────────────────────────────────────────────
# Вспомогательные функции времени/дат
# ─────────────────────────────────────────────────────────────────────────────


def _tz() -> pytz.BaseTzInfo:
    tz_name = (CONFIG.get("reports", {}) or {}).get("timezone", settings.timezone)
    return pytz.timezone(tz_name or "Europe/Podgorica")


def compute_week_range() -> tuple[date, date, date]:
    """
    Всегда возвращает прошлую учебную неделю (пн–пт) в локальной таймзоне:
      date1 = прошлый понедельник
      date2 = прошлый пятница
      run_date = текущая локальная дата (для плейсхолдера {{date}} во 2-м отчёте)
    """
    now_local = datetime.now(_tz()).date()
    cur_mon = now_local - timedelta(days=now_local.weekday())
    date1 = cur_mon - timedelta(days=7)
    date2 = date1 + timedelta(days=4)
    return date1, date2, now_local


def _already_done(cur, report_key: str, report_date: date, programme_code: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM rep.report_run
        WHERE report_key = %s
          AND report_date = %s
          AND programme_code = %s
          AND pdf_drive_id IS NOT NULL
        LIMIT 1
        """,
        (report_key, report_date, programme_code),
    )
    return cur.fetchone() is not None


def month_partition_folder(d: date) -> str:
    """Папка-месяц формата MMYYYY (например, 102025)."""
    return d.strftime("%m%Y")


# ─────────────────────────────────────────────────────────────────────────────
# SQL-хелперы
# ─────────────────────────────────────────────────────────────────────────────

SQL_WEEKLY_SRC = """
SELECT
  week_start, week_end_mf,
  programme_code, programme_name,
  staff_id, staff_name, staff_email,
  lessons_total_week, lessons_unmarked_week, percent_unmarked
FROM rep.v_coord_weekly_attendance_by_staff
WHERE week_start = %s
"""

SQL_COORDINATORS = """
SELECT programme_code, programme_name, staff_id, full_name, email, is_primary
FROM core.v_programme_coordinators_active
"""

SQL_ACAD_DIRECTOR = """
SELECT full_name, email
FROM core.v_academic_director_active
LIMIT 1
"""

SQL_INSERT_RUN = """
INSERT INTO rep.report_run
  (report_key, report_date, programme_code, programme_name,
   pdf_drive_id, pdf_drive_path, page_count, row_count)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
RETURNING run_id
"""

SQL_INSERT_DELIVERY = """
INSERT INTO rep.report_delivery_log
  (run_id, email_from, email_to, email_cc, subject, message_id, success, details)
VALUES (%s, %s, %s, %s::text[], %s, %s, %s, %s)
"""


# ─────────────────────────────────────────────────────────────────────────────
# Бизнес-логика отчёта
# ─────────────────────────────────────────────────────────────────────────────


def load_weekly_rows(conn, week_start: date) -> List[dict]:
    """Читает строки агрегата по учителям за неделю, начиная с week_start (понедельник)."""
    with conn.cursor() as cur:
        cur.execute(SQL_WEEKLY_SRC, (week_start,))
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def load_assessment_rows_period(conn, period_start: date) -> List[dict]:
    # latest-only по (group_id, lesson_date) + берём только реально "без формы"
    sql = """
      WITH latest AS (
        SELECT group_id, lesson_date, MAX(report_date) AS latest_report_date
        FROM rep.v_coord_daily_assessment_lessons
        GROUP BY group_id, lesson_date
      )
      SELECT
        a.report_date, a.programme_code, a.programme_name,
        a.group_id, a.group_name, a.lesson_date,
        a.staff_id, a.staff_name, a.staff_email, a.has_unweighted
      FROM rep.v_coord_daily_assessment_lessons a
      JOIN latest l
        ON l.group_id = a.group_id
       AND l.lesson_date = a.lesson_date
       AND l.latest_report_date = a.report_date
      WHERE a.report_date >= %s
      ORDER BY a.programme_code, a.staff_name NULLS LAST, a.group_name, a.lesson_date;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (period_start,))
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def load_programme_coordinators(conn) -> Dict[str, List[dict]]:
    with conn.cursor() as cur:
        cur.execute(SQL_COORDINATORS)
        cols = [d.name for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    by_prog = defaultdict(list)
    for r in rows:
        by_prog[r["programme_code"]].append(r)
    for k in list(by_prog.keys()):
        by_prog[k].sort(key=lambda x: (0 if x["is_primary"] else 1, x["full_name"]))
    return by_prog


def load_academic_director_email(conn) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(SQL_ACAD_DIRECTOR)
        row = cur.fetchone()
        if not row:
            return None
        return row[1]


def aggregate_weekly_metrics(rows: List[dict]) -> tuple[int, int, int, float]:
    """
    Для weekly-строк ОДНОЙ программы (агрегат по учителям):
      allcount = SUM(lessons_total_week)
      unregcount = SUM(lessons_unmarked_week)
      regcount = all - unreg
      percent = unreg / all * 100
    """
    allcount = sum(r.get("lessons_total_week", 0) for r in rows)
    unregcount = sum(r.get("lessons_unmarked_week", 0) for r in rows)
    regcount = max(allcount - unregcount, 0)
    percent = (unregcount / allcount * 100.0) if allcount else 0.0
    return allcount, regcount, unregcount, percent


def build_weekly_teacher_rows(rows: List[dict]) -> List[tuple[str, str, str, str]]:
    """
    Детализация weekly: строки по учителям:
      (teacher_name, AX(total), BX(unmarked), CX(percent))
    """
    items: List[tuple[str, str, str, str]] = []
    for r in rows:
        teacher = r.get("staff_name") or ""
        ax = int(r.get("lessons_total_week") or 0)
        bx = int(r.get("lessons_unmarked_week") or 0)
        if bx == 0:
            continue  # скрываем учителей без проблемных уроков
        cx = float(r.get("percent_unmarked") or 0.0)
        items.append((teacher, str(ax), str(bx), f"{cx:.1f}"))

    items.sort(key=lambda x: (-float(x[3]), x[0]))
    return items


def aggregate_assessment_metrics(rows: List[dict]) -> tuple[int, int, int]:
    """
    rows — все уроки с оценками за период (для программы),
    где каждая строка = (group_id, lesson_date), has_unweighted — признак.
    Возвращаем:
      allcountmarklessons, unformcountlessons, formcountlessons
    """
    if not rows:
        return 0, 0, 0
    keys = {(r["group_id"], r["lesson_date"]) for r in rows}
    allc = len(keys)
    unform = {
        (r["group_id"], r["lesson_date"]) for r in rows if r.get("has_unweighted")
    }
    unformc = len(unform)
    formc = allc - unformc
    return allc, unformc, formc


def build_assessment_detail_rows(rows: List[dict]) -> List[Tuple[str, str, str]]:
    """
    Детализация — только уроки с has_unweighted = TRUE.
    Возвращаем список кортежей: (teacher_name, "YYYY-MM-DD", group_name)
    """
    details: List[Tuple[str, str, str]] = []
    for r in rows:
        if not r.get("has_unweighted"):
            continue
        teacher = r.get("staff_name") or ""
        lesson_date_str = (
            r["lesson_date"].strftime("%Y-%m-%d") if r.get("lesson_date") else ""
        )
        group_name = r.get("group_name") or ""
        details.append((teacher, lesson_date_str, group_name))
    details.sort(key=lambda x: (x[0], x[1], x[2]))
    return details


def choose_coordinator_line(coordinators: List[dict]) -> str:
    if not coordinators:
        return ""
    prim = [c for c in coordinators if c["is_primary"]]
    if prim:
        return prim[0]["full_name"]
    return ", ".join(c["full_name"] for c in coordinators)


def extract_first_name(full_name: str) -> str:
    if not full_name:
        return ""
    parts = full_name.strip().split()
    if len(parts) >= 2:
        return parts[1]
    return parts[0]


def build_email_html(
    first_name: str,
    date1_str: str,
    date2_str: str,
    programme: str,
    allcount: int,
    regcount: int,
    unregcount: int,
    percent_unreg: float,
    all_m: int,
    form_m: int,
    unform_m: int,
) -> str:
    optional_zero = ""
    if unregcount == 0:
        optional_zero = (
            '<p style="margin:12px 0 0 0;color:#333;">'
            "За отчётную неделю все уроки отмечены полностью."
            "</p>"
        )
    percent_str = f"{percent_unreg:.1f}"
    date_range = f"{date1_str} – {date2_str}"

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width">
  <title>Недельный отчёт</title>
</head>
<body style="margin:0;padding:0;background:#ffffff;">
  <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="background:#ffffff;">
    <tr>
      <td>
        <table role="presentation" cellpadding="0" cellspacing="0" width="600" style="width:600px;max-width:100%;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;color:#111;line-height:1.6;">
          <tr>
            <td style="padding:24px 24px 8px 24px;">
              <p style="margin:0 0 12px 0;font-size:16px;">
                Уважаемая(ый), <strong>{first_name}</strong>
              </p>
              <p style="margin:0;color:#555;">
                Это недельный отчёт по регистрации посещаемости и оцениванию.
              </p>
            </td>
          </tr>

          <tr>
            <td style="padding:8px 24px;">
              <hr style="border:0;border-top:1px solid #eaeaea;margin:0;">
            </td>
          </tr>

          <tr>
            <td style="padding:16px 24px 8px 24px;">
              <p style="margin:0 0 4px 0;font-size:14px;color:#555;">Отчёт за</p>
              <p style="margin:0 0 12px 0;font-size:16px;"><strong>{date_range}</strong></p>

              <p style="margin:0 0 4px 0;font-size:14px;color:#555;">Программа</p>
              <p style="margin:0 0 0 0;font-size:16px;"><strong>{programme}</strong></p>
            </td>
          </tr>

          <!-- Блок 1: посещаемость -->
          <tr>
            <td style="padding:16px 24px 8px 24px;">
              <p style="margin:0 0 8px 0;font-size:16px;"><strong>Итоги за {date_range}:</strong></p>
              <ul style="margin:0;padding:0 0 0 18px;">
                <li style="margin:0 0 4px 0;">Всего уроков: <strong>{allcount}</strong></li>
                <li style="margin:0 0 4px 0;">Количество отмеченных уроков: <strong>{regcount}</strong></li>
                <li style="margin:0 0 0 0;">Количество не отмеченных уроков: <strong>{unregcount}</strong> (<strong>{percent_str}%</strong>)</li>
              </ul>
              {optional_zero}
            </td>
          </tr>

          <tr>
            <td style="padding:16px 24px 24px 24px;">
              <div style="border-left:3px solid #eaeaea;padding:12px 16px;background:#fafafa;">
                <p style="margin:0 0 6px 0;color:#333;">
                  Подробности — во вложении (PDF). В таблице по каждому учителю указано:
                  <em>всего уроков</em>, <em>не отмечено</em>, <em>%</em>.
                </p>
              </div>
            </td>
          </tr>

          <!-- Блок 2: оценки без выбора форм (весь учебный период) -->
          <tr>
            <td style="padding:16px 24px 8px 24px;">
              <p style="margin:0 0 8px 0;font-size:16px;"><strong>Оценки, выставленные учителями за весь учебный период:</strong></p>
              <ul style="margin:0;padding:0 0 0 18px;">
                <li style="margin:0 0 4px 0;">Общее количество уроков с оцениванием: <strong>{all_m}</strong></li>
                <li style="margin:0 0 4px 0;">Количество уроков с выбором форм работ: <strong>{form_m}</strong></li>
                <li style="margin:0 0 0 0;">Количество уроков без выбора форм работ: <strong>{unform_m}</strong></li>
              </ul>
            </td>
          </tr>

          <tr>
            <td style="padding:16px 24px 24px 24px;">
              <div style="border-left:3px solid #eaeaea;padding:12px 16px;background:#fafafa;">
                <p style="margin:0 0 6px 0;color:#333;">
                  При наличии уроков без выбранной формы работ смотри вложение (PDF) — указаны преподаватель, дата урока и группа.
                </p>
              </div>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


def chunk(lst: List, size: int) -> List[List]:
    return [lst[i : i + size] for i in range(0, len(lst), size)]


# ─────────────────────────────────────────────────────────────────────────────
# Отрисовка в Slides и PDF + сохранение в Drive
# ─────────────────────────────────────────────────────────────────────────────


def upload_pdf_to_drive(drive, parent_id: str, filename: str, pdf_bytes: bytes) -> str:
    media = MediaIoBaseUpload(
        io.BytesIO(pdf_bytes), mimetype="application/pdf", resumable=False
    )
    meta = {"name": filename, "parents": [parent_id], "mimeType": "application/pdf"}
    created = with_retries(
        lambda: drive.files().create(body=meta, media_body=media, fields="id").execute()
    )
    return created["id"]


def make_per_slide_mappings(
    header: Dict[str, str], rows: List[Tuple[str, str, str]], per_slide_max: int
) -> List[Dict[str, Optional[str]]]:
    mappings = []
    for pack in chunk(rows, per_slide_max) or [[]]:
        m = dict(header)
        for idx in range(1, per_slide_max + 1):
            if idx <= len(pack):
                teacher, b, c = pack[idx - 1]
                m[f"teacher_{idx}"] = teacher
                m[f"B{idx}"] = b
                m[f"C{idx}"] = c
            else:
                m[f"teacher_{idx}"] = None
                m[f"B{idx}"] = None
                m[f"C{idx}"] = None
        mappings.append(m)
    return mappings


def make_per_slide_mappings_weekly_att(
    header: Dict[str, str],
    rows: List[tuple[str, str, str, str]],
    per_slide_max: int,
) -> List[Dict[str, Optional[str]]]:
    mappings = []
    for pack in chunk(rows, per_slide_max) or [[]]:
        m = dict(header)
        for idx in range(1, per_slide_max + 1):
            if idx <= len(pack):
                teacher, a, b, c = pack[idx - 1]
                m[f"teacher_{idx}"] = teacher
                m[f"A{idx}"] = a
                m[f"B{idx}"] = b
                m[f"C{idx}"] = c
            else:
                m[f"teacher_{idx}"] = None
                m[f"A{idx}"] = None
                m[f"B{idx}"] = None
                m[f"C{idx}"] = None
        mappings.append(m)
    return mappings


# ─────────────────────────────────────────────────────────────────────────────
# Главный сценарий
# ─────────────────────────────────────────────────────────────────────────────


def main():
    with advisory_lock(1004):
        date1, date2, run_date = compute_week_range()
        # отображение (для письма и плейсхолдеров в PDF-шапке)
        date1_disp = date1.strftime("%Y/%m/%d")
        date2_disp = date2.strftime("%Y/%m/%d")
        # безопасно для имени файла и путей
        date1_file = date1.strftime("%Y-%m-%d")
        date2_file = date2.strftime("%Y-%m-%d")
        run_date_str = run_date.strftime("%Y-%m-%d")

        reports_cfg = CONFIG.get("reports", {}) or {}
        sender = (reports_cfg.get("email", {}) or {}).get("sender")

        wa_cfg = reports_cfg.get("coordinator_weekly_attendance", {}) or {}
        wa_template_id = wa_cfg.get("template_id")
        wa_per_slide_max = int(wa_cfg.get("per_slide_max_rows", 30))
        wa_parent_folder_id = wa_cfg.get("parent_folder_id")
        wa_filename_pattern = wa_cfg.get(
            "filename_pattern",
            "{date}_{programme}_coordinator_weekly_attendance_report.pdf",
        )

        ws_cfg = reports_cfg.get("coordinator_weekly_assessment", {}) or {}
        ws_template_id = ws_cfg.get("template_id")
        ws_per_slide_max = int(ws_cfg.get("per_slide_max_rows", 30))
        ws_filename_pattern = ws_cfg.get(
            "filename_pattern",
            "{date}_{programme}_coordinator_weekly_assessment_report.pdf",
        )

        ws_parent_folder_id = ws_cfg.get("parent_folder_id")
        if not ws_parent_folder_id:
            raise RuntimeError(
                "Missing parent_folder_id in reports.coordinator_weekly_assessment"
            )

        period_start_str = (
            reports_cfg.get("weekly_assessment_period_start") or ""
        ).strip()
        if not period_start_str:
            raise RuntimeError(
                "Missing reports.weekly_assessment_period_start in config.yaml"
            )
        period_start = datetime.strptime(period_start_str, "%Y-%m-%d").date()

        if not (sender and wa_template_id and wa_parent_folder_id):
            raise RuntimeError(
                "Missing required config in config.yaml -> reports.coordinator_weekly_attendance"
            )

        drive, slides, gmail = build_services()

        with get_conn() as conn:
            acad_cc = []
            acad_email = load_academic_director_email(conn)
            if acad_email:
                acad_cc = [acad_email]

            weekly_rows = load_weekly_rows(conn, date1)
            ass_rows = load_assessment_rows_period(conn, period_start)

            ass_by_programme: Dict[str, List[dict]] = defaultdict(list)
            for r in ass_rows:
                ass_by_programme[r["programme_code"]].append(r)

            rows_by_programme: Dict[str, List[dict]] = defaultdict(list)
            for r in weekly_rows:
                rows_by_programme[r["programme_code"]].append(r)

            coords_by_programme = load_programme_coordinators(conn)
            programme_codes = sorted(coords_by_programme.keys())

            month_folder = month_partition_folder(date2)

            for pcode in programme_codes:
                coordinators = coords_by_programme.get(pcode, [])
                if not coordinators:
                    continue
                pname = coordinators[0]["programme_name"]

                with conn.cursor() as cur:
                    if _already_done(cur, REPORT_KEY, date2, pcode):
                        print(
                            f"[report] skip weekly: already exists for {date1_file}-{date2_file} programme={pname}"
                        )
                        continue

                prog_rows = rows_by_programme.get(pcode, [])

                allc, regc, unregc, percent = aggregate_weekly_metrics(prog_rows)
                detail_week = build_weekly_teacher_rows(prog_rows)

                header = {
                    "date1": date1_disp,
                    "date2": date2_disp,
                    "programme": pname,
                    "coordinator": choose_coordinator_line(coordinators),
                    "allcountlessons": str(allc),
                    "regcountlessons": str(regc),
                    "unregcountlessons": str(unregc),
                    "percentunreglessons": f"{percent:.1f}",
                }

                per_slide_maps = make_per_slide_mappings_weekly_att(
                    header, detail_week, wa_per_slide_max
                )

                prog_folder_id = ensure_subfolder(drive, wa_parent_folder_id, pname)
                month_folder_id = ensure_subfolder(drive, prog_folder_id, month_folder)

                title = f"tmp_{REPORT_KEY}_{pcode}_{date1_file}_{date2_file}"
                pres_id, _pages = prepare_presentation_from_template(
                    wa_template_id, title, month_folder_id
                )

                try:

                    # выбираем базовый слайд безопасно: если в шаблоне >= 2 слайда — клоним второй (1), иначе первый (0)
                    slide_count_att = len(_pages) if _pages else 0
                    base_slide_index_att = 1 if slide_count_att >= 2 else 0

                    pdf_bytes_1 = render_and_export_pdf(
                        pres_id, per_slide_maps, base_slide_index=base_slide_index_att
                    )

                    filename_1 = wa_filename_pattern.format(
                        date=date2_file,
                        programme=pname.replace("/", "-"),
                    )
                    pdf_file_id_1 = upload_pdf_to_drive(
                        drive, month_folder_id, filename_1, pdf_bytes_1
                    )

                    with conn.cursor() as cur:
                        cur.execute(
                            SQL_INSERT_RUN,
                            (
                                REPORT_KEY,
                                date2,
                                pcode,
                                pname,
                                pdf_file_id_1,
                                f"mojo_reports/coordinator_weekly_report/{pname}/{month_folder}/{filename_1}",
                                len(per_slide_maps),
                                len(detail_week),
                            ),
                        )

                        run_id_att = cur.fetchone()[0]
                    conn.commit()

                    ass_prog_rows = ass_by_programme.get(pcode, [])
                    all_m, unform_m, form_m = aggregate_assessment_metrics(
                        ass_prog_rows
                    )
                    detail2 = build_assessment_detail_rows(ass_prog_rows)

                    pres2_id = None
                    pdf_bytes_2 = None
                    filename_2 = None
                    pdf_file_id_2 = None

                    # антидубль для второго отчёта
                    with conn.cursor() as cur:
                        if _already_done(cur, REPORT_KEY2, date2, pcode):
                            print(
                                f"[report] skip weekly assessment: already exists for {date1_file}-{date2_file} programme={pname}"
                            )
                            # при дубле просто не формируем второй PDF, метрики в письмо всё равно попадут
                            unform_m = 0  # чтобы ниже не заходить в генерацию PDF #2

                    if unform_m > 0 and ws_template_id:
                        # подпапки для второго отчёта — свой корень weekly_assessment
                        prog_folder_id2 = ensure_subfolder(
                            drive, ws_parent_folder_id, pname
                        )
                        month_folder_id2 = ensure_subfolder(
                            drive, prog_folder_id2, month_folder
                        )

                        title2 = f"tmp_{REPORT_KEY2}_{pcode}_{date2_file}"
                        pres2_id, _pages2 = prepare_presentation_from_template(
                            ws_template_id, title2, month_folder_id2
                        )

                        header2 = {
                            "date": run_date_str,
                            "programme": pname,
                            "coordinator": choose_coordinator_line(coordinators),
                            "allcountmarklessons": str(all_m),
                            "unformcountlessons": str(unform_m),
                            "formcountlessons": str(form_m),
                        }
                        per_slide_maps2 = make_per_slide_mappings(
                            header2, detail2, ws_per_slide_max
                        )

                        slide_count_ass = len(_pages2) if _pages2 else 0
                        base_slide_index_ass = 1 if slide_count_ass >= 2 else 0
                        pdf_bytes_2 = render_and_export_pdf(
                            pres2_id,
                            per_slide_maps2,
                            base_slide_index=base_slide_index_ass,
                        )

                        filename_2 = ws_filename_pattern.format(
                            date=date2_file,
                            programme=pname.replace("/", "-"),
                        )
                        pdf_file_id_2 = upload_pdf_to_drive(
                            drive, month_folder_id2, filename_2, pdf_bytes_2
                        )

                        with conn.cursor() as cur:
                            cur.execute(
                                SQL_INSERT_RUN,
                                (
                                    REPORT_KEY2,
                                    date2,
                                    pcode,
                                    pname,
                                    pdf_file_id_2,
                                    f"mojo_reports/coordinator_weekly_report/{pname}/{month_folder}/{filename_2}",
                                    len(per_slide_maps2),
                                    len(detail2),
                                ),
                            )

                        conn.commit()

                    to_addrs = [c["email"] for c in coordinators if c.get("email")]
                    to_addrs_str = ", ".join(to_addrs)
                    subject = f"Weekly coordinator report {pname}"

                    if not to_addrs:
                        # логируем «нет получателей» и не пытаемся отправлять
                        with conn.cursor() as cur:
                            cur.execute(
                                SQL_INSERT_DELIVERY,
                                (
                                    run_id_att,  # есть, т.к. мы уже создали run для attendance
                                    sender,
                                    to_addrs_str,  # пустая строка
                                    acad_cc or [],
                                    subject,
                                    "",  # message_id
                                    False,  # success
                                    "No recipients found for programme coordinators",
                                ),
                            )
                        conn.commit()
                        # attachments не рассылаем, переходим к следующей программе
                        continue

                    greet_full_name = next(
                        (c["full_name"] for c in coordinators if c.get("is_primary")),
                        coordinators[0]["full_name"],
                    )
                    first_name = extract_first_name(greet_full_name)

                    html_body_final = build_email_html(
                        first_name=first_name,
                        date1_str=date1_disp,
                        date2_str=date2_disp,
                        programme=pname,
                        allcount=allc,
                        regcount=regc,
                        unregcount=unregc,
                        percent_unreg=percent,
                        all_m=all_m,
                        form_m=form_m,
                        unform_m=unform_m,
                    )

                    attachments = [(pdf_bytes_1, filename_1)]
                    if pdf_bytes_2 and filename_2:
                        attachments.append((pdf_bytes_2, filename_2))

                    message_id = ""
                    error_text = None
                    try:
                        message_id = (
                            send_email_with_attachments(
                                gmail=gmail,
                                sender=sender,
                                to=to_addrs,
                                cc=acad_cc,
                                subject=subject,
                                html_body=html_body_final,
                                attachments=attachments,
                            )
                            or ""
                        )
                        ok = True
                    except Exception as e:
                        ok = False
                        error_text = str(e)

                    with conn.cursor() as cur:
                        cur.execute(
                            SQL_INSERT_DELIVERY,
                            (
                                run_id_att,
                                sender,
                                to_addrs_str,
                                acad_cc or [],
                                subject,
                                message_id,
                                ok,
                                error_text,
                            ),
                        )
                    conn.commit()

                finally:
                    try:
                        delete_file(drive, pres_id)
                    except Exception:
                        pass
                    try:
                        if pres2_id:
                            delete_file(drive, pres2_id)
                    except Exception:
                        pass


if __name__ == "__main__":
    main()
