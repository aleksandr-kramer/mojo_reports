"""
Ежедневный отчёт координатора по посещаемости за прошедший учебный день.

Запуск:
  python -m src.reports.coordinator_daily_attendance_report --date 2025-10-21
или без флага --date (возьмёт "вчерашний учебный день" в таймзоне Europe/Podgorica).

Результат по каждой программе:
  - копия шаблона Slides -> заполнение -> экспорт PDF -> загрузка PDF в Drive
  - письмо с PDF ко всем координаторам программы (cc академдиректор)
  - запись в rep.report_run + rep.report_delivery_log
"""

from __future__ import annotations

import argparse
import io
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pytz
from googleapiclient.http import MediaIoBaseUpload

from ..db import advisory_lock, get_conn
from ..google.clients import build_services
from ..google.gmail_sender import (
    send_email_with_attachment,
    send_email_with_attachments,
)
from ..google.retry import with_retries
from ..google.slides_export import (
    delete_file,
    ensure_subfolder,
    prepare_presentation_from_template,
    render_and_export_pdf,
)
from ..settings import CONFIG, settings

REPORT_KEY = "coord_daily_attendance"
REPORT_KEY2 = "coord_daily_assessment"


# ─────────────────────────────────────────────────────────────────────────────
# Вспомогательные функции времени/дат
# ─────────────────────────────────────────────────────────────────────────────


def _tz() -> pytz.BaseTzInfo:
    tz_name = (CONFIG.get("reports", {}) or {}).get("timezone", settings.timezone)
    return pytz.timezone(tz_name or "Europe/Podgorica")


def compute_report_date(explicit: Optional[str] = None) -> date:
    """
    Если передана дата (YYYY-MM-DD) — используем её.
    Без даты: разрешено только вт–сб → берём «вчера».
    В вс/пн без --date выходим с ошибкой (отчёт в эти дни не запускается).
    """
    if explicit:
        return datetime.strptime(explicit, "%Y-%m-%d").date()

    now_local = datetime.now(_tz())
    weekday = now_local.weekday()  # 0=Mon .. 6=Sun
    if weekday in (1, 2, 3, 4, 5):  # Tue..Sat -> yesterday
        return (now_local - timedelta(days=1)).date()

    # Sun/Mon without explicit date -> hard stop
    raise SystemExit("Report is disabled on Sunday/Monday. Use --date=YYYY-MM-DD.")


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

SQL_SRC_BY_DATE = """
SELECT
  report_date, programme_code, programme_name,
  lesson_id, group_name, lesson_start, lesson_finish,
  staff_id, staff_name, staff_email,
  cnt_unmarked, students_expected, events_total
FROM rep.v_coord_daily_attendance_src
WHERE report_date = %s
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


def load_source_rows(conn, report_date: date) -> List[dict]:
    """Читает строки из вью-источника на нужную дату, используя переданное соединение."""
    with conn.cursor() as cur:
        cur.execute(SQL_SRC_BY_DATE, (report_date,))
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def load_assessment_rows(conn, report_date: date) -> List[dict]:
    sql = """
      SELECT report_date, programme_code, programme_name,
             group_id, group_name, lesson_date,
             staff_id, staff_name, staff_email, has_unweighted
      FROM rep.v_coord_daily_assessment_lessons
      WHERE report_date = %s
      ORDER BY programme_code, staff_name NULLS LAST, group_name, lesson_date;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (report_date,))
        cols = [d.name for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def load_programme_coordinators(conn) -> Dict[str, List[dict]]:
    """Возвращает словарь programme_code -> список координаторов (dict) через переданное соединение."""
    with conn.cursor() as cur:
        cur.execute(SQL_COORDINATORS)
        cols = [d.name for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]

    by_prog = defaultdict(list)
    for r in rows:
        by_prog[r["programme_code"]].append(r)
    # сортируем: primary сначала, затем по имени
    for k in list(by_prog.keys()):
        by_prog[k].sort(key=lambda x: (0 if x["is_primary"] else 1, x["full_name"]))
    return by_prog


def load_academic_director_email(conn) -> Optional[str]:
    with conn.cursor() as cur:
        cur.execute(SQL_ACAD_DIRECTOR)
        row = cur.fetchone()
        if not row:
            return None
        return row[1]  # email


def aggregate_metrics(rows: List[dict]) -> Tuple[int, int, int, float]:
    """
    Возвращает (allcount, regcount, unregcount, percent_unreg) для набора строк ОДНОЙ программы.
    Логика «проблемного» урока:
      cnt_unmarked > 0 OR events_total < students_expected
    """
    lesson_ids = set()
    unreg_ids = set()
    for r in rows:
        lesson_ids.add(r["lesson_id"])
        if r["cnt_unmarked"] > 0 or r["events_total"] < r["students_expected"]:
            unreg_ids.add(r["lesson_id"])

    allcount = len(lesson_ids)
    unregcount = len(unreg_ids)
    regcount = max(allcount - unregcount, 0)
    percent = (unregcount / allcount * 100.0) if allcount else 0.0
    return allcount, regcount, unregcount, percent


def build_detail_rows(rows: List[dict]) -> List[Tuple[str, str, str]]:
    """
    Детализация: только проблемные уроки.
    Формат строки -> (teacher_name, "HH:MM-HH:MM", group_name)
    Сортировка: преподаватель → время начала.
    """
    details = []
    for r in rows:
        if r["cnt_unmarked"] > 0 or r["events_total"] < r["students_expected"]:
            time_span = f'{r["lesson_start"].strftime("%H:%M")}-{r["lesson_finish"].strftime("%H:%M")}'
            details.append((r["staff_name"], time_span, r["group_name"]))
    details.sort(key=lambda x: (x[0], x[1]))
    return details


def aggregate_assessment_metrics(rows: List[dict]) -> tuple[int, int, int]:
    """
    rows — все уроки с оценками за report_date (для программы),
    где каждая строка = (group_id, lesson_date), has_unweighted — признак.
    Возвращаем:
      allcountmarklessons, unformcountlessons, formcountlessons
    """
    if not rows:
        return 0, 0, 0
    # уникализируем по (group_id, lesson_date)
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
    Возвращаем список кортежей:
      (teacher_name, "YYYY-MM-DD", group_name)
    — под формат make_per_slide_mappings(...)
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
    # можно отсортировать по учителю и дате, чтобы было стабильно
    details.sort(key=lambda x: (x[0], x[1], x[2]))
    return details


def choose_coordinator_line(coordinators: List[dict]) -> str:
    """
    Возвращает строку для {{coordinator}}:
    - если есть primary — берём его имя;
    - иначе все имена через запятую.
    """
    if not coordinators:
        return ""
    prim = [c for c in coordinators if c["is_primary"]]
    if prim:
        return prim[0]["full_name"]
    return ", ".join(c["full_name"] for c in coordinators)


def extract_first_name(full_name: str) -> str:
    """
    В core.staff.full_name хранится 'Фамилия Имя' (иногда 'Фамилия Имя Отчество').
    Нужно убрать фамилию и оставить только имя.
    Логика: если слов 2+ — берём второе; иначе — единственное слово.
    """
    if not full_name:
        return ""
    parts = full_name.strip().split()
    if len(parts) >= 2:
        return parts[1]
    return parts[0]


def build_email_html(
    first_name: str,
    date_str: str,
    programme: str,
    allcount: int,
    regcount: int,
    unregcount: int,
    percent_unreg: float,
    all_m: int,
    form_m: int,
    unform_m: int,
) -> str:
    """
    Формирует HTML-тело письма.
    Блок 1 — посещаемость (как было).
    Блок 2 — оценки, выставленные учителями в отчётный день (новый раздел, стиль идентичен блоку 1).
    """
    optional_zero = ""
    if unregcount == 0:
        optional_zero = (
            '<p style="margin:12px 0 0 0;color:#333;">'
            "На дату отчёта все уроки отмечены полностью."
            "</p>"
        )

    percent_str = f"{percent_unreg:.1f}"

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width">
  <title>Ежедневный отчёт</title>
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
                Данное письмо является ежедневным отчётом по регистрации учителями посещаемости и оценивании на уроках.
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
              <p style="margin:0 0 12px 0;font-size:16px;"><strong>{date_str}</strong></p>

              <p style="margin:0 0 4px 0;font-size:14px;color:#555;">Программа</p>
              <p style="margin:0 0 0 0;font-size:16px;"><strong>{programme}</strong></p>
            </td>
          </tr>

          <!-- Блок 1: посещаемость -->
          <tr>
            <td style="padding:16px 24px 8px 24px;">
              <p style="margin:0 0 8px 0;font-size:16px;"><strong>Итоги за {date_str}:</strong></p>
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
                  Подробности — во вложении (PDF).
                </p>
                <p style="margin:0;color:#333;">
                  По каждому проблемному уроку указаны преподаватель, время и название группы.
                </p>
              </div>
            </td>
          </tr>

          <!-- Блок 2: оценки без выбора форм -->
          <tr>
            <td style="padding:16px 24px 8px 24px;">
              <p style="margin:0 0 8px 0;font-size:16px;"><strong>Оценки, выставленные учителями {date_str}:</strong></p>
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
                  При наличии уроков без выбора формы работ смотри вложение (PDF).
                </p>
                <p style="margin:0;color:#333;">
                  По каждому проблемному уроку указаны преподаватель, дата урока и название группы.
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
    """
    Загружает PDF (байты) в указанную папку Drive. Возвращает fileId.
    """
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
    """
    Собирает массив mapping'ов: на каждый слайд по 30 строк (по конфигу).
    На каждый слайд кладём и шапку (date/programme/coordinator/метрики).
    """
    mappings = []
    for pack in chunk(rows, per_slide_max) or [
        []
    ]:  # хотя бы один слайд, даже при пустом списке
        m = dict(header)  # копия шапки
        # Заполняем teacher_X, BX, CX
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


# ─────────────────────────────────────────────────────────────────────────────
# Главный сценарий
# ─────────────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Coordinator daily attendance report")
    parser.add_argument("--date", help="YYYY-MM-DD (report date)")
    args = parser.parse_args()

    # защита от параллельного запуска отчётов — на ВЕСЬ прогон
    with advisory_lock(1003):
        report_date = compute_report_date(args.date)

        # Конфиг отчёта
        reports_cfg = CONFIG.get("reports", {}) or {}
        rpt_cfg = reports_cfg.get("coordinator_daily_attendance", {}) or {}
        sender = (reports_cfg.get("email", {}) or {}).get("sender")
        template_id = rpt_cfg.get("template_id")
        per_slide_max = int(rpt_cfg.get("per_slide_max_rows", 30))
        parent_folder_id = rpt_cfg.get("parent_folder_id")
        filename_pattern = rpt_cfg.get(
            "filename_pattern",
            "{date}_{programme}_coordinator_daily_attendance_report.pdf",
        )

        # настройки второго PDF (оценки без формы)
        rpt2_cfg = (
            CONFIG.get("reports", {}).get("coordinator_daily_assessment", {}) or {}
        )
        template2_id = rpt2_cfg.get("template_id")
        per_slide2_max = int(rpt2_cfg.get("per_slide_max_rows", 30))
        filename2_pattern = rpt2_cfg.get(
            "filename_pattern",
            "{date}_{programme}_coordinator_daily_assessment_report.pdf",
        )

        if not (sender and template_id and parent_folder_id):
            raise RuntimeError(
                "Missing required config in config.yaml -> reports.coordinator_daily_attendance"
            )

        # Google клиенты
        drive, slides, gmail = build_services()

        # ОДНО соединение к БД на весь прогон
        with get_conn() as conn:
            # Почтовые роли (через одно соединение)
            acad_cc = []
            acad_email = load_academic_director_email(conn)
            if acad_email:
                acad_cc = [acad_email]

            # Источник данных (вью) — читаем за дату
            all_rows = load_source_rows(conn, report_date)

            # Второй источник: уроки с оценками (все) + флаг has_unweighted
            ass_rows = load_assessment_rows(conn, report_date)
            ass_by_programme: Dict[str, List[dict]] = defaultdict(list)
            for r in ass_rows:
                ass_by_programme[r["programme_code"]].append(r)

            rows_by_programme: Dict[str, List[dict]] = defaultdict(list)
            for r in all_rows:
                rows_by_programme[r["programme_code"]].append(r)

            # Координаторы (по программам)
            coords_by_programme = load_programme_coordinators(conn)

            # Список программ для рассылки = все, у которых есть координаторы
            programme_codes = sorted(coords_by_programme.keys())

            # Папки для сохранения
            month_folder = month_partition_folder(report_date)

            for pcode in programme_codes:
                coordinators = coords_by_programme.get(pcode, [])
                if not coordinators:
                    continue  # подстраховка

                pname = coordinators[0]["programme_name"]  # у всех одинаковое

                # ⬇️ анти-дубль: проверка выполненных прогонов
                with conn.cursor() as cur:
                    # если _already_done ожидает programme_code — передаём pcode;
                    # если у вас версия с programme_id — передайте id.
                    if _already_done(cur, REPORT_KEY, report_date, pcode):
                        print(
                            f"[report] skip: already exists for {report_date} programme={pname}"
                        )
                        continue

                # Может быть пусто -> нулевой отчёт
                prog_rows = rows_by_programme.get(pcode, [])

                allc, regc, unregc, percent = aggregate_metrics(prog_rows)
                detail = build_detail_rows(prog_rows)

                # Шапка для плейсхолдеров
                header = {
                    "date": report_date.strftime("%Y-%m-%d"),
                    "programme": pname,
                    "coordinator": choose_coordinator_line(coordinators),
                    "allcountlessons": str(allc),
                    "regcountlessons": str(regc),
                    "unregcountlessons": str(unregc),
                    "percentunreglessons": f"{percent:.1f}",
                }

                # Пер-слайд маппинги (по 30 строк)
                per_slide_maps = make_per_slide_mappings(header, detail, per_slide_max)

                # Папки Drive: программа/месяц
                prog_folder_id = ensure_subfolder(drive, parent_folder_id, pname)
                month_folder_id = ensure_subfolder(drive, prog_folder_id, month_folder)

                # Временная презентация для рендера
                title = f"tmp_{REPORT_KEY}_{pcode}_{report_date.isoformat()}"
                pres_id, _pages = prepare_presentation_from_template(
                    template_id, title, month_folder_id
                )

                try:
                    # ── PDF #1 (attendance): рендер + загрузка
                    pdf_bytes_1 = render_and_export_pdf(
                        pres_id, per_slide_maps, base_slide_index=0
                    )

                    filename_1 = filename_pattern.format(
                        date=report_date.strftime("%Y-%m-%d"),
                        programme=pname.replace("/", "-"),
                    )

                    pdf_file_id_1 = upload_pdf_to_drive(
                        drive, month_folder_id, filename_1, pdf_bytes_1
                    )

                    # Лог rep.report_run по первому PDF
                    with conn.cursor() as cur:
                        cur.execute(
                            SQL_INSERT_RUN,
                            (
                                REPORT_KEY,
                                report_date,
                                pcode,
                                pname,
                                pdf_file_id_1,
                                f"mojo_reports/coordinator_daily_attendance_report/{pname}/{month_folder}/{filename_1}",
                                len(per_slide_maps),
                                len(detail),
                            ),
                        )
                        run_id_att = cur.fetchone()[0]
                    conn.commit()

                    # ── Готовим второй (assessment): метрики + (опционально) PDF #2
                    ass_prog_rows = ass_by_programme.get(pcode, [])
                    all_m, unform_m, form_m = aggregate_assessment_metrics(
                        ass_prog_rows
                    )
                    detail2 = build_assessment_detail_rows(ass_prog_rows)

                    pres2_id = None
                    pdf_bytes_2 = None
                    filename_2 = None
                    pdf_file_id_2 = None

                    if unform_m > 0 and template2_id:
                        # Вторая временная презентация (assessment)
                        title2 = f"tmp_{REPORT_KEY2}_{pcode}_{report_date.isoformat()}"
                        pres2_id, _pages2 = prepare_presentation_from_template(
                            template2_id, title2, month_folder_id
                        )

                        # Шапка для второго PDF
                        header2 = {
                            "date": report_date.strftime("%Y-%m-%d"),
                            "programme": pname,
                            "coordinator": choose_coordinator_line(coordinators),
                            "allcountmarklessons": str(all_m),
                            "unformcountlessons": str(unform_m),
                            "formcountlessons": str(form_m),
                        }
                        per_slide_maps2 = make_per_slide_mappings(
                            header2, detail2, per_slide2_max
                        )

                        # Рендер + загрузка PDF #2
                        pdf_bytes_2 = render_and_export_pdf(
                            pres2_id, per_slide_maps2, base_slide_index=0
                        )
                        filename_2 = filename2_pattern.format(
                            date=report_date.strftime("%Y-%m-%d"),
                            programme=pname.replace("/", "-"),
                        )
                        pdf_file_id_2 = upload_pdf_to_drive(
                            drive, month_folder_id, filename_2, pdf_bytes_2
                        )

                        # Лог rep.report_run по второму PDF
                        with conn.cursor() as cur:
                            cur.execute(
                                SQL_INSERT_RUN,
                                (
                                    REPORT_KEY2,
                                    report_date,
                                    pcode,
                                    pname,
                                    pdf_file_id_2,
                                    f"mojo_reports/coordinator_daily_assessment_report/{pname}/{month_folder}/{filename_2}",
                                    len(per_slide_maps2),
                                    len(detail2),
                                ),
                            )
                        conn.commit()

                    # ── Письмо: HTML (attendance) + доп.блок по оценкам
                    to_addrs = [c["email"] for c in coordinators if c.get("email")]
                    to_addrs_str = ", ".join(to_addrs)
                    subject = (
                        f"Daily report · {report_date.strftime('%Y-%m-%d')} · {pname}"
                    )

                    greet_full_name = next(
                        (c["full_name"] for c in coordinators if c.get("is_primary")),
                        coordinators[0]["full_name"],
                    )
                    first_name = extract_first_name(greet_full_name)

                    html_body_final = build_email_html(
                        first_name=first_name,
                        date_str=report_date.strftime("%Y-%m-%d"),
                        programme=pname,
                        allcount=allc,
                        regcount=regc,
                        unregcount=unregc,
                        percent_unreg=percent,
                        all_m=all_m,
                        form_m=form_m,
                        unform_m=unform_m,
                    )

                    # Вложения: всегда attendance; assessment — только если есть проблемные уроки
                    attachments = [(pdf_bytes_1, filename_1)]
                    if pdf_bytes_2 and filename_2:
                        attachments.append((pdf_bytes_2, filename_2))

                    # Единая отправка
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

                    # Лог доставки (одна запись на письмо, привязываем к run_id_att)
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
                    # Удаляем временные копии Slides (обе, если создавали)
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
