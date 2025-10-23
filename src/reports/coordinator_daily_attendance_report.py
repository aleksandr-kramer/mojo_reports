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
import calendar
import io
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pytz
from googleapiclient.http import MediaIoBaseUpload

from ..db import get_conn
from ..google.clients import build_services
from ..google.gmail_sender import send_email_with_attachment
from ..google.slides_export import (
    delete_file,
    ensure_subfolder,
    prepare_presentation_from_template,
    render_and_export_pdf,
)
from ..settings import CONFIG, settings

REPORT_KEY = "coord_daily_attendance"


# ─────────────────────────────────────────────────────────────────────────────
# Вспомогательные функции времени/дат
# ─────────────────────────────────────────────────────────────────────────────


def _tz() -> pytz.BaseTzInfo:
    tz_name = (CONFIG.get("reports", {}) or {}).get("timezone", settings.timezone)
    return pytz.timezone(tz_name or "Europe/Podgorica")


def compute_report_date(explicit: Optional[str] = None) -> date:
    """
    Если передана дата в формате YYYY-MM-DD — используем её.
    Иначе берём 'вчерашний учебный день' в таймзоне Europe/Podgorica:
      - если сегодня Вт–Сб → вчера
      - если сегодня Вс или Пн → пятница прошлой недели
    """
    if explicit:
        return datetime.strptime(explicit, "%Y-%m-%d").date()

    now_local = datetime.now(_tz())
    weekday = now_local.weekday()  # 0=Mon ... 6=Sun
    if weekday in (1, 2, 3, 4):  # Tue..Fri -> yesterday
        return (now_local - timedelta(days=1)).date()
    elif weekday == 5:  # Sat -> Friday
        return (now_local - timedelta(days=1)).date()
    elif weekday == 6:  # Sun -> Friday
        return (now_local - timedelta(days=2)).date()
    else:  # Mon -> Friday
        return (now_local - timedelta(days=3)).date()


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
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
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
) -> str:
    """
    Формирует минималистичное HTML-тело письма.
    Если unregcount == 0 — добавляет строку 'На дату отчёта все уроки отмечены полностью.'
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
                Данное письмо является ежедневным отчётом по регистрации учителями посещаемости уроков.
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
    created = drive.files().create(body=meta, media_body=media, fields="id").execute()
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

            # ⬇️ вставка анти-дубля:
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
                # Рендер и экспорт PDF (байты)
                pdf_bytes = render_and_export_pdf(
                    pres_id, per_slide_maps, base_slide_index=0
                )

                # Имя PDF
                filename = filename_pattern.format(
                    date=report_date.strftime("%Y-%m-%d"),
                    programme=pname.replace("/", "-"),
                )

                # Загрузка PDF в Drive (в папку месяца)
                pdf_file_id = upload_pdf_to_drive(
                    drive, month_folder_id, filename, pdf_bytes
                )

                # Лог rep.report_run (через текущее соединение, БЕЗ нового get_conn)
                with conn.cursor() as cur:
                    cur.execute(
                        SQL_INSERT_RUN,
                        (
                            REPORT_KEY,
                            report_date,
                            pcode,
                            pname,
                            pdf_file_id,
                            f"mojo_reports/coordinator_daily_attendance_report/{pname}/{month_folder}/{filename}",
                            len(per_slide_maps),
                            len(detail),
                        ),
                    )
                    run_id = cur.fetchone()[0]
                conn.commit()

                # Письмо
                to_addrs = [c["email"] for c in coordinators if c.get("email")]
                subject = (
                    f"Attendance. Ежедневный координаторский отчёт. "
                    f"{report_date.strftime('%Y-%m-%d')}. Программа: {pname}"
                )

                # Имя для обращения: primary, иначе — первый
                greet_full_name = next(
                    (c["full_name"] for c in coordinators if c.get("is_primary")),
                    coordinators[0]["full_name"],
                )
                first_name = extract_first_name(greet_full_name)

                # HTML-тело письма (минималистичный шаблон)
                html_body = build_email_html(
                    first_name=first_name,
                    date_str=report_date.strftime("%Y-%m-%d"),
                    programme=pname,
                    allcount=allc,
                    regcount=regc,
                    unregcount=unregc,
                    percent_unreg=percent,
                )

                message_id = ""
                error_text = None
                try:
                    message_id = send_email_with_attachment(
                        sender=sender,
                        to_addrs=to_addrs,
                        cc_addrs=acad_cc,
                        subject=subject,
                        html_body=html_body,
                        attachment_bytes=pdf_bytes,
                        attachment_filename=filename,
                    )
                    ok = True
                except Exception as e:
                    ok = False
                    error_text = str(e)

                # Лог rep.report_delivery_log (то же соединение)
                with conn.cursor() as cur:
                    cur.execute(
                        SQL_INSERT_DELIVERY,
                        (
                            run_id,
                            sender,
                            ", ".join(to_addrs),
                            acad_cc if acad_cc else None,
                            subject,
                            message_id,
                            ok,
                            error_text,
                        ),
                    )
                conn.commit()

            finally:
                # Удаляем временную копию Slides
                try:
                    delete_file(drive, pres_id)
                except Exception:
                    pass


if __name__ == "__main__":
    main()
