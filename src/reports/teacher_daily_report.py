# src/reports/teacher_daily_report.py
from __future__ import annotations

"""
Ежедневный отчёт для УЧИТЕЛЕЙ (e-mail без PDF).

Логика:
- Отчётный день = «вчерашний учебный день» в локальной TZ (вт–сб), либо --date=YYYY-MM-DD.
- Получатели: все учителя, у которых были уроки в отчётный день (из rep.v_coord_daily_attendance_src).
- Письмо отправляется КАЖДОМУ такому учителю, НО только если есть хотя бы один пункт:
  Блок 1: уроки за отчётный день с неполной регистрацией (rep.v_teacher_daily_bad_attendance).
  Блок 2: уроки за учебный период (reports.weekly_assessment_period_start .. now) с оценками без выбранной формы работ
          (rep.v_teacher_unweighted_marks).
- Без вложений. CC не заполняем.
- Для почтовых лимитов добавлена пауза между письмами из CONFIG.google.rate_limits.gmail.min_seconds_between_sends.

Запуск:
  python -m src.reports.teacher_daily_report
  python -m src.reports.teacher_daily_report --date 2025-10-24
"""

import argparse
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pytz

from ..db import advisory_lock, get_conn
from ..google.clients import build_services
from ..google.gmail_sender import send_email_with_attachments
from ..settings import CONFIG, settings

# ─────────────────────────────────────────────────────────────────────────────
# Константы/ключи
# ─────────────────────────────────────────────────────────────────────────────

ADVISORY_LOCK_KEY = 1006  # уникальный ключ под teacher_daily
REPORT_CAMPAIGN_KEY = "teacher_daily"  # для логов/идентификации кампании (строковый)

# ─────────────────────────────────────────────────────────────────────────────
# Тайм-зона и дата отчёта
# ─────────────────────────────────────────────────────────────────────────────


def _tz() -> pytz.BaseTzInfo:
    tz_name = (CONFIG.get("reports", {}) or {}).get("timezone", settings.timezone)
    return pytz.timezone(tz_name or "Europe/Podgorica")


def compute_report_date(explicit: Optional[str] = None) -> date:
    """
    Если передана дата (YYYY-MM-DD) — используем её.
    Без даты: разрешено только вт–сб → берём «вчера».
    В вс/пн без --date выходим с ошибкой.
    """
    if explicit:
        return datetime.strptime(explicit, "%Y-%m-%d").date()

    now_local = datetime.now(_tz())
    weekday = now_local.weekday()  # 0=Mon .. 6=Sun
    if weekday in (1, 2, 3, 4, 5):  # Tue..Sat -> yesterday
        return (now_local - timedelta(days=1)).date()
    raise SystemExit("Report is disabled on Sunday/Monday. Use --date=YYYY-MM-DD.")


# ─────────────────────────────────────────────────────────────────────────────
# SQL-запросы
# ─────────────────────────────────────────────────────────────────────────────

SQL_TEACHERS_WITH_LESSONS = """
SELECT DISTINCT staff_id, staff_name, staff_email
FROM rep.v_coord_daily_attendance_src
WHERE report_date = %s AND staff_id IS NOT NULL
ORDER BY staff_name;
"""

SQL_BAD_ATTENDANCE_BY_TEACHER = """
SELECT report_date, staff_id, staff_name, staff_email, group_name, lesson_start, lesson_finish
FROM rep.v_teacher_daily_bad_attendance
WHERE report_date = %s AND staff_id = %s
ORDER BY lesson_start, group_name;
"""

SQL_UNWEIGHTED_BY_TEACHER_PERIOD = """
SELECT report_date, lesson_date, staff_id, staff_name, staff_email, group_id, group_name
FROM rep.v_teacher_unweighted_marks
WHERE staff_id = %s AND report_date >= %s
ORDER BY lesson_date, group_name;
"""

SQL_INSERT_DELIVERY = """
INSERT INTO rep.report_delivery_log
  (run_id, email_from, email_to, email_cc, subject, message_id, success, details)
VALUES (NULL, %s, %s, %s::text[], %s, %s, %s, %s)
"""


# ─────────────────────────────────────────────────────────────────────────────
# Модели и утилиты
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class Teacher:
    staff_id: int
    staff_name: str
    staff_email: Optional[str]


def extract_first_name(full_name: str) -> str:
    """
    Из 'Фамилия Имя [Отчество]' оставить Имя, иначе единственное слово.
    """
    if not full_name:
        return ""
    parts = full_name.strip().split()
    if len(parts) >= 2:
        return parts[1]
    return parts[0]


def fmt_time_span(start: Optional[datetime], finish: Optional[datetime]) -> str:
    if not start or not finish:
        return ""
    return f"{start.strftime('%H:%M')}-{finish.strftime('%H:%M')}"


def build_email_html(
    teacher_name: str,
    report_date_str: str,
    rows_bad: List[Tuple[str, str]],  # [(time_span, group_name)]
    rows_unweighted: List[Tuple[str, str]],  # [(lesson_date_str, group_name)]
) -> str:
    """
    Формирует компактное HTML-письмо с двумя блоками. Без вложений.
    """
    first_name = extract_first_name(teacher_name)

    # Блок 1
    if rows_bad:
        block1_lines = "".join(
            f"<li><strong>{t}</strong> — {g}</li>" for t, g in rows_bad
        )
        block1_html = f"""
        <p style="margin:0 0 8px 0;"><strong>Блок 1. Регистрация посещаемости ({report_date_str})</strong></p>
        <ul style="margin:0 0 16px 18px; padding:0;">
          {block1_lines}
        </ul>
        """
    else:
        block1_html = f"""
        <p style="margin:0 0 8px 0;"><strong>Блок 1. Регистрация посещаемости ({report_date_str})</strong></p>
        <p style="margin:0 0 16px 0;color:#555;">Замечаний нет — все ваши уроки за день отмечены корректно.</p>
        """

    # Блок 2
    if rows_unweighted:
        block2_lines = "".join(
            f"<li><strong>{d}</strong> — {g}</li>" for d, g in rows_unweighted
        )
        block2_html = f"""
        <p style="margin:8px 0 8px 0;"><strong>Блок 2. Оценки без выбранной формы работ (за учебный период)</strong></p>
        <ul style="margin:0 0 16px 18px; padding:0;">
          {block2_lines}
        </ul>
        """
    else:
        block2_html = """
        <p style="margin:8px 0 8px 0;"><strong>Блок 2. Оценки без выбранной формы работ (за учебный период)</strong></p>
        <p style="margin:0 0 16px 0;color:#555;">Замечаний нет.</p>
        """

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width">
  <title>Ежедневный отчёт учителя</title>
</head>
<body style="margin:0;padding:0;background:#ffffff;">
  <table role="presentation" cellpadding="0" cellspacing="0" width="100%" style="background:#ffffff;">
    <tr>
      <td>
        <table role="presentation" cellpadding="0" cellspacing="0" width="600" style="width:600px;max-width:100%;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Arial,sans-serif;color:#111;line-height:1.55;">
          <tr>
            <td style="padding:20px 24px 6px 24px;">
              <p style="margin:0 0 10px 0;font-size:16px;">Уважаемая(ый), <strong>{first_name}</strong></p>
              <p style="margin:0;color:#555;">Это ваш ежедневный отчёт по посещаемости и оцениванию.</p>
            </td>
          </tr>

          <tr><td style="padding:8px 24px;"><hr style="border:0;border-top:1px solid #eaeaea;margin:0;"></td></tr>

          <tr>
            <td style="padding:12px 24px 6px 24px;">
              <p style="margin:0 0 6px 0;font-size:14px;color:#555;">Отчёт за</p>
              <p style="margin:0 0 0 0;font-size:16px;"><strong>{report_date_str}</strong></p>
            </td>
          </tr>

          <tr><td style="padding:8px 24px 0 24px;">{block1_html}</td></tr>
          <tr><td style="padding:0 24px 12px 24px;">{block2_html}</td></tr>

          <tr>
            <td style="padding:4px 24px 24px 24px;color:#777;font-size:12px;">
              <p style="margin:0;">Если у вас есть вопросы, пожалуйста, обратитесь к координатору программы или академическому отделу.</p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# Загрузка данных из БД
# ─────────────────────────────────────────────────────────────────────────────


def load_teachers_with_lessons(conn, report_date: date) -> List[Teacher]:
    with conn.cursor() as cur:
        cur.execute(SQL_TEACHERS_WITH_LESSONS, (report_date,))
        rows = cur.fetchall()
    res: List[Teacher] = []
    for staff_id, staff_name, staff_email in rows:
        res.append(
            Teacher(
                staff_id=staff_id, staff_name=staff_name or "", staff_email=staff_email
            )
        )
    return res


def load_bad_attendance_for_teacher(
    conn, report_date: date, staff_id: int
) -> List[Tuple[str, str]]:
    """
    Возвращает список [(time_span, group_name)] только для проблемных уроков.
    """
    with conn.cursor() as cur:
        cur.execute(SQL_BAD_ATTENDANCE_BY_TEACHER, (report_date, staff_id))
        rows = cur.fetchall()
    out: List[Tuple[str, str]] = []
    for _rdate, _sid, _sname, _email, group_name, lesson_start, lesson_finish in rows:
        out.append((fmt_time_span(lesson_start, lesson_finish), group_name or ""))
    return out


def load_unweighted_for_teacher(
    conn, staff_id: int, period_start: date
) -> List[Tuple[str, str]]:
    """
    Возвращает список [(lesson_date_str, group_name)] для уроков с оценками без формы.
    """
    with conn.cursor() as cur:
        cur.execute(SQL_UNWEIGHTED_BY_TEACHER_PERIOD, (staff_id, period_start))
        rows = cur.fetchall()
    out: List[Tuple[str, str]] = []
    for _rdate, lesson_date, _sid, _sname, _email, _gid, group_name in rows:
        date_str = lesson_date.strftime("%Y-%m-%d") if lesson_date else ""
        out.append((date_str, group_name or ""))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Главный сценарий
# ─────────────────────────────────────────────────────────────────────────────


def main():
    parser = argparse.ArgumentParser(description="Teacher daily email report (no PDF)")
    parser.add_argument("--date", help="YYYY-MM-DD (report date)")
    args = parser.parse_args()

    # advisory-lock на весь прогон
    with advisory_lock(ADVISORY_LOCK_KEY):
        report_date = compute_report_date(args.date)
        report_date_str = report_date.strftime("%Y-%m-%d")

        # конфиг
        reports_cfg = CONFIG.get("reports", {}) or {}
        td_cfg = reports_cfg.get("teacher_daily", {}) or {}
        subject = td_cfg.get("subject", "Mojo _ Daily Reports")

        # период для блока 2
        period_start_str = reports_cfg.get("weekly_assessment_period_start")
        if not period_start_str:
            raise RuntimeError(
                "Missing reports.weekly_assessment_period_start in config.yaml"
            )
        period_start = datetime.strptime(period_start_str, "%Y-%m-%d").date()

        # лимиты отправки
        rl = ((CONFIG.get("google", {}) or {}).get("rate_limits", {}) or {}).get(
            "gmail", {}
        ) or {}
        min_gap = int(rl.get("min_seconds_between_sends", 0))

        # сервисы
        _drive, _slides, gmail = build_services()
        sender = (reports_cfg.get("email", {}) or {}).get("sender")
        if not sender:
            raise RuntimeError("Missing reports.email.sender in config.yaml")

        with get_conn() as conn:
            teachers = load_teachers_with_lessons(conn, report_date)

            for t in teachers:
                # пропускаем, если нет e-mail
                if not t.staff_email:
                    # Логируем отсутствие получателя
                    with conn.cursor() as cur:
                        cur.execute(
                            SQL_INSERT_DELIVERY,
                            (
                                sender,
                                "",  # email_to (пусто)
                                [],  # email_cc
                                subject,
                                "",  # message_id
                                False,  # success
                                f"No email for teacher staff_id={t.staff_id}",
                            ),
                        )
                    conn.commit()
                    continue

                rows_bad = load_bad_attendance_for_teacher(
                    conn, report_date, t.staff_id
                )
                rows_unw = load_unweighted_for_teacher(conn, t.staff_id, period_start)

                # Письмо только если есть хоть что-то в Блоке 1 или 2
                if not rows_bad and not rows_unw:
                    continue

                html_body = build_email_html(
                    teacher_name=t.staff_name,
                    report_date_str=report_date_str,
                    rows_bad=rows_bad,
                    rows_unweighted=rows_unw,
                )

                message_id = ""
                error_text = None
                ok = False
                try:
                    message_id = (
                        send_email_with_attachments(
                            gmail=gmail,
                            sender=sender,
                            to=[t.staff_email],
                            cc=None,  # без CC
                            subject=subject,
                            html_body=html_body,
                            attachments=[],  # без вложений
                        )
                        or ""
                    )
                    ok = True
                except Exception as e:
                    ok = False
                    error_text = str(e)

                # лог доставки (run_id = NULL)
                with conn.cursor() as cur:
                    cur.execute(
                        SQL_INSERT_DELIVERY,
                        (
                            sender,
                            t.staff_email,
                            [],  # email_cc
                            subject,
                            message_id,
                            ok,
                            error_text,
                        ),
                    )
                conn.commit()

                # простая пауза между письмами, чтобы не превысить «шапку» по Gmail API
                if min_gap > 0:
                    time.sleep(min_gap)


if __name__ == "__main__":
    main()
