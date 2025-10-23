SET client_encoding TO 'UTF8';

-- ============================================================================
-- СХЕМА ОТЧЁТОВ
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS rep;

-- ----------------------------------------------------------------------------
-- 1) Сервисные таблицы для всех отчётов
-- ----------------------------------------------------------------------------

-- Факт генерации отчёта (одна запись = один PDF для конкретного среза)
CREATE TABLE IF NOT EXISTS rep.report_run (
  run_id          BIGSERIAL PRIMARY KEY,
  report_key      text    NOT NULL, -- напр. 'coord_daily_attendance'
  report_date     date    NOT NULL, -- отчётная дата (день уроков)
  programme_code  text    REFERENCES core.ref_programme(programme_code)
                           ON UPDATE RESTRICT ON DELETE SET NULL,
  programme_name  text    NOT NULL,
  pdf_drive_id    text,             -- fileId PDF в Google Drive
  pdf_drive_path  text,             -- путь/иерархия в Drive (читаемо)
  page_count      int,
  row_count       int,
  generated_at    timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT uq_report_run UNIQUE (report_key, report_date, programme_code)
);

-- Лог доставки писем
CREATE TABLE IF NOT EXISTS rep.report_delivery_log (
  id              BIGSERIAL PRIMARY KEY,
  run_id          BIGINT REFERENCES rep.report_run(run_id) ON DELETE CASCADE,
  email_from      text    NOT NULL,
  email_to        text    NOT NULL, -- склеенный список через запятую
  email_cc        text[],           -- массив адресов в копии
  subject         text    NOT NULL,
  message_id      text,
  sent_at         timestamptz NOT NULL DEFAULT now(),
  success         boolean NOT NULL DEFAULT false,
  details         text
);

-- ----------------------------------------------------------------------------
-- 2) Источник данных для ежедневного отчёта координатора посещаемости
--     Представление: одна строка = один урок с привязкой к доминирующей программе
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW rep.v_coord_daily_attendance_src AS
WITH lessons AS (
  SELECT
    l.lesson_id,
    l.lesson_date      AS report_date,
    l.lesson_start,
    l.lesson_finish,
    ts.group_id,
    tg.group_name
  FROM core.lesson l
  JOIN core.timetable_schedule ts ON ts.schedule_id = l.schedule_id
  JOIN core.teaching_group tg     ON tg.group_id     = ts.group_id
),
-- активные члены группы на дату урока
members_on_date AS (
  SELECT
    l.lesson_id,
    l.report_date,
    gsm.group_id,
    gsm.student_id
  FROM lessons l
  JOIN core.group_student_membership gsm
    ON gsm.group_id = l.group_id
   AND gsm.valid_from <= l.report_date
   AND (gsm.valid_to IS NULL OR gsm.valid_to >= l.report_date)
),
-- модальная (доминирующая) программа урока по студентам
dom_prog AS (
  SELECT
    l.lesson_id,
    s.programme_code,
    ROW_NUMBER() OVER (
      PARTITION BY l.lesson_id
      ORDER BY COUNT(*) DESC, COALESCE(s.programme_code, '') ASC
    ) AS rn
  FROM lessons l
  JOIN members_on_date m   ON m.lesson_id = l.lesson_id
  JOIN core.student s      ON s.student_id = m.student_id
  WHERE s.programme_code IS NOT NULL
  GROUP BY l.lesson_id, s.programme_code
),
lesson_prog AS (
  SELECT lesson_id, programme_code
  FROM dom_prog
  WHERE rn = 1
),
-- преподаватель урока (приоритет primary)
lesson_teacher AS (
  SELECT
    ls.lesson_id,
    st.staff_id,
    st.staff_name,
    st.email,
    ROW_NUMBER() OVER (
      PARTITION BY ls.lesson_id
      ORDER BY (CASE WHEN ls.is_primary THEN 0 ELSE 1 END), st.staff_id
    ) AS rn
  FROM core.lesson_staff ls
  JOIN core.staff st ON st.staff_id = ls.staff_id
),
lt AS (
  SELECT lesson_id, staff_id, staff_name, email
  FROM lesson_teacher
  WHERE rn = 1
),
-- сколько студентов ожидается по группе на дату урока
exp_students AS (
  SELECT
    l.lesson_id,
    COUNT(*)::int AS students_expected
  FROM lessons l
  JOIN members_on_date m ON m.lesson_id = l.lesson_id
  GROUP BY l.lesson_id
),
-- метрики посещаемости по уроку
attn_counts AS (
  SELECT
    a.lesson_id,
    COUNT(*)::int AS events_total,
    COUNT(*) FILTER (WHERE a.status_code = 0)::int AS cnt_unmarked
  FROM core.attendance_event a
  GROUP BY a.lesson_id
)
SELECT
  l.report_date,
  lp.programme_code,
  rp.programme_name,
  l.lesson_id,
  l.group_name,
  l.lesson_start,
  l.lesson_finish,
  lt.staff_id,
  lt.staff_name,
  lt.email          AS staff_email,
  COALESCE(ac.cnt_unmarked, 0)                AS cnt_unmarked,
  COALESCE(es.students_expected, 0)           AS students_expected,
  COALESCE(ac.events_total, 0)                AS events_total
FROM lessons l
JOIN lesson_prog lp      ON lp.lesson_id = l.lesson_id
JOIN core.ref_programme rp ON rp.programme_code = lp.programme_code
LEFT JOIN lt              ON lt.lesson_id = l.lesson_id
LEFT JOIN exp_students es ON es.lesson_id = l.lesson_id
LEFT JOIN attn_counts ac  ON ac.lesson_id = l.lesson_id;
