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


-- ----------------------------------------------------------------------------
-- 3) Источник данных для НЕДЕЛЬНОГО отчёта координатора по посещаемости
--     Агрегация по преподавателям за рабочие дни (пн–пт), неделя = ISO (пн..вс)
--     Поля:
--       week_start, week_end_mf
--       programme_code / programme_name
--       staff_id / staff_name / staff_email
--       lessons_total_week (AX), lessons_unmarked_week (BX), percent_unmarked (CX)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW rep.v_coord_weekly_attendance_by_staff AS
WITH base AS (
  SELECT
    v.report_date,                    -- дата урока (DATE)
    v.programme_code,
    v.programme_name,
    v.staff_id,
    v.staff_name,
    v.staff_email,
    (v.cnt_unmarked > 0)::int AS is_unmarked_lesson
  FROM rep.v_coord_daily_attendance_src v
  WHERE v.report_date IS NOT NULL
),
-- отбираем только рабочие дни (пн..пт)
mf_days AS (
  SELECT *
  FROM base
  WHERE EXTRACT(ISODOW FROM report_date) BETWEEN 1 AND 5
),
-- считаем начало недели (ISO: пн) и конец рабочей недели (пт)
mf AS (
  SELECT
    (date_trunc('week', report_date::timestamp))::date AS week_start,
    (date_trunc('week', report_date::timestamp))::date + 4 AS week_end_mf,
    programme_code,
    programme_name,
    staff_id,
    staff_name,
    staff_email,
    is_unmarked_lesson
  FROM mf_days
)
SELECT
  week_start,
  week_end_mf,
  programme_code,
  programme_name,
  staff_id,
  staff_name,
  staff_email,
  COUNT(*)::int                                      AS lessons_total_week,   -- AX
  SUM(is_unmarked_lesson)::int                       AS lessons_unmarked_week, -- BX
  CASE WHEN COUNT(*) > 0
       THEN ROUND(100.0 * SUM(is_unmarked_lesson) / COUNT(*), 1)
       ELSE 0 END                                    AS percent_unmarked       -- CX
FROM mf
GROUP BY
  week_start, week_end_mf,
  programme_code, programme_name,
  staff_id, staff_name, staff_email
;


CREATE TABLE IF NOT EXISTS rep.email_queue (
    id                bigserial PRIMARY KEY,
    campaign_id       text        NOT NULL,    -- логическая рассылка/отчёт
    recipient_email   text        NOT NULL,
    subject           text        NOT NULL,
    html_body         text        NOT NULL,
    attachment_bytes  bytea,                   -- PDF; можно NULL, если без вложения
    attachment_name   text,
    status            text        NOT NULL DEFAULT 'pending',  -- pending|sent|error
    error_msg         text,
    created_at        timestamptz NOT NULL DEFAULT now(),
    sent_at           timestamptz,
    try_count         int         NOT NULL DEFAULT 0
);

-- Антидубль на «кампания+получатель»
CREATE UNIQUE INDEX IF NOT EXISTS uq_email_queue_campaign_recipient
ON rep.email_queue (campaign_id, recipient_email) WHERE status <> 'error';

-- статус: pending|processing|sent|error
CREATE INDEX IF NOT EXISTS ix_email_queue_pending_created
ON rep.email_queue (status, created_at);

-- ---------------------------------------------------------------------------
-- VIEВ: rep.v_coord_daily_assessment_lessons
-- Единица: (report_date, group_id, lesson_date) где В ТУ ЖЕ report_date учитель ставил любые оценки.
-- Поля:
--   programme_code / programme_name   -- для маршрутизации к координатору
--   staff_id / staff_name / staff_email -- основной преподаватель по правилу: core.group_staff_assignment на дату,
--                                         при ко-ведении предпочитаем is_primary из lesson_staff; иначе min(staff_id)
--   group_name
--   has_unweighted  -- признак: есть ли в этом "уроке" любая оценка с weight_pct IS NULL
-- ВАЖНО: "report_date" = created_at_src::date (день, когда ставили оценки), "lesson_date" = дата самого урока.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW rep.v_coord_daily_assessment_lessons AS
WITH marks AS (
  SELECT
    -- учитываем локальную TZ отчёта
    (mc.created_at_src AT TIME ZONE 'Europe/Podgorica')::date AS report_date,
    mc.group_id,
    mc.lesson_date::date AS lesson_date,
    COUNT(*) AS cnt_marks,
    COUNT(*) FILTER (WHERE mc.weight_pct IS NULL) AS cnt_unweighted
  FROM core.mark_current mc
  WHERE mc.created_at_src IS NOT NULL
  GROUP BY 1,2,3
),
groups AS (
  SELECT tg.group_id, tg.group_name
  FROM core.teaching_group tg
),
-- доминирующая программа группы на дату lesson_date
members_on_date AS (
  SELECT
    m.group_id,
    m.lesson_date AS ref_date,
    st.programme_code
  FROM marks m
  JOIN core.group_student_membership gsm
    ON gsm.group_id = m.group_id
   AND m.lesson_date BETWEEN gsm.valid_from AND COALESCE(gsm.valid_to, m.lesson_date)
  JOIN core.student st ON st.student_id = gsm.student_id
),
dom_programme AS (
  SELECT
    m.group_id,
    m.lesson_date AS ref_date,
    (ARRAY_AGG(programme_code ORDER BY cnt DESC, programme_code))[1] AS programme_code
  FROM (
    SELECT group_id, ref_date, programme_code, COUNT(*) AS cnt
    FROM members_on_date
    GROUP BY 1,2,3
  ) AS s
  JOIN (SELECT DISTINCT group_id, lesson_date FROM marks) m
    ON m.group_id = s.group_id AND m.lesson_date = s.ref_date
  GROUP BY 1,2
),
prog_named AS (
  SELECT
    dp.group_id,
    dp.ref_date,
    dp.programme_code,
    rp.programme_name
  FROM dom_programme dp
  LEFT JOIN core.ref_programme rp ON rp.programme_code = dp.programme_code
),
-- кандидаты преподавателей по интервалам
staff_candidates AS (
  SELECT
    m.report_date,
    m.group_id,
    m.lesson_date,
    gsa.staff_id
  FROM marks m
  JOIN core.group_staff_assignment gsa
    ON gsa.group_id = m.group_id
   AND m.lesson_date BETWEEN gsa.valid_from AND COALESCE(gsa.valid_to, m.lesson_date)
),

-- флаг is_primary по урокам на ту же дату (если есть расписание); иначе NULL
primary_flags AS (
  SELECT DISTINCT
    ts.group_id,
    l.lesson_date::date AS lesson_date,
    ls.staff_id,
    ls.is_primary
  FROM core.lesson l
  JOIN core.timetable_schedule ts ON ts.schedule_id = l.schedule_id
  JOIN core.lesson_staff ls       ON ls.lesson_id   = l.lesson_id
),
staff_picked AS (
  SELECT sc.report_date, sc.group_id, sc.lesson_date, sc.staff_id,
         ROW_NUMBER() OVER (
           PARTITION BY sc.report_date, sc.group_id, sc.lesson_date
           ORDER BY COALESCE(pf.is_primary, FALSE) DESC, sc.staff_id
         ) AS rn
  FROM staff_candidates sc
  LEFT JOIN primary_flags pf
    ON pf.group_id   = sc.group_id
   AND pf.lesson_date = sc.lesson_date
   AND pf.staff_id    = sc.staff_id
),
main_staff AS (
  SELECT report_date, group_id, lesson_date, staff_id
  FROM staff_picked
  WHERE rn = 1
)
SELECT
  m.report_date,
  COALESCE(pn.programme_code, 'UNKNOWN') AS programme_code,
  COALESCE(pn.programme_name, 'Unknown programme') AS programme_name,
  m.group_id,
  g.group_name,
  m.lesson_date,
  st.staff_id,
  s.staff_name,
  s.email AS staff_email,
  (m.cnt_unweighted > 0) AS has_unweighted
FROM marks m
JOIN groups g       ON g.group_id = m.group_id
LEFT JOIN prog_named pn ON pn.group_id = m.group_id AND pn.ref_date = m.lesson_date
LEFT JOIN main_staff st ON st.group_id = m.group_id AND st.lesson_date = m.lesson_date AND st.report_date = m.report_date
LEFT JOIN core.staff s  ON s.staff_id = st.staff_id
;

-- ----------------------------------------------------------------------------
-- 4) Вью-помощник для ежедневного ПИСЬМА УЧИТЕЛЮ: проблемные уроки за дату
--     Оставляем только уроки, где регистрация неполная по правилу:
--     (cnt_unmarked > 0) OR (events_total < students_expected)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW rep.v_teacher_daily_bad_attendance AS
SELECT
  v.report_date,
  v.staff_id,
  v.staff_name,
  v.staff_email,
  v.group_name,
  v.lesson_id,
  v.lesson_start,
  v.lesson_finish
FROM rep.v_coord_daily_attendance_src v
WHERE (v.cnt_unmarked > 0 OR v.events_total < v.students_expected);

-- ----------------------------------------------------------------------------
-- 5) Вью-помощник: уроки c оценками БЕЗ формы (по учителю) за период
--     Выводим все такие случаи; период зададим в приложении (WHERE report_date >= :period_start)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE VIEW rep.v_teacher_unweighted_marks AS
WITH latest AS (
  SELECT group_id, lesson_date, MAX(report_date) AS latest_report_date
  FROM rep.v_coord_daily_assessment_lessons
  GROUP BY group_id, lesson_date
)
SELECT
  a.report_date,
  a.lesson_date,
  a.staff_id,
  a.staff_name,
  a.staff_email,
  a.group_id,
  a.group_name
FROM rep.v_coord_daily_assessment_lessons a
JOIN latest l
  ON l.group_id = a.group_id
 AND l.lesson_date = a.lesson_date
 AND l.latest_report_date = a.report_date
WHERE a.has_unweighted = TRUE;


-- 6) WEEKLY (Учитель): детализация проблемной регистрации за учебную неделю (пн–пт).
--    Используется для Блока 1 PDF и для списков в письме.
CREATE OR REPLACE VIEW rep.v_teacher_weekly_attendance_detail AS
WITH base AS (
  SELECT
    v.report_date,
    (date_trunc('week', v.report_date::timestamp))::date          AS week_start,
    (date_trunc('week', v.report_date::timestamp))::date + 4      AS week_end_mf,
    v.staff_id,
    v.staff_name,
    v.staff_email,
    v.group_name,
    rp.programme_name,
    v.lesson_start,
    v.lesson_finish,
    v.cnt_unmarked,
    v.events_total,
    v.students_expected
  FROM rep.v_coord_daily_attendance_src v
  LEFT JOIN core.ref_programme rp ON rp.programme_code = v.programme_code
  WHERE v.report_date IS NOT NULL
    AND EXTRACT(ISODOW FROM v.report_date) BETWEEN 1 AND 5  -- Mon..Fri
)
SELECT
  report_date, week_start, week_end_mf,
  staff_id, staff_name, staff_email,
  group_name, programme_name,
  lesson_start, lesson_finish
FROM base
WHERE (cnt_unmarked > 0 OR events_total < students_expected);

-- 7) WEEKLY (Учитель): свод по урокам за неделю (всего и с проблемной регистрацией).
--    Используется для расчёта {{allcount}}, {{unregcount}}, {{regcount}}.
CREATE OR REPLACE VIEW rep.v_teacher_weekly_attendance_summary AS
WITH base AS (
  SELECT
    v.report_date,
    (date_trunc('week', v.report_date::timestamp))::date     AS week_start,
    (date_trunc('week', v.report_date::timestamp))::date + 4 AS week_end_mf,
    v.staff_id,
    v.staff_name,
    v.staff_email,
    (v.cnt_unmarked > 0 OR v.events_total < v.students_expected)::int AS is_bad
  FROM rep.v_coord_daily_attendance_src v
  WHERE v.report_date IS NOT NULL
    AND EXTRACT(ISODOW FROM v.report_date) BETWEEN 1 AND 5
)
SELECT
  week_start, week_end_mf,
  staff_id, staff_name, staff_email,
  COUNT(*)::int                      AS lessons_total_week,
  SUM(is_bad)::int                   AS lessons_bad_week
FROM base
GROUP BY
  week_start, week_end_mf,
  staff_id, staff_name, staff_email;