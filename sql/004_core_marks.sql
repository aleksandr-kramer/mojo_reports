-- sql/004_core_marks.sql
SET client_encoding TO 'UTF8';

CREATE SCHEMA IF NOT EXISTS core;
SET search_path TO core, public;

-- 1) Текущие оценки
CREATE TABLE IF NOT EXISTS mark_current (
  mark_id            integer      PRIMARY KEY,                             -- /marks/current.id
  student_id         integer      NOT NULL REFERENCES student(student_id)
                                  ON UPDATE CASCADE ON DELETE CASCADE,
  group_id           integer      NOT NULL REFERENCES teaching_group(group_id)
                                  ON UPDATE CASCADE ON DELETE RESTRICT,    -- маппинг по group_name
  period_id          integer      NOT NULL REFERENCES ref_academic_period(period_id)
                                  ON UPDATE RESTRICT ON DELETE RESTRICT,   -- маппинг по period + lesson_date
  lesson_date        date         NOT NULL,                                 -- /marks/current.date
  created_at_src     timestamptz  NOT NULL,                                 -- /marks.current.created (в UTC)
  value              numeric(6,2) NOT NULL,                                 -- допускаем дробные
  assessment_scheme  text         NOT NULL,                                 -- из поля "assesment"
  is_control         boolean,                                               -- "control"
  form_id            integer      REFERENCES ref_work_form(form_id)
                                  ON UPDATE RESTRICT ON DELETE SET NULL,    -- по form_name
  weight_pct         integer      NOT NULL CHECK (weight_pct BETWEEN 0 AND 100),
  CONSTRAINT mark_current_minimal_ck CHECK (value >= 0)
);

-- Индексы
CREATE INDEX IF NOT EXISTS mark_current_student_idx
  ON mark_current (student_id, lesson_date);
CREATE INDEX IF NOT EXISTS mark_current_group_date_idx
  ON mark_current (group_id, lesson_date);
CREATE INDEX IF NOT EXISTS mark_current_period_idx
  ON mark_current (period_id);
CREATE INDEX IF NOT EXISTS mark_current_sgp_idx
  ON mark_current (student_id, group_id, period_id);

-- 2) Итоговые оценки
CREATE TABLE IF NOT EXISTS mark_final (
  final_mark_id       integer      PRIMARY KEY,                             -- /marks/final.id
  student_id          integer      NOT NULL REFERENCES student(student_id)
                                   ON UPDATE CASCADE ON DELETE CASCADE,
  group_id            integer      NOT NULL REFERENCES teaching_group(group_id)
                                   ON UPDATE CASCADE ON DELETE RESTRICT,
  period_id           integer      NOT NULL REFERENCES ref_academic_period(period_id)
                                   ON UPDATE RESTRICT ON DELETE RESTRICT,
  value               numeric(6,2) NOT NULL,
  final_criterion_raw text,                                                -- "как есть" (optional)
  assessment_scheme   text         NOT NULL,                                -- из "assesment"
  created_at_src      timestamptz  NOT NULL,
  CONSTRAINT mark_final_one_per_period_uniq UNIQUE (student_id, group_id, period_id),
  CONSTRAINT mark_final_minimal_ck CHECK (value >= 0)
);

-- Индексы
CREATE INDEX IF NOT EXISTS mark_final_student_idx
  ON mark_final (student_id, period_id);
CREATE INDEX IF NOT EXISTS mark_final_group_idx
  ON mark_final (group_id, period_id);
CREATE INDEX IF NOT EXISTS mark_final_sg_idx
  ON mark_final (student_id, group_id);
