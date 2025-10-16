-- sql/000_full_schema.sql
SET client_encoding TO 'UTF8';

-- 0) Схема и расширения/утилиты
CREATE SCHEMA IF NOT EXISTS core;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- Функция для updated_at
CREATE OR REPLACE FUNCTION core.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

-- 1) Справочники -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.ref_attendance_status (
  status_code smallint PRIMARY KEY,
  name_en     text NOT NULL,
  name_ru     text NOT NULL,
  CONSTRAINT ref_attendance_status_code_ck CHECK (status_code IN (0,1,2,3,6,7)),
  CONSTRAINT ref_attendance_status_name_en_uniq UNIQUE (name_en),
  CONSTRAINT ref_attendance_status_name_ru_uniq UNIQUE (name_ru)
);

INSERT INTO core.ref_attendance_status (status_code, name_en, name_ru) VALUES
(0, 'Not marked',                  'не отмечен'),
(1, 'Present',                     'присутствовал'),
(2, 'Late',                        'опоздал'),
(3, 'Left early',                  'ушел раньше'),
(6, 'Late and left early',         'опоздал и ушел раньше'),
(7, 'Absent',                      'отсутствовал')
ON CONFLICT (status_code) DO UPDATE
SET name_en = EXCLUDED.name_en, name_ru = EXCLUDED.name_ru;

CREATE TABLE IF NOT EXISTS core.ref_programme (
  programme_code text PRIMARY KEY,
  programme_name text NOT NULL
);

INSERT INTO core.ref_programme (programme_code, programme_name) VALUES
('PEARSON','Pearson'),
('IPC','International Primary Curriculum'),
('IB','International Baccalaureate'),
('STATE','State Standard')
ON CONFLICT (programme_code) DO UPDATE
SET programme_name = EXCLUDED.programme_name;

CREATE TABLE IF NOT EXISTS core.ref_department (
  department_id   serial PRIMARY KEY,
  department_name text NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS core.ref_work_form (
  form_id          integer PRIMARY KEY,
  form_name        text NOT NULL UNIQUE,
  form_description text,
  is_control       boolean NOT NULL DEFAULT false,
  weight_pct       integer NOT NULL DEFAULT 0 CHECK (weight_pct BETWEEN 0 AND 100),
  form_percent_raw integer,
  created_at_src   timestamptz,
  archived_at_src  timestamptz,
  deleted_at_src   timestamptz
);

CREATE TABLE IF NOT EXISTS core.ref_subject (
  subject_id    integer PRIMARY KEY,
  subject_title text NOT NULL UNIQUE,
  in_curriculum boolean NOT NULL DEFAULT false,
  in_olymp      boolean NOT NULL DEFAULT false,
  department_id integer REFERENCES core.ref_department(department_id)
                ON UPDATE RESTRICT ON DELETE SET NULL,
  is_closed     boolean NOT NULL DEFAULT false
);

CREATE TABLE IF NOT EXISTS core.ref_academic_period (
  period_id   serial PRIMARY KEY,
  period_name text NOT NULL,
  school_year text NOT NULL,
  start_date  date NOT NULL,
  end_date    date NOT NULL,
  CONSTRAINT ref_academic_period_dates_ck CHECK (start_date <= end_date),
  CONSTRAINT uq_ref_academic_period_name_year UNIQUE (period_name, school_year)
);

INSERT INTO core.ref_academic_period (period_name, school_year, start_date, end_date) VALUES
('First academic semester',  '2025-2026', DATE '2025-09-01', DATE '2025-12-26'),
('Second academic semester', '2025-2026', DATE '2026-01-12', DATE '2026-06-26')
ON CONFLICT (period_name, school_year) DO UPDATE
SET school_year = EXCLUDED.school_year,
    start_date  = EXCLUDED.start_date,
    end_date    = EXCLUDED.end_date;

-- 2) Мастер-данные ----------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.student (
  student_id      integer PRIMARY KEY,
  first_name      text    NOT NULL,
  last_name       text    NOT NULL,
  gender          text,
  dob             date,
  email           text    NOT NULL UNIQUE,
  programme_code  text    REFERENCES core.ref_programme(programme_code)
                           ON UPDATE RESTRICT ON DELETE SET NULL,
  cohort          integer,
  active          boolean NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS core.class (
  class_id    serial PRIMARY KEY,
  class_code  text   NOT NULL UNIQUE,
  cohort      integer
);

CREATE TABLE IF NOT EXISTS core.staff (
  staff_id    integer PRIMARY KEY,
  staff_name  text    NOT NULL,
  email       text    NOT NULL UNIQUE,
  gender      text,
  dob         date,
  phone       text,
  active      boolean NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS core.staff_department (
  staff_id       integer NOT NULL REFERENCES core.staff(staff_id)
                           ON UPDATE CASCADE ON DELETE CASCADE,
  department_id  integer NOT NULL REFERENCES core.ref_department(department_id)
                           ON UPDATE RESTRICT ON DELETE RESTRICT,
  position_title text,
  CONSTRAINT staff_department_pk PRIMARY KEY (staff_id, department_id)
);

CREATE TABLE IF NOT EXISTS core.parent (
  parent_id   integer PRIMARY KEY,
  parent_name text    NOT NULL,
  email       text,
  phone       text,
  active      boolean NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS core.student_parent (
  student_id  integer NOT NULL REFERENCES core.student(student_id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
  parent_id   integer NOT NULL REFERENCES core.parent(parent_id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT student_parent_pk PRIMARY KEY (student_id, parent_id)
);

CREATE TABLE IF NOT EXISTS core.class_teacher (
  class_id    integer NOT NULL REFERENCES core.class(class_id)
                       ON UPDATE CASCADE ON DELETE CASCADE,
  staff_id    integer NOT NULL REFERENCES core.staff(staff_id)
                       ON UPDATE CASCADE ON DELETE RESTRICT,
  valid_from  date    NOT NULL,
  valid_to    date,
  CONSTRAINT class_teacher_pk PRIMARY KEY (class_id, valid_from),
  CONSTRAINT class_teacher_dates_ck CHECK (valid_to IS NULL OR valid_to >= valid_from),
  CONSTRAINT class_teacher_no_overlap
    EXCLUDE USING gist (
      class_id WITH =,
      daterange(valid_from, COALESCE(valid_to, 'infinity'::date), '[]') WITH &&
    ) DEFERRABLE INITIALLY IMMEDIATE
);

CREATE TABLE IF NOT EXISTS core.student_class_enrolment (
  student_id  integer NOT NULL REFERENCES core.student(student_id)
                       ON UPDATE CASCADE ON DELETE CASCADE,
  class_id    integer NOT NULL REFERENCES core.class(class_id)
                       ON UPDATE CASCADE ON DELETE RESTRICT,
  valid_from  date    NOT NULL,
  valid_to    date,
  CONSTRAINT student_class_enrolment_pk PRIMARY KEY (student_id, class_id, valid_from),
  CONSTRAINT student_class_enrolment_dates_ck CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

CREATE TABLE IF NOT EXISTS core.teaching_group (
  group_id    integer PRIMARY KEY,
  group_name  text    NOT NULL UNIQUE,
  subject_id  integer REFERENCES core.ref_subject(subject_id)
                       ON UPDATE RESTRICT ON DELETE SET NULL,
  active      boolean NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS core.group_staff_assignment (
  group_id   integer NOT NULL REFERENCES core.teaching_group(group_id)
                      ON UPDATE CASCADE ON DELETE CASCADE,
  staff_id   integer NOT NULL REFERENCES core.staff(staff_id)
                      ON UPDATE CASCADE ON DELETE RESTRICT,
  valid_from date    NOT NULL,
  valid_to   date,
  CONSTRAINT group_staff_assignment_pk PRIMARY KEY (group_id, staff_id, valid_from),
  CONSTRAINT group_staff_assignment_dates_ck CHECK (valid_to IS NULL OR valid_to >= valid_from),
  CONSTRAINT group_staff_assignment_no_overlap
    EXCLUDE USING gist (
      group_id WITH =,
      staff_id WITH =,
      daterange(valid_from, COALESCE(valid_to, 'infinity'::date), '[]') WITH &&
    ) DEFERRABLE INITIALLY IMMEDIATE
);

CREATE TABLE IF NOT EXISTS core.group_student_membership (
  group_id   integer NOT NULL REFERENCES core.teaching_group(group_id)
                      ON UPDATE CASCADE ON DELETE CASCADE,
  student_id integer NOT NULL REFERENCES core.student(student_id)
                      ON UPDATE CASCADE ON DELETE CASCADE,
  valid_from date    NOT NULL,
  valid_to   date,
  CONSTRAINT group_student_membership_pk PRIMARY KEY (group_id, student_id, valid_from),
  CONSTRAINT group_student_membership_dates_ck CHECK (valid_to IS NULL OR valid_to >= valid_from)
);

-- 3) Расписание и посещаемость ---------------------------------------------

CREATE TABLE IF NOT EXISTS core.timetable_schedule (
  schedule_id           integer PRIMARY KEY,
  group_id              integer NOT NULL REFERENCES core.teaching_group(group_id)
                            ON UPDATE CASCADE ON DELETE RESTRICT,
  subject_id            integer     REFERENCES core.ref_subject(subject_id)
                            ON UPDATE RESTRICT ON DELETE SET NULL,
  room                  text,
  replaced_schedule_id  integer     REFERENCES core.timetable_schedule(schedule_id)
                            ON UPDATE CASCADE ON DELETE SET NULL,
  schedule_start        date    NOT NULL,
  schedule_finish       date,
  CONSTRAINT timetable_schedule_dates_ck
    CHECK (schedule_finish IS NULL OR schedule_finish >= schedule_start)
);

CREATE TABLE IF NOT EXISTS core.lesson (
  lesson_id      integer PRIMARY KEY,
  schedule_id    integer NOT NULL REFERENCES core.timetable_schedule(schedule_id)
                        ON UPDATE CASCADE ON DELETE RESTRICT,
  lesson_date    date    NOT NULL,
  day_number     smallint NOT NULL CHECK (day_number BETWEEN 1 AND 7),
  lesson_start   time    NOT NULL,
  lesson_finish  time    NOT NULL,
  is_replacement boolean NOT NULL DEFAULT false,
  replaced_schedule_id integer REFERENCES core.timetable_schedule(schedule_id)
                        ON UPDATE CASCADE ON DELETE SET NULL,
  CONSTRAINT lesson_time_ck CHECK (lesson_finish > lesson_start)
);

CREATE INDEX IF NOT EXISTS lesson_date_idx     ON core.lesson (lesson_date);
CREATE INDEX IF NOT EXISTS lesson_schedule_idx ON core.lesson (schedule_id);

CREATE TABLE IF NOT EXISTS core.lesson_staff (
  lesson_id integer NOT NULL REFERENCES core.lesson(lesson_id)
                    ON UPDATE CASCADE ON DELETE CASCADE,
  staff_id  integer NOT NULL REFERENCES core.staff(staff_id)
                    ON UPDATE CASCADE ON DELETE RESTRICT,
  is_primary boolean NOT NULL DEFAULT true,
  CONSTRAINT lesson_staff_pk PRIMARY KEY (lesson_id, staff_id)
);

CREATE INDEX IF NOT EXISTS lesson_staff_staff_idx ON core.lesson_staff (staff_id);

CREATE TABLE IF NOT EXISTS core.attendance_event (
  attendance_id     integer PRIMARY KEY,
  student_id        integer NOT NULL REFERENCES core.student(student_id)
                           ON UPDATE CASCADE ON DELETE CASCADE,
  lesson_id         integer NOT NULL REFERENCES core.lesson(lesson_id)
                           ON UPDATE CASCADE ON DELETE CASCADE,
  attendance_date   date    NOT NULL,
  status_code       smallint NOT NULL REFERENCES core.ref_attendance_status(status_code)
                           ON UPDATE RESTRICT ON DELETE RESTRICT,
  period_id         integer  REFERENCES core.ref_academic_period(period_id)
                           ON UPDATE RESTRICT ON DELETE SET NULL,
  subject_id        integer  REFERENCES core.ref_subject(subject_id)
                           ON UPDATE RESTRICT ON DELETE SET NULL,
  grade_cohort      integer,
  student_name_src  text,
  CONSTRAINT attendance_event_uniq UNIQUE (student_id, lesson_id)
);

CREATE INDEX IF NOT EXISTS attendance_event_date_idx    ON core.attendance_event (attendance_date);
CREATE INDEX IF NOT EXISTS attendance_event_student_idx ON core.attendance_event (student_id);
CREATE INDEX IF NOT EXISTS attendance_event_lesson_idx  ON core.attendance_event (lesson_id);

-- 4) Оценки -----------------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.mark_current (
  mark_id            integer      PRIMARY KEY,
  student_id         integer      NOT NULL REFERENCES core.student(student_id)
                                  ON UPDATE CASCADE ON DELETE CASCADE,
  group_id           integer      NOT NULL REFERENCES core.teaching_group(group_id)
                                  ON UPDATE CASCADE ON DELETE RESTRICT,
  period_id          integer      NOT NULL REFERENCES core.ref_academic_period(period_id)
                                  ON UPDATE RESTRICT ON DELETE RESTRICT,
  lesson_date        date         NOT NULL,
  created_at_src     timestamptz  NOT NULL,
  value              numeric(6,2) NOT NULL,
  assessment_scheme  text         NOT NULL,
  is_control         boolean,
  form_id            integer      REFERENCES core.ref_work_form(form_id)
                                  ON UPDATE RESTRICT ON DELETE SET NULL,
  weight_pct         integer      NOT NULL CHECK (weight_pct BETWEEN 0 AND 100),
  CONSTRAINT mark_current_minimal_ck CHECK (value >= 0)
);

CREATE INDEX IF NOT EXISTS mark_current_student_idx
  ON core.mark_current (student_id, lesson_date);
CREATE INDEX IF NOT EXISTS mark_current_group_date_idx
  ON core.mark_current (group_id, lesson_date);
CREATE INDEX IF NOT EXISTS mark_current_period_idx
  ON core.mark_current (period_id);
CREATE INDEX IF NOT EXISTS mark_current_sgp_idx
  ON core.mark_current (student_id, group_id, period_id);

CREATE TABLE IF NOT EXISTS core.mark_final (
  final_mark_id       integer      PRIMARY KEY,
  student_id          integer      NOT NULL REFERENCES core.student(student_id)
                                   ON UPDATE CASCADE ON DELETE CASCADE,
  group_id            integer      NOT NULL REFERENCES core.teaching_group(group_id)
                                   ON UPDATE CASCADE ON DELETE RESTRICT,
  period_id           integer      NOT NULL REFERENCES core.ref_academic_period(period_id)
                                   ON UPDATE RESTRICT ON DELETE RESTRICT,
  value               numeric(6,2) NOT NULL,
  final_criterion_raw text,
  assessment_scheme   text         NOT NULL,
  created_at_src      timestamptz  NOT NULL,
  CONSTRAINT mark_final_one_per_period_uniq UNIQUE (student_id, group_id, period_id),
  CONSTRAINT mark_final_minimal_ck CHECK (value >= 0)
);

CREATE INDEX IF NOT EXISTS mark_final_student_idx
  ON core.mark_final (student_id, period_id);
CREATE INDEX IF NOT EXISTS mark_final_group_idx
  ON core.mark_final (group_id, period_id);
CREATE INDEX IF NOT EXISTS mark_final_sg_idx
  ON core.mark_final (student_id, group_id);

-- 5) Служебные таблицы ------------------------------------------------------

CREATE TABLE IF NOT EXISTS core.sync_state (
  endpoint                text PRIMARY KEY,
  last_successful_sync_at timestamptz,
  last_seen_updated_at    timestamptz,
  window_from             date,
  window_to               date,
  next_cursor             text,
  params                  jsonb NOT NULL DEFAULT '{}'::jsonb,
  notes                   text,
  created_at              timestamptz NOT NULL DEFAULT now(),
  updated_at              timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT sync_state_window_ck
    CHECK (window_to IS NULL OR window_from IS NULL OR window_to >= window_from)
);

DROP TRIGGER IF EXISTS trg_sync_state_updated_at ON core.sync_state;
CREATE TRIGGER trg_sync_state_updated_at
BEFORE UPDATE ON core.sync_state
FOR EACH ROW EXECUTE FUNCTION core.set_updated_at();

CREATE TABLE IF NOT EXISTS core.ingest_file_log (
  source_name   text        NOT NULL,
  file_name     text        NOT NULL,
  file_checksum text        NOT NULL,
  imported_at   timestamptz NOT NULL DEFAULT now(),
  row_count     integer,
  success       boolean     NOT NULL,
  details       text,
  CONSTRAINT ingest_file_log_pk PRIMARY KEY (source_name, file_checksum)
);

CREATE INDEX IF NOT EXISTS ingest_file_log_last_idx
  ON core.ingest_file_log (source_name, imported_at DESC);
