-- sql/003_core_schedule.sql
SET client_encoding TO 'UTF8';

CREATE SCHEMA IF NOT EXISTS core;
SET search_path TO core, public;

-- 1) TIMETABLE_SCHEDULE — «правило» расписания
CREATE TABLE IF NOT EXISTS timetable_schedule (
  schedule_id           integer PRIMARY KEY,  -- /schedule.schedule_id
  group_id              integer NOT NULL REFERENCES teaching_group(group_id)
                            ON UPDATE CASCADE ON DELETE RESTRICT,
  subject_id            integer     REFERENCES ref_subject(subject_id)
                            ON UPDATE RESTRICT ON DELETE SET NULL,
  room                  text,
  replaced_schedule_id  integer     REFERENCES timetable_schedule(schedule_id)
                            ON UPDATE CASCADE ON DELETE SET NULL,
  schedule_start        date    NOT NULL,
  schedule_finish       date,
  CONSTRAINT timetable_schedule_dates_ck
    CHECK (schedule_finish IS NULL OR schedule_finish >= schedule_start)
);

-- 2) LESSON — конкретный урок (дата/время)
CREATE TABLE IF NOT EXISTS lesson (
  lesson_id      integer PRIMARY KEY,   -- /schedule.lesson_id
  schedule_id    integer NOT NULL REFERENCES timetable_schedule(schedule_id)
                        ON UPDATE CASCADE ON DELETE RESTRICT,
  lesson_date    date    NOT NULL,
  day_number     smallint NOT NULL CHECK (day_number BETWEEN 1 AND 7),
  lesson_start   time    NOT NULL,
  lesson_finish  time    NOT NULL,
  is_replacement boolean NOT NULL DEFAULT false,
  replaced_schedule_id integer REFERENCES timetable_schedule(schedule_id)
                        ON UPDATE CASCADE ON DELETE SET NULL,
  CONSTRAINT lesson_time_ck CHECK (lesson_finish > lesson_start)
);

-- (Необязательный натуральный ключ — включать по желанию)
-- CREATE UNIQUE INDEX IF NOT EXISTS lesson_natural_uniq
--   ON lesson (schedule_id, lesson_date, lesson_start, lesson_finish);

CREATE INDEX IF NOT EXISTS lesson_date_idx     ON lesson (lesson_date);
CREATE INDEX IF NOT EXISTS lesson_schedule_idx ON lesson (schedule_id);

-- 3) LESSON_STAFF — кто реально вёл урок (M:N)
CREATE TABLE IF NOT EXISTS lesson_staff (
  lesson_id integer NOT NULL REFERENCES lesson(lesson_id)
                    ON UPDATE CASCADE ON DELETE CASCADE,
  staff_id  integer NOT NULL REFERENCES staff(staff_id)
                    ON UPDATE CASCADE ON DELETE RESTRICT,
  is_primary boolean NOT NULL DEFAULT true,
  CONSTRAINT lesson_staff_pk PRIMARY KEY (lesson_id, staff_id)
);

CREATE INDEX IF NOT EXISTS lesson_staff_staff_idx ON lesson_staff (staff_id);

-- 4) ATTENDANCE_EVENT — посещаемость «ученик×урок»
CREATE TABLE IF NOT EXISTS attendance_event (
  attendance_id     integer PRIMARY KEY,        -- /attendance.id
  student_id        integer NOT NULL REFERENCES student(student_id)
                           ON UPDATE CASCADE ON DELETE CASCADE,
  lesson_id         integer NOT NULL REFERENCES lesson(lesson_id)
                           ON UPDATE CASCADE ON DELETE CASCADE,
  attendance_date   date    NOT NULL,           -- как в источнике
  status_code       smallint NOT NULL REFERENCES ref_attendance_status(status_code)
                           ON UPDATE RESTRICT ON DELETE RESTRICT,
  period_id         integer  REFERENCES ref_academic_period(period_id)
                           ON UPDATE RESTRICT ON DELETE SET NULL,
  subject_id        integer  REFERENCES ref_subject(subject_id)
                           ON UPDATE RESTRICT ON DELETE SET NULL,
  grade_cohort      integer,                    -- /attendance.grade
  student_name_src  text,                       -- имя из источника
  CONSTRAINT attendance_event_uniq UNIQUE (student_id, lesson_id)
);

CREATE INDEX IF NOT EXISTS attendance_event_date_idx    ON attendance_event (attendance_date);
CREATE INDEX IF NOT EXISTS attendance_event_student_idx ON attendance_event (student_id);
CREATE INDEX IF NOT EXISTS attendance_event_lesson_idx  ON attendance_event (lesson_id);
