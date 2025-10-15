-- sql/001_core_refs.sql (каноничная миграция refs)
SET client_encoding TO 'UTF8';

-- 0) Схема
CREATE SCHEMA IF NOT EXISTS core;
SET search_path TO core, public;

-- 1) ref_attendance_status
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

-- 2) ref_programme
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

-- 3) ref_department
CREATE TABLE IF NOT EXISTS core.ref_department (
  department_id   serial PRIMARY KEY,
  department_name text NOT NULL UNIQUE
);

-- 5) ref_work_form
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

-- 6) ref_subject
CREATE TABLE IF NOT EXISTS core.ref_subject (
  subject_id    integer PRIMARY KEY,
  subject_title text NOT NULL UNIQUE,
  in_curriculum boolean NOT NULL DEFAULT false,
  in_olymp      boolean NOT NULL DEFAULT false,
  department_id integer REFERENCES core.ref_department(department_id)
                ON UPDATE RESTRICT ON DELETE SET NULL,
  is_closed     boolean NOT NULL DEFAULT false
);

-- 7) ref_academic_period (уникальность по паре)
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
