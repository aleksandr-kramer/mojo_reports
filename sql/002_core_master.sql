-- sql/002_core_master.sql
SET client_encoding TO 'UTF8';

-- схема и расширение для EXCLUDE-ограничений по периодам
CREATE SCHEMA IF NOT EXISTS core;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- 1) STUDENT
-- источник: Excel "Список учеников.xlsx"
CREATE TABLE IF NOT EXISTS core.student (
  student_id      integer PRIMARY KEY,          -- = Id (равен student_id / id_student в API)
  first_name      text    NOT NULL,
  last_name       text    NOT NULL,
  gender          text,
  dob             date,
  email           text    NOT NULL UNIQUE,      -- школьная почта
  programme_code  text    REFERENCES core.ref_programme(programme_code)
                           ON UPDATE RESTRICT ON DELETE SET NULL,
  cohort          integer,                      -- год обучения (grade)
  active          boolean NOT NULL DEFAULT true
);

-- 2) CLASS
-- источник: Excel "Список классов.xlsx"
CREATE TABLE IF NOT EXISTS core.class (
  class_id    serial PRIMARY KEY,
  class_code  text   NOT NULL UNIQUE,           -- Title (например, '2A')
  cohort      integer
);

-- 3) STAFF
-- источник: Excel "Список сотрудников.xlsx"; staff_id совместим с /schedule.staff
CREATE TABLE IF NOT EXISTS core.staff (
  staff_id    integer PRIMARY KEY,              -- = Id
  staff_name  text    NOT NULL,                 -- "Фамилия Имя"
  email       text    NOT NULL UNIQUE,          -- доменная почта
  gender      text,
  dob         date,
  phone       text,
  active      boolean NOT NULL DEFAULT true
);

-- 4) STAFF_DEPARTMENT (сотрудник может состоять в нескольких департаментах)
-- источник: Excel "Список сотрудников.xlsx"
CREATE TABLE IF NOT EXISTS core.staff_department (
  staff_id       integer NOT NULL REFERENCES core.staff(staff_id)
                           ON UPDATE CASCADE ON DELETE CASCADE,
  department_id  integer NOT NULL REFERENCES core.ref_department(department_id)
                           ON UPDATE RESTRICT ON DELETE RESTRICT,
  position_title text,
  CONSTRAINT staff_department_pk PRIMARY KEY (staff_id, department_id)
);

-- 5) PARENT
-- источник: Excel "Список родителей.xlsx" (и/или "Список учеников.xlsx")
-- email/phone одиночные (не массивы), без UNIQUE
CREATE TABLE IF NOT EXISTS core.parent (
  parent_id   integer PRIMARY KEY,
  parent_name text    NOT NULL,
  email       text,
  phone       text,
  active      boolean NOT NULL DEFAULT true
);

-- 6) STUDENT_PARENT (M:N связь родитель—ребёнок)
CREATE TABLE IF NOT EXISTS core.student_parent (
  student_id  integer NOT NULL REFERENCES core.student(student_id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
  parent_id   integer NOT NULL REFERENCES core.parent(parent_id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
  CONSTRAINT student_parent_pk PRIMARY KEY (student_id, parent_id)
);

-- 7) CLASS_TEACHER (классный руководитель, история без пересечений)
-- источник: Excel "Список классов.xlsx" (Staff member)
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

-- 8) STUDENT_CLASS_ENROLMENT (история принадлежности ученика к классу)
-- источник: Excel "Список учеников.xlsx" (Class) + правила импорта
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

-- 9) TEACHING_GROUP (академические группы: предмет × набор; маппятся с /schedule и /marks.*)
CREATE TABLE IF NOT EXISTS core.teaching_group (
  group_id    integer PRIMARY KEY,              -- /schedule.group_id
  group_name  text    NOT NULL UNIQUE,          -- /schedule.group, /marks.*.group_name
  subject_id  integer REFERENCES core.ref_subject(subject_id)
                       ON UPDATE RESTRICT ON DELETE SET NULL,
  active      boolean NOT NULL DEFAULT true
);

-- 10) GROUP_STAFF_ASSIGNMENT (постоянный преподаватель группы, без разовых замен)
-- В момент времени у группы только один постоянный учитель.
CREATE TABLE IF NOT EXISTS core.group_staff_assignment (
  group_id   integer NOT NULL REFERENCES core.teaching_group(group_id)
                      ON UPDATE CASCADE ON DELETE CASCADE,
  staff_id   integer NOT NULL REFERENCES core.staff(staff_id)
                      ON UPDATE CASCADE ON DELETE RESTRICT,
  valid_from date    NOT NULL,
  valid_to   date,
  CONSTRAINT group_staff_assignment_pk PRIMARY KEY (group_id, valid_from),
  CONSTRAINT group_staff_assignment_dates_ck CHECK (valid_to IS NULL OR valid_to >= valid_from),
  CONSTRAINT group_staff_assignment_no_overlap
    EXCLUDE USING gist (
      group_id WITH =,
      daterange(valid_from, COALESCE(valid_to, 'infinity'::date), '[]') WITH &&
    ) DEFERRABLE INITIALLY IMMEDIATE
);

-- 11) GROUP_STUDENT_MEMBERSHIP (история принадлежности ученика к учебной группе)
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
