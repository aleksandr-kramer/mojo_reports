-- sql/raw_schema.sql
SET client_encoding TO 'UTF8';

-- 1. Схема RAW
CREATE SCHEMA IF NOT EXISTS raw;

-- 2. Родительская таблица RAW.attendance с партиционированием по дате урока
CREATE TABLE IF NOT EXISTS raw.attendance (
  id               BIGINT       NOT NULL,                 -- ID записи из Mojo
  student_id       BIGINT,
  lesson_id        BIGINT,
  student          TEXT,
  grade            INT,
  attendance_date  DATE         NOT NULL,                 -- дата урока (partition key)
  status           SMALLINT,
  period_name      TEXT,
  subject_name     TEXT,

  -- служебные поля
  src_day          DATE         NOT NULL,
  source_system    TEXT         NOT NULL DEFAULT 'mojo',
  endpoint         TEXT         NOT NULL DEFAULT '/attendance',
  raw_json         JSONB        NOT NULL,
  ingested_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
  source_hash      TEXT         NOT NULL,
  batch_id         TEXT         NOT NULL,

  -- PK ДОЛЖЕН включать колонку партиционирования
  CONSTRAINT attendance_pk PRIMARY KEY (id, attendance_date)
) PARTITION BY RANGE (attendance_date);

-- 3. Индексы (партиционированные)
CREATE INDEX IF NOT EXISTS attendance_src_day_idx    ON raw.attendance (src_day);
CREATE INDEX IF NOT EXISTS attendance_student_idx    ON raw.attendance (student_id);
CREATE INDEX IF NOT EXISTS attendance_lesson_idx     ON raw.attendance (lesson_id);
CREATE INDEX IF NOT EXISTS attendance_status_idx     ON raw.attendance (status);
CREATE INDEX IF NOT EXISTS attendance_subject_idx    ON raw.attendance (subject_name);

-- 4. Функция для "ленивого" создания месячной партиции
CREATE OR REPLACE FUNCTION raw.ensure_attendance_partition(p_month DATE)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  d_from DATE := date_trunc('month', p_month)::date;
  d_to   DATE := (date_trunc('month', p_month)::date + INTERVAL '1 month')::date;
  part   TEXT := format('attendance_p%s', to_char(d_from, 'YYYYMM'));
  sqltxt TEXT;
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'raw' AND c.relname = part
  ) THEN
    sqltxt := format(
      'CREATE TABLE raw.%I PARTITION OF raw.attendance
         FOR VALUES FROM (%L) TO (%L);',
      part, d_from, d_to
    );
    EXECUTE sqltxt;

    -- локальные индексы (опционально)
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_src_day_idx   ON raw.%I (src_day);',   part||'_srcday', part);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_student_idx   ON raw.%I (student_id);', part||'_st',     part);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_lesson_idx    ON raw.%I (lesson_id);',  part||'_ls',     part);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_status_idx    ON raw.%I (status);',     part||'_stt',    part);
  END IF;
END;
$$;

-- RAW.marks_current: оценки за уроки (текущие)
CREATE TABLE IF NOT EXISTS raw.marks_current (
  id            BIGINT      NOT NULL,      -- ID оценки в Mojo
  period        TEXT,
  mark_date     DATE        NOT NULL,      -- дата урока (partition key)
  subject       TEXT,
  group_name    TEXT,
  id_student    BIGINT,
  value         NUMERIC(6,2),
  created       TIMESTAMPTZ,
  assesment     TEXT,
  control       SMALLINT,
  flex          INT,
  weight        NUMERIC(6,2),
  form          TEXT,
  grade         INT,
  student       TEXT,

  -- служебные поля
  src_day       DATE        NOT NULL,
  source_system TEXT        NOT NULL DEFAULT 'mojo',
  endpoint      TEXT        NOT NULL DEFAULT '/marks/current',
  raw_json      JSONB       NOT NULL,
  ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_hash   TEXT        NOT NULL,
  batch_id      TEXT        NOT NULL,

  CONSTRAINT marks_current_pk PRIMARY KEY (id, mark_date)
) PARTITION BY RANGE (mark_date);

CREATE INDEX IF NOT EXISTS marks_current_src_day_idx  ON raw.marks_current (src_day);
CREATE INDEX IF NOT EXISTS marks_current_student_idx  ON raw.marks_current (id_student);
CREATE INDEX IF NOT EXISTS marks_current_subject_idx  ON raw.marks_current (subject);
CREATE INDEX IF NOT EXISTS marks_current_group_idx    ON raw.marks_current (group_name);

-- партиции для /marks/current
CREATE OR REPLACE FUNCTION raw.ensure_marks_current_partition(p_month DATE)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  d_from DATE := date_trunc('month', p_month)::date;
  d_to   DATE := (date_trunc('month', p_month)::date + INTERVAL '1 month')::date;
  part   TEXT := format('marks_current_p%s', to_char(d_from, 'YYYYMM'));
  sqltxt TEXT;
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'raw' AND c.relname = part
  ) THEN
    sqltxt := format(
      'CREATE TABLE raw.%I PARTITION OF raw.marks_current
         FOR VALUES FROM (%L) TO (%L);',
      part, d_from, d_to
    );
    EXECUTE sqltxt;

    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_src_day_idx  ON raw.%I (src_day);',   part||'_srcday', part);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_student_idx  ON raw.%I (id_student);', part||'_st',     part);
  END IF;
END;
$$;

-- RAW.marks_final: итоговые оценки
CREATE TABLE IF NOT EXISTS raw.marks_final (
  id               BIGINT      NOT NULL,      -- ID финальной оценки
  period           TEXT,
  created_date     DATE        NOT NULL,      -- ДАТА из поля 'created' (partition key)
  subject          TEXT,
  subject_id       BIGINT,
  group_name       TEXT,
  id_student       BIGINT,
  value            NUMERIC(10,4),
  final_criterion  TEXT,
  assesment        TEXT,
  created          TIMESTAMPTZ,               -- исходное 'created' (с временем/зоной)
  grade            INT,
  student          TEXT,

  -- служебные поля
  src_day          DATE        NOT NULL,
  source_system    TEXT        NOT NULL DEFAULT 'mojo',
  endpoint         TEXT        NOT NULL DEFAULT '/marks/final',
  raw_json         JSONB       NOT NULL,
  ingested_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_hash      TEXT        NOT NULL,
  batch_id         TEXT        NOT NULL,

  CONSTRAINT marks_final_pk PRIMARY KEY (id, created_date)
) PARTITION BY RANGE (created_date);

CREATE INDEX IF NOT EXISTS marks_final_src_day_idx   ON raw.marks_final (src_day);
CREATE INDEX IF NOT EXISTS marks_final_student_idx   ON raw.marks_final (id_student);
CREATE INDEX IF NOT EXISTS marks_final_subject_idx   ON raw.marks_final (subject);
CREATE INDEX IF NOT EXISTS marks_final_subjectid_idx ON raw.marks_final (subject_id);
CREATE INDEX IF NOT EXISTS marks_final_period_idx    ON raw.marks_final (period);

-- функция партиций по created_date
CREATE OR REPLACE FUNCTION raw.ensure_marks_final_partition(p_month DATE)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  d_from DATE := date_trunc('month', p_month)::date;
  d_to   DATE := (date_trunc('month', p_month)::date + INTERVAL '1 month')::date;
  part   TEXT := format('marks_final_p%s', to_char(d_from, 'YYYYMM'));
  sqltxt TEXT;
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'raw' AND c.relname = part
  ) THEN
    sqltxt := format(
      'CREATE TABLE raw.%I PARTITION OF raw.marks_final
         FOR VALUES FROM (%L) TO (%L);',
      part, d_from, d_to
    );
    EXECUTE sqltxt;

    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_src_day_idx  ON raw.%I (src_day);',   part||'_srcday', part);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_student_idx  ON raw.%I (id_student);', part||'_st',     part);
  END IF;
END;
$$;

-- RAW.schedule_lessons: расписание, 1 строка = 1 урок в конкретный день
CREATE TABLE IF NOT EXISTS raw.schedule_lessons (
  schedule_id          BIGINT,                -- id слота в сетке недели
  schedule_start       DATE,
  schedule_finish      DATE,
  group_id             BIGINT,
  building_id          BIGINT,
  group_name           TEXT,
  subject_name         TEXT,
  room                 TEXT,
  is_replacement       SMALLINT,              -- 0/1
  replaced_schedule_id BIGINT,

  lesson_id            BIGINT    NOT NULL,    -- уникальный id урока
  lesson_date          DATE      NOT NULL,    -- ключ партиции
  day_number           SMALLINT,
  lesson_start         TIME,
  lesson_finish        TIME,
  staff_json           JSONB,                 -- как пришло: { "<id>": "ФИО", ... }

  -- служебные поля
  src_day              DATE      NOT NULL,    -- день забора (сегодня)
  source_system        TEXT      NOT NULL DEFAULT 'mojo',
  endpoint             TEXT      NOT NULL DEFAULT '/schedule',
  raw_json             JSONB     NOT NULL,    -- оригинал
  ingested_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_hash          TEXT      NOT NULL,
  batch_id             TEXT      NOT NULL,

  CONSTRAINT schedule_lessons_pk PRIMARY KEY (lesson_id, lesson_date)
) PARTITION BY RANGE (lesson_date);

CREATE INDEX IF NOT EXISTS schedule_lessons_group_idx      ON raw.schedule_lessons (group_id);
CREATE INDEX IF NOT EXISTS schedule_lessons_groupname_idx  ON raw.schedule_lessons (group_name);
CREATE INDEX IF NOT EXISTS schedule_lessons_subject_idx    ON raw.schedule_lessons (subject_name);
CREATE INDEX IF NOT EXISTS schedule_lessons_scheduleid_idx ON raw.schedule_lessons (schedule_id);
CREATE INDEX IF NOT EXISTS schedule_lessons_room_idx       ON raw.schedule_lessons (room);
CREATE INDEX IF NOT EXISTS schedule_lessons_repl_idx       ON raw.schedule_lessons (is_replacement);
CREATE INDEX IF NOT EXISTS schedule_lessons_srcday_idx     ON raw.schedule_lessons (src_day);

-- Партиции помесячно
CREATE OR REPLACE FUNCTION raw.ensure_schedule_lessons_partition(p_month DATE)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
  d_from DATE := date_trunc('month', p_month)::date;
  d_to   DATE := (date_trunc('month', p_month)::date + INTERVAL '1 month')::date;
  part   TEXT := format('schedule_lessons_p%s', to_char(d_from, 'YYYYMM'));
  sqltxt TEXT;
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'raw' AND c.relname = part
  ) THEN
    sqltxt := format(
      'CREATE TABLE raw.%I PARTITION OF raw.schedule_lessons
         FOR VALUES FROM (%L) TO (%L);',
      part, d_from, d_to
    );
    EXECUTE sqltxt;

    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_src_day_idx  ON raw.%I (src_day);',   part||'_srcday', part);
    EXECUTE format('CREATE INDEX IF NOT EXISTS %I_group_idx    ON raw.%I (group_id);',  part||'_grp',    part);
  END IF;
END;
$$;

-- RAW.subjects: справочник предметов (актуальное состояние на id)
CREATE TABLE IF NOT EXISTS raw.subjects (
  id                BIGINT      PRIMARY KEY,
  title             TEXT,
  in_curriculum     SMALLINT,          -- 0/1
  in_olymp          SMALLINT,          -- 0/1
  department        TEXT,
  closed            SMALLINT,          -- 0/1

  -- служебные поля/метки
  first_seen_src_day DATE       NOT NULL,
  last_seen_src_day  DATE       NOT NULL,
  src_day            DATE       NOT NULL,      -- день текущей загрузки (последний)
  source_system      TEXT       NOT NULL DEFAULT 'mojo',
  endpoint           TEXT       NOT NULL DEFAULT '/subjects',
  raw_json           JSONB      NOT NULL,
  ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_hash        TEXT       NOT NULL,
  batch_id           TEXT       NOT NULL
);

CREATE INDEX IF NOT EXISTS subjects_title_idx       ON raw.subjects (title);
CREATE INDEX IF NOT EXISTS subjects_department_idx  ON raw.subjects (department);
CREATE INDEX IF NOT EXISTS subjects_closed_idx      ON raw.subjects (closed);
CREATE INDEX IF NOT EXISTS subjects_last_seen_idx   ON raw.subjects (last_seen_src_day);

-- RAW.work_forms: справочник форм работ (актуальное состояние по id_form)
CREATE TABLE IF NOT EXISTS raw.work_forms (
  id_form           BIGINT      PRIMARY KEY,
  form_name         TEXT,
  form_description  TEXT,
  form_area         SMALLINT,          -- область/категория (число)
  form_control      SMALLINT,          -- 0/1 (контрольная форма)
  form_weight       NUMERIC(6,2),      -- вес, иногда 0..100, храним с дробью на всякий случай
  form_percent      SMALLINT,          -- 0/1 (признак «в процентах») если используется
  form_created      TIMESTAMPTZ,
  form_archived     TIMESTAMPTZ,
  form_deleted      TIMESTAMPTZ,

  -- служебные метки (как в subjects)
  first_seen_src_day DATE       NOT NULL,
  last_seen_src_day  DATE       NOT NULL,
  src_day            DATE       NOT NULL,
  source_system      TEXT       NOT NULL DEFAULT 'mojo',
  endpoint           TEXT       NOT NULL DEFAULT '/work_forms',
  raw_json           JSONB      NOT NULL,
  ingested_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_hash        TEXT       NOT NULL,
  batch_id           TEXT       NOT NULL
);

CREATE INDEX IF NOT EXISTS work_forms_name_idx       ON raw.work_forms (form_name);
CREATE INDEX IF NOT EXISTS work_forms_area_idx       ON raw.work_forms (form_area);
CREATE INDEX IF NOT EXISTS work_forms_control_idx    ON raw.work_forms (form_control);
CREATE INDEX IF NOT EXISTS work_forms_last_seen_idx  ON raw.work_forms (last_seen_src_day);

-- RAW: справочник учеников (актуальная версия на id)
CREATE TABLE IF NOT EXISTS raw.students_ref (
  student_id           BIGINT PRIMARY KEY,
  first_name           TEXT,
  last_name            TEXT,
  gender               TEXT,
  dob                  DATE,
  email                TEXT,
  cohort               TEXT,
  class_name           TEXT,
  program              TEXT,

  -- из Excel у учеников часто есть «родители» в одной ячейке через «/»
  parents_raw          TEXT,

  -- служебные метки
  first_seen_src_day   DATE       NOT NULL,
  last_seen_src_day    DATE       NOT NULL,
  src_day              DATE       NOT NULL,
  source_system        TEXT       NOT NULL DEFAULT 'drive',
  endpoint             TEXT       NOT NULL DEFAULT 'excel/students',
  raw_json             JSONB      NOT NULL,
  ingested_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_hash          TEXT       NOT NULL,
  batch_id             TEXT       NOT NULL
);

CREATE INDEX IF NOT EXISTS students_ref_email_idx      ON raw.students_ref (email);
CREATE INDEX IF NOT EXISTS students_ref_class_idx      ON raw.students_ref (class_name);
CREATE INDEX IF NOT EXISTS students_ref_last_seen_idx  ON raw.students_ref (last_seen_src_day);

-- RAW: родители (уникальность по e-mail)
-- RAW: родители (уникальность по e-mail)
CREATE TABLE IF NOT EXISTS raw.parents_ref (
  parent_email        TEXT PRIMARY KEY,               -- ключ (в lower)
  parent_id           BIGINT,                         -- A: Id (неуникальный, может быть пуст)
  parent_name         TEXT,                           -- B: Parent (ФИО родителя)

  -- служебные метки
  first_seen_src_day  DATE        NOT NULL,
  last_seen_src_day   DATE        NOT NULL,
  src_day             DATE        NOT NULL,

  source_system       TEXT        NOT NULL DEFAULT 'drive',
  endpoint            TEXT        NOT NULL DEFAULT 'excel/parents',
  raw_json            JSONB       NOT NULL,
  ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_hash         TEXT        NOT NULL,
  batch_id            TEXT        NOT NULL
);

CREATE INDEX IF NOT EXISTS parents_ref_last_seen_idx ON raw.parents_ref (last_seen_src_day);
CREATE INDEX IF NOT EXISTS parents_ref_name_idx      ON raw.parents_ref (parent_name);

-- RAW: связи родитель ↔ ученик из Excel (ключ по строке из файла)
CREATE TABLE IF NOT EXISTS raw.student_parent_links (
  parent_email        TEXT        NOT NULL,
  student_name        TEXT        NOT NULL,            -- C: "Фамилия Имя" из файла родителей
  grade               TEXT        NOT NULL,            -- E: Grade как текст (без .0); часть PK
  student_id          BIGINT,                          -- если сопоставили с raw.students_ref, иначе NULL
  parent_id           BIGINT,                          -- A: Id из файла родителей (может быть NULL)

  -- служебные метки
  first_seen_src_day  DATE        NOT NULL,
  last_seen_src_day   DATE        NOT NULL,
  src_day             DATE        NOT NULL,

  source_system       TEXT        NOT NULL DEFAULT 'drive',
  endpoint            TEXT        NOT NULL DEFAULT 'excel/parents_links',
  raw_json            JSONB       NOT NULL,
  ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_hash         TEXT        NOT NULL,
  batch_id            TEXT        NOT NULL,

  PRIMARY KEY (parent_email, student_name, grade)
);

CREATE INDEX IF NOT EXISTS student_parent_links_student_idx   ON raw.student_parent_links (student_id);
CREATE INDEX IF NOT EXISTS student_parent_links_parentid_idx  ON raw.student_parent_links (parent_id);

-- RAW: сотрудники (уникальность по e-mail)
CREATE TABLE IF NOT EXISTS raw.staff_ref (
  staff_email         TEXT PRIMARY KEY,               -- ключ (lower)
  staff_id            BIGINT,                         -- A: Id (может повторяться/быть пустым)
  staff_name          TEXT,                           -- B: Staff (ФИО)
  gender              TEXT,                           -- D: Gender (как в Excel)

  -- служебные метки
  first_seen_src_day  DATE        NOT NULL,
  last_seen_src_day   DATE        NOT NULL,
  src_day             DATE        NOT NULL,

  source_system       TEXT        NOT NULL DEFAULT 'drive',
  endpoint            TEXT        NOT NULL DEFAULT 'excel/staff',
  raw_json            JSONB       NOT NULL,
  ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_hash         TEXT        NOT NULL,
  batch_id            TEXT        NOT NULL
);

CREATE INDEX IF NOT EXISTS staff_ref_last_seen_idx ON raw.staff_ref (last_seen_src_day);
CREATE INDEX IF NOT EXISTS staff_ref_name_idx      ON raw.staff_ref (staff_name);


-- RAW: позиции сотрудника (несколько строк на одного)
CREATE TABLE IF NOT EXISTS raw.staff_positions (
  staff_email         TEXT        NOT NULL,           -- ссылка на staff_ref.email
  department          TEXT,                           -- E: Department (как в Excel)
  position            TEXT,                           -- F: Position (может быть пусто — это ок)

  -- ключевые нормализованные поля (NOT NULL), чтобы PK работал при пустых значениях
  department_key      TEXT        NOT NULL,           -- lower/trim или '' если нет департамента
  position_key        TEXT        NOT NULL,           -- lower/trim или '' если нет позиции

  -- служебные метки
  first_seen_src_day  DATE        NOT NULL,
  last_seen_src_day   DATE        NOT NULL,
  src_day             DATE        NOT NULL,

  source_system       TEXT        NOT NULL DEFAULT 'drive',
  endpoint            TEXT        NOT NULL DEFAULT 'excel/staff_positions',
  raw_json            JSONB       NOT NULL,
  ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_hash         TEXT        NOT NULL,
  batch_id            TEXT        NOT NULL,

  PRIMARY KEY (staff_email, department_key, position_key)
);

CREATE INDEX IF NOT EXISTS staff_positions_dept_idx   ON raw.staff_positions (department_key);
CREATE INDEX IF NOT EXISTS staff_positions_email_idx  ON raw.staff_positions (staff_email);

-- RAW: классы (уникальность по названию)
CREATE TABLE IF NOT EXISTS raw.classes_ref (
  title               TEXT PRIMARY KEY,               -- A: Title
  cohort              TEXT,                           -- B: Cohort (без ".0")
  homeroom_short      TEXT,                           -- C: "Фамилия И."
  students_count      INTEGER,                        -- D: Number of students

  homeroom_email      TEXT,                           -- e-mail классного руководителя (если сматчилось)
  homeroom_staff_id   BIGINT,                         -- id сотрудника из staff_ref (если сматчилось)
  match_status        TEXT,                           -- matched / not_found / ambiguous
  match_method        TEXT,                           -- например: surname+initial

  -- служебные метки
  first_seen_src_day  DATE        NOT NULL,
  last_seen_src_day   DATE        NOT NULL,
  src_day             DATE        NOT NULL,

  source_system       TEXT        NOT NULL DEFAULT 'drive',
  endpoint            TEXT        NOT NULL DEFAULT 'excel/classes',
  raw_json            JSONB       NOT NULL,
  ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  source_hash         TEXT        NOT NULL,
  batch_id            TEXT        NOT NULL
);

CREATE INDEX IF NOT EXISTS classes_ref_cohort_idx        ON raw.classes_ref (cohort);
CREATE INDEX IF NOT EXISTS classes_ref_homeroom_email_idx ON raw.classes_ref (homeroom_email);
CREATE INDEX IF NOT EXISTS classes_ref_match_status_idx   ON raw.classes_ref (match_status);
