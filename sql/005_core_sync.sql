-- sql/005_core_sync.sql
SET client_encoding TO 'UTF8';

CREATE SCHEMA IF NOT EXISTS core;
SET search_path TO core, public;

-- вспомогательная функция для updated_at
CREATE OR REPLACE FUNCTION core.set_updated_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  NEW.updated_at := now();
  RETURN NEW;
END;
$$;

-- 1) Состояние синхронизаций по источникам
-- Примеры endpoint: '/attendance', '/marks/current', '/marks/final', '/schedule',
--                   '/subjects', '/work_forms', 'students_xlsx', 'staff_xlsx', 'parents_xlsx', 'classes_xlsx'
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

-- 2) Журнал импорта файлов (Excel и т.п.)
CREATE TABLE IF NOT EXISTS core.ingest_file_log (
  source_name   text        NOT NULL,                   -- 'students_xlsx' | 'staff_xlsx' | ...
  file_name     text        NOT NULL,
  file_checksum text        NOT NULL,                   -- SHA256 содержимого
  imported_at   timestamptz NOT NULL DEFAULT now(),
  row_count     integer,
  success       boolean     NOT NULL,
  details       text,
  CONSTRAINT ingest_file_log_pk PRIMARY KEY (source_name, file_checksum)
);

-- частые выборки последних загрузок
CREATE INDEX IF NOT EXISTS ingest_file_log_last_idx
  ON core.ingest_file_log (source_name, imported_at DESC);
