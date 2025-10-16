-- sql/005_gsa_multistaff_patch.sql
SET client_encoding TO 'UTF8';
SET search_path TO core, public;

-- Снять старые ограничения
ALTER TABLE group_staff_assignment
  DROP CONSTRAINT IF EXISTS group_staff_assignment_no_overlap;

ALTER TABLE group_staff_assignment
  DROP CONSTRAINT IF EXISTS group_staff_assignment_pk;

-- Новый PK: добавляем staff_id
ALTER TABLE group_staff_assignment
  ADD CONSTRAINT group_staff_assignment_pk PRIMARY KEY (group_id, staff_id, valid_from);

-- Новый EXCLUDE: запрещаем пересечения только по одной и той же паре (group_id, staff_id)
ALTER TABLE group_staff_assignment
  ADD CONSTRAINT group_staff_assignment_no_overlap
  EXCLUDE USING gist (
    group_id WITH =,
    staff_id WITH =,
    daterange(valid_from, COALESCE(valid_to, 'infinity'::date), '[]') WITH &&
  ) DEFERRABLE INITIALLY IMMEDIATE;
