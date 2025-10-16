# вставь блок ниже целиком, затем на новой строке напиши EOF и нажми Enter
cat > sql/003_core_master_patch.sql << 'EOF'
-- sql/003_core_master_patch.sql
SET client_encoding TO 'UTF8';
SET search_path TO core, public;
CREATE EXTENSION IF NOT EXISTS btree_gist;

-- 1) CLASS_TEACHER: убрать лишний ручной индекс (если когда-то создавали)
DROP INDEX IF EXISTS class_teacher_gist_idx;

-- пересоздать EXCLUDE как DEFERRABLE INITIALLY IMMEDIATE
ALTER TABLE class_teacher
  DROP CONSTRAINT IF EXISTS class_teacher_no_overlap;
ALTER TABLE class_teacher
  ADD CONSTRAINT class_teacher_no_overlap
  EXCLUDE USING gist (
    class_id WITH =,
    daterange(valid_from, COALESCE(valid_to, 'infinity'::date), '[]') WITH &&
  ) DEFERRABLE INITIALLY IMMEDIATE;

-- 2) GROUP_STAFF_ASSIGNMENT: аналогично
DROP INDEX IF EXISTS group_staff_assignment_gist_idx;

ALTER TABLE group_staff_assignment
  DROP CONSTRAINT IF EXISTS group_staff_assignment_no_overlap;
ALTER TABLE group_staff_assignment
  ADD CONSTRAINT group_staff_assignment_no_overlap
  EXCLUDE USING gist (
    group_id WITH =,
    daterange(valid_from, COALESCE(valid_to, 'infinity'::date), '[]') WITH &&
  ) DEFERRABLE INITIALLY IMMEDIATE;
EOF
