-- ============================================================================
-- 09_grant_looker_core.sql — extend looker_reader to read core
-- ----------------------------------------------------------------------------
-- The Phase 5 export script (scripts/export_to_sheets.py) needs to read
-- core.hospitals (for name/type) and core.patient_experience (for the long
-- per-dimension sheet) in addition to marts. Grant looker_reader read-only
-- access to the core schema.
--
-- Run as hospital_app (the schema owner):
--   psql -U hospital_app -d hospital_dashboard -f sql/09_grant_looker_core.sql
-- ============================================================================

\set ON_ERROR_STOP on

GRANT USAGE ON SCHEMA core TO looker_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO looker_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE hospital_app IN SCHEMA core
    GRANT SELECT ON TABLES TO looker_reader;
