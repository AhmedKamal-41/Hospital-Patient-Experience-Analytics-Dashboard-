-- Bootstrap script: creates the database, the application role, and the
-- raw / core / marts schemas. Run as a PostgreSQL superuser:
--
--   psql -U postgres -f sql/00_init.sql -v app_password="'change-me'"
--
-- The pipeline (src/) connects as hospital_app with the password set here.
-- Idempotent: safe to re-run.

\set ON_ERROR_STOP on

-- 1. Role (created at cluster level, before the DB exists).
SELECT 'CREATE ROLE hospital_app LOGIN PASSWORD ' || quote_literal(:app_password)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'hospital_app')
\gexec

-- 2. Database. CREATE DATABASE can't run inside a transaction or via \gexec
--    when already connected, so guard with a DO block + dblink-free check.
SELECT 'CREATE DATABASE hospital_dashboard OWNER hospital_app'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'hospital_dashboard')
\gexec

-- 3. Switch into the new DB to set up schemas.
\connect hospital_dashboard

CREATE SCHEMA IF NOT EXISTS raw   AUTHORIZATION hospital_app;
CREATE SCHEMA IF NOT EXISTS core  AUTHORIZATION hospital_app;
CREATE SCHEMA IF NOT EXISTS marts AUTHORIZATION hospital_app;

-- Default privileges so future tables created by hospital_app are usable.
ALTER DEFAULT PRIVILEGES FOR ROLE hospital_app IN SCHEMA raw, core, marts
    GRANT SELECT ON TABLES TO hospital_app;

-- Looker Studio will connect as a read-only role; create it now so 02_*
-- schema migrations can grant against it.
SELECT 'CREATE ROLE looker_reader LOGIN PASSWORD ' || quote_literal(:app_password)
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'looker_reader')
\gexec

GRANT CONNECT ON DATABASE hospital_dashboard TO looker_reader;
GRANT USAGE ON SCHEMA marts TO looker_reader;
ALTER DEFAULT PRIVILEGES FOR ROLE hospital_app IN SCHEMA marts
    GRANT SELECT ON TABLES TO looker_reader;
