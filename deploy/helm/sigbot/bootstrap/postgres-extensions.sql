\set ON_ERROR_STOP on
-- Run explicitly as the database/extension owner for an existing PostgreSQL data directory.
-- Init scripts run only for an empty PGDATA; this script is also safe to repeat afterwards.
BEGIN;
SELECT pg_advisory_xact_lock(1936287587);
DO $$ BEGIN
    IF (SELECT COUNT(*) FROM pg_available_extensions WHERE name IN ('timescaledb','vector')) <> 2 THEN
        RAISE EXCEPTION 'PostgreSQL image must provide both timescaledb and vector extensions';
    END IF;
    IF NOT 'timescaledb'=ANY(string_to_array(replace(current_setting('shared_preload_libraries'),' ',''),',')) THEN
        RAISE EXCEPTION 'TimescaleDB must be in shared_preload_libraries; configure it and restart PostgreSQL before bootstrap';
    END IF;
    IF NOT (SELECT rolsuper FROM pg_roles WHERE rolname=current_user) THEN
        RAISE EXCEPTION 'extension bootstrap requires a PostgreSQL superuser installation role';
    END IF;
END $$;
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS vector;
COMMIT;
SELECT extname,extversion FROM pg_extension WHERE extname IN ('timescaledb','vector') ORDER BY extname;
