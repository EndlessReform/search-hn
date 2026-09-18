-- Install the utility, not cache contents: buffers disappear on each restart.
-- pg_prewarm is not trusted, so initial installation requires a superuser.
-- Production's non-superuser admin may run this migration after a superuser
-- executes this same file to provision the extension. IF NOT EXISTS supports
-- that path without editing Diesel's ledger or duplicating extension SQL.
SET LOCAL lock_timeout = '5s';
CREATE EXTENSION IF NOT EXISTS pg_prewarm WITH SCHEMA public;
