-- Leave the shared utility installed, consistent with the search migration's
-- extension rollback policy. Other operational consumers may now depend on it.
-- No cache contents or source data were created by this migration.
SELECT 1;
