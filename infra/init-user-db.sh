#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create the exporter user
    CREATE USER exporter WITH PASSWORD '$POSTGRES_EXPORTER_PASSWORD';
    ALTER USER exporter SET SEARCH_PATH TO exporter,pg_catalog;

    -- Grant permissions for Prometheus postgres_exporter
    -- Basic connection permission
    GRANT CONNECT ON DATABASE postgres TO exporter;

    -- Schema usage permission
    GRANT USAGE ON SCHEMA public TO exporter;

    -- Permissions on specific catalog views and functions
    GRANT SELECT ON pg_stat_database TO exporter;
    GRANT SELECT ON pg_stat_database_conflicts TO exporter;
    GRANT SELECT ON pg_stat_bgwriter TO exporter;
    GRANT SELECT ON pg_stat_activity TO exporter;
    GRANT SELECT ON pg_stat_archiver TO exporter;
    GRANT SELECT ON pg_stat_replication TO exporter;
    GRANT SELECT ON pg_stat_wal_receiver TO exporter;
    GRANT SELECT ON pg_stat_subscription TO exporter;
    GRANT SELECT ON pg_stat_ssl TO exporter;
    GRANT SELECT ON pg_stat_progress_vacuum TO exporter;
    GRANT SELECT ON pg_stat_progress_analyze TO exporter;
    GRANT SELECT ON pg_stat_progress_cluster TO exporter;
    GRANT SELECT ON pg_stat_progress_create_index TO exporter;
    GRANT SELECT ON pg_stat_progress_basebackup TO exporter;
    GRANT SELECT ON pg_stat_user_tables TO exporter;
    GRANT SELECT ON pg_stat_user_indexes TO exporter;
    GRANT SELECT ON pg_statio_user_tables TO exporter;
    GRANT SELECT ON pg_statio_user_indexes TO exporter;
    GRANT SELECT ON pg_statio_user_sequences TO exporter;
    GRANT SELECT ON pg_statistic TO exporter;

    -- If you need to monitor specific tables, you might want to add:
    -- GRANT SELECT ON ALL TABLES IN SCHEMA public TO exporter;

    -- For custom queries, you might need additional permissions
    -- GRANT EXECUTE ON FUNCTION pg_stat_file(text) TO exporter;

    -- If you're using pgBouncer, you might need:
    -- GRANT SELECT ON pg_catalog.pg_authid TO exporter;

    -- For WAL monitoring:
    GRANT EXECUTE ON FUNCTION pg_ls_dir(text) TO exporter;
    GRANT EXECUTE ON FUNCTION pg_read_file(text) TO exporter;
    GRANT EXECUTE ON FUNCTION pg_stat_file(text) TO exporter;

    -- If you're using pg_stat_statements:
    -- GRANT SELECT ON pg_stat_statements TO exporter;

    -- If you need to check table sizes:
    GRANT EXECUTE ON FUNCTION pg_relation_size(regclass) TO exporter;
    GRANT EXECUTE ON FUNCTION pg_table_size(regclass) TO exporter;
    GRANT EXECUTE ON FUNCTION pg_indexes_size(regclass) TO exporter;
    GRANT EXECUTE ON FUNCTION pg_total_relation_size(regclass) TO exporter;

    -- If you need to check for bloat:
    GRANT EXECUTE ON FUNCTION pg_stat_get_live_tuples(oid) TO exporter;
    GRANT EXECUTE ON FUNCTION pg_stat_get_dead_tuples(oid) TO exporter;

    -- Allow exporter to see all databases
    GRANT SELECT ON pg_database TO exporter;

    -- If you're using replication slots:
    GRANT SELECT ON pg_replication_slots TO exporter;
EOSQL