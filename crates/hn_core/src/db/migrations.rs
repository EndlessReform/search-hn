use diesel::pg::PgConnection;
use diesel_migrations::{FileBasedMigrations, MigrationHarness};

const POSTGRES_MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/migrations");
const SQLITE_TEST_MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/sqlite_migrations");

/// Loads the canonical Postgres migration set owned by `hn_core`.
///
/// Keeping this path in one place prevents service crates from hard-coding their own
/// migration directory assumptions.
pub fn postgres_migrations() -> FileBasedMigrations {
    FileBasedMigrations::from_path(POSTGRES_MIGRATIONS_DIR)
        .expect("failed to load hn_core postgres migrations")
}

/// Loads the SQLite parity migration set used by DB-backed tests.
///
/// The SQLite migration stream mirrors the Postgres schema shape where possible so test
/// harnesses can validate SQL-heavy logic without needing a live Postgres instance.
pub fn sqlite_test_migrations() -> FileBasedMigrations {
    FileBasedMigrations::from_path(SQLITE_TEST_MIGRATIONS_DIR)
        .expect("failed to load hn_core sqlite test migrations")
}

/// Runs all pending Postgres migrations owned by `hn_core` against an open connection.
pub fn run_postgres_migrations(conn: &mut PgConnection) {
    conn.run_pending_migrations(postgres_migrations())
        .expect("failed to run hn_core postgres migrations");
}
