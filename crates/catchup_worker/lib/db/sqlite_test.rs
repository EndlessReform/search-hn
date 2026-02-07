use diesel::connection::SimpleConnection;
use diesel::sqlite::SqliteConnection;
use diesel::Connection;
use diesel_migrations::{FileBasedMigrations, MigrationHarness};

const SQLITE_MIGRATIONS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/sqlite_migrations");

/// Builds a fresh in-memory SQLite database and runs the local SQLite test migrations.
pub fn setup_in_memory_sqlite() -> SqliteConnection {
    let mut conn = SqliteConnection::establish(":memory:")
        .expect("failed to open in-memory sqlite database for tests");

    conn.batch_execute(
        r#"
        PRAGMA foreign_keys = ON;
        PRAGMA busy_timeout = 5000;
        "#,
    )
    .expect("failed to configure sqlite test database pragmas");

    run_sqlite_migrations(&mut conn);
    conn
}

/// Runs the dedicated SQLite migration set used by fast DB-backed tests.
pub fn run_sqlite_migrations(conn: &mut SqliteConnection) {
    let migrations = FileBasedMigrations::from_path(SQLITE_MIGRATIONS_DIR)
        .expect("failed to load sqlite test migrations");

    conn.run_pending_migrations(migrations)
        .expect("failed to run sqlite test migrations");
}
