use diesel::connection::SimpleConnection;
use diesel::sqlite::SqliteConnection;
use diesel::Connection;
use diesel_migrations::MigrationHarness;

use crate::db::migrations::sqlite_test_migrations;

/// Builds a fresh in-memory SQLite database and runs the canonical hn_core SQLite migrations.
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

/// Runs the SQLite migration stream used for DB-backed tests.
pub fn run_sqlite_migrations(conn: &mut SqliteConnection) {
    conn.run_pending_migrations(sqlite_test_migrations())
        .expect("failed to run sqlite test migrations");
}
