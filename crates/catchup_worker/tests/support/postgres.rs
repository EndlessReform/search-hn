//! Each test gets a fresh database on the explicitly configured scratch server.
//! No DATABASE_URL fallback: a developer's production configuration is never used.
//!
//! The scratch server mirrors production's role layout so the diesel migrations
//! under test exercise the same ownership and grants prod uses. Production keeps
//! source tables owned by `admin` with a least-privilege `catchup_worker` service
//! role; the `20260906000012` migration grants that role its derived-table and
//! function privileges unconditionally. These roles are created once per scratch
//! server (idempotently) so a fresh checkout can run migrations without any
//! out-of-band setup, and without pushing grants into an unreviewed deploy sidecar.
use diesel::{prelude::*, sql_query};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

pub struct TempPostgres {
    admin: String,
    url: String,
    name: String,
}

impl TempPostgres {
    pub fn start() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let admin = std::env::var("TEST_SEARCH_DATABASE_URL").expect(
            "set TEST_SEARCH_DATABASE_URL to the isolated PG17 server (deploy/search-postgres)",
        );
        let mut url = reqwest::Url::parse(&admin).expect("valid scratch URL");
        assert!(
            matches!(url.host_str(), Some("127.0.0.1" | "localhost" | "[::1]")),
            "tests require an explicitly local scratch server"
        );
        let name = format!(
            "searchhn_test_{}_{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let mut conn = PgConnection::establish(&admin).expect("connect to scratch admin database");
        // Mirror production roles. NOLOGIN is enough: tests keep connecting as the
        // scratch superuser, but the migration's OWNER TO / GRANT statements then
        // run against the same role names prod uses instead of being skipped.
        sql_query(
            "DO $$ BEGIN \
                 BEGIN CREATE ROLE admin NOLOGIN; \
                 EXCEPTION WHEN duplicate_object THEN NULL; END; \
                 BEGIN CREATE ROLE catchup_worker NOLOGIN; \
                 EXCEPTION WHEN duplicate_object THEN NULL; END; \
             END $$;",
        )
        .execute(&mut conn)
        .expect("ensure prod-like roles exist on scratch server");
        sql_query(format!("CREATE DATABASE {name}"))
            .execute(&mut conn)
            .expect("create isolated test database");
        url.set_path(&format!("/{name}"));
        Self {
            admin,
            url: url.to_string(),
            name,
        }
    }

    pub fn database_url(&self) -> String {
        self.url.clone()
    }
}

impl Drop for TempPostgres {
    fn drop(&mut self) {
        if let Ok(mut conn) = PgConnection::establish(&self.admin) {
            if let Err(err) =
                sql_query(format!("DROP DATABASE {} WITH (FORCE)", self.name)).execute(&mut conn)
            {
                eprintln!("scratch database cleanup failed for {}: {err}", self.name);
            }
        }
    }
}
