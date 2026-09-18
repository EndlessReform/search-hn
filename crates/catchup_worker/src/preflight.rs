//! Read-only deployment checks under the application's database role.
//!
//! These checks inspect compatibility and permissions, not production write paths.
//! No migrations, inference requests, or synthetic writes are performed.
use crate::config::UpdaterArgs;
use diesel::{
    migration::MigrationSource,
    pg::Pg,
    sql_query,
    sql_types::{Bool, Text},
    QueryableByName,
};
use diesel_async::{RunQueryDsl, SimpleAsyncConnection};
use hn_core::db::{build_db_pool, story_search::SearchPool};
use std::time::Duration;

const MIGRATIONS: diesel_migrations::EmbeddedMigrations =
    diesel_migrations::embed_migrations!("../hn_core/migrations");
const RECIPE: &str = "pplx-0.6b-2c4d510dd4a7-vllm0.28.0-bf16-flash-mean-2048-tanh127-rne-v1";
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(QueryableByName)]
struct Flag {
    #[diesel(sql_type = Bool)]
    ok: bool,
}
#[derive(QueryableByName)]
struct Value {
    #[diesel(sql_type = Text)]
    value: String,
}

/// Bounded existence check; empty-table startup guard does not count/scan history.
pub async fn has_search_rows(pool: &SearchPool) -> Result<bool> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut conn = pool.get().await?;
        Ok(
            sql_query("SELECT EXISTS(SELECT 1 FROM public.story_search LIMIT 1) AS ok")
                .get_result::<Flag>(&mut conn)
                .await?
                .ok,
        )
    })
    .await
    .map_err(|_| "search startup check timed out")?
}

/// Bound the entire preflight as well as individual SQL statements.
pub async fn check(args: &UpdaterArgs) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(30), inspect(args))
        .await
        .map_err(|_| "database preflight exceeded 30 seconds")?
}

async fn inspect(args: &UpdaterArgs) -> Result<()> {
    let url = args.database_url.as_ref().ok_or("database_url missing")?;
    let pool = build_db_pool(url, 1).await?;
    let mut conn = tokio::time::timeout(Duration::from_secs(5), pool.get())
        .await
        .map_err(|_| "database connection timed out")??;
    conn.batch_execute(
        "BEGIN READ ONLY; SET LOCAL statement_timeout='5s'; SET LOCAL lock_timeout='1s';",
    )
    .await?;
    let identity = sql_query("SELECT current_database() || ' as ' || current_user AS value")
        .get_result::<Value>(&mut conn)
        .await?;
    println!("PASS database connection: {}", identity.value);
    let writable = sql_query("SELECT NOT pg_is_in_recovery() AS ok")
        .get_result::<Flag>(&mut conn)
        .await?;
    if !writable.ok {
        return Err("database is a standby; updater requires a writable primary".into());
    }
    let applied: Vec<Value> =
        sql_query("SELECT version::text AS value FROM public.__diesel_schema_migrations")
            .load(&mut conn)
            .await?;
    let search = args.embeddings.enabled == Some(true);
    for migration in <_ as MigrationSource<Pg>>::migrations(&MIGRATIONS)? {
        let version = migration.name().version().to_string();
        if !search && version == "20260906000012" {
            continue;
        }
        if !applied.iter().any(|v| v.value == version) {
            return Err(format!("missing required migration {version}").into());
        }
    }
    println!("PASS required migrations (additional migrations allowed)");
    // Generate column probes from the same Diesel schema used by ingestion.
    // debug_query emits one LIMIT bind; retain it instead of duplicating columns.
    use diesel::QueryDsl;
    use hn_core::db::schema;
    macro_rules! probe {
        ($($table:ident),+ $(,)?) => { $(
            sql_query(diesel::debug_query::<Pg, _>(&schema::$table::table.limit(0)).to_string())
                .bind::<diesel::sql_types::BigInt,_>(0).execute(&mut conn).await?;
        )+ };
    }
    probe!(
        items,
        kids,
        ingest_segments,
        ingest_exceptions,
        ingest_dlq_items,
        updater_state
    );
    for table in [
        "items",
        "kids",
        "ingest_segments",
        "ingest_exceptions",
        "ingest_dlq_items",
        "updater_state",
    ] {
        for privilege in ["SELECT", "INSERT", "UPDATE"] {
            let ok = sql_query("SELECT has_table_privilege(current_user,$1,$2) AS ok")
                .bind::<Text, _>(format!("public.{table}"))
                .bind::<Text, _>(privilege)
                .get_result::<Flag>(&mut conn)
                .await?
                .ok;
            if !ok {
                return Err(format!("missing {privilege} privilege on {table}").into());
            }
        }
    }
    let seq = sql_query("SELECT coalesce(has_sequence_privilege(current_user,pg_get_serial_sequence('public.ingest_dlq_items','dlq_id'),'USAGE'),false) AS ok")
        .get_result::<Flag>(&mut conn).await?;
    if !seq.ok {
        return Err("missing USAGE privilege on ingest_dlq_items sequence".into());
    }
    println!("PASS ingestion schema and privileges");
    if search {
        sql_query("SELECT story_id,title,url,embedding::halfvec(1024),retry_after FROM public.story_search LIMIT 0").execute(&mut conn).await?;
        for privilege in ["SELECT", "INSERT", "UPDATE", "DELETE"] {
            if !sql_query("SELECT has_table_privilege(current_user,'public.story_search',$1) AS ok")
                .bind::<Text, _>(privilege)
                .get_result::<Flag>(&mut conn)
                .await?
                .ok
            {
                return Err(format!("missing {privilege} privilege on story_search").into());
            }
        }
        for (name, version) in [("vector", "0.8.6"), ("pg_textsearch", "1.4.0")] {
            if !sql_query("SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname=$1 AND extversion=$2) AS ok")
                .bind::<Text,_>(name).bind::<Text,_>(version).get_result::<Flag>(&mut conn).await?.ok {
                return Err(format!("required extension {name} {version} is unavailable").into());
            }
        }
        for function in [
            "story_search_eligible(public.items)",
            "sync_story_search(public.items)",
            "story_search_source_changed()",
        ] {
            if !sql_query("SELECT has_function_privilege(current_user,$1,'EXECUTE') AS ok")
                .bind::<Text, _>(format!("public.{function}"))
                .get_result::<Flag>(&mut conn)
                .await?
                .ok
            {
                return Err(format!("missing EXECUTE privilege on {function}").into());
            }
        }
        let recipe = sql_query("SELECT coalesce(obj_description('public.story_search'::regclass,'pg_class'),'') AS value")
            .get_result::<Value>(&mut conn).await?;
        if recipe.value != RECIPE {
            return Err("story_search recipe is incompatible with this binary".into());
        }
        if !sql_query("SELECT EXISTS(SELECT 1 FROM public.story_search LIMIT 1) AS ok")
            .get_result::<Flag>(&mut conn)
            .await?
            .ok
        {
            println!("WARN story_search is empty: populate history and restart; embedding will remain disabled");
        }
        println!("PASS search schema, privileges, extensions and recipe");
    }
    conn.batch_execute("ROLLBACK").await?;
    Ok(())
}
