use catchup_worker_lib::{
    build_info,
    db::build_db_pool,
    logging::{format_error_report, init_logging},
};
use clap::Parser;
use diesel::sql_query;
use diesel::sql_types::BigInt;
use diesel::QueryableByName;
use diesel_async::RunQueryDsl;
use dotenv::dotenv;
use std::env;
use std::time::{Duration, Instant};
use tokio::time::sleep;
use tracing::{error, info, warn};

/// One-off chunked backfill for `items.story_id`.
///
/// The process is intentionally resumable:
/// - It updates only rows where `type='comment' AND story_id IS NULL`.
/// - Each batch is one short transaction.
/// - It exits after configurable idle passes with no updates.
#[derive(Debug, Parser)]
#[command(
    about = "Backfill items.story_id in chunks",
    version = build_info::VERSION_WITH_COMMIT,
    long_version = build_info::VERSION_WITH_COMMIT
)]
struct Args {
    #[arg(long = "database-url")]
    database_url: Option<String>,

    #[arg(long = "batch-size", default_value_t = 250_000)]
    batch_size: i64,

    #[arg(long = "duty-cycle", default_value_t = 0.8)]
    duty_cycle: f64,

    #[arg(long = "idle-passes", default_value_t = 2)]
    idle_passes: u32,

    #[arg(long = "idle-sleep-ms", default_value_t = 2_000)]
    idle_sleep_ms: u64,

    #[arg(long = "progress-every", default_value_t = 20)]
    progress_every: u64,

    #[arg(long = "max-batches")]
    max_batches: Option<u64>,

    #[arg(long = "db-pool-size", default_value_t = 2)]
    db_pool_size: usize,

    #[arg(long = "log-level", default_value = "info")]
    log_level: String,
}

#[derive(Debug, QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    count: i64,
}

fn resolve_database_url(args: &Args) -> Result<String, String> {
    if let Some(value) = &args.database_url {
        return Ok(value.clone());
    }

    env::var("DATABASE_URL")
        .map_err(|_| "DATABASE_URL is required (env var or --database-url)".to_string())
}

fn validate_args(args: &Args) -> Result<(), String> {
    if args.batch_size <= 0 {
        return Err(format!("--batch-size must be > 0, got {}", args.batch_size));
    }
    if !(0.0 < args.duty_cycle && args.duty_cycle <= 1.0) {
        return Err(format!(
            "--duty-cycle must be in (0.0, 1.0], got {}",
            args.duty_cycle
        ));
    }
    if args.idle_passes == 0 {
        return Err("--idle-passes must be > 0".to_string());
    }
    if args.progress_every == 0 {
        return Err("--progress-every must be > 0".to_string());
    }
    if args.db_pool_size == 0 {
        return Err("--db-pool-size must be > 0".to_string());
    }
    Ok(())
}

fn throttle_sleep(elapsed: Duration, duty_cycle: f64) -> Option<Duration> {
    if duty_cycle >= 1.0 {
        return None;
    }
    let elapsed_secs = elapsed.as_secs_f64();
    let pause_secs = elapsed_secs * ((1.0 - duty_cycle) / duty_cycle);
    if pause_secs <= 0.0 {
        return None;
    }
    Some(Duration::from_secs_f64(pause_secs))
}

async fn seed_story_rows(conn: &mut diesel_async::AsyncPgConnection) -> Result<i64, diesel::result::Error> {
    let rows: Vec<CountRow> = sql_query(
        r#"
        WITH updated AS (
            UPDATE items
            SET story_id = id
            WHERE type = 'story' AND story_id IS NULL
            RETURNING 1
        )
        SELECT COUNT(*)::bigint AS count FROM updated
        "#,
    )
    .load(conn)
    .await?;

    Ok(rows.as_slice().first().map(|row| row.count).unwrap_or(0))
}

async fn backfill_batch(
    conn: &mut diesel_async::AsyncPgConnection,
    batch_size: i64,
) -> Result<i64, diesel::result::Error> {
    let rows: Vec<CountRow> = sql_query(
        r#"
        WITH batch AS (
            SELECT c.id, COALESCE(p.story_id, c.parent) AS resolved_story_id
            FROM items c
            JOIN items p ON p.id = c.parent
            WHERE c.type = 'comment'
              AND c.story_id IS NULL
              AND (p.story_id IS NOT NULL OR p.type = 'story')
            ORDER BY c.id
            LIMIT $1
        ),
        updated AS (
            UPDATE items c
            SET story_id = b.resolved_story_id
            FROM batch b
            WHERE c.id = b.id
            RETURNING 1
        )
        SELECT COUNT(*)::bigint AS count FROM updated
        "#,
    )
    .bind::<BigInt, _>(batch_size)
    .load(conn)
    .await?;

    Ok(rows.as_slice().first().map(|row| row.count).unwrap_or(0))
}

async fn unresolved_comments(conn: &mut diesel_async::AsyncPgConnection) -> Result<i64, diesel::result::Error> {
    let rows: Vec<CountRow> = sql_query(
        r#"
        SELECT COUNT(*)::bigint AS count
        FROM items
        WHERE type = 'comment' AND story_id IS NULL
        "#,
    )
    .load(conn)
    .await?;

    Ok(rows.as_slice().first().map(|row| row.count).unwrap_or(0))
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    let args = Args::parse();
    let logging_context = init_logging("catchup_worker", "story_id_backfill", &args.log_level);
    let run_span = tracing::info_span!(
        "backfill_run",
        service = %logging_context.service,
        environment = %logging_context.environment,
        mode = %logging_context.mode,
        run_id = %logging_context.run_id,
        build_version = %logging_context.build_version,
        build_commit = %logging_context.build_commit
    );
    let _run_guard = run_span.enter();
    info!(
        event = "story_id_backfill_starting",
        "starting story_id backfill run"
    );

    if let Err(err) = validate_args(&args) {
        eprintln!("{err}");
        std::process::exit(2);
    }

    let db_url = match resolve_database_url(&args) {
        Ok(value) => value,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(2);
        }
    };

    let pool = match build_db_pool(&db_url, args.db_pool_size).await {
        Ok(pool) => pool,
        Err(err) => {
            let report = format_error_report(&err);
            error!(
                event = "story_id_backfill_pool_init_failed",
                error = %err,
                error_report = %report,
                "failed to build db pool"
            );
            eprintln!("{report}");
            std::process::exit(1);
        }
    };

    let mut conn = match pool.get().await {
        Ok(conn) => conn,
        Err(err) => {
            error!(
                event = "story_id_backfill_connection_failed",
                error = %err,
                "failed to acquire db connection"
            );
            std::process::exit(1);
        }
    };

    match seed_story_rows(&mut conn).await {
        Ok(seed_count) => {
            info!(
                event = "story_id_backfill_seed_complete",
                stories_seeded = seed_count,
                "seeded story rows with story_id=id"
            );
        }
        Err(err) => {
            let report = format_error_report(&err);
            error!(
                event = "story_id_backfill_seed_failed",
                error = %err,
                error_report = %report,
                "failed while seeding story rows"
            );
            eprintln!("{report}");
            std::process::exit(1);
        }
    }

    let mut total_updated: i64 = 0;
    let mut batch_number: u64 = 0;
    let mut consecutive_idle_passes: u32 = 0;
    let started_at = Instant::now();

    loop {
        if let Some(max_batches) = args.max_batches {
            if batch_number >= max_batches {
                warn!(
                    event = "story_id_backfill_stopped_at_max_batches",
                    max_batches,
                    total_updated,
                    "stopping backfill because --max-batches was reached"
                );
                break;
            }
        }

        batch_number = batch_number.saturating_add(1);
        let batch_start = Instant::now();
        let updated = match backfill_batch(&mut conn, args.batch_size).await {
            Ok(count) => count,
            Err(err) => {
                let report = format_error_report(&err);
                error!(
                    event = "story_id_backfill_batch_failed",
                    batch_number,
                    error = %err,
                    error_report = %report,
                    "backfill batch failed"
                );
                eprintln!("{report}");
                std::process::exit(1);
            }
        };
        let elapsed = batch_start.elapsed();

        if updated > 0 {
            total_updated = total_updated.saturating_add(updated);
            consecutive_idle_passes = 0;
            info!(
                event = "story_id_backfill_batch_complete",
                batch_number,
                rows_updated = updated,
                total_updated,
                elapsed_ms = elapsed.as_millis() as u64,
                rows_per_sec = if elapsed.as_secs_f64() > 0.0 {
                    (updated as f64 / elapsed.as_secs_f64()) as u64
                } else {
                    0
                },
                "backfill batch completed"
            );
        } else {
            consecutive_idle_passes = consecutive_idle_passes.saturating_add(1);
            let remaining = match unresolved_comments(&mut conn).await {
                Ok(count) => count,
                Err(err) => {
                    let report = format_error_report(&err);
                    error!(
                        event = "story_id_backfill_unresolved_count_failed",
                        batch_number,
                        error = %err,
                        error_report = %report,
                        "failed to count unresolved comment rows"
                    );
                    eprintln!("{report}");
                    std::process::exit(1);
                }
            };
            info!(
                event = "story_id_backfill_idle_pass",
                batch_number,
                idle_pass = consecutive_idle_passes,
                idle_pass_target = args.idle_passes,
                unresolved_comments = remaining,
                "no rows updated in this pass"
            );

            if consecutive_idle_passes >= args.idle_passes {
                info!(
                    event = "story_id_backfill_converged",
                    batches = batch_number,
                    total_updated,
                    unresolved_comments = remaining,
                    elapsed_seconds = started_at.elapsed().as_secs(),
                    "stopping because required idle passes were reached"
                );
                break;
            }
            sleep(Duration::from_millis(args.idle_sleep_ms)).await;
            continue;
        }

        if batch_number % args.progress_every == 0 {
            match unresolved_comments(&mut conn).await {
                Ok(remaining) => {
                    info!(
                        event = "story_id_backfill_progress",
                        batch_number,
                        total_updated,
                        unresolved_comments = remaining,
                        elapsed_seconds = started_at.elapsed().as_secs(),
                        "periodic backfill progress snapshot"
                    );
                }
                Err(err) => {
                    warn!(
                        event = "story_id_backfill_progress_count_failed",
                        batch_number,
                        error = %err,
                        "failed to gather unresolved comment count for progress snapshot"
                    );
                }
            }
        }

        if let Some(pause) = throttle_sleep(elapsed, args.duty_cycle) {
            sleep(pause).await;
        }
    }

    info!(
        event = "story_id_backfill_finished",
        total_updated,
        total_batches = batch_number,
        elapsed_seconds = started_at.elapsed().as_secs(),
        duty_cycle = args.duty_cycle,
        "story_id backfill finished"
    );
}

#[cfg(test)]
mod tests {
    use super::{throttle_sleep, validate_args, Args};
    use clap::Parser;
    use std::time::Duration;

    #[test]
    fn throttle_sleep_none_at_full_duty_cycle() {
        assert!(throttle_sleep(Duration::from_secs(1), 1.0).is_none());
    }

    #[test]
    fn throttle_sleep_halves_runtime_at_point_five_duty_cycle() {
        let pause = throttle_sleep(Duration::from_secs(2), 0.5).expect("expected pause");
        assert_eq!(pause, Duration::from_secs(2));
    }

    #[test]
    fn validate_args_rejects_invalid_duty_cycle() {
        let args = Args::parse_from([
            "story_id_backfill",
            "--database-url",
            "postgres://x",
            "--duty-cycle",
            "1.2",
        ]);
        let error = validate_args(&args).expect_err("expected validation failure");
        assert!(error.contains("--duty-cycle"));
    }
}
