use catchup_worker_lib::commands::{run_catchup_once, CatchupArgs};
use clap::Parser;

#[tokio::main]
async fn main() {
    let args = CatchupArgs::parse();
    let code = run_catchup_once(args, "catchup_only").await;
    if code != 0 {
        std::process::exit(code);
    }
}
