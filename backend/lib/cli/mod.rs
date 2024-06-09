use clap::Parser;

#[derive(Parser, Debug)]
#[clap(about = "Crawler for search-hn")]
pub struct Cli {
    #[clap(short, long)]
    /// Disable catchup on previous data
    pub no_catchup: bool,

    #[clap(short, long)]
    /// Listen for HN updates and persist them to DB
    pub realtime: bool,

    #[clap(long)]
    /// Start catch-up from this ID
    pub catchup_start: Option<i64>,

    #[clap(long)]
    /// Max number of records to catch up
    pub catchup_amt: Option<i64>,
}

pub fn parse_args() -> Cli {
    Cli::parse()
}
