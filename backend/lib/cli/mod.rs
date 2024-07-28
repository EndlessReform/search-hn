use clap::{Parser, ValueEnum};
use std::fmt;

#[derive(Debug, Clone, ValueEnum, PartialEq)]
pub enum Mode {
    Worker,
    Leader,
    All,
}

impl fmt::Display for Mode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mode_str = match self {
            Mode::Worker => "worker",
            Mode::Leader => "leader",
            Mode::All => "all",
        };
        write!(f, "{}", mode_str)
    }
}

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

    #[clap(long, default_value_t = Mode::All)]
    pub mode: Mode,
}

pub fn parse_args() -> Cli {
    Cli::parse()
}
