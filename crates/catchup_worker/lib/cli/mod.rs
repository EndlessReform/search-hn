use crate::build_info;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(
    about = "Crawler for search-hn",
    version = build_info::VERSION_WITH_COMMIT,
    long_version = build_info::VERSION_WITH_COMMIT
)]
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

#[cfg(test)]
mod tests {
    use super::Cli;
    use crate::build_info;
    use clap::{error::ErrorKind, Parser};

    #[test]
    fn version_short_circuits_other_flags() {
        let err = Cli::try_parse_from([
            "catchup_worker",
            "--version",
            "--this-flag-does-not-exist",
        ])
        .expect_err("expected clap to stop parsing after --version");

        assert_eq!(err.kind(), ErrorKind::DisplayVersion);
        assert!(
            err.to_string().contains(build_info::VERSION_WITH_COMMIT),
            "version output should include semver+commit hash"
        );
    }
}
