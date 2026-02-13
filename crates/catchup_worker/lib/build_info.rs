/// Build metadata used by CLI output, logs, and metrics.
///
/// `VERSION_WITH_COMMIT` is intentionally the single source of truth for what
/// we report as the running build identity.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const GIT_COMMIT_HASH: &str = env!("CATCHUP_WORKER_GIT_COMMIT_HASH");
pub const VERSION_WITH_COMMIT: &str = concat!(
    env!("CARGO_PKG_VERSION"),
    "+",
    env!("CATCHUP_WORKER_GIT_COMMIT_HASH")
);

/// Returns a short git hash suitable for log context and labels.
///
/// We preserve `"unknown"` when git metadata is unavailable at build time.
pub fn short_commit_hash() -> &'static str {
    if GIT_COMMIT_HASH == "unknown" {
        return GIT_COMMIT_HASH;
    }

    let short_len = 12usize.min(GIT_COMMIT_HASH.len());
    &GIT_COMMIT_HASH[..short_len]
}

#[cfg(test)]
mod tests {
    use super::{GIT_COMMIT_HASH, VERSION, VERSION_WITH_COMMIT};

    #[test]
    fn version_with_commit_contains_semver_prefix() {
        assert!(
            VERSION_WITH_COMMIT.starts_with(VERSION),
            "version string should begin with semver"
        );
    }

    #[test]
    fn version_with_commit_includes_separator() {
        assert!(
            VERSION_WITH_COMMIT.contains('+'),
            "version string should include semver+commit separator"
        );
    }

    #[test]
    fn commit_hash_is_non_empty() {
        assert!(
            !GIT_COMMIT_HASH.is_empty(),
            "commit hash should never be an empty string"
        );
    }
}
