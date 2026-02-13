use std::process::Command;

const UNKNOWN_COMMIT: &str = "unknown";

fn main() {
    println!("cargo:rerun-if-env-changed=SOURCE_COMMIT_HASH");
    // Rebuild when git metadata changes during local development.
    println!("cargo:rerun-if-changed=.git/HEAD");
    println!("cargo:rerun-if-changed=../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/HEAD");

    let commit_hash = std::env::var("SOURCE_COMMIT_HASH")
        .ok()
        .or_else(read_head_commit)
        .unwrap_or_else(|| UNKNOWN_COMMIT.to_string());

    println!("cargo:rustc-env=CATCHUP_WORKER_GIT_COMMIT_HASH={commit_hash}");
}

fn read_head_commit() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--verify", "HEAD"])
        .output()
        .ok()?;

    if !output.status.success() {
        return None;
    }

    let hash = String::from_utf8(output.stdout).ok()?;
    let trimmed = hash.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}
