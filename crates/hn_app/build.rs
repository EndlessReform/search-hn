//! Embed the release source identity; local builds are explicitly marked local.
fn main() {
    println!("cargo:rerun-if-env-changed=SOURCE_COMMIT_HASH");
    let commit = std::env::var("SOURCE_COMMIT_HASH").unwrap_or_else(|_| "local".into());
    println!("cargo:rustc-env=HN_APP_COMMIT={commit}");
}
