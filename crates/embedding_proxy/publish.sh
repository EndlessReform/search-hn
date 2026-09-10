#!/usr/bin/env bash
# Build only this independent crate, publish a named image, and print its immutable reference.
set -euo pipefail
if (( $# != 2 )); then
  echo "Usage: $0 REGISTRY/REPOSITORY TAG" >&2
  exit 2
fi
repository=$1
tag=$2
crate_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
context=$(mktemp -d)
trap 'rm -rf "$context"' EXIT
# Resolve inheritance before extracting the crate; the container stays standalone.
version=$(cargo metadata --locked --no-deps --format-version 1 --manifest-path "$crate_dir/Cargo.toml" | jq -er '.packages[] | select(.name == "embedding_proxy") | .version')
sed "s/^version.workspace = true$/version = \"$version\"/" "$crate_dir/Cargo.toml" > "$context/Cargo.toml"
cp "$crate_dir/Dockerfile" "$context/"
cp -R "$crate_dir/src" "$crate_dir/docs" "$crate_dir/fixtures" "$context/"
# Extraction is supported: a standalone checkout owns Cargo.lock; this workspace owns it above.
if [[ -f "$crate_dir/Cargo.lock" ]]; then
  cp "$crate_dir/Cargo.lock" "$context/Cargo.lock"
else
  cp "$crate_dir/../Cargo.lock" "$context/Cargo.lock"
fi
# Remove unrelated workspace entries without upgrading any locked dependency.
cargo +1.93.0 metadata --format-version 1 --filter-platform x86_64-unknown-linux-gnu --manifest-path "$context/Cargo.toml" >/dev/null
docker buildx build --platform linux/amd64 --push \
  --tag "$repository:$tag" --metadata-file "$context/image-metadata.json" "$context"
digest=$(jq -er '.["containerimage.digest"]' "$context/image-metadata.json")
printf '%s@%s\n' "$repository" "$digest"
