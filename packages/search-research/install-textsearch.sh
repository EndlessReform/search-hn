#!/usr/bin/env bash
# Install only into the named scratch container; retain the official artifact.
set -euo pipefail
scratch_container=searchhn-pg-bakeoff-20260904
artifact_root=data/pg-duckdb-bakeoff-20260904
archive="$artifact_root/pg-textsearch-v1.4.0-pg17-arm64.zip"
curl -fL https://github.com/timescale/pg_textsearch/releases/download/v1.4.0/pg-textsearch-v1.4.0-pg17-arm64.zip -o "$archive"
actual_sha=$(shasum -a 256 "$archive" | cut -d ' ' -f 1)
test "$actual_sha" = c084c942caa9d6e35a84aaff8b21e6c51afa4126030aabf1d5e76f03f4f2a320
unzip -n "$archive" -d "$artifact_root/pg-textsearch-release"
docker start "$scratch_container"
docker cp "$artifact_root/pg-textsearch-release/pg-textsearch-postgresql-17_1.4.0-1_arm64.deb" "$scratch_container:/tmp/pg-textsearch.deb"
docker exec "$scratch_container" dpkg -i /tmp/pg-textsearch.deb
docker exec "$scratch_container" psql -U postgres -d search_bakeoff -c "ALTER SYSTEM SET shared_preload_libraries='pg_textsearch';"
docker restart "$scratch_container"
