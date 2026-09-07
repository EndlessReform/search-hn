#!/usr/bin/env python3
"""Stream one database backup off its host; optionally copy the verified files to Garage.

No dump file is created on the source. A failed dump leaves a .partial directory;
only successful pg_dump, archive inspection and hashing publish a backup directory.
Restore testing is deliberately an ordinary pg_restore operation, documented nearby.
"""
import argparse
import hashlib
import json
import re
from pathlib import Path
import shlex
import subprocess
from datetime import datetime, timezone


def run(argv: list[str], **kwargs):
    """Fail visibly on command errors; never put database passwords in arguments."""
    return subprocess.run(argv, check=True, **kwargs)


def finish_backup(partial: Path, destination: Path, source_host: str,
                  database: str, started: str, pg_restore: str):
    """Validate and publish a dump after its producing process has exited successfully."""
    dump = partial / 'database.dump'
    with (partial / 'contents.txt').open('x') as contents:
        run([pg_restore, '--list', str(dump)], stdout=contents)
    with dump.open('rb') as source:
        digest = hashlib.file_digest(source, 'sha256').hexdigest()
    (partial / 'SHA256SUMS').write_text(f'{digest}  database.dump\n')
    (partial / 'manifest.json').write_text(json.dumps({
        'source': source_host, 'database': database, 'started_utc': started,
        'finished_utc': datetime.now(timezone.utc).isoformat(),
        'scope': 'Entire database: all schemas, tables, sequences, functions, indexes and ACLs. No cluster globals or host configuration.',
        'bytes': dump.stat().st_size, 'sha256': digest,
        'restore_verified': False,
    }, indent=2) + '\n')
    partial.rename(destination)
    print(f'Backup complete (restore not yet verified): {destination}', flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', type=Path, required=True)
    parser.add_argument('--pg-restore', default='pg_restore', help='Path to a matching/newer PostgreSQL client')
    parser.add_argument('--source', default='root@searchhn-pg', help='SSH destination')
    parser.add_argument('--database', default='searchhn_test')
    parser.add_argument('--garage', help='Existing rclone remote:bucket/prefix destination')
    args = parser.parse_args()
    local_version = run([args.pg_restore, '--version'], capture_output=True, text=True).stdout
    source_version = run(['ssh', '-o', 'BatchMode=yes', args.source,
                          'runuser -u postgres -- pg_dump --version'],
                         capture_output=True, text=True).stdout
    local_major = int(re.search(r'(\d+)\.', local_version)[1])
    source_major = int(re.search(r'(\d+)\.', source_version)[1])
    if local_major < source_major:
        parser.error(f'pg_restore {local_major} is older than source {source_major}; use --pg-restore PATH')
    started = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    name = f'{args.database}-{started}'
    destination = args.directory.resolve() / name
    partial = destination.with_name(name + '.partial')
    partial.mkdir(parents=True, mode=0o700, exist_ok=False)
    dump = partial / 'database.dump'
    remote = ['runuser', '-u', 'postgres', '--', 'pg_dump', '--format=custom',
              '--compress=1', '--lock-wait-timeout=10s', '--verbose',
              '--dbname', args.database]
    print(f'Streaming {args.source}:{args.database} to {dump}', flush=True)
    with dump.open('xb') as output, (partial / 'dump.log').open('x') as log:
        run(['ssh', '-o', 'BatchMode=yes', args.source, shlex.join(remote)],
            stdout=output, stderr=log)
    finish_backup(partial, destination, args.source, args.database, started, args.pg_restore)
    if args.garage:
        target = args.garage.rstrip('/') + '/' + name
        run(['rclone', 'copy', str(destination), target])
        # Download comparison avoids relying on multipart S3 ETags as MD5 checksums.
        run(['rclone', 'check', '--download', str(destination), target])
        print(f'Garage copy verified: {target}', flush=True)


if __name__ == '__main__':
    main()
