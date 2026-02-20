# Systemd Batch Job Activation Guide

This guide explains how to activate and configure the catchup worker systemd batch jobs on Debian 13.

## Prerequisites

The catchup worker requires:
- A dedicated system user and group: `catchup`
- Environment configuration file: `/etc/search-hn/catchup-worker.env`
- Working directory: `/var/lib/search-hn`
- Binaries:
  - `/usr/local/bin/catchup_only`
  - `/usr/local/bin/story_id_backfill`

## Idiomatic Unit File Location

On Debian 13, systemd unit files should be placed in:

```bash
/etc/systemd/system/
```

This is the standard location that systemd automatically loads on boot. Do NOT place files in `/lib/systemd/system/` unless you're packaging the service as part of the OS distribution.

## Quick Start

1. **Copy unit files** to `/etc/systemd/system/`:
   ```bash
   sudo cp infra/systemd/catchup-worker-*.service /etc/systemd/system/
   ```

2. **Enable the service** (choose your mode):
   ```bash
   # Shakedown mode (IDs 1-100000, 250 RPS)
   sudo systemctl enable catchup-worker-shakedown-250rps.service

   # Full mode (all IDs, 1000 RPS)
   sudo systemctl enable catchup-worker-full-1000rps.service

   # One-off story_id backfill job
   sudo systemctl enable catchup-worker-story-id-backfill.service
   ```

3. **Start the service**:
   ```bash
   sudo systemctl start catchup-worker-shakedown-250rps.service
   # or
   sudo systemctl start catchup-worker-full-1000rps.service
   # or
   sudo systemctl start catchup-worker-story-id-backfill.service
   ```

## Configuration Steps

### 1. Create System User and Group (REQUIRED)

The systemd unit files explicitly reference `User=catchup` and `Group=catchup`, so you MUST create the user first.

```bash
sudo groupadd catchup
sudo useradd -r -g catchup -s /usr/sbin/nologin catchup
```

### 2. Setup Working Directory

```bash
sudo mkdir -p /var/lib/search-hn
sudo chown -R catchup:catchup /var/lib/search-hn
```

### 3. Configure Environment File

Create `/etc/search-hn/catchup-worker.env`:

```bash
sudo mkdir -p /etc/search-hn
sudo touch /etc/search-hn/catchup-worker.env
sudo chown catchup:catchup /etc/search-hn/catchup-worker.env
```

Add your database configuration:

```env
DATABASE_URL=postgresql://user:password@host:port/hn_database
HN_API_URL="https://hacker-news.firebaseio.com/v0"
```

**Security Note**: The catchup user is a system user without a home directory, so `~/.pgpass` won't work. Set the full `DATABASE_URL` with credentials in this file. For production, consider rotating credentials regularly or using a separate credentials file with strict permissions.

### 4. Install Binary

Copy the compiled binaries to `/usr/local/bin/`:

```bash
sudo cp dist/debian13/catchup_only /usr/local/bin/
sudo cp dist/debian13/story_id_backfill /usr/local/bin/
sudo chmod +x /usr/local/bin/catchup_only
sudo chmod +x /usr/local/bin/story_id_backfill
```

Or use your preferred location if you modify the unit file paths.

## Service Management

### Check Service Status

```bash
sudo systemctl status catchup-worker-shakedown-250rps.service
```

### View Logs

```bash
sudo journalctl -u catchup-worker-shakedown-250rps.service -f
```

### Restart Service

```bash
sudo systemctl restart catchup-worker-shakedown-250rps.service
```

### Stop Service

```bash
sudo systemctl stop catchup-worker-shakedown-250rps.service
```

### Disable Service

```bash
sudo systemctl disable catchup-worker-shakedown-250rps.service
```

## Customization

### Different Unit File

To use a custom unit file location:

```bash
sudo cp my-custom.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable my-custom.service
```

### Override Options

For runtime overrides without modifying unit files:

```bash
sudo systemctl edit catchup-worker-shakedown-250rps.service
```

This creates an override file that takes precedence over the main unit file.

## Troubleshooting

### Service Won't Start

1. Check logs: `sudo journalctl -u catchup-worker-shakedown-250rps.service -n 50`
2. Verify binary exists: `ls -la /usr/local/bin/catchup_only`
3. Check environment file: `cat /etc/search-hn/catchup-worker.env`
4. Verify working directory permissions: `ls -la /var/lib/search-hn`

### Permission Denied

Ensure the service user has proper permissions:

```bash
sudo chown -R catchup:catchup /var/lib/search-hn
sudo chown catchup:catchup /etc/search-hn/catchup-worker.env
```

### Restart Loop

If the service keeps restarting:

1. Check for errors in logs
2. Verify database connectivity from the service user
3. Ensure `DATABASE_URL` is correctly configured
4. Check that the binary is executable

For the `catchup-worker-story-id-backfill.service` unit, restart attempts are capped:
- `StartLimitBurst=3`
- `StartLimitIntervalSec=1h`

## Metrics and Health

The catchup worker exposes metrics on port 3000 (configured in unit files):

- `/metrics` - Prometheus metrics endpoint
- `/health` - Health check endpoint

Access from another machine:

```bash
curl http://localhost:3000/metrics
```

## Production Considerations

1. **Monitoring**: Integrate with your monitoring system (Prometheus, Grafana, etc.)
2. **Logging**: Configure log rotation for systemd journal or use external logging
3. **Backup**: Ensure `/var/lib/search-hn` is backed up regularly
4. **Security**: Review and harden systemd security directives in unit files
5. **Update**: Rebuild binary and reload systemd after updates:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl restart catchup-worker-shakedown-250rps.service
   ```

## Related Documentation

- Catchup worker CLI options: `crates/catchup_worker/README.md`
- Rust deployment guide: See `crates/catchup_worker/README.md` "Rust (for LXC)" section
- Database migrations: See `crates/catchup_worker/README.md` "Database migrations"
