# Systemd Deployment (Current)

This directory contains the recommended systemd units for `catchup_worker`.

## Recommended Units

- `catchup-worker-updater.service`
  - Long-running updater (`catchup_worker updater`)
  - Runs SSE listener + supervised realtime workers + startup replay window
  - Name-compatible with existing Alloy `catchup-worker-*.service` filters
- `catchup-worker-catchup.service`
  - One-shot/manual catchup run (`catchup_worker catchup ...`)
- `catchup-worker-catchup.timer`
  - Optional nightly trigger for `catchup-worker-catchup.service`

## Prerequisites

- User/group: `catchup`
- Env file: `/etc/search-hn/catchup-worker.env`
- Working directory: `/var/lib/search-hn`
- Binary: `/usr/local/bin/catchup_worker`

Example env file (`/etc/search-hn/catchup-worker.env`):

```env
DATABASE_URL=postgresql://user:password@host:5432/searchhn
HN_API_URL=https://hacker-news.firebaseio.com/v0
```

## Install

```bash
sudo cp infra/systemd/catchup-worker-updater.service /etc/systemd/system/
sudo cp infra/systemd/catchup-worker-catchup.service /etc/systemd/system/
sudo cp infra/systemd/catchup-worker-catchup.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

## Enable

Updater only:

```bash
sudo systemctl enable --now catchup-worker-updater.service
```

Updater + nightly catchup sweep:

```bash
sudo systemctl enable --now catchup-worker-updater.service
sudo systemctl enable --now catchup-worker-catchup.timer
```

## Useful Commands

```bash
sudo systemctl status catchup-worker-updater.service
sudo systemctl status catchup-worker-catchup.timer
sudo journalctl -u catchup-worker-updater.service -f
sudo journalctl -u catchup-worker-catchup.service -f
```

## Notes

- Keep migrations as a separate deploy step before starting/updating units.
- `catchup-worker-catchup.timer` is optional.
- If updater restarts (`Restart=on-failure`) are enough for your recovery model, skip the timer.
- Timer value is periodic no-restart sweep while updater is healthy; it is not required for crash recovery.
