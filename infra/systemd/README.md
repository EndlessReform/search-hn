# Systemd Deployment (Current)

This directory contains the recommended systemd units for `catchup_worker`.

## Recommended Units

- `search-hn-updater.service`
  - Long-running updater (`catchup_worker updater`)
  - Runs SSE listener + supervised realtime workers + startup replay window
- `search-hn-catchup.service`
  - One-shot/manual catchup run (`catchup_worker catchup ...`)
- `search-hn-catchup.timer`
  - Optional nightly trigger for `search-hn-catchup.service`

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
sudo cp infra/systemd/search-hn-updater.service /etc/systemd/system/
sudo cp infra/systemd/search-hn-catchup.service /etc/systemd/system/
sudo cp infra/systemd/search-hn-catchup.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

## Enable

Updater only:

```bash
sudo systemctl enable --now search-hn-updater.service
```

Updater + nightly catchup sweep:

```bash
sudo systemctl enable --now search-hn-updater.service
sudo systemctl enable --now search-hn-catchup.timer
```

## Useful Commands

```bash
sudo systemctl status search-hn-updater.service
sudo systemctl status search-hn-catchup.timer
sudo journalctl -u search-hn-updater.service -f
sudo journalctl -u search-hn-catchup.service -f
```

## Notes

- Keep migrations as a separate deploy step before starting/updating units.
- `search-hn-catchup.timer` is optional; remove if you prefer manual catchup only.
