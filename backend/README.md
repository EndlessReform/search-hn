Backend server for `search-hn` to sync Hacker News posts and comments from the upstream Firebase.

## Usage

### CLI

#### Arguments

- `-n, --no_catchup`  
  Disable catchup on previous data.

- `-r, --realtime`  
  Listen for HN updates and persist them to the database.

- `--catchup_start <ID>`  
  Start catchup from this post ID.

- `--catchup_amt <N>`  
  Max number of records to catch up. Mostly for debugging.

#### Example Usage

```bash
# Disable catchup and enable realtime updates
./instruct-hn --no_catchup --realtime

# Start catchup from ID 1000 with a maximum of 500 records
./instruct-hn --catchup_start 1000 --catchup_amt 500
```

### Configuration

In the environment (preferably in a `.env` file), set the following parameters:

```env
DATABASE_URL=postgresql://user:password@host:port/hn_database
# Optional: defaults to current endpoint
# HN_API_URL="https://hacker-news.firebaseio.com/v0"
```

## Local setup

```bash
cargo build
# If setting up the DB for the first time
cargo install diesel_cli --no-default-features --features postgres
```

If you need to set up the database from scratch, create database `hn_database`, then run migrations:

```bash
diesel migration run
```
