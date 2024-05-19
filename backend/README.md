## Usage

### Configuration

In the environment (preferably in a `.env` file), set the following parameters:

```env
DATABASE_URL=postgresql://user:password@host:port/hn_database
# Optional: defaults to current endpoint
# HN_API_URL="https://hacker-news.firebaseio.com/v0"
```

### Local setup

```bash
cargo build
# If setting up the DB for the first time
cargo install diesel_cli --no-default-features --features postgres
```

If you need to set up the DB from scratch, run migrations:

```bash
diesel migration run
```
