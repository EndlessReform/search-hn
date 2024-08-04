# SearchHN

<img alt="SearchHN logo" width="480" src="./docs/assets/searchhn-splash.png" />

SearchHN is a crawler for the [Hacker News](https://news.ycombinator.com/news) corpus from the [official Firebase API](https://github.com/HackerNews/API).  Using SearchHN, you can mirror the corpus in real time to Postgres and upload to HuggingFace Datasets, for use in downstream projects. 

We maintain our own [nightly updated crawl](https://huggingface.co/collections/jkeisling/hn-corpus-66ad74579a973eeae405407a) on HuggingFace.

## Using the dataset
SearchHN maintains two HuggingFace datasets:
- [`hn-items`](https://huggingface.co/datasets/jkeisling/hn-items): mirrors the [Items](https://github.com/HackerNews/API?tab=readme-ov-file#items) table from the upstream API, minus `kids` (see below)
- [`hn-crosswalk`](https://huggingface.co/datasets/jkeisling/hn-crosswalk): keeps fields `item`, `kid`, and `display_order` for each item which children. This allows for easy SQL joins to reconstruct the comment tree.

For the full schema, please refer to the [HN API documentation](https://github.com/HackerNews/API?tab=readme-ov-file#items). Usage examples are forthcoming.

### Why use this data dump?
Compared to the official HN [BigQuery](https://console.cloud.google.com/bigquery?pli=1&project=master-chariot-391822&ws=!1m4!1m3!3m2!1sbigquery-public-data!2shacker_news) dataset, this mirror has:
- **Daily updated records.** The BigQuery dataset cuts off at September 2022.
- **Original comment ordering.** Unlike the BigQuery dataset, API data preserves the original order on the `kids` table. This can be useful for reconstructing the original discussions or extracting a weak preference signal for individual comments.

## Running your own instance
### Using Docker (recommended)
We recommend using SearchHN using the Docker Compose production stack. 

Clone this repo. Then, in the `infra` directory, set environment variables:
```bash
# Backend settings (required)
# Postgres connection string
DATABASE_URL=""
REDIS_URL=""

# Postgres database settings (if using the stack)
POSTGRES_USER=""
POSTGRES_PASSWORD=""
POSTGRES_DB=""

# Grafana settings (if using monitoring)
GRAFANA_USER=""
GRAFANA_PASSWORD=""

# If running your own mirroring
HUGGINGFACE_TOKEN=""
POSTGRES_EXPORTER_PASSWORD=""
```

Then from `infra`, run the production instance:
```bash
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up
```

For initial catchup, the system is rate-limited at 2K RPS/node and takes approximately 5 hours to download. Realtime update requirements are minimal.

**System requirements:**
- At least 32GB of disk (for dataset)

## Prior art
SearchHN is far from the only HN mirroring codebase. Check out:
- [Anant Naryanan's ChatGPT plugin](https://www.kix.in/2023/05/05/hacker-news-chatgpt-plugin/#keeping-the-data--index-updated). This also has realtime updates, but  
- [Hacker Search](https://jnnnthnn.com/building-hacker-search.html), which additionally crawls the top 100 pages for downstream indexing. However, it's meant for one-off crawling.
- Wilson Lin's [Hackerverse](https://blog.wilsonl.in/hackerverse/), which also has a Node-based one-off ingest. 


