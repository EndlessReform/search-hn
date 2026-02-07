# Docker Compose stacks

## System requirements

### Host requirements

- Prometheus [node_exporter](https://github.com/prometheus/node_exporter) set up on port 9100
- Docker and Docker Compose
- Git

### Environment variables

Set:

```bash
POSTGRES_USER=""
POSTGRES_PASSWORD=""
POSTGRES_DB=""
# For Prometheus.
# Remember that catchup_worker is running on this network in prod on 3000
CATCHUP_WORKER_URL=""
# For catchup_worker.
# In prod, remember database is running on this network on 5432
DATABASE_URL=""
# For exporter
HUGGINGFACE_TOKEN=""
```

## Local development

```bash
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up
```

## Production

```bash
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up
```

To update:

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml  pull
```
