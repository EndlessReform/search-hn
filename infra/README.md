# Docker Compose stacks

## Environment variables

Set:

```bash
POSTGRES_USER=""
POSTGRES_PASSWORD=""
POSTGRES_DB=""
# For Prometheus.
# Remember that backend is running on this network in prod on 3000
BACKEND_URL=""
# For backend.
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