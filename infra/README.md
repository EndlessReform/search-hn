# Docker Compose stacks

## Environment variables

Set:

```bash
POSTGRES_USER=""
POSTGRES_PASSWORD=""
POSTGRES_DB=""
BACKEND_URL=""
```

## Local development

```bash
docker-compose -f docker-compose.yml -f docker-compose.dev.yml up
```

## Production

```bash
docker-compose -f docker-compose.yml -f docker-compose.prod.yml up
```
