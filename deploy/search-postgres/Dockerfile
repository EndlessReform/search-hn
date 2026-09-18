# Scratch/test image; also verifies the PG17 extension build on linux/amd64.
FROM postgres:17.11-bookworm
ARG PGVECTOR_REF=v0.8.6
ARG PGTEXTSEARCH_REF=7a932505b537d50ad8d2d068a053cd1dd7b646ea
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential ca-certificates git postgresql-server-dev-17 \
    && git clone --depth 1 --branch ${PGVECTOR_REF} https://github.com/pgvector/pgvector.git /tmp/vector \
    && make -C /tmp/vector OPTFLAGS='' && make -C /tmp/vector install \
    && git clone https://github.com/timescale/pg_textsearch.git /tmp/textsearch \
    && git -C /tmp/textsearch checkout ${PGTEXTSEARCH_REF} \
    && make -C /tmp/textsearch && make -C /tmp/textsearch install \
    && rm -rf /tmp/vector /tmp/textsearch /var/lib/apt/lists/*
CMD ["postgres", "-c", "shared_preload_libraries=pg_textsearch"]
