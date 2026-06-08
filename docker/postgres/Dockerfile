FROM postgres:15-alpine

LABEL maintainer="Sicred Data Engineering"
LABEL description="PostgreSQL com schema Sicred"
LABEL version="1.0.0"

ENV POSTGRES_DB=sicred
ENV POSTGRES_USER=sicred_user
ENV POSTGRES_PASSWORD=sicored_pass
ENV PGDATA=/var/lib/postgresql/data/pgdata

COPY db/ddl/01_create_schema.sql    /docker-entrypoint-initdb.d/01_create_schema.sql
COPY db/ddl/02_create_tables.sql    /docker-entrypoint-initdb.d/02_create_tables.sql
COPY db/ddl/03_create_indexes.sql   /docker-entrypoint-initdb.d/03_create_indexes.sql
COPY db/dml/04_insert_seed_data.sql /docker-entrypoint-initdb.d/04_insert_seed_data.sql

EXPOSE 5432

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=5 \
    CMD pg_isready -U sicred_user -d sicred || exit 1