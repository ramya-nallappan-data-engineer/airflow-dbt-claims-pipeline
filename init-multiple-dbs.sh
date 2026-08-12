#!/bin/bash
set -e

# The 'airflow' db already exists (POSTGRES_USER/DB default).
# This creates the second database dbt will write into.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE claims_warehouse;
    GRANT ALL PRIVILEGES ON DATABASE claims_warehouse TO $POSTGRES_USER;
EOSQL
