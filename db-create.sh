#!/usr/bin/env bash

# sui_network=devnet ./db-create.sh
# sui_network=testnet ./db-create.sh

source ../infra/env.sh

env | grep '^db' | sort

# Create database schema, users, grants

export PGHOST=${db_host} && \
export PGPASSWORD=${db_password} && \
export PGUSER=postgres && \
export PGDATABASE=${namespace}

env | grep '^PG' | sort

envsubst < db-create.sql | psql --file -

# Create tables

export PGUSER=sui_archive && \
export PGPASSWORD=${db_password_sui_archive} && \
export PGDATABASE=${namespace}

env | grep '^PG' | sort

envsubst < schema.sql | psql --file -