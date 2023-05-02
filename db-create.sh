#!/usr/bin/env bash

# run with
# sui_network=devnet ./db-create.sh
# sui_network=testnet ./db-create.sh
# sui_shard=20230502 sui_network=testnet ./db-create.sh

source ../infra/env.sh

env | grep '^db' | sort

# as postgres user: create database user sui_archive, grants, schema

export PGHOST=${db_host} && \
export PGPASSWORD=${db_password} && \
export PGUSER=postgres && \
export PGDATABASE=${namespace}

env | grep '^PG' | sort

envsubst < db-create.sql | psql --file -

# as sui_archive user: create tables and default privileges for reader users

export PGUSER=sui_archive && \
export PGPASSWORD=${db_password_sui_archive} && \
export PGDATABASE=${namespace}

env | grep '^PG' | sort

envsubst < schema.sql | psql --file -