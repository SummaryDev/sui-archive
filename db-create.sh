#!/usr/bin/env bash

source ./env.sh

env | grep '^db' | sort

# Create database schema users grants

export PGHOST=${db_host} && \
export PGPASSWORD=${db_password} && \
export PGUSER=postgres && \
export PGDATABASE=${namespace}

env | grep '^PG' | sort

envsubst < db-create.sql | psql --file -

# Create tables

export PGUSER=sui && \
export PGPASSWORD=${db_password_sui} && \
export PGDATABASE=${namespace}

env | grep '^PG' | sort

envsubst < schema.sql | psql --file -