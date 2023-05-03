-- db-create-users
create user sui_archive with password '${db_password_sui_archive}';

-- db-grant-on-database
grant connect on database ${namespace} to sui_archive;

-- db-grant-on-schema
create schema if not exists sui_${sui_network}${sui_shard};

grant usage, create on schema sui_${sui_network}${sui_shard} to sui_archive;
grant select, insert, update, delete on all tables in schema sui_${sui_network}${sui_shard} to sui_archive;
grant select, update, usage on all sequences in schema sui_${sui_network}${sui_shard} to sui_archive;

grant usage on schema sui_${sui_network}${sui_shard} to redash_sui, hasura, metabase, superset, graphile;

grant select on all tables in schema sui_${sui_network}${sui_shard} to redash_sui, hasura, metabase, superset, graphile;
