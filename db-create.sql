-- db-create-users
create user sui_archive with password '${db_password_sui_archive}';

-- db-grant-on-database
grant connect on database ${namespace} to sui_archive;

-- db-grant-on-schema
create schema if not exists sui_${sui_network};

grant usage, create on schema sui_${sui_network} to sui_archive;
grant select, insert, update, delete on all tables in schema sui_${sui_network} to sui_archive;
grant select, update, usage on all sequences in schema sui_${sui_network} to sui_archive;

grant usage on schema sui_${sui_network} to redash_sui, hasura, metabase, superset;
