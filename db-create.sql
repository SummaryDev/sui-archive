-- db-create-users
create user sui_archive with password '${db_password_sui_archive}';

-- db-grant-on-database
grant connect on database ${namespace} to sui_archive;

-- db-grant-on-schema
create schema if not exists sui_devnet;

grant usage, create on schema sui_devnet to sui_archive;
grant select, insert, update, delete on all tables in schema sui_devnet to sui_archive;
grant select, update, usage on all sequences in schema sui_devnet to sui_archive;
