-- db-create-users
create user sui with password '${db_password_sui}';

-- db-grant-on-database
grant connect on database ${namespace} to sui;

-- db-grant-on-schema
create schema if not exists sui_devnet;

grant usage, create on schema sui_devnet to sui;
grant select, insert, update, delete on all tables in schema sui_devnet to sui;
grant select, update, usage on all sequences in schema sui_devnet to sui
