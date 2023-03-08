-- db-default-privileges
alter default privileges for role sui in schema sui_devnet grant select on tables to hasura;
alter default privileges for role sui in schema sui_devnet grant select on tables to superset;
alter default privileges for role sui in schema sui_devnet grant select on tables to metabase;
alter default privileges for role sui in schema sui_devnet grant select on tables to redash;

set search_path to sui_devnet;

-- drop table if exists event cascade;
-- txDigest,eventSeq,name,data,timestamp
create table event (txDigest text, eventSeq int, name text, data jsonb, timestamp timestamp, primary key(txDigest, eventSeq));
create index on event (txDigest);
create index on event (name);
create index on event (timestamp);
